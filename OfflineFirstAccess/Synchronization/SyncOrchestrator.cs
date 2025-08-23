using System;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using OfflineFirstAccess.Data;
using OfflineFirstAccess.ChangeTracking;
using OfflineFirstAccess.Conflicts;
using OfflineFirstAccess.Models;
using System.Collections.Generic;

namespace OfflineFirstAccess.Synchronization
{
    /// <summary>
    /// The internal engine that performs the synchronization logic.
    /// </summary>
    public class SyncOrchestrator
    {
        private readonly IDataProvider _localProvider;
        private readonly IDataProvider _remoteProvider;
        private readonly IChangeTracker _changeTracker;
        private readonly IConflictResolver _conflictResolver;
        private readonly SyncConfiguration _config;
        

        public SyncOrchestrator(
            IDataProvider localProvider, 
            IDataProvider remoteProvider, 
            IChangeTracker changeTracker, 
            IConflictResolver conflictResolver,
            SyncConfiguration config)
        {
            _localProvider = localProvider;
            _remoteProvider = remoteProvider;
            _changeTracker = changeTracker;
            _conflictResolver = conflictResolver;
            _config = config;
        }

        public async Task<SyncResult> SynchronizeAsync(Action<int, string> onProgress)
        {
            var syncStartTime = DateTime.UtcNow;
            var allUnresolvedConflicts = new System.Collections.Generic.List<Conflict>();

            try
            {
                // --- PHASE 1: PUSH (Client -> Server) ---
                onProgress?.Invoke(10, "Getting local changes to push...");
                var allLocalChanges = await _changeTracker.GetUnsyncedChangesAsync();
                var localTotal = allLocalChanges?.Count() ?? 0;
                onProgress?.Invoke(15, $"Local unsynced changes: {localTotal}");
                var changesByTable = allLocalChanges.GroupBy(c => c.TableName);

                foreach (var tableGroup in changesByTable)
                {
                    var tableName = tableGroup.Key;
                    var ids = tableGroup.Select(c => c.RecordId).ToList();
                    onProgress?.Invoke(20, $"Pushing changes for table {tableName} (ids={ids.Count})...");
                    var recordsToPush = await _localProvider.GetRecordsByIds(tableName, ids);
                    var pushCount = recordsToPush?.Count() ?? 0;
                    onProgress?.Invoke(25, $"Push apply {pushCount} record(s) into remote for {tableName}");
                    await _remoteProvider.ApplyChangesAsync(tableName, recordsToPush);
                    await _changeTracker.MarkChangesAsSyncedAsync(tableGroup.Select(c => c.Id));
                    onProgress?.Invoke(30, $"Marked {ids.Count} local change(s) as synced for {tableName}");
                }

                // --- PHASE 2: PULL (Server -> Client) ---
                onProgress?.Invoke(50, "Pulling remote changes...");
                var lastSyncTimestamp = await GetLastSyncTimestampAsync();
                var anchorStr = lastSyncTimestamp.HasValue ? lastSyncTimestamp.Value.ToString("o", CultureInfo.InvariantCulture) : "<null>";
                onProgress?.Invoke(52, $"Using anchor: {anchorStr}");

                foreach (var tableName in _config.TablesToSync)
                {
                    onProgress?.Invoke(60, $"Pulling changes for table {tableName}...");
                    var remoteChanges = await _remoteProvider.GetChangesAsync(tableName, lastSyncTimestamp);
                    var remoteCount = remoteChanges?.Count() ?? 0;
                    onProgress?.Invoke(62, $"Remote returned {remoteCount} change(s) for {tableName}");

                    if (remoteChanges.Any())
                    {
                        var localChangesForTable = allLocalChanges.Where(c => c.TableName == tableName).ToList();
                        var localIds = new HashSet<string>(localChangesForTable.Select(c => c.RecordId));

                        // Partition remote changes based on ID membership
                        var cleanRemoteChanges = new System.Collections.Generic.List<System.Collections.Generic.Dictionary<string, object>>();
                        var potentialConflictRemoteChanges = new System.Collections.Generic.List<System.Collections.Generic.Dictionary<string, object>>();

                        foreach (var r in remoteChanges)
                        {
                            if (!r.TryGetValue(_config.PrimaryKeyColumn, out var idObj) || idObj == null)
                            {
                                // No ID -> treat as clean to avoid dropping data
                                cleanRemoteChanges.Add(r);
                                continue;
                            }
                            var id = idObj.ToString();
                            if (localIds.Contains(id)) potentialConflictRemoteChanges.Add(r);
                            else cleanRemoteChanges.Add(r);
                        }

                        var detection = await _conflictResolver.DetectConflicts(potentialConflictRemoteChanges, localChangesForTable);
                        var conflicts = detection.conflicts;
                        var nonConflictsFromDetection = detection.nonConflicts;
                        onProgress?.Invoke(68, $"Partitioned remote: clean={cleanRemoteChanges.Count}, potentialConflicts={potentialConflictRemoteChanges.Count}, detectedConflicts={conflicts.Count}, nonConflictsFromDetection={nonConflictsFromDetection.Count}");
                        allUnresolvedConflicts.AddRange(conflicts);
                        var resolvedChanges = await _conflictResolver.Resolve(conflicts);
                        onProgress?.Invoke(70, $"Resolved conflicts: {resolvedChanges.Count()}");

                        var changesToApply = cleanRemoteChanges.Concat(nonConflictsFromDetection).Concat(resolvedChanges);
                        var toApplyCount = changesToApply.Count();
                        onProgress?.Invoke(72, $"Applying {toApplyCount} change(s) to local for {tableName}");
                        
                        if (changesToApply.Any())
                        {
                            await _localProvider.ApplyChangesAsync(tableName, changesToApply);
                        }
                    }
                    
                    // After processing each table, update the timestamp to mark progress.
                    // This makes the sync process more resilient to interruptions.
                    await SetLastSyncTimestampAsync(syncStartTime);
                    onProgress?.Invoke(90, $"Anchor updated to: {syncStartTime.ToString("o", CultureInfo.InvariantCulture)} after table {tableName}");
                }

                onProgress?.Invoke(100, "Synchronization finished.");

                return new SyncResult { Success = true, UnresolvedConflicts = allUnresolvedConflicts, Message = "Synchronization completed successfully." };
            }
            catch (Exception ex)
            {
                return new SyncResult { Success = false, ErrorDetails= ex.Message, Message = ex.Message };
            }
        }

        private async Task<DateTime?> GetLastSyncTimestampAsync()
        {
            var timestampStr = await _localProvider.GetParameterAsync("LastSyncTimestamp");
            if (string.IsNullOrWhiteSpace(timestampStr))
                return null;

            // Prefer ISO 8601 (Round-trip) if present
            if (DateTime.TryParseExact(timestampStr, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var isoTs))
                return isoTs;

            // Back-compat: older builds stored OADate (double as string)
            if (double.TryParse(timestampStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var oa))
            {
                try { return DateTime.FromOADate(oa); } catch { }
            }

            // Last resort generic parse
            if (DateTime.TryParse(timestampStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var generic))
                return generic;

            return null;
        }

        private async Task SetLastSyncTimestampAsync(DateTime timestamp)
        {
            // Store as ISO 8601 for robust round-trip and cross-locale parsing
            await _localProvider.SetParameterAsync("LastSyncTimestamp", timestamp.ToString("o", CultureInfo.InvariantCulture)); 
        }
    }
}
