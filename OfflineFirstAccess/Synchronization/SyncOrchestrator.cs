using System;
using System.Linq;
using System.Threading.Tasks;
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
                var changesByTable = allLocalChanges.GroupBy(c => c.TableName);

                foreach (var tableGroup in changesByTable)
                {
                    var tableName = tableGroup.Key;
                    onProgress?.Invoke(20, $"Pushing changes for table {tableName}...");
                    var recordsToPush = await _localProvider.GetRecordsByIds(tableName, tableGroup.Select(c => c.RecordId));
                    await _remoteProvider.ApplyChangesAsync(tableName, recordsToPush);
                    await _changeTracker.MarkChangesAsSyncedAsync(tableGroup.Select(c => c.Id));
                }

                // --- PHASE 2: PULL (Server -> Client) ---
                onProgress?.Invoke(50, "Pulling remote changes...");
                var lastSyncTimestamp = await GetLastSyncTimestampAsync();

                foreach (var tableName in _config.TablesToSync)
                {
                    onProgress?.Invoke(60, $"Pulling changes for table {tableName}...");
                    var remoteChanges = await _remoteProvider.GetChangesAsync(tableName, lastSyncTimestamp);

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
                        allUnresolvedConflicts.AddRange(conflicts);
                        var resolvedChanges = await _conflictResolver.Resolve(conflicts);

                        var changesToApply = cleanRemoteChanges.Concat(nonConflictsFromDetection).Concat(resolvedChanges);
                        
                        if (changesToApply.Any())
                        {
                            await _localProvider.ApplyChangesAsync(tableName, changesToApply);
                        }
                    }
                    
                    // After processing each table, update the timestamp to mark progress.
                    // This makes the sync process more resilient to interruptions.
                    await SetLastSyncTimestampAsync(syncStartTime);
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
            if (timestampStr != null && DateTime.TryParse(timestampStr, out var timestamp))
            {
                return timestamp;
            }
            return null;
        }

        private async Task SetLastSyncTimestampAsync(DateTime timestamp)
        {
            await _localProvider.SetParameterAsync("LastSyncTimestamp", timestamp.ToOADate().ToString()); 
        }
    }
}
