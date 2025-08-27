using System;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using OfflineFirstAccess.Data;
using OfflineFirstAccess.ChangeTracking;
using OfflineFirstAccess.Conflicts;
using OfflineFirstAccess.Models;
using System.Collections.Generic;
using OfflineFirstAccess.Helpers;

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
                    LogManager.Debug($"PUSH start: Table={tableName} LocalChanges={tableGroup.Count()}");
                    // Build per-ID operation map and union UPDATE column sets across multiple changes
                    var opById = new Dictionary<string, (string Op, HashSet<string> Columns)>(StringComparer.OrdinalIgnoreCase);
                    foreach (var change in tableGroup.OrderBy(c => c.TimestampUTC))
                    {
                        var id = change.RecordId;
                        var (op, cols) = ParseOperationColumns(change.OperationType);
                        if (!opById.TryGetValue(id, out var existing))
                        {
                            opById[id] = (op, cols ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                            continue;
                        }

                        // Merge semantics: DELETE overrides everything; INSERT overrides UPDATE; UPDATE unions columns
                        if (string.Equals(op, "DELETE", StringComparison.OrdinalIgnoreCase))
                        {
                            opById[id] = ("DELETE", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                        }
                        else if (string.Equals(op, "INSERT", StringComparison.OrdinalIgnoreCase))
                        {
                            opById[id] = ("INSERT", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                        }
                        else if (string.Equals(op, "UPDATE", StringComparison.OrdinalIgnoreCase))
                        {
                            var target = existing.Columns ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            if (cols != null)
                            {
                                foreach (var c in cols) target.Add(c);
                            }
                            opById[id] = ("UPDATE", target);
                        }
                    }

                    // Log the merged ops per ID
                    foreach (var kv in opById)
                    {
                        var colsStr = (kv.Value.Columns != null && kv.Value.Columns.Count > 0) ? string.Join(",", kv.Value.Columns) : "<none>";
                        LogManager.Debug($"PUSH op merged: Table={tableName} ID={kv.Key} Op={kv.Value.Op} Columns=[{colsStr}]");
                    }

                    var ids = opById.Keys.ToList();
                    onProgress?.Invoke(20, $"Pushing changes for table {tableName} (ids={ids.Count})...");

                    // Fetch full records, then trim per Operation
                    var fullRecords = await _localProvider.GetRecordsByIds(tableName, ids);
                    var toApply = new List<Dictionary<string, object>>();
                    int insertCount = 0, deleteCount = 0, updatePartialCount = 0, updateFullCount = 0;
                    foreach (var rec in fullRecords)
                    {
                        if (!rec.TryGetValue(_config.PrimaryKeyColumn, out var idVal) || idVal == null)
                            continue;
                        var idStr = idVal.ToString();
                        if (!opById.TryGetValue(idStr, out var opInfo))
                            continue;

                        if (string.Equals(opInfo.Op, "INSERT", StringComparison.OrdinalIgnoreCase))
                        {
                            // Full record for inserts
                            toApply.Add(new Dictionary<string, object>(rec));
                            insertCount++;
                            LogManager.Debug($"PUSH payload: Table={tableName} ID={idStr} Op=INSERT Keys=[{string.Join(",", rec.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase))}]");
                        }
                        else if (string.Equals(opInfo.Op, "DELETE", StringComparison.OrdinalIgnoreCase))
                        {
                            // Signal deletion via IsDeleted flag if configured
                            var del = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                            {
                                [_config.PrimaryKeyColumn] = idVal,
                                [_config.IsDeletedColumn] = true
                            };
                            toApply.Add(del);
                            deleteCount++;
                            LogManager.Debug($"PUSH payload: Table={tableName} ID={idStr} Op=DELETE Keys=[{string.Join(",", del.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase))}]");
                        }
                        else // UPDATE
                        {
                            // If we have explicit columns, send only those; else fallback to full record (legacy logs)
                            Dictionary<string, object> partial;
                            if (opInfo.Columns != null && opInfo.Columns.Count > 0)
                            {
                                partial = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                                {
                                    [_config.PrimaryKeyColumn] = idVal
                                };
                                foreach (var col in opInfo.Columns)
                                {
                                    if (string.Equals(col, _config.PrimaryKeyColumn, StringComparison.OrdinalIgnoreCase)) continue;
                                    if (rec.ContainsKey(col)) partial[col] = rec[col];
                                }
                                updatePartialCount++;
                            }
                            else
                            {
                                // Legacy UPDATE without column list -> send full record to remain backward compatible
                                partial = new Dictionary<string, object>(rec, StringComparer.OrdinalIgnoreCase);
                                updateFullCount++;
                            }

                            // Ensure LastModified is bumped on the target to keep anchor-based pulls consistent
                            partial[_config.LastModifiedColumn] = DateTime.UtcNow;
                            // If ModifiedBy column exists in this table, propagate it as well
                            if (rec.ContainsKey("ModifiedBy") && !partial.ContainsKey("ModifiedBy"))
                            {
                                partial["ModifiedBy"] = rec["ModifiedBy"];
                            }
                            toApply.Add(partial);

                            var colsStr = (opInfo.Columns != null && opInfo.Columns.Count > 0) ? string.Join(",", opInfo.Columns) : "<legacy-full>";
                            var keysStr = string.Join(",", partial.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase));
                            var hasModBy = partial.ContainsKey("ModifiedBy");
                            LogManager.Debug($"PUSH payload: Table={tableName} ID={idStr} Op=UPDATE Columns=[{colsStr}] Keys=[{keysStr}] ModifiedByIncluded={hasModBy}");
                        }
                    }

                    var pushCount = toApply.Count;
                    onProgress?.Invoke(25, $"Push apply {pushCount} record(s) into remote for {tableName}");
                    LogManager.Debug($"PUSH summary: Table={tableName} total={pushCount} INSERT={insertCount} DELETE={deleteCount} UPDATE_partial={updatePartialCount} UPDATE_full={updateFullCount}");
                    if (toApply.Count > 0)
                    {
                        await _remoteProvider.ApplyChangesAsync(tableName, toApply);
                    }
                    await _changeTracker.MarkChangesAsSyncedAsync(tableGroup.Select(c => c.Id));
                    onProgress?.Invoke(30, $"Marked {ids.Count} local change(s) as synced for {tableName}");
                }

                // After PUSH, refresh the local change list to reflect any remaining unsynced changes
                // (e.g., concurrent edits during sync). These are the only ones relevant for conflict detection.
                allLocalChanges = await _changeTracker.GetUnsyncedChangesAsync();

                // --- PHASE 2: PULL (Server -> Client) ---
                 onProgress?.Invoke(50, "Pulling remote changes...");
                var lastSyncTimestamp = await GetLastSyncTimestampAsync();
                if (!lastSyncTimestamp.HasValue)
                {
                    // Safety: initialize missing anchor to the sync start time to avoid full historical pull
                    lastSyncTimestamp = syncStartTime;
                    await SetLastSyncTimestampAsync(syncStartTime);
                    onProgress?.Invoke(51, $"Anchor missing -> initialized to now: {syncStartTime.ToString("o", CultureInfo.InvariantCulture)}");
                }
                var anchorStr = lastSyncTimestamp.Value.ToString("o", CultureInfo.InvariantCulture);
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
                    
                    // Removed per-table anchor update. We'll set a single consolidated anchor at the end.
                }

                // Set the anchor once at the end to the current time, ensuring we do not re-pull
                // records modified during this run (including those created by the PUSH phase).
                var newAnchor = DateTime.UtcNow;
                await SetLastSyncTimestampAsync(newAnchor);
                onProgress?.Invoke(95, $"Anchor updated to: {newAnchor.ToString("o", CultureInfo.InvariantCulture)}");

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

        // Parses an Operation string from ChangeLog into a normalized op and a set of changed columns (for UPDATE)
        private static (string Op, HashSet<string> Columns) ParseOperationColumns(string operationType)
        {
            if (string.IsNullOrWhiteSpace(operationType))
            {
                return ("UPDATE", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            }

            var op = operationType.Trim();
            if (op.StartsWith("UPDATE(", StringComparison.OrdinalIgnoreCase) && op.EndsWith(")", StringComparison.OrdinalIgnoreCase))
            {
                var inner = op.Substring(7, op.Length - 8);
                var cols = inner
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Trim())
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                return ("UPDATE", cols);
            }

            if (op.Equals("UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                // Legacy logs without column list
                return ("UPDATE", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            }

            if (op.Equals("INSERT", StringComparison.OrdinalIgnoreCase)) return ("INSERT", null);
            if (op.Equals("DELETE", StringComparison.OrdinalIgnoreCase)) return ("DELETE", null);

            // Unknown -> pass through as UPDATE with no columns to be safe
            return ("UPDATE", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        }
    }
}
