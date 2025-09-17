using OfflineFirstAccess.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Helpers;
using RecoTool.Services.Helpers;

namespace RecoTool.Services
{
    // Partial: push/pull synchronization operations
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Pousse les changements locaux de réconciliation s'il y en a, si le réseau est disponible et aucun lock global.
        /// </summary>
        public async Task<bool> PushReconciliationIfPendingAsync(string countryId)
        {
            try
            {
                // Global kill-switch: do not push if background pushes are disabled
                if (!AllowBackgroundPushes)
                {
                    return false;
                }
                if (string.IsNullOrWhiteSpace(countryId)) return false;
                // Basculer si besoin
                if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                {
                    var ok = await SetCurrentCountryAsync(countryId, suppressPush: true);
                    if (!ok) return false;
                }

                var cid = _currentCountryId;
                var diag = IsDiagSyncEnabled();

                // Debounce: skip if a push just happened very recently
                var last = _lastPushTimesUtc.TryGetValue(cid, out var t) ? t : DateTime.MinValue;
                if (DateTime.UtcNow - last < _pushCooldown)
                {
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Skipped due to cooldown. SinceLast={(DateTime.UtcNow - last).TotalSeconds:F1}s, Cooldown={_pushCooldown.TotalSeconds}s");
                        try { LogManager.Info($"[PUSH][{cid}] Skipped due to cooldown"); } catch { }
                    }
                    return false;
                }

                if (diag)
                {
                    var lastDbg = _lastPushTimesUtc.TryGetValue(cid, out var tdbg) ? tdbg : DateTime.MinValue;
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Enter PushReconciliationIfPendingAsync. NowUtc={DateTime.UtcNow:o}, LastPushUtc={lastDbg:o}");
                    try { LogManager.Info($"[PUSH][{cid}] Enter PushReconciliationIfPendingAsync"); } catch { }
                }

                // Vérifier réseau et lock
                if (!IsNetworkSyncAvailable)
                {
                    // offline: report pending status if any
                    try { await RaiseSyncStateAsync(cid, SyncStateKind.OfflinePending); } catch { }
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Skipped: network unavailable");
                        try { LogManager.Info($"[PUSH][{cid}] Skipped: network unavailable"); } catch { }
                    }
                    return false;
                }
                bool lockActiveByOthers = false; try { lockActiveByOthers = await IsGlobalLockActiveByOthersAsync(); } catch { lockActiveByOthers = false; }
                if (lockActiveByOthers)
                {
                    try { await RaiseSyncStateAsync(cid, SyncStateKind.OfflinePending); } catch { }
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Skipped: global lock active (held by another process)");
                        try { LogManager.Info($"[PUSH][{cid}] Skipped: global lock active (others)"); } catch { }
                    }
                    return false;
                }

                // Y a-t-il des changements non synchronisés ? (filtrer sur T_Reconciliation uniquement)
                bool hasPending = true;
                List<OfflineFirstAccess.Models.ChangeLogEntry> unsynced = null;
                List<OfflineFirstAccess.Models.ChangeLogEntry> recoUnsynced = null;
                try
                {
                    var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(cid));
                    var tmp = await tracker.GetUnsyncedChangesAsync();
                    unsynced = tmp?.ToList();
                    // Ne pousser que T_Reconciliation (plus simple et ciblé)
                    recoUnsynced = unsynced?.Where(e => string.Equals(e?.TableName, "T_Reconciliation", StringComparison.OrdinalIgnoreCase)).ToList()
                                   ?? new List<OfflineFirstAccess.Models.ChangeLogEntry>();
                    hasPending = recoUnsynced.Any();
                    if (diag)
                    {
                        var cnt = recoUnsynced?.Count() ?? 0;
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Pending check (T_Reconciliation): {cnt} unsynced change(s)");
                        try { LogManager.Info($"[PUSH][{cid}] Pending check (T_Reconciliation): {cnt} unsynced"); } catch { }
                    }
                    if (!hasPending)
                    {
                        try { await RaiseSyncStateAsync(cid, SyncStateKind.UpToDate, pendingOverride: 0); } catch { }
                    }
                    else
                    {
                        try { await RaiseSyncStateAsync(cid, SyncStateKind.SyncInProgress, pendingOverride: recoUnsynced.Count); } catch { }
                    }
                }
                catch { /* en cas d'erreur, tenter quand même */ }

                if (!hasPending)
                {
                    _lastPushTimesUtc[cid] = DateTime.UtcNow;
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] No pending changes for T_Reconciliation. Marking UpToDate and exiting.");
                        try { LogManager.Info($"[PUSH][{cid}] No pending changes for T_Reconciliation"); } catch { }
                    }
                    return true;
                }

                // Appel du push granulaire (T_Reconciliation uniquement) AVEC verrou global court pour éviter la course avec import
                if (diag)
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Attempting short global lock for Lite push...");
                    try { LogManager.Info($"[PUSH][{cid}] Attempting short global lock for Lite push"); } catch { }
                }
                using (var gl = await AcquireGlobalLockAsync(cid, "LitePush-Reconciliation", TimeSpan.FromSeconds(30)))
                {
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Global lock acquired. Invoking PushPendingChangesToNetworkAsync (Lite/T_Reconciliation)...");
                        try { LogManager.Info($"[PUSH][{cid}] Global lock acquired. Invoking PushPendingChangesToNetworkAsync (Lite/T_Reconciliation)"); } catch { }
                    }
                    await PushPendingChangesToNetworkAsync(cid, assumeLockHeld: true, source: nameof(PushReconciliationIfPendingAsync) + "/Lite", preloadedUnsynced: recoUnsynced);
                    // Two-way: pull network changes back to local (LastModified first, then Version)
                    try
                    {
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PULL][{cid}] Starting PullReconciliationFromNetworkAsync after push...");
                            try { LogManager.Info($"[PULL][{cid}] Starting PullReconciliationFromNetworkAsync after push"); } catch { }
                        }
                        var pulled = await PullReconciliationFromNetworkAsync(cid);
                        try { ReconciliationService.InvalidateReconciliationViewCache(cid); } catch { }
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PULL][{cid}] Completed pull. Applied {pulled} row(s) from network to local.");
                            try { LogManager.Info($"[PULL][{cid}] Completed pull. Applied {pulled} row(s)"); } catch { }
                        }
                    }
                    catch (Exception exPull)
                    {
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PULL][{cid}] PullReconciliationFromNetworkAsync failed: {exPull.Message}");
                            try { LogManager.Error($"[PULL][{cid}] PullReconciliationFromNetworkAsync failed: {exPull.Message}", exPull); } catch { }
                        }
                        // best-effort: do not fail the overall operation
                    }
                }
                _lastPushTimesUtc[cid] = DateTime.UtcNow;
                try { await RaiseSyncStateAsync(cid, SyncStateKind.UpToDate, pendingOverride: 0); } catch { }
                // Consider success if operation completed (even if 0 changes were pushed)

                // Completed successfully
                try { await RaiseSyncStateAsync(cid, SyncStateKind.UpToDate, pendingOverride: 0); } catch { }
                if (diag)
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Completed PushReconciliationIfPendingAsync successfully.");
                    try { LogManager.Info($"[PUSH][{cid}] Completed PushReconciliationIfPendingAsync"); } catch { }
                }
                return true;
            }
            catch (Exception ex)
            {
                try { await RaiseSyncStateAsync(countryId, SyncStateKind.Error, error: ex); } catch { }
                try { LogManager.Error($"[PUSH] PushReconciliationIfPendingAsync failed for {countryId}: {ex}", ex); } catch { }
                return false;
            }
        }

        /// <summary>
        /// Pull network changes for T_Reconciliation back into the local database.
        /// Comparison rule: prefer LastModified (if present); if not newer by LastModified, compare Version.
        /// Applies UPDATE for existing rows and INSERT for missing ones. Returns number of rows applied.
        /// </summary>
        private async Task<int> PullReconciliationFromNetworkAsync(string countryId)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            int applied = 0;

            using (var localConn = new OleDbConnection(GetCountryConnectionString(countryId)))
            using (var netConn = new OleDbConnection(GetNetworkCountryConnectionString(countryId)))
            {
                await localConn.OpenAsync();
                await netConn.OpenAsync();

                var pkCol = await GetPrimaryKeyColumnAsync(localConn, "T_Reconciliation");
                if (string.IsNullOrWhiteSpace(pkCol)) throw new InvalidOperationException("Impossible de déterminer la clé primaire de T_Reconciliation");

                var localCols = await GetTableColumnsAsync(localConn, "T_Reconciliation");
                var netCols = await GetTableColumnsAsync(netConn, "T_Reconciliation");
                var commonCols = new HashSet<string>(localCols.Intersect(netCols, StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
                // Always include PK
                commonCols.Add(pkCol);
                // Make sure we have these for comparison if available
                bool hasVer = commonCols.Contains("Version");
                bool hasLM = commonCols.Contains("LastModified");

                // Build column list for network SELECT
                var selectCols = commonCols.ToList();
                // stable order: PK first
                selectCols = selectCols
                    .OrderBy(c => string.Equals(c, pkCol, StringComparison.OrdinalIgnoreCase) ? 0 : 1)
                    .ThenBy(c => c, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                // Build local map: pk -> (Version, LastModified)
                var localMap = new Dictionary<string, (long ver, DateTime? lm)>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    var sb = new StringBuilder();
                    sb.Append($"SELECT [{pkCol}]");
                    if (hasVer) sb.Append(", [Version]");
                    if (hasLM) sb.Append(", [LastModified]");
                    sb.Append(" FROM [T_Reconciliation]");
                    using (var cmd = new OleDbCommand(sb.ToString(), localConn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var pk = Convert.ToString(reader[pkCol]);
                            long ver = 0; DateTime? lm = null;
                            if (hasVer && !reader.IsDBNull(reader.GetOrdinal("Version")))
                                ver = Convert.ToInt64(reader["Version"]);
                            if (hasLM && !reader.IsDBNull(reader.GetOrdinal("LastModified")))
                                lm = Convert.ToDateTime(reader["LastModified"]);
                            if (!string.IsNullOrWhiteSpace(pk)) localMap[pk] = (ver, lm);
                        }
                    }
                }
                catch { /* proceed best-effort */ }

                var localTypes = await OleDbSchemaHelper.GetColumnTypesAsync(localConn, "T_Reconciliation");

                // Determine filters based on latest local LastModified (preferred) or Version
                DateTime? lastLocalLm = null;
                long lastLocalVer = -1;
                try
                {
                    if (hasLM)
                    {
                        lastLocalLm = await OleDbUtils.GetMaxLastModifiedAsync(localConn, "T_Reconciliation");
                        if (!lastLocalLm.HasValue && _lastSyncTimes.TryGetValue(countryId, out var ts))
                        {
                            lastLocalLm = ts;
                        }
                    }
                }
                catch { }
                try
                {
                    if (hasVer)
                    {
                        lastLocalVer = await OleDbUtils.GetMaxVersionAsync(localConn, "T_Reconciliation");
                    }
                }
                catch { }

                // Network scan: compare and apply with server-side filter when possible
                var colList = string.Join(", ", selectCols.Select(c => $"[{c}]"));
                string where = null;
                var ncmd = new OleDbCommand();
                ncmd.Connection = netConn;
                if (hasLM && lastLocalLm.HasValue)
                {
                    where = " WHERE [LastModified] > ?";
                    ncmd.CommandText = $"SELECT {colList} FROM [T_Reconciliation]{where}";
                    ncmd.Parameters.Add(new OleDbParameter("@pLM", OleDbType.Date) { Value = OleDbSchemaHelper.CoerceValueForOleDb(lastLocalLm.Value, OleDbType.Date) });
                }
                else if (hasVer && lastLocalVer >= 0)
                {
                    where = " WHERE [Version] > ?";
                    ncmd.CommandText = $"SELECT {colList} FROM [T_Reconciliation]{where}";
                    ncmd.Parameters.Add(new OleDbParameter("@pV", OleDbType.BigInt) { Value = lastLocalVer });
                }
                else
                {
                    ncmd.CommandText = $"SELECT {colList} FROM [T_Reconciliation]";
                }
                using (var nrd = await ncmd.ExecuteReaderAsync())
                {
                    while (await nrd.ReadAsync())
                    {
                        // Read PK
                        var pkVal = nrd[pkCol];
                        if (pkVal == null || pkVal == DBNull.Value) continue;
                        var pkStr = Convert.ToString(pkVal);

                        // Read compare values from network
                        long nVer = 0; DateTime? nLm = null;
                        if (hasVer)
                        {
                            var o = nrd["Version"]; if (o != null && o != DBNull.Value) nVer = Convert.ToInt64(o);
                        }
                        if (hasLM)
                        {
                            var o = nrd["LastModified"]; if (o != null && o != DBNull.Value) nLm = Convert.ToDateTime(o);
                        }

                        bool existsLocal = localMap.TryGetValue(pkStr, out var loc);
                        bool shouldApply = false;
                        if (!existsLocal)
                        {
                            shouldApply = true; // missing locally -> insert
                        }
                        else
                        {
                            // Compare by LastModified first (if present)
                            if (hasLM && nLm.HasValue)
                            {
                                var lLm = loc.lm;
                                // Fallback to the table-wide max local LM if this row's LM is null
                                var lLmEff = lLm.HasValue ? lLm : lastLocalLm;
                                if (lLmEff.HasValue)
                                {
                                    // Tolerance to absorb timezone/precision differences between local and network DBs
                                    var tolerance = TimeSpan.FromSeconds(2);
                                    var delta = nLm.Value - lLmEff.Value;
                                    if (delta > tolerance)
                                        shouldApply = true;
                                }
                                // If no effective local LM, skip LM path and rely on Version comparison below
                            }
                            // If not newer by LM, compare by Version
                            if (!shouldApply && hasVer)
                            {
                                var lVer = loc.ver;
                                if (nVer > lVer)
                                    shouldApply = true;
                            }
                        }

                        if (!shouldApply) continue;

                        // Build values dictionary from network row for common columns (excluding PK for update set)
                        var rowVals = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                        foreach (var c in selectCols)
                        {
                            rowVals[c] = nrd[c];
                        }

                        if (existsLocal)
                        {
                            // UPDATE local
                            var setCols = selectCols.Where(c => !string.Equals(c, pkCol, StringComparison.OrdinalIgnoreCase)).ToList();
                            var setParts = setCols.Select((c, i) => $"[{c}] = @p{i}").ToList();
                            using (var up = new OleDbCommand($"UPDATE [T_Reconciliation] SET {string.Join(", ", setParts)} WHERE [{pkCol}] = @key", localConn))
                            {
                                for (int i = 0; i < setCols.Count; i++)
                                {
                                    var c = setCols[i];
                                    localTypes.TryGetValue(c, out var t);
                                    var p = new OleDbParameter($"@p{i}", t == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(rowVals[c]) : t)
                                    {
                                        Value = OleDbSchemaHelper.CoerceValueForOleDb(rowVals[c], t == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(rowVals[c]) : t)
                                    };
                                    up.Parameters.Add(p);
                                }
                                // key
                                localTypes.TryGetValue(pkCol, out var tkey);
                                up.Parameters.Add(new OleDbParameter("@key", tkey == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(pkVal) : tkey)
                                {
                                    Value = OleDbSchemaHelper.CoerceValueForOleDb(pkVal, tkey == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(pkVal) : tkey)
                                });
                                await up.ExecuteNonQueryAsync();
                            }
                            applied++;
                        }
                        else
                        {
                            // INSERT local for all columns (selectCols)
                            var insCols = selectCols.ToList();
                            var ph = string.Join(", ", insCols.Select((c, i) => $"@p{i}"));
                            var colListIns = string.Join(", ", insCols.Select(c => $"[{c}]"));
                            using (var ins = new OleDbCommand($"INSERT INTO [T_Reconciliation] ({colListIns}) VALUES ({ph})", localConn))
                            {
                                for (int i = 0; i < insCols.Count; i++)
                                {
                                    var c = insCols[i];
                                    localTypes.TryGetValue(c, out var t);
                                    var v = rowVals[c];
                                    var p = new OleDbParameter($"@p{i}", t == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(v) : t)
                                    {
                                        Value = OleDbSchemaHelper.CoerceValueForOleDb(v, t == 0 ? OleDbSchemaHelper.InferOleDbTypeFromValue(v) : t)
                                    };
                                    ins.Parameters.Add(p);
                                }
                                await ins.ExecuteNonQueryAsync();
                            }
                            applied++;
                        }
                    }
                }
            }

            return applied;
        }

        /// <summary>
        /// Pousse de manière robuste les changements locaux en attente vers la base réseau sous verrou global.
        /// Applique INSERT/UPDATE/DELETE sur la base réseau à partir de l'état local pour chaque ChangeLog non synchronisé trouvé,
        /// puis marque uniquement ces entrées comme synchronisées. Ignore les entrées qui ne correspondent pas à une ligne locale.
        /// </summary>
        public async Task<int> PushPendingChangesToNetworkAsync(string countryId, bool assumeLockHeld = false, CancellationToken token = default, string source = null, IEnumerable<OfflineFirstAccess.Models.ChangeLogEntry> preloadedUnsynced = null)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            var diag = IsDiagSyncEnabled();
            if (diag)
            {
                var src = string.IsNullOrWhiteSpace(source) ? "-" : source;
                System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Enter PushPendingChangesToNetworkAsync (assumeLockHeld={assumeLockHeld}, src={src})");
                try { LogManager.Info($"[PUSH][{countryId}] Enter PushPendingChangesToNetworkAsync (assumeLockHeld={assumeLockHeld}, src={src})"); } catch { }
            }

            // Coalesce concurrent calls per country in a race-free way: only one task is created.
            async Task<int> RunAsync()
            {
                var sem = _pushSemaphores.GetOrAdd(countryId, _ => new SemaphoreSlim(1, 1));
                await sem.WaitAsync(token);
                try { return await PushPendingChangesToNetworkCoreAsync(countryId, assumeLockHeld, token, preloadedUnsynced); }
                finally { try { sem.Release(); } catch { } }
            }

            bool created = false;
            var task = _activePushes.GetOrAdd(countryId, _ => { created = true; return RunAsync(); });
            if (!created)
            {
                // A push is already in progress for this country: wait for it to finish and return its result
                // Do not start a new push and avoid extra logs/noise.
                try { return await task; }
                finally { /* no-op: the remover will run when original creator finishes */ }
            }
            try { return await task; }
            finally { _activePushes.TryRemove(countryId, out _); }
        }

        // Core implementation separated so we can gate/coalesce the public entry
        private async Task<int> PushPendingChangesToNetworkCoreAsync(string countryId, bool assumeLockHeld, CancellationToken token, IEnumerable<OfflineFirstAccess.Models.ChangeLogEntry> preloadedUnsynced)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // S'assurer que le service est positionné sur le bon pays (AcquireGlobalLockAsync utilise _currentCountryId)
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                var ok = await SetCurrentCountryAsync(countryId, suppressPush: true);
                if (!ok) throw new InvalidOperationException($"Impossible d'initialiser la base locale pour {countryId}");
            }

            var diag = IsDiagSyncEnabled();
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));

            // Start a per-run watchdog early to capture stalls in FetchUnsynced as well
            var runId = Guid.NewGuid();
            _pushRunIds[countryId] = runId;
            var watchdogCts = new CancellationTokenSource();
            if (diag)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), watchdogCts.Token);
                        if (watchdogCts.Token.IsCancellationRequested) return;
                        if (_pushRunIds.TryGetValue(countryId, out var currentId) && currentId == runId)
                        {
                            _pushStages.TryGetValue(countryId, out var stageMsg);
                            System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Still running after 30s... stage={stageMsg}");
                            try { LogManager.Info($"[PUSH][{countryId}] Still running after 30s... stage={stageMsg}"); } catch { }
                        }
                    }
                    catch { }
                });
            }

            // Récupérer les entrées non synchronisées (utiliser la liste préchargée si fournie)
            List<OfflineFirstAccess.Models.ChangeLogEntry> unsynced;
            if (preloadedUnsynced != null)
            {
                _pushStages[countryId] = "UsePreloadedUnsynced";
                unsynced = preloadedUnsynced.ToList();
                if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Using preloaded unsynced: {unsynced.Count}"); try { LogManager.Info($"[PUSH][{countryId}] Using preloaded unsynced: {unsynced.Count}"); } catch { } }
            }
            else
            {
                _pushStages[countryId] = "FetchUnsynced";
                if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Fetching unsynced changes from ChangeLog..."); try { LogManager.Info($"[PUSH][{countryId}] Fetching unsynced changes from ChangeLog..."); } catch { } }
                // Service-level hard cap slightly above inner 15s reader timeout
                var fetchTask = tracker.GetUnsyncedChangesAsync();
                var fetchCompleted = await Task.WhenAny(fetchTask, Task.Delay(TimeSpan.FromSeconds(20), token)) == fetchTask;
                if (!fetchCompleted)
                {
                    if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Timeout fetching unsynced changes after 20s"); try { LogManager.Info($"[PUSH][{countryId}] Timeout fetching unsynced changes after 20s"); } catch { } }
                    try { watchdogCts.Cancel(); } catch { }
                    try { watchdogCts.Dispose(); } catch { }
                    try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
                    throw new TimeoutException("Timeout fetching unsynced changes from ChangeLog after 20s");
                }
                unsynced = (await fetchTask)?.ToList() ?? new List<OfflineFirstAccess.Models.ChangeLogEntry>();
            }
            if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Unsynced fetched: {unsynced.Count}"); try { LogManager.Info($"[PUSH][{countryId}] Unsynced fetched: {unsynced.Count}"); } catch { } }
            if (unsynced.Count == 0)
            {
                try { await RaiseSyncStateAsync(countryId, SyncStateKind.UpToDate, pendingOverride: 0); } catch { }
                try { watchdogCts.Cancel(); } catch { }
                try { watchdogCts.Dispose(); } catch { }
                try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
                return 0;
            }

            // Notify start
            try { await RaiseSyncStateAsync(countryId, SyncStateKind.SyncInProgress, pendingOverride: unsynced.Count); } catch { }
            if (diag)
            {
                System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Push core start. Unsynced={unsynced.Count}");
                try { LogManager.Info($"[PUSH][{countryId}] Push core start. Unsynced={unsynced.Count}"); } catch { }
            }

            // Acquérir le verrou global si non détenu par l'appelant
            IDisposable globalLock = null;
            if (!assumeLockHeld)
            {
                _pushStages[countryId] = "AcquireLock";
                if (diag)
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Acquiring global lock...");
                    try { LogManager.Info($"[PUSH][{countryId}] Acquiring global lock..."); } catch { }
                }
                int lockSecs = 20; // configurable acquire timeout
                try { lockSecs = Math.Max(5, Math.Min(120, int.Parse(GetParameter("GlobalLockAcquireTimeoutSeconds") ?? "20"))); } catch { }
                var acquireTask = AcquireGlobalLockAsync(countryId, "PushPendingChanges", TimeSpan.FromMinutes(5), token);
                var completed = await Task.WhenAny(acquireTask, Task.Delay(TimeSpan.FromSeconds(lockSecs), token)) == acquireTask;
                if (!completed)
                {
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Timeout acquiring global lock after {lockSecs}s");
                        try { LogManager.Info($"[PUSH][{countryId}] Timeout acquiring global lock after {lockSecs}s"); } catch { }
                    }
                    try { watchdogCts.Cancel(); } catch { }
                    try { watchdogCts.Dispose(); } catch { }
                    try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
                    throw new TimeoutException($"Timeout acquiring global lock after {lockSecs}s");
                }
                globalLock = await acquireTask;
                if (globalLock == null)
                {
                    try { watchdogCts.Cancel(); } catch { }
                    try { watchdogCts.Dispose(); } catch { }
                    try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
                    throw new InvalidOperationException($"Impossible d'acquérir le verrou global pour {countryId} (PushPendingChanges)");
                }
                if (diag)
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Global lock acquired.");
                    try { LogManager.Info($"[PUSH][{countryId}] Global lock acquired"); } catch { }
                }
            }
            try
            {
                var appliedIds = new List<long>();

                // Préparer connexions
                // Preflight: ensure network reconciliation DB exists; if missing, create and publish it.
                _pushStages[countryId] = "EnsureNetworkDb";
                try
                {
                    var networkDbPathPre = GetNetworkReconciliationDbPath(countryId);
                    if (string.IsNullOrWhiteSpace(networkDbPathPre)) throw new InvalidOperationException("Network DB path is empty");
                    if (!File.Exists(networkDbPathPre))
                    {
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Network DB missing. Recreating...");
                            try { LogManager.Info($"[PUSH][{countryId}] Network DB missing. Recreating..."); } catch { }
                        }
                        var recreator = new DatabaseRecreationService();
                        var rep = await recreator.RecreateReconciliationAsync(this, countryId);
                        if (!rep.Success)
                        {
                            throw new InvalidOperationException($"Failed to (re)create network reconciliation DB: {string.Join(" | ", rep.Errors ?? new System.Collections.Generic.List<string>())}");
                        }
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Network DB recreated successfully.");
                            try { LogManager.Info($"[PUSH][{countryId}] Network DB recreated successfully."); } catch { }
                        }
                    }
                }
                catch (Exception exEnsure)
                {
                    // Fail fast: cannot proceed without a network DB
                    try { watchdogCts.Cancel(); } catch { }
                    try { watchdogCts.Dispose(); } catch { }
                    try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] EnsureNetworkDb failed: {exEnsure.Message}");
                        try { LogManager.Error($"[PUSH][{countryId}] EnsureNetworkDb failed: {exEnsure.Message}", exEnsure); } catch { }
                    }
                    throw;
                }

                using (var localConn = new OleDbConnection(GetCountryConnectionString(countryId)))
                using (var netConn = new OleDbConnection(GetNetworkCountryConnectionString(countryId)))
                {
                    // Configurable open timeout
                    int openSecs = 20;
                    try { openSecs = Math.Max(5, Math.Min(120, int.Parse(GetParameter("NetworkOpenTimeoutSeconds") ?? "20"))); } catch { }
                    var openTimeout = TimeSpan.FromSeconds(openSecs);

                    _pushStages[countryId] = "OpenLocal";
                    await OleDbUtils.OpenWithTimeoutAsync(localConn, openTimeout, token, IsDiagSyncEnabled() ? countryId : null, isNetwork:false);
                    if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Local DB opened."); try { LogManager.Info($"[PUSH][{countryId}] Local DB opened"); } catch { } }
                    _pushStages[countryId] = "OpenNetwork";
                    await OleDbUtils.OpenWithTimeoutAsync(netConn, openTimeout, token, IsDiagSyncEnabled() ? countryId : null, isNetwork:true);
                    if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Network DB opened."); try { LogManager.Info($"[PUSH][{countryId}] Network DB opened"); } catch { } }

                    _pushStages[countryId] = "BeginTx";
                    using (var tx = netConn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            // Caches de schéma
                            var tableColsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                            var pkColCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            var colTypeCache = new Dictionary<string, Dictionary<string, OleDbType>>(StringComparer.OrdinalIgnoreCase);

                            Func<string, Task<HashSet<string>>> getColsAsync = async (table) =>
                            {
                                if (tableColsCache.TryGetValue(table, out var set)) return set;
                                set = await GetTableColumnsAsync(netConn, table);
                                tableColsCache[table] = set;
                                return set;
                            };

                            Func<string, Task<string>> getPkAsync = async (table) =>
                            {
                                if (pkColCache.TryGetValue(table, out var pkc)) return pkc;
                                pkc = await GetPrimaryKeyColumnAsync(netConn, table) ?? "ID";
                                pkColCache[table] = pkc;
                                return pkc;
                            };

                            // Network schema column types for robust parameter binding
                            Func<string, Task<Dictionary<string, OleDbType>>> getColTypesAsync = async (table) =>
                            {
                                if (colTypeCache.TryGetValue(table, out var map)) return map;
                                var dt = netConn.GetSchema("Columns", new string[] { null, null, table, null });
                                map = new Dictionary<string, OleDbType>(StringComparer.OrdinalIgnoreCase);
                                foreach (System.Data.DataRow row in dt.Rows)
                                {
                                    var colName = Convert.ToString(row["COLUMN_NAME"]);
                                    if (string.IsNullOrEmpty(colName)) continue;
                                    // DATA_TYPE is an Int16 OLE DB type enum; cast to OleDbType
                                    var typeCode = Convert.ToInt32(row["DATA_TYPE"]);
                                    map[colName] = (OleDbType)typeCode;
                                }
                                colTypeCache[table] = map;
                                return map;
                            };

                            // Helper for robust parameter creation with type coercion
                            Action<OleDbCommand, string, object, string, Dictionary<string, OleDbType>> addParam = (cmd, name, value, col, typeMap) =>
                            {
                                object v = value;
                                OleDbType? t = null;
                                if (typeMap != null && col != null && typeMap.TryGetValue(col, out var mapped)) t = mapped;

                                if (v == null)
                                {
                                    var p = cmd.Parameters.Add(name, t ?? OleDbType.Variant);
                                    p.Value = DBNull.Value;
                                    return;
                                }

                                // Normalize Date/Time values (avoid OADate; use DateTime/DateTimeOffset)
                                if (t.HasValue && (t.Value == OleDbType.DBDate || t.Value == OleDbType.DBTime || t.Value == OleDbType.DBTimeStamp || t.Value == OleDbType.Date))
                                {
                                    if (v is DateTimeOffset dto) v = dto.UtcDateTime;
                                    else if (v is string s)
                                    {
                                        DateTime parsed;
                                        if (DateTime.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeLocal, out parsed)) v = parsed;
                                        else if (DateTime.TryParse(s, System.Globalization.CultureInfo.GetCultureInfo("fr-FR"), System.Globalization.DateTimeStyles.AssumeLocal, out parsed)) v = parsed;
                                        else if (DateTime.TryParse(s, out parsed)) v = parsed;
                                    }
                                }

                                if (t.HasValue)
                                {
                                    var p = cmd.Parameters.Add(name, t.Value);
                                    p.Value = v;
                                }
                                else
                                {
                                    // Fallback: infer from value and coerce
                                    var it = OleDbSchemaHelper.InferOleDbTypeFromValue(v);
                                    var p = new OleDbParameter(name, it) { Value = OleDbSchemaHelper.CoerceValueForOleDb(v, it) };
                                    cmd.Parameters.Add(p);
                                }
                            };

                            // Helper to execute non-query with retry on common Access lock violations
                            Func<OleDbCommand, Task> execWithRetryAsync = async (cmd) =>
                            {
                                _pushStages[countryId] = "ProcessChanges";
                                const int maxRetries = 5;
                                int attempt = 0;
                                while (true)
                                {
                                    try
                                    {
                                        await cmd.ExecuteNonQueryAsync();
                                        return;
                                    }
                                    catch (OleDbException ex)
                                    {
                                        // Access/Jet lock violations often surface with these native error codes
                                        bool isLock = false;
                                        foreach (OleDbError err in ex.Errors)
                                        {
                                            if (err.NativeError == 3218 || err.NativeError == 3260 || err.NativeError == 3188)
                                            {
                                                isLock = true; break;
                                            }
                                        }
                                        var msg = ex.Message?.ToLowerInvariant() ?? string.Empty;
                                        if (!isLock)
                                        {
                                            if (msg.Contains("locked") || msg.Contains("verrou")) isLock = true;
                                        }

                                        if (isLock && attempt < maxRetries)
                                        {
                                            attempt++;
                                            await Task.Delay(200 * attempt, token);
                                            continue;
                                        }
                                        throw;
                                    }
                                }
                            };

                            foreach (var entry in unsynced)
                            {
                                if (token.IsCancellationRequested) break;
                                if (string.IsNullOrWhiteSpace(entry?.TableName) || string.IsNullOrWhiteSpace(entry?.RecordId)) continue;

                                var table = entry.TableName;
                                var op = (entry.OperationType ?? string.Empty).Trim().ToUpperInvariant();

                                var cols = await getColsAsync(table);
                                if (cols == null || cols.Count == 0) continue;
                                var pkCol = await getPkAsync(table);
                                var typeMap = await getColTypesAsync(table);

                                // 1) Lire la ligne locale (si elle existe)
                                object localPkVal = entry.RecordId;
                                object Prepare(object v) => v ?? DBNull.Value;

                                Dictionary<string, object> localValues = null;
                                using (var lcCmd = new OleDbCommand($"SELECT * FROM [{table}] WHERE [{pkCol}] = @k", localConn))
                                {
                                    var localTypeMap = await OleDbSchemaHelper.GetColumnTypesAsync(localConn, table);
                                    var kType = localTypeMap.TryGetValue(pkCol, out var ktt) ? ktt : OleDbSchemaHelper.InferOleDbTypeFromValue(localPkVal);
                                    var pK = new OleDbParameter("@k", kType) { Value = OleDbSchemaHelper.CoerceValueForOleDb(localPkVal, kType) };
                                    lcCmd.Parameters.Add(pK);
                                    using (var r = await lcCmd.ExecuteReaderAsync())
                                    {
                                        if (await r.ReadAsync())
                                        {
                                            localValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                                            for (int i = 0; i < r.FieldCount; i++)
                                            {
                                                var c = r.GetName(i);
                                                if (!cols.Contains(c)) continue; // garder uniquement les colonnes connues côté réseau
                                                localValues[c] = r.IsDBNull(i) ? null : r.GetValue(i);
                                            }
                                        }
                                    }
                                }

                                // 2) Appliquer sur réseau
                                if (op == "DELETE")
                                {
                                    // Soft-delete si possible
                                    bool hasIsDeleted = cols.Contains(_syncConfig.IsDeletedColumn);
                                    bool hasDeleteDate = cols.Contains("DeleteDate");
                                    bool hasLastMod = cols.Contains(_syncConfig.LastModifiedColumn) || cols.Contains("LastModified");

                                    if (hasIsDeleted || hasDeleteDate)
                                    {
                                        var setParts = new List<string>();
                                        var paramCols = new List<string>();
                                        var parameters = new List<object>();
                                        if (hasIsDeleted) setParts.Add($"[{_syncConfig.IsDeletedColumn}] = true");
                                        if (hasDeleteDate) { setParts.Add("[DeleteDate] = @p0"); parameters.Add(DateTime.UtcNow); paramCols.Add("DeleteDate"); }
                                        if (hasLastMod)
                                        {
                                            var col = cols.Contains(_syncConfig.LastModifiedColumn) ? _syncConfig.LastModifiedColumn : "LastModified";
                                            setParts.Add($"[{col}] = @p1"); parameters.Add(DateTime.UtcNow); paramCols.Add(col);
                                        }
                                        using (var cmd = new OleDbCommand($"UPDATE [{table}] SET {string.Join(", ", setParts)} WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            for (int i = 0; i < parameters.Count; i++) addParam(cmd, $"@p{i}", parameters[i], i < paramCols.Count ? paramCols[i] : null, typeMap);
                                            addParam(cmd, "@key", localPkVal, pkCol, typeMap);
                                            await execWithRetryAsync(cmd);
                                        }
                                    }
                                    else
                                    {
                                        using (var cmd = new OleDbCommand($"DELETE FROM [{table}] WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            addParam(cmd, "@key", localPkVal, pkCol, typeMap);
                                            await execWithRetryAsync(cmd);
                                        }
                                    }
                                    appliedIds.Add(entry.Id);
                                }
                                else // INSERT / UPDATE
                                {
                                    if (localValues == null)
                                    {
                                        // Rien à appliquer pour cette entrée (probablement créée ailleurs) -> ignorer
                                        continue;
                                    }

                                    // Existence sur réseau
                                    int exists;
                                    using (var exCmd = new OleDbCommand($"SELECT COUNT(*) FROM [{table}] WHERE [{pkCol}] = @key", netConn, tx))
                                    {
                                        addParam(exCmd, "@key", localPkVal, pkCol, typeMap);
                                        exists = Convert.ToInt32(await exCmd.ExecuteScalarAsync());
                                    }

                                    // Préparer listes colonnes/valeurs (exclure PK en update)
                                    var allCols = localValues.Keys.Where(c => !string.Equals(c, pkCol, StringComparison.OrdinalIgnoreCase)).ToList();

                                    if (exists > 0)
                                    {
                                        // UPDATE
                                        var setParts = new List<string>();
                                        // If the table has a Version column, handle it via increment expression only (case-insensitive)
                                        bool hasVersionCol = cols != null && cols.Any(n => string.Equals(n, "Version", StringComparison.OrdinalIgnoreCase));
                                        var effectiveCols = hasVersionCol
                                            ? allCols.Where(c => !string.Equals(c, "Version", StringComparison.OrdinalIgnoreCase)).ToList()
                                            : allCols;
                                        for (int i = 0; i < effectiveCols.Count; i++) setParts.Add($"[{effectiveCols[i]}] = @p{i}");
                                        if (hasVersionCol)
                                        {
                                            setParts.Add("[Version] = [Version] + 1");
                                        }
                                        using (var up = new OleDbCommand($"UPDATE [{table}] SET {string.Join(", ", setParts)} WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            for (int i = 0; i < effectiveCols.Count; i++) addParam(up, $"@p{i}", localValues[effectiveCols[i]], effectiveCols[i], typeMap);
                                            addParam(up, "@key", localPkVal, pkCol, typeMap);
                                            await execWithRetryAsync(up);
                                        }

                                        // Mirror Version increment locally (best-effort) so local stays aligned without a pull
                                        if (hasVersionCol)
                                        {
                                            try
                                            {
                                                using (var lup = new OleDbCommand($"UPDATE [{table}] SET [Version] = [Version] + 1 WHERE [{pkCol}] = @key", localConn))
                                                {
                                                    var localTypes = await OleDbSchemaHelper.GetColumnTypesAsync(localConn, table);
                                                    addParam(lup, "@key", localPkVal, pkCol, localTypes);
                                                    await lup.ExecuteNonQueryAsync();
                                                }
                                            }
                                            catch { /* keep push resilient */ }
                                        }
                                    }
                                    else
                                    {
                                        // INSERT
                                        var insertCols = localValues.Keys.ToList();
                                        // If table has Version column and it's missing/null in localValues, set to 1 on insert
                                        bool hasVersionCol = cols.Contains("Version");
                                        if (hasVersionCol)
                                        {
                                            var lvHasVersion = localValues.ContainsKey("Version") && localValues["Version"] != null && localValues["Version"] != DBNull.Value;
                                            if (!lvHasVersion && !insertCols.Contains("Version", StringComparer.OrdinalIgnoreCase))
                                            {
                                                insertCols.Add("Version");
                                            }
                                        }
                                        var ph = string.Join(", ", insertCols.Select((c, i) => $"@p{i}"));
                                        var colList = string.Join(", ", insertCols.Select(c => $"[{c}]"));
                                        using (var ins = new OleDbCommand($"INSERT INTO [{table}] ({colList}) VALUES ({ph})", netConn, tx))
                                        {
                                            for (int i = 0; i < insertCols.Count; i++)
                                            {
                                                var colName = insertCols[i];
                                                object val;
                                                if (string.Equals(colName, "Version", StringComparison.OrdinalIgnoreCase))
                                                {
                                                    var lvHasVersion = localValues.ContainsKey("Version") && localValues["Version"] != null && localValues["Version"] != DBNull.Value;
                                                    val = lvHasVersion ? localValues["Version"] : (object)1;
                                                }
                                                else
                                                {
                                                    val = localValues.ContainsKey(colName) ? localValues[colName] : DBNull.Value;
                                                }
                                                addParam(ins, $"@p{i}", val, colName, typeMap);
                                            }
                                            await execWithRetryAsync(ins);
                                        }

                                        // If Version was added/set to 1 on network insert, mirror locally when missing
                                        if (hasVersionCol)
                                        {
                                            try
                                            {
                                                using (var lins = new OleDbCommand($"UPDATE [{table}] SET [Version] = [Version] + IIF([Version] <= 0, 1, 0) WHERE [{pkCol}] = @key", localConn))
                                                {
                                                    var localTypes = await OleDbSchemaHelper.GetColumnTypesAsync(localConn, table);
                                                    addParam(lins, "@key", localPkVal, pkCol, localTypes);
                                                    await lins.ExecuteNonQueryAsync();
                                                }
                                            }
                                            catch { /* best-effort */ }
                                        }
                                    }

                                    appliedIds.Add(entry.Id);
                                }
                            }

                            tx.Commit();
                        }
                        catch
                        {
                            try { tx.Rollback(); } catch { }
                            if (diag)
                            {
                                System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Transaction failed. Rolling back.");
                                try { LogManager.Info($"[PUSH][{countryId}] Transaction failed. Rolling back"); } catch { }
                            }
                            throw;
                        }
                    }
                }

                // Marquer uniquement les id appliqués
                if (appliedIds.Count > 0)
                {
                    await tracker.MarkChangesAsSyncedAsync(appliedIds);
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Marked {appliedIds.Count} ChangeLog entries as synced.");
                        try { LogManager.Info($"[PUSH][{countryId}] Marked {appliedIds.Count} entries as synced"); } catch { }
                    }
                }

                // Notify completion as UpToDate
                try { await RaiseSyncStateAsync(countryId, SyncStateKind.UpToDate, pendingOverride: 0); } catch { }

                if (diag)
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Push core completed. Applied={appliedIds.Count}");
                    try { LogManager.Info($"[PUSH][{countryId}] Push core completed. Applied={appliedIds.Count}"); } catch { }
                }
                return appliedIds.Count;
            }
            finally
            {
                try { globalLock?.Dispose(); } catch { }
                try { watchdogCts.Cancel(); } catch { }
                try { watchdogCts.Dispose(); } catch { }
                try { if (_pushRunIds.TryGetValue(countryId, out var id) && id == runId) _pushRunIds.TryRemove(countryId, out _); } catch { }
            }
        }
    }
}
