using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Models;
using OfflineFirstAccess.Models;
using OfflineFirstAccess.Synchronization;
using OfflineFirstAccess.Helpers;
using System.Threading;
using System.Collections.Concurrent;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.IO.Compression;
using System.Timers;
using System.Text;
using System.Globalization;

namespace RecoTool.Services
{
    /// <summary>
    /// Service de gestion des accès offline-first aux bases de données Access
    /// Gère deux types de bases :
    /// - Référentielles : chargées en mémoire une seule fois (lecture seule)
    /// - Par country : synchronisation offline-first avec OfflineFirstAccess.dll
    /// </summary>
    public class OfflineFirstService : IDisposable
    {
        #region Cache mémoire pour les référentiels
        
        // Cache singleton pour les référentiels (partagé entre toutes les instances)
        private static readonly object _referentialLock = new object();
        private static bool _referentialsLoaded = false;
        private static DateTime _referentialsLoadTime;
        
        // Collections en mémoire pour les tables référentielles
        private static List<AmbreImportField> _ambreImportFields = new List<AmbreImportField>();
        private static List<AmbreTransactionCode> _ambreTransactionCodes = new List<AmbreTransactionCode>();
        private static List<AmbreTransform> _ambreTransforms = new List<AmbreTransform>();
        private static List<Country> _countries = new List<Country>();
        private static List<UserField> _userFields = new List<UserField>();
        private static List<UserFilter> _userFilters = new List<UserFilter>();
        private static List<Param> _params = new List<Param>();

        #endregion

        #region Sync State Notifications

        public enum SyncStateKind
        {
            UpToDate,
            SyncInProgress,
            OfflinePending,
            Error
        }

        /// <summary>
        /// Effectue un File.Replace avec quelques tentatives et backoff exponentiel pour absorber les violations de partage temporaires.
        /// Ne réessaie que sur IOException likely partage/locking. Autres exceptions sont relancées immédiatement.
        /// </summary>
        private async Task FileReplaceWithRetriesAsync(string sourceFileName, string destinationFileName, string destinationBackupFileName, int maxAttempts, int initialDelayMs)
        {
            if (maxAttempts < 1) maxAttempts = 1;
            int attempt = 0;
            int delay = Math.Max(50, initialDelayMs);
            for (;;)
            {
                try
                {
                    File.Replace(sourceFileName, destinationFileName, destinationBackupFileName);
                    // Cleanup policy: we don't keep local .bak backups after a successful atomic replace
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(destinationBackupFileName) && File.Exists(destinationBackupFileName))
                        {
                            File.Delete(destinationBackupFileName);
                        }
                    }
                    catch
                    {
                        // Ignore cleanup failures; leaving a .bak is acceptable but undesirable
                    }
                    return;
                }
                catch (IOException ioex)
                {
                    attempt++;
                    // Heuristic: treat as share/lock if message hints a sharing violation or file in use
                    var msg = ioex.Message?.ToLowerInvariant() ?? string.Empty;
                    bool isSharing = msg.Contains("being used") || msg.Contains("process cannot access the file") || msg.Contains("sharing violation") || msg.Contains("used by another process");
                    if (!isSharing || attempt >= maxAttempts)
                        throw;
                    try
                    {
                        System.Diagnostics.Debug.WriteLine($"[Replace][Retry] Attempt {attempt}/{maxAttempts} failed with sharing violation. Retrying in {delay}ms...");
                        await Task.Delay(delay);
                    }
                    catch { }
                    delay = Math.Min(delay * 2, 5000);
                }
            }
        }

        private static async Task CopyFileAsync(string sourceFileName, string destinationFileName, bool overwrite, CancellationToken cancellationToken = default)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(destinationFileName) ?? string.Empty);
            var mode = overwrite ? FileMode.Create : FileMode.CreateNew;
            using (var source = new FileStream(sourceFileName, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true))
            using (var dest = new FileStream(destinationFileName, mode, FileAccess.Write, FileShare.None, 81920, useAsync: true))
            {
                await source.CopyToAsync(dest, 81920, cancellationToken).ConfigureAwait(false);
            }
        }

        public sealed class SyncStateChangedEventArgs : EventArgs
        {
            public string CountryId { get; set; }
            public SyncStateKind State { get; set; }
            public int PendingCount { get; set; }
            public Exception LastError { get; set; }
            public DateTime TimestampUtc { get; set; } = DateTime.UtcNow;
        }

        public event EventHandler<SyncStateChangedEventArgs> SyncStateChanged;

        private async Task RaiseSyncStateAsync(string countryId, SyncStateKind state, int? pendingOverride = null, Exception error = null)
        {
            try
            {
                int pending = 0;
                if (pendingOverride.HasValue)
                {
                    pending = pendingOverride.Value;
                }
                else if (!string.IsNullOrWhiteSpace(countryId))
                {
                    try { pending = await GetUnsyncedChangeCountAsync(countryId); } catch { pending = 0; }
                }

                var args = new SyncStateChangedEventArgs
                {
                    CountryId = countryId,
                    State = state,
                    PendingCount = pending,
                    LastError = error,
                    TimestampUtc = DateTime.UtcNow
                };
                SyncStateChanged?.Invoke(this, args);
            }
            catch { /* never throw from event raise */ }
        }

        #endregion

        #region Background Push Control

        // Prevent overlapping background pushes and storm of triggers
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _pushSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, DateTime> _lastPushTimesUtc = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private static readonly TimeSpan _pushCooldown = TimeSpan.FromSeconds(5);
        // Diagnostic: track current stage per country push
        private static readonly ConcurrentDictionary<string, string> _pushStages = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Diagnostic: per-run identifier to avoid stale watchdog logs
        private static readonly ConcurrentDictionary<string, Guid> _pushRunIds = new ConcurrentDictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Local ChangeLog Feature Flag

        // When true, ChangeLog is stored in the LOCAL database instead of the Control/Lock DB on the network.
        // This guarantees durable pending-changes metadata when offline.
        private readonly bool _useLocalChangeLog = true;

        private string GetChangeLogConnectionString(string countryId)
        {
            // Always use a dedicated LOCAL ChangeLog database per country for durability
            var path = GetLocalChangeLogDbPath(countryId);
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};";
        }

        /// <summary>
        /// Returns the full path to the dedicated local ChangeLog database for a country.
        /// Example: DataDirectory\\ChangeLog_{countryId}.accdb
        /// </summary>
        private string GetLocalChangeLogDbPath(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId is required", nameof(countryId));
            string dataDirectory = GetParameter("DataDirectory");
            if (string.IsNullOrWhiteSpace(dataDirectory))
                throw new InvalidOperationException("Paramètre DataDirectory manquant (T_Param)");
            string fileName = $"ChangeLog_{countryId}.accdb";
            return Path.Combine(dataDirectory, fileName);
        }

        // Returns true if the local ChangeLog contains unsynchronized entries
        private async Task<bool> HasUnsyncedLocalChangesAsync(string countryId)
        {
            if (!_useLocalChangeLog) return false;
            if (string.IsNullOrWhiteSpace(countryId)) return false;
            try
            {
                var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));
                var list = await tracker.GetUnsyncedChangesAsync();
                return list != null && list.Any();
            }
            catch { return false; }
        }

        /// <summary>
        /// Exécute une poussée initiale des changements locaux en attente pour tous les pays connus
        /// (ou au minimum le pays courant) si le réseau est disponible. Best-effort, parallelisé légèrement.
        /// À appeler au démarrage de l'application, avant toute synchronisation.
        /// </summary>
        public async Task RunStartupPushAsync(CancellationToken token = default)
        {
            try
            {
                if (!_useLocalChangeLog) return; // uniquement pertinent si ChangeLog est local
                if (!IsNetworkSyncAvailable) return;

                // Ne pas itérer sur tous les pays: ne traiter que le pays courant
                var cid = _currentCountryId;
                if (string.IsNullOrWhiteSpace(cid)) return;
                if (token.IsCancellationRequested) return;
                try
                {
                    await PushReconciliationIfPendingAsync(cid);
                }
                catch { /* best-effort */ }
            }
            catch { /* best-effort */ }
        }

        #endregion

        #region Per-country Sync Gate

        // Serialize SynchronizeAsync per country to avoid overlapping syncs
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _syncSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
        // Coalesce concurrent SynchronizeAsync calls: if one is already running, return the same Task
        private static readonly ConcurrentDictionary<string, Task<SyncResult>> _activeSyncs = new ConcurrentDictionary<string, Task<SyncResult>>(StringComparer.OrdinalIgnoreCase);
        // Debounce background sync requests per country
        private static readonly ConcurrentDictionary<string, DateTime> _lastBgSyncRequestUtc = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Per-country Push Gate

        // Reuse _pushSemaphores declared in Background Push Control region
        // Coalesce concurrent PushPendingChangesToNetworkAsync calls: if one is already running, return the same Task<int>
        private static readonly ConcurrentDictionary<string, Task<int>> _activePushes = new ConcurrentDictionary<string, Task<int>>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Background Sync Scheduler

        /// <summary>
        /// Schedule a background synchronization if conditions are met.
        /// - Optional debounce via minInterval
        /// - Optionally only if there are pending local changes
        /// Uses the coalesced SynchronizeAsync internally, so multiple concurrent calls will share the same work.
        /// </summary>
        public async Task ScheduleSyncIfNeededAsync(string countryId, TimeSpan? minInterval = null, bool onlyIfPending = true, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;
            if (!IsInitialized) return;
            if (!IsNetworkSyncAvailable) return;

            // Debounce
            var now = DateTime.UtcNow;
            var cooldown = minInterval ?? TimeSpan.FromMilliseconds(500);
            var last = _lastBgSyncRequestUtc.GetOrAdd(countryId, DateTime.MinValue);
            if (now - last < cooldown)
            {
                return; // too soon
            }

            // Check pending only if requested
            if (onlyIfPending)
            {
                try
                {
                    var pending = await GetUnsyncedChangeCountAsync(countryId).ConfigureAwait(false);
                    if (pending <= 0)
                    {
                        return; // nothing to do
                    }
                }
                catch (Exception ex)
                {
                    // Don't block scheduling on a transient count error; log and continue
                    try { LogManager.Warn($"[BG-SYNC] Unable to query pending count for {countryId}: {ex.Message}"); } catch { }
                }
            }

            _lastBgSyncRequestUtc[countryId] = now;

            // Enqueue background sync work instead of spinning a separate thread
            BackgroundTaskQueue.Instance.Enqueue(async () =>
            {
                try
                {
                    await SynchronizeAsync(countryId, cancellationToken, null).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    try { LogManager.Error($"[BG-SYNC] Background synchronization failed for {countryId}: {ex}", ex); } catch { }
                }
            });
        }

        #endregion

        #region Lock Helpers

        private string GetRemoteLockConnectionString(string countryId)
        {
            // Prefer a Control DB per country if configured; fallback to legacy per-country lock file next to data DBs
            var perCountryControl = GetControlDbPath(countryId);
            if (!string.IsNullOrWhiteSpace(perCountryControl))
                return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={perCountryControl};";

            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string lockPath = Path.Combine(remoteDir, $"{countryDatabasePrefix}{countryId}_lock.accdb");
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={lockPath};";
        }

        /// <summary>
        /// Returns the absolute path to the remote lock/control database for the given country.
        /// Mirrors <see cref="GetRemoteLockConnectionString"/> logic but returns a file path instead of a connection string.
        /// </summary>
        private string GetRemoteLockDbPath(string countryId)
        {
            var perCountryControl = GetControlDbPath(countryId);
            if (!string.IsNullOrWhiteSpace(perCountryControl))
                return perCountryControl;

            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{countryDatabasePrefix}{countryId}_lock.accdb");
        }

        /// <summary>
        /// S'assure que les instantanés locaux AMBRE et DW sont à jour versus le réseau.
        /// Copie réseau -> local si différence détectée. Lecture seule; sans lock global.
        /// </summary>
        public Task EnsureLocalSnapshotsUpToDateAsync(string countryId)
        {
            return EnsureLocalSnapshotsUpToDateAsync(countryId, null);
        }

        /// <summary>
        /// Variante avec reporting de progression.
        /// </summary>
        public async Task EnsureLocalSnapshotsUpToDateAsync(string countryId, Action<int, string> onProgress)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId requis", nameof(countryId));
            // Safety: only operate on the currently selected country to avoid cross-country copies
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            onProgress?.Invoke(0, "Vérification des instantanés locaux...");

            // Helper local pour comparer et copier si besoin
            async Task CopyIfDifferentAsync(string networkPath, string localPath)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(networkPath) || string.IsNullOrWhiteSpace(localPath)) return;
                    if (!File.Exists(networkPath)) return; // rien à copier

                    var netFi = new FileInfo(networkPath);
                    var locFi = new FileInfo(localPath);
                    bool needCopy = !locFi.Exists || !FilesAreEqual(locFi, netFi);
                    if (needCopy)
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(localPath) ?? string.Empty);
                        // Copie atomique au mieux: copier vers temp puis replace
                        string tmp = localPath + ".tmp_copy";
                        await CopyFileAsync(networkPath, tmp, overwrite: true).ConfigureAwait(false);
                        // Remplace en conservant ACL; File.Replace nécessite un backup, sinon fallback move
                        try { await FileReplaceWithRetriesAsync(tmp, localPath, localPath + ".bak", maxAttempts: 5, initialDelayMs: 200).ConfigureAwait(false); }
                        catch
                        {
                            try { if (File.Exists(localPath)) File.Delete(localPath); } catch { }
                            File.Move(tmp, localPath);
                        }
                        // Cleanup backup best-effort
                        try { var bak = localPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }
                    }
                }
                catch { /* best-effort */ }
                await Task.CompletedTask.ConfigureAwait(false);
            }

            // AMBRE (préférez ZIP si présent)
            try
            {
                onProgress?.Invoke(10, "AMBRE: vérification ZIP/copie...");
                var netAmbreZip = GetNetworkAmbreZipPath(countryId);
                var locAmbreDb = GetLocalAmbreDbPath(countryId);
                if (!string.IsNullOrWhiteSpace(netAmbreZip) && File.Exists(netAmbreZip))
                {
                    var locZip = GetLocalAmbreZipCachePath(countryId);
                    try
                    {
                        var copied = await CopyZipIfDifferentAsync(netAmbreZip, locZip);
                        if (copied)
                        {
                            onProgress?.Invoke(25, "AMBRE: extraction en cours...");
                            await ExtractAmbreZipToLocalAsync(countryId, locZip, locAmbreDb);
                            onProgress?.Invoke(35, "AMBRE: prêt");
                        }
                        else if (!File.Exists(locAmbreDb))
                        {
                            // Première extraction
                            onProgress?.Invoke(25, "AMBRE: première extraction...");
                            await ExtractAmbreZipToLocalAsync(countryId, locZip, locAmbreDb);
                            onProgress?.Invoke(35, "AMBRE: prêt");
                        }
                    }
                    catch { }
                }
                else
                {
                    // Fallback: copie brute .accdb si aucun ZIP AMBRE côté réseau
                    var netAmbre = GetNetworkAmbreDbPath(countryId);
                    await CopyIfDifferentAsync(netAmbre, locAmbreDb);
                    onProgress?.Invoke(40, "AMBRE: prêt");
                }
            }
            catch { }

            // DWINGS (préférez ZIP si présent)
            try
            {
                onProgress?.Invoke(55, "DW: vérification ZIP/copie...");
                var netDwZip = GetNetworkDwZipPath(countryId);
                var locDwDb = GetLocalDwDbPath(countryId);
                if (!string.IsNullOrWhiteSpace(netDwZip) && File.Exists(netDwZip))
                {
                    var locZip = GetLocalDwZipCachePath(countryId);
                    try
                    {
                        var copied = await CopyZipIfDifferentAsync(netDwZip, locZip);
                        if (copied)
                        {
                            onProgress?.Invoke(70, "DW: extraction en cours...");
                            await ExtractDwZipToLocalAsync(countryId, locZip, locDwDb);
                            onProgress?.Invoke(85, "DW: prêt");
                        }
                        else if (!File.Exists(locDwDb))
                        {
                            // Première extraction si DB absente
                            onProgress?.Invoke(70, "DW: première extraction...");
                            await ExtractDwZipToLocalAsync(countryId, locZip, locDwDb);
                            onProgress?.Invoke(85, "DW: prêt");
                        }
                    }
                    catch { }
                }
                else
                {
                    var netDw = GetNetworkDwDbPath(countryId);
                    var locDw = GetLocalDwDbPath(countryId);
                    await CopyIfDifferentAsync(netDw, locDw);
                    onProgress?.Invoke(90, "DW: prêt");
                }
            }
            catch { }

            onProgress?.Invoke(100, "Instantanés locaux à jour");
        }

        /// <summary>
        /// Pousse les changements locaux de réconciliation s'il y en a, si le réseau est disponible et aucun lock global.
        /// </summary>
        public async Task<bool> PushReconciliationIfPendingAsync(string countryId)
        {
            try
            {
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
                    bool lockActive = false; try { lockActive = await IsGlobalLockActiveAsync(); } catch { lockActive = false; }
                    if (lockActive)
                    {
                        try { await RaiseSyncStateAsync(cid, SyncStateKind.OfflinePending); } catch { }
                        if (diag)
                        {
                            System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Skipped: global lock active");
                            try { LogManager.Info($"[PUSH][{cid}] Skipped: global lock active"); } catch { }
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
                using (var gl = await AcquireGlobalLockAsync(cid, "LitePush-Reconciliation", TimeSpan.FromSeconds(8)))
                {
                    if (diag)
                    {
                        System.Diagnostics.Debug.WriteLine($"[PUSH][{cid}] Global lock acquired. Invoking PushPendingChangesToNetworkAsync (Lite/T_Reconciliation)...");
                        try { LogManager.Info($"[PUSH][{cid}] Global lock acquired. Invoking PushPendingChangesToNetworkAsync (Lite/T_Reconciliation)"); } catch { }
                    }
                    await PushPendingChangesToNetworkAsync(cid, assumeLockHeld: false, source: nameof(PushReconciliationIfPendingAsync) + "/Lite", preloadedUnsynced: recoUnsynced);
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
        /// Returns the Control DB connection string. Control DB is MANDATORY for KPI snapshots and sync metadata.
        /// Tries the explicit single ControlDatabasePath first, then per-country Control DB built from
        /// ControlDatabaseDirectory/ControlDatabasePrefix (with Country* fallbacks). Throws if not resolvable.
        /// </summary>
        public string GetControlConnectionString(string countryId = null)
        {
            // Control DB uses the same Access file as the global lock database
            var cid = countryId ?? CurrentCountryId;
            return GetRemoteLockConnectionString(cid);
        }

        /// <summary>
        /// Builds the country-specific Control DB path using CountryDatabaseDirectory and an optional ControlDatabasePrefix
        /// (falls back to CountryDatabasePrefix). Returns null if not enough info to construct.
        /// </summary>
        private string GetControlDbPath(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return null;

            // Directory always comes from CountryDatabaseDirectory (single place)
            string dir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(dir))
                dir = GetParameter("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(dir)) return null;

            string prefix = GetCentralConfig("ControlDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix))
                prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix))
                prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";

            string file = $"{prefix}{countryId}.accdb";
            return Path.Combine(dir, file);
        }

        /// <summary>
        /// Ensure core Control DB schema exists: _SyncConfig and T_ConfigParameters.
        /// Safe to call multiple times.
        /// </summary>
        public async Task EnsureControlSchemaAsync()
        {
            var connStr = GetControlConnectionString(CurrentCountry?.CNT_Id);
            using (var connection = new OleDbConnection(connStr))
            {
                await connection.OpenAsync();

                // Existing tables snapshot
                var tables = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                bool Has(string name) => tables != null && tables.Rows.OfType<System.Data.DataRow>()
                    .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), name, StringComparison.OrdinalIgnoreCase));

                // _SyncConfig
                if (!Has("_SyncConfig"))
                {
                    using (var cmd = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // T_ConfigParameters removed: configuration is read from referential T_Param only

                // SyncLocks (global locking metadata)
                if (!Has("SyncLocks"))
                {
                    var sql = @"CREATE TABLE SyncLocks (
                        LockID TEXT(255) PRIMARY KEY,
                        Reason MEMO,
                        CreatedAt DATETIME,
                        ExpiresAt DATETIME,
                        MachineName TEXT(100),
                        ProcessId LONG,
                        SyncStatus TEXT(50)
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // ImportRuns (import auditing)
                if (!Has("ImportRuns"))
                {
                    var sql = @"CREATE TABLE ImportRuns (
                        Id COUNTER PRIMARY KEY,
                        CountryId TEXT(50),
                        Source TEXT(255),
                        StartedAtUtc DATETIME,
                        CompletedAtUtc DATETIME,
                        Status TEXT(50),
                        Message MEMO,
                        Version TEXT(255)
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // SystemVersion (app/control DB versioning)
                if (!Has("SystemVersion"))
                {
                    var sql = @"CREATE TABLE SystemVersion (
                        Id COUNTER PRIMARY KEY,
                        Component TEXT(100),
                        Version TEXT(50),
                        AppliedAtUtc DATETIME
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// Reads configuration from referential table T_Param only.
        /// Returns fallback if key is missing or empty.
        /// </summary>
        private string GetCentralConfig(string key, string fallback = null)
        {
            if (string.IsNullOrWhiteSpace(key)) return fallback;
            var tparam = GetParameter(key);
            return string.IsNullOrWhiteSpace(tparam) ? fallback : tparam;
        }

        /// <summary>
        /// Returns network path for Ambre DB for a country.
        /// Directory comes from CountryDatabaseDirectory; prefix can use AmbreDatabasePrefix with fallback to CountryDatabasePrefix.
        /// </summary>
        private string GetNetworkAmbreDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns network path for Reconciliation DB for a country.
        /// Directory comes from CountryDatabaseDirectory; prefix uses CountryDatabasePrefix.
        /// </summary>
        private string GetNetworkReconciliationDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns local path for Ambre DB for a country. Uses DataDirectory + Ambre prefix fallback.
        /// </summary>
        private string GetLocalAmbreDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns network path for Ambre ZIP for a country. Uses same prefix logic as Ambre DB and appends .zip
        /// </summary>
        private string GetNetworkAmbreZipPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.zip");
        }

        /// <summary>
        /// Returns local cached ZIP path for Ambre content. Uses same prefix logic and appends .zip
        /// </summary>
        private string GetLocalAmbreZipCachePath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.zip");
        }

        /// <summary>
        /// Vérifie si le ZIP AMBRE local correspond au ZIP réseau (comparaison taille et contenu).
        /// Renvoie true si le ZIP réseau est absent (rien à comparer) ou si les deux existent et correspondent, sinon false.
        /// </summary>
        public Task<bool> IsLocalAmbreZipInSyncWithNetworkAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            try
            {
                string networkZip = GetNetworkAmbreZipPath(countryId);
                if (string.IsNullOrWhiteSpace(networkZip) || !File.Exists(networkZip))
                {
                    // Aucun ZIP réseau -> on considère l'état local comme à jour
                    return Task.FromResult(true);
                }

                string localZip = GetLocalAmbreZipCachePath(countryId);
                var netFi = new FileInfo(networkZip);
                var locFi = new FileInfo(localZip);

                if (!locFi.Exists) return Task.FromResult(false);

                bool same = FilesAreEqual(locFi, netFi);
                return Task.FromResult(same);
            }
            catch
            {
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Extracts the .accdb from a local Ambre ZIP cache and atomically replaces the local AMBRE DB.
        /// </summary>
        private async Task ExtractAmbreZipToLocalAsync(string countryId, string localZipPath, string localDbPath)
        {
            string dataDirectory = Path.GetDirectoryName(localDbPath) ?? GetParameter("DataDirectory");
            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);

            using (var archive = ZipFile.OpenRead(localZipPath))
            {
                var accdbEntry = archive.Entries
                    .OrderByDescending(e => e.Length)
                    .FirstOrDefault(e => e.FullName.EndsWith(".accdb", StringComparison.OrdinalIgnoreCase));

                if (accdbEntry == null)
                {
                    System.Diagnostics.Debug.WriteLine($"AMBRE: Aucun .accdb dans l'archive {localZipPath}");
                    throw new FileNotFoundException("Aucun fichier .accdb dans l'archive AMBRE", localZipPath);
                }

                string baseNameLocal = Path.GetFileNameWithoutExtension(localDbPath);
                string tempLocalFromZip = Path.Combine(dataDirectory, $"{baseNameLocal}.accdb.tmp_{Guid.NewGuid():N}");
                string backupLocalZip = Path.Combine(dataDirectory, $"{baseNameLocal}.accdb.bak");

                accdbEntry.ExtractToFile(tempLocalFromZip, true);
                if (File.Exists(localDbPath))
                    await FileReplaceWithRetriesAsync(tempLocalFromZip, localDbPath, backupLocalZip, maxAttempts: 5, initialDelayMs: 300);
                else
                    File.Move(tempLocalFromZip, localDbPath);
            }
        }

        /// <summary>
        /// Copie un ZIP depuis le réseau vers un cache local si différent (taille/contenu) de manière atomique. Renvoie true si une copie a été effectuée.
        /// </summary>
        private async Task<bool> CopyZipIfDifferentAsync(string networkZipPath, string localZipPath)
        {
            var netFi = new FileInfo(networkZipPath);
            var locFi = new FileInfo(localZipPath);
            bool needZipCopy = !locFi.Exists || !FilesAreEqual(locFi, netFi);
            if (!needZipCopy) return false;

            Directory.CreateDirectory(Path.GetDirectoryName(localZipPath) ?? string.Empty);
            string tmp = localZipPath + ".tmp_copy";
            File.Copy(networkZipPath, tmp, true);
            try { await FileReplaceWithRetriesAsync(tmp, localZipPath, localZipPath + ".bak", maxAttempts: 5, initialDelayMs: 200); }
            catch
            {
                try { if (File.Exists(localZipPath)) File.Delete(localZipPath); } catch { }
                File.Move(tmp, localZipPath);
            }
            try { var bak = localZipPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }
            return true;
        }

        private static bool FilesAreEqual(FileInfo first, FileInfo second)
        {
            try
            {
                if (!first.Exists || !second.Exists) return false;
                if (first.Length != second.Length) return false;
                return first.LastWriteTimeUtc.Date == second.LastWriteTimeUtc.Date;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Retourne le chemin du ZIP DW réseau le plus pertinent pour un pays (le plus récent contenant le pays et "DW/DWINGS"). Peut renvoyer null.
        /// </summary>
        private string GetNetworkDwZipPath(string countryId)
        {
            string networkDbPath = GetNetworkDwDbPath(countryId);
            string remoteDir = Path.GetDirectoryName(networkDbPath);
            if (string.IsNullOrWhiteSpace(remoteDir) || !Directory.Exists(remoteDir)) return null;

            try
            {
                var candidates = Directory.EnumerateFiles(remoteDir, "*.zip", SearchOption.TopDirectoryOnly)
                    .Where(f => f.IndexOf(countryId, StringComparison.OrdinalIgnoreCase) >= 0)
                    .Where(f =>
                    {
                        var n = Path.GetFileName(f);
                        return n.IndexOf("DW", StringComparison.OrdinalIgnoreCase) >= 0
                               || n.IndexOf("DWINGS", StringComparison.OrdinalIgnoreCase) >= 0;
                    });
                var best = candidates
                    .Select(f => new FileInfo(f))
                    .OrderByDescending(fi => fi.LastWriteTimeUtc)
                    .FirstOrDefault();
                return best?.FullName;
            }
            catch { return null; }
        }

        /// <summary>
        /// Retourne le chemin du cache local ZIP DW (nom stable).
        /// </summary>
        private string GetLocalDwZipCachePath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}_DW.zip");
        }

        /// <summary>
        /// Vérifie si le ZIP DW local correspond au ZIP DW réseau (taille/contenu). True si pas de ZIP réseau ou si identiques.
        /// </summary>
        public Task<bool> IsLocalDwZipInSyncWithNetworkAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            try
            {
                string networkZip = GetNetworkDwZipPath(countryId);
                if (string.IsNullOrWhiteSpace(networkZip) || !File.Exists(networkZip))
                {
                    return Task.FromResult(true);
                }
                string localZip = GetLocalDwZipCachePath(countryId);
                var netFi = new FileInfo(networkZip);
                var locFi = new FileInfo(localZip);
                if (!locFi.Exists) return Task.FromResult(false);
                bool same = FilesAreEqual(locFi, netFi);
                return Task.FromResult(same);
            }
            catch { return Task.FromResult(false); }
        }

        /// <summary>
        /// Diagnostics détaillés sur l'état de synchronisation ZIP AMBRE (réseau vs cache local).
        /// </summary>
        public string GetAmbreZipDiagnostics(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return "countryId manquant";
            try
            {
                var net = GetNetworkAmbreZipPath(countryId);
                var loc = GetLocalAmbreZipCachePath(countryId);
                var sb = new System.Text.StringBuilder();
                sb.AppendLine($"Network ZIP: {(string.IsNullOrWhiteSpace(net) ? "(introuvable)" : net)}");
                if (!string.IsNullOrWhiteSpace(net) && File.Exists(net))
                {
                    var fi = new FileInfo(net);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                sb.AppendLine($"Local ZIP: {(string.IsNullOrWhiteSpace(loc) ? "(introuvable)" : loc)}");
                if (!string.IsNullOrWhiteSpace(loc) && File.Exists(loc))
                {
                    var fi = new FileInfo(loc);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                return $"Diagnostics AMBRE indisponibles: {ex.Message}";
            }
        }

        /// <summary>
        /// Diagnostics détaillés sur l'état de synchronisation ZIP DW (réseau vs cache local).
        /// </summary>
        public string GetDwZipDiagnostics(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return "countryId manquant";
            try
            {
                var net = GetNetworkDwZipPath(countryId);
                var loc = GetLocalDwZipCachePath(countryId);
                var sb = new System.Text.StringBuilder();
                sb.AppendLine($"Network ZIP: {(string.IsNullOrWhiteSpace(net) ? "(introuvable)" : net)}");
                if (!string.IsNullOrWhiteSpace(net) && File.Exists(net))
                {
                    var fi = new FileInfo(net);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                sb.AppendLine($"Local ZIP: {(string.IsNullOrWhiteSpace(loc) ? "(introuvable)" : loc)}");
                if (!string.IsNullOrWhiteSpace(loc) && File.Exists(loc))
                {
                    var fi = new FileInfo(loc);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                return $"Diagnostics DW indisponibles: {ex.Message}";
            }
        }

        /// <summary>
        /// Extrait le .accdb depuis un ZIP DW local et remplace atomiquement la base DW locale.
        /// </summary>
        private async Task ExtractDwZipToLocalAsync(string countryId, string localZipPath, string localDbPath)
        {
            string dataDirectory = Path.GetDirectoryName(localDbPath) ?? GetParameter("DataDirectory");
            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);
            using (var archive = ZipFile.OpenRead(localZipPath))
            {
                // Prefer explicit DW_Data.accdb if present (new unified DW format)
                var accdbEntry = archive.Entries.FirstOrDefault(e => string.Equals(Path.GetFileName(e.FullName), "DW_Data.accdb", StringComparison.OrdinalIgnoreCase));
                if (accdbEntry == null)
                {
                    // Fallback: pick the largest .accdb (legacy zips with multiple databases)
                    accdbEntry = archive.Entries
                        .Where(e => e.FullName.EndsWith(".accdb", StringComparison.OrdinalIgnoreCase))
                        .OrderByDescending(e => e.Length)
                        .FirstOrDefault();
                }
                if (accdbEntry == null)
                {
                    System.Diagnostics.Debug.WriteLine($"DW: Aucun .accdb dans l'archive {localZipPath}");
                    throw new FileNotFoundException("Aucun fichier .accdb dans l'archive DW", localZipPath);
                }
                string baseNameLocal = Path.GetFileNameWithoutExtension(localDbPath);
                string tempLocalFromZip = Path.Combine(dataDirectory, $"{baseNameLocal}.accdb.tmp_{Guid.NewGuid():N}");
                string backupLocalZip = Path.Combine(dataDirectory, $"{baseNameLocal}.accdb.bak");
                accdbEntry.ExtractToFile(tempLocalFromZip, true);
                if (File.Exists(localDbPath))
                    await FileReplaceWithRetriesAsync(tempLocalFromZip, localDbPath, backupLocalZip, maxAttempts: 5, initialDelayMs: 300);
                else
                    File.Move(tempLocalFromZip, localDbPath);
            }
        }

        /// <summary>
        /// Public accessor for the local Ambre database path for a given country (or current country if null).
        /// </summary>
        public string GetLocalAmbreDatabasePath(string countryId = null)
        {
            var cid = string.IsNullOrWhiteSpace(countryId) ? _currentCountryId : countryId;
            if (string.IsNullOrWhiteSpace(cid)) return null;
            try { return GetLocalAmbreDbPath(cid); } catch { return null; }
        }

        /// <summary>
        /// Returns local path for Reconciliation DB for a country. Uses DataDirectory + Recon prefix fallback.
        /// </summary>
        private string GetLocalReconciliationDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns network path for DWINGS (DW) DB for a country.
        /// Directory comes from CountryDatabaseDirectory; prefix uses DWDatabasePrefix with fallback to CountryDatabasePrefix.
        /// </summary>
        private string GetNetworkDwDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns local path for DWINGS (DW) DB for a country. Uses DataDirectory + DW prefix fallback.
        /// </summary>
        private string GetLocalDwDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Tente un Compact & Repair de la base Access en utilisant Access.Application (COM) en late-binding.
        /// Retourne le chemin du fichier compacté si succès, sinon null. Best-effort: ne jette pas en cas d'absence d'Access.
        /// </summary>
        /// <param name="sourcePath">Chemin de la base source (.accdb)</param>
        /// <returns>Chemin du fichier compacté temporaire, ou null en cas d'échec</returns>
        private async Task<string> TryCompactAccessDatabaseAsync(string sourcePath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(sourcePath) || !File.Exists(sourcePath)) return null;
                string dir = Path.GetDirectoryName(sourcePath);
                string nameNoExt = Path.GetFileNameWithoutExtension(sourcePath);
                string tempCompact = Path.Combine(dir ?? "", $"{nameNoExt}.compact_{Guid.NewGuid():N}.accdb");

                return await Task.Run(() =>
                {
                    try
                    {
                        // Late-bind Access.Application to avoid adding COM references
                        var accType = Type.GetTypeFromProgID("Access.Application");
                        if (accType == null) return null;
                        dynamic app = Activator.CreateInstance(accType);
                        try
                        {
                            // Some versions return bool, some void; rely on file existence afterwards
                            try { var _ = app.CompactRepair(sourcePath, tempCompact, true); }
                            catch { app.CompactRepair(sourcePath, tempCompact, true); }

                            return File.Exists(tempCompact) ? tempCompact : null;
                        }
                        finally
                        {
                            try { app.Quit(); } catch { }
                        }
                    }
                    catch
                    {
                        return null;
                    }
                });
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Deletes all synchronized ChangeLog entries from the control/lock database and then attempts a Compact & Repair.
        /// Safe to call multiple times. Should be called while holding the global lock to avoid external access.
        /// </summary>
        public async Task CleanupChangeLogAndCompactAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // 1) Delete synchronized rows from ChangeLog (best-effort if table exists)
            try
            {
                var connStr = GetRemoteLockConnectionString(countryId);
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync();

                    // Verify table exists before deleting
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool hasChangeLog = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), "ChangeLog", StringComparison.OrdinalIgnoreCase));
                    if (hasChangeLog)
                    {
                        using (var cmd = new OleDbCommand("DELETE FROM ChangeLog WHERE Synchronized = TRUE", connection))
                        {
                            try { await cmd.ExecuteNonQueryAsync(); } catch { /* ignore delete errors */ }
                        }
                    }
                }
            }
            catch { /* best-effort cleanup */ }

            // 2) Compact & Repair the lock/control database to reclaim space
            try
            {
                var dbPath = GetRemoteLockDbPath(countryId);
                var compacted = await TryCompactAccessDatabaseAsync(dbPath);
                if (!string.IsNullOrWhiteSpace(compacted) && File.Exists(compacted))
                {
                    try
                    {
                        await FileReplaceWithRetriesAsync(compacted, dbPath, dbPath + ".bak", maxAttempts: 6, initialDelayMs: 300);
                    }
                    catch
                    {
                        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
                        File.Move(compacted, dbPath);
                    }
                    // Cleanup backup if present
                    try { var bak = dbPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }
                }
            }
            catch { /* ignore compaction errors */ }
        }

        /// <summary>
        /// Chaîne de connexion vers la base locale d'un pays donné.
        /// Ne nécessite pas que le pays soit le courant.
        /// </summary>
        public string GetCountryConnectionString(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            string dataDirectory = GetParameter("DataDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            if (string.IsNullOrWhiteSpace(dataDirectory))
                throw new InvalidOperationException("Paramètre DataDirectory manquant (T_Param)");
            string localDbPath = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb");
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={localDbPath};";
        }

        /// <summary>
        /// Met à jour explicitement l'ancre LastSyncTimestamp dans la base locale du pays donné.
        /// Utilise un format ISO 8601 (Round-trip) pour robustesse.
        /// </summary>
        public async Task SetLastSyncAnchorAsync(string countryId, DateTime utcNow)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            string iso = utcNow.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            // 1) Tentative sur la base de contrôle (centralisée) — best effort, ne bloque pas la suite
            try
            {
                var controlConnStr = GetControlConnectionString(countryId);
                using (var connection = new OleDbConnection(controlConnStr))
                {
                    await connection.OpenAsync();

                    // Assurer le schéma de contrôle si nécessaire
                    try { await EnsureControlSchemaAsync(); } catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"[SYNC][WARN] EnsureControlSchemaAsync a échoué: {ex.Message}");
                    }

                    // S'assurer que la table _SyncConfig existe
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool tableExists = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(r["TABLE_NAME"].ToString(), "_SyncConfig", StringComparison.OrdinalIgnoreCase));
                    if (!tableExists)
                    {
                        using (var create = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection))
                        {
                            await create.ExecuteNonQueryAsync();
                        }
                    }

                    using (var update = new OleDbCommand("UPDATE _SyncConfig SET ConfigValue = @val WHERE ConfigKey = 'LastSyncTimestamp'", connection))
                    {
                        update.Parameters.Add(new OleDbParameter("@val", OleDbType.LongVarWChar) { Value = (object)iso ?? DBNull.Value });
                        int rows = await update.ExecuteNonQueryAsync();
                        if (rows == 0)
                        {
                            using (var insert = new OleDbCommand("INSERT INTO _SyncConfig (ConfigKey, ConfigValue) VALUES ('LastSyncTimestamp', @val)", connection))
                            {
                                insert.Parameters.Add(new OleDbParameter("@val", OleDbType.LongVarWChar) { Value = (object)iso ?? DBNull.Value });
                                await insert.ExecuteNonQueryAsync();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC][WARN] Echec mise à jour LastSyncTimestamp sur la base de contrôle ({countryId}): {ex.Message}. Tentative sur la base locale.");
            }

            // 2) Toujours écrire l'ancre dans la base LOCALE du pays (source pour le SyncOrchestrator)
            try
            {
                var localConnStr = GetCountryConnectionString(countryId);
                using (var connection = new OleDbConnection(localConnStr))
                {
                    await connection.OpenAsync();

                    // S'assurer que la table _SyncConfig existe côté local
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool tableExists = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(r["TABLE_NAME"].ToString(), "_SyncConfig", StringComparison.OrdinalIgnoreCase));
                    if (!tableExists)
                    {
                        using (var create = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection))
                        {
                            await create.ExecuteNonQueryAsync();
                        }
                    }

                    using (var update = new OleDbCommand("UPDATE _SyncConfig SET ConfigValue = @val WHERE ConfigKey = 'LastSyncTimestamp'", connection))
                    {
                        update.Parameters.Add(new OleDbParameter("@val", OleDbType.LongVarWChar) { Value = (object)iso ?? DBNull.Value });
                        int rows = await update.ExecuteNonQueryAsync();
                        if (rows == 0)
                        {
                            using (var insert = new OleDbCommand("INSERT INTO _SyncConfig (ConfigKey, ConfigValue) VALUES ('LastSyncTimestamp', @val)", connection))
                            {
                                insert.Parameters.Add(new OleDbParameter("@val", OleDbType.LongVarWChar) { Value = (object)iso ?? DBNull.Value });
                                await insert.ExecuteNonQueryAsync();
                            }
                        }
                    }
                }
                System.Diagnostics.Debug.WriteLine($"[SYNC][INFO] LastSyncTimestamp stocké côté LOCAL pour {countryId} (fallback).");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC][ERROR] Impossible de stocker LastSyncTimestamp (control et local ont échoué) pour {countryId}: {ex.Message}");
                throw; // remonter l'erreur pour que l'appelant puisse informer l'utilisateur
            }
        }

        /// <summary>
        /// Applique en lot des ajouts/mises à jour/archivages dans une seule connexion et transaction.
        /// Réduit drastiquement le coût des imports volumineux.
        /// </summary>
        /// <param name="identifier">Country identifier.</param>
        /// <param name="toAdd">Entities to insert.</param>
        /// <param name="toUpdate">Entities to update.</param>
        /// <param name="toArchive">Entities to logically delete/archive.</param>
        /// <param name="suppressChangeLog">Quand true, n'enregistre pas les changements dans la table ChangeLog (utile pour imports Ambre).</param>
        public async Task<bool> ApplyEntitiesBatchAsync(string identifier, List<Entity> toAdd, List<Entity> toUpdate, List<Entity> toArchive, bool suppressChangeLog = false)
        {
            EnsureInitialized();
            toAdd = toAdd ?? new List<Entity>();
            toUpdate = toUpdate ?? new List<Entity>();
            toArchive = toArchive ?? new List<Entity>();

            if (toAdd.Count == 0 && toUpdate.Count == 0 && toArchive.Count == 0)
                return true;

            // Choose target DB based on involved tables.
            // If the batch exclusively targets AMBRE table, use the AMBRE local DB.
            // Otherwise use the default local (reconciliation) DB.
            var allTables = toAdd.Select(e => e.TableName)
                                 .Concat(toUpdate.Select(e => e.TableName))
                                 .Concat(toArchive.Select(e => e.TableName))
                                 .Where(t => !string.IsNullOrWhiteSpace(t))
                                 .Distinct(StringComparer.OrdinalIgnoreCase)
                                 .ToList();

            string selectedConnStr;
            if (allTables.Count == 1 && string.Equals(allTables[0], "T_Data_Ambre", StringComparison.OrdinalIgnoreCase))
            {
                var ambrePath = GetLocalAmbreDbPath(identifier);
                selectedConnStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ambrePath};";
            }
            else
            {
                selectedConnStr = GetLocalConnectionString();
            }

            using (var connection = new OleDbConnection(selectedConnStr))
            {
                await connection.OpenAsync();
                using (var tx = connection.BeginTransaction())
                {
                    var changeTuples = new List<(string TableName, string RecordId, string OperationType)>();
                    // Caches must be declared outside try to be visible in finally
                    var tableColsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                    var colTypeCache = new Dictionary<string, Dictionary<string, OleDbType>>(StringComparer.OrdinalIgnoreCase);
                    var archiveCmdCache = new Dictionary<string, OleDbCommand>(StringComparer.OrdinalIgnoreCase);
                    var insertCmdCache = new Dictionary<string, (OleDbCommand Cmd, List<string> Cols)>(StringComparer.OrdinalIgnoreCase);
                    var updateCmdCache = new Dictionary<string, (OleDbCommand Cmd, List<string> Cols, int KeyIndex)>(StringComparer.OrdinalIgnoreCase);
                    var pkColCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        // Use a single batch timestamp to reduce DateTime.UtcNow calls
                        var nowUtc = DateTime.UtcNow;
                        // caches declared above

                        Func<string, Task<HashSet<string>>> getColsAsync = async (table) =>
                        {
                            if (!tableColsCache.TryGetValue(table, out var cols))
                            {
                                cols = await GetTableColumnsAsync(connection, table);
                                tableColsCache[table] = cols;
                            }
                            return cols;
                        };

                        Func<string, Task<string>> getPkColAsync = async (table) =>
                        {
                            if (pkColCache.TryGetValue(table, out var pk)) return pk;
                            pk = await GetPrimaryKeyColumnAsync(connection, table) ?? "ID";
                            pkColCache[table] = pk;
                            return pk;
                        };

                        // No RowGuid usage anymore; rely on primary key

                        // INSERTS
                        foreach (var entity in toAdd)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            var isDeletedCol = cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.IsDeletedColumn : (cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase) ? "DeleteDate" : null);

                            if (lastModCol != null)
                                entity.Properties[lastModCol] = nowUtc;
                            if (isDeletedCol != null)
                            {
                                if (isDeletedCol.Equals(_syncConfig.IsDeletedColumn, StringComparison.OrdinalIgnoreCase))
                                    entity.Properties[isDeletedCol] = false;
                                else if (isDeletedCol.Equals("DeleteDate", StringComparison.OrdinalIgnoreCase))
                                    entity.Properties[isDeletedCol] = DBNull.Value;
                            }

                            // CRC on INSERT for T_Data_Ambre when CRC column exists
                            bool isAmbreInsert = string.Equals(entity.TableName, "T_Data_Ambre", StringComparison.OrdinalIgnoreCase);
                            bool hasCrcInsert = isAmbreInsert && cols.Contains("CRC", StringComparer.OrdinalIgnoreCase);
                            if (hasCrcInsert)
                            {
                                var exclude = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                {
                                    // Exclude tech columns from CRC
                                    await getPkColAsync(entity.TableName),
                                    "CRC",
                                    lastModCol ?? string.Empty,
                                    _syncConfig.IsDeletedColumn,
                                    "DeleteDate",
                                    "CreationDate",
                                    "ModifiedBy",
                                    "Version"
                                };
                                var orderedCols = cols.Where(c => !exclude.Contains(c)).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList();
                                var crcVal = (int)ComputeCrc32ForEntity(entity, orderedCols);
                                entity.Properties["CRC"] = crcVal;
                            }

                            var validCols = entity.Properties.Keys.Where(k => cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();

                            if (validCols.Count == 0) continue;
                            var sig = $"{entity.TableName}||{string.Join("|", validCols)}";
                            if (!insertCmdCache.TryGetValue(sig, out var tup))
                            {
                                var colList = string.Join(", ", validCols.Select(c => $"[{c}]"));
                                var paramList = string.Join(", ", validCols.Select((c, i) => $"@p{i}"));
                                var sql = $"INSERT INTO [{entity.TableName}] ({colList}) VALUES ({paramList})";
                                var cmd = new OleDbCommand(sql, connection, tx);
                                var typeMap = colTypeCache.TryGetValue(entity.TableName, out var tm)
                                    ? tm
                                    : (colTypeCache[entity.TableName] = await GetColumnTypesAsync(connection, entity.TableName));
                                for (int i = 0; i < validCols.Count; i++)
                                {
                                    var colName = validCols[i];
                                    if (!typeMap.TryGetValue(colName, out var t))
                                    {
                                        t = InferOleDbTypeFromValue(entity.Properties[colName]);
                                    }
                                    var p = new OleDbParameter($"@p{i}", t) { Value = DBNull.Value };
                                    cmd.Parameters.Add(p);
                                }

                                insertCmdCache[sig] = (cmd, validCols.ToList());
                                tup = insertCmdCache[sig];
                            }
                            // Set parameter values for this row
                            for (int i = 0; i < tup.Cols.Count; i++)
                            {
                                var p = tup.Cmd.Parameters[i];
                                p.Value = CoerceValueForOleDb(entity.Properties[tup.Cols[i]], p.OleDbType);
                            }
                            await tup.Cmd.ExecuteNonQueryAsync();
                            // Determine PK value for change logging: prefer provided PK else fetch last identity
                            var pkColumn = await getPkColAsync(entity.TableName);
                            object keyVal = null;
                            if (entity.Properties.ContainsKey(pkColumn) && entity.Properties[pkColumn] != null)
                            {
                                keyVal = entity.Properties[pkColumn];
                            }
                            else
                            {
                                using (var idCmd = new OleDbCommand("SELECT @@IDENTITY", connection, tx))
                                {
                                    keyVal = await idCmd.ExecuteScalarAsync();
                                }
                            }
                            string chKey = keyVal?.ToString();
                            if (!suppressChangeLog)
                                changeTuples.Add((entity.TableName, chKey, "INSERT"));
                        }

                        // Helper to format a key literal for IN clauses (Access SQL)
                        string FormatKeyLiteral(object key)
                        {
                            if (key == null || key == DBNull.Value) return null; // skip NULLs in IN
                            switch (Type.GetTypeCode(key.GetType()))
                            {
                                case TypeCode.Byte:
                                case TypeCode.SByte:
                                case TypeCode.Int16:
                                case TypeCode.UInt16:
                                case TypeCode.Int32:
                                case TypeCode.UInt32:
                                case TypeCode.Int64:
                                case TypeCode.UInt64:
                                case TypeCode.Decimal:
                                case TypeCode.Double:
                                case TypeCode.Single:
                                    return Convert.ToString(key, CultureInfo.InvariantCulture);
                                case TypeCode.Boolean:
                                    return ((bool)key) ? "1" : "0";
                                case TypeCode.DateTime:
                                    // Use #...# for dates in Access, but PKs should rarely be dates; fall back to string
                                    var ds = ((DateTime)key).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                    return $"#{ds}#";
                                default:
                                    var s = Convert.ToString(key, CultureInfo.InvariantCulture);
                                    s = (s ?? string.Empty).Replace("'", "''");
                                    return $"'{s}'";
                            }
                        }

                        // Cache business columns per table for CRC computation
                        var businessColsCache = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

                        // Prefetch CRCs per table for all keys in toUpdate to avoid per-row SELECTs
                        var dbCrcCachePerTable = new Dictionary<string, Dictionary<string, int?>>(StringComparer.OrdinalIgnoreCase);
                        if (toUpdate.Count > 0)
                        {
                            var tables = toUpdate.Select(e => e.TableName).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                            foreach (var tbl in tables)
                            {
                                var colsTbl = await getColsAsync(tbl);
                                if (!(colsTbl.Contains("CRC", StringComparer.OrdinalIgnoreCase))) continue; // only relevant when table has CRC
                                var pkTbl = await getPkColAsync(tbl);
                                // Collect keys present in this batch
                                var keys = toUpdate.Where(e => string.Equals(e.TableName, tbl, StringComparison.OrdinalIgnoreCase))
                                                   .Select(e => e.Properties.ContainsKey(pkTbl) ? e.Properties[pkTbl] : null)
                                                   .Where(k => k != null && k != DBNull.Value)
                                                   .ToList();
                                if (keys.Count == 0) continue;
                                var keyLiterals = keys.Select(k => FormatKeyLiteral(k)).Where(x => !string.IsNullOrEmpty(x)).Distinct().ToList();
                                var map = new Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase);
                                const int chunkSize = 200;
                                for (int i = 0; i < keyLiterals.Count; i += chunkSize)
                                {
                                    var chunk = keyLiterals.Skip(i).Take(chunkSize).ToList();
                                    var inList = string.Join(",", chunk);
                                    var sql = $"SELECT [{pkTbl}] AS K, [CRC] FROM [{tbl}] WHERE [{pkTbl}] IN ({inList})";
                                    using (var cmd = new OleDbCommand(sql, connection, tx))
                                    using (var reader = await cmd.ExecuteReaderAsync())
                                    {
                                        while (await reader.ReadAsync())
                                        {
                                            var k = reader["K"];
                                            var kStr = Convert.ToString(k, CultureInfo.InvariantCulture);
                                            int? cval = reader.IsDBNull(reader.GetOrdinal("CRC")) ? (int?)null : Convert.ToInt32(reader["CRC"], CultureInfo.InvariantCulture);
                                            map[kStr] = cval;
                                        }
                                    }
                                }
                                dbCrcCachePerTable[tbl] = map;
                            }
                        }

                        // UPDATES
                        foreach (var entity in toUpdate)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            if (lastModCol != null)
                                entity.Properties[lastModCol] = nowUtc;

                            var pkColumn = await getPkColAsync(entity.TableName);
                            var updatable = entity.Properties.Keys.Where(k => !string.Equals(k, pkColumn, StringComparison.OrdinalIgnoreCase) && cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                            if (updatable.Count == 0) continue;

                            // Determine key
                            string keyColumn = pkColumn;
                            if (!entity.Properties.ContainsKey(keyColumn))
                                throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                            object keyValue = entity.Properties[keyColumn];

                            // If table is T_Data_Ambre and CRC column exists, compute CRC across business fields
                            bool isAmbre = string.Equals(entity.TableName, "T_Data_Ambre", StringComparison.OrdinalIgnoreCase);
                            bool hasCrc = isAmbre && cols.Contains("CRC", StringComparer.OrdinalIgnoreCase);
                            int? crcValue = null;
                            if (hasCrc)
                            {
                                // Cache business column order per table
                                if (!businessColsCache.TryGetValue(entity.TableName, out var orderedCols))
                                {
                                    var exclude = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                    {
                                        pkColumn,
                                        "CRC",
                                        lastModCol ?? string.Empty,
                                        _syncConfig.IsDeletedColumn,
                                        "DeleteDate",
                                        "CreationDate",
                                        "ModifiedBy",
                                        "Version"
                                    };
                                    orderedCols = cols.Where(c => !exclude.Contains(c)).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList();
                                    businessColsCache[entity.TableName] = orderedCols;
                                }
                                crcValue = (int)ComputeCrc32ForEntity(entity, orderedCols);
                                // Ensure CRC is part of SET
                                if (!updatable.Contains("CRC", StringComparer.OrdinalIgnoreCase))
                                    updatable.Add("CRC");
                                entity.Properties["CRC"] = crcValue.Value;
                            }

                            // Fast-path using preloaded DB CRC map
                            if (hasCrc && dbCrcCachePerTable.TryGetValue(entity.TableName, out var tableMap))
                            {
                                var keyStr = Convert.ToString(keyValue, CultureInfo.InvariantCulture);
                                if (keyStr != null && tableMap.TryGetValue(keyStr, out var dbCrc) && crcValue.HasValue && dbCrc.HasValue && dbCrc.Value == crcValue.Value)
                                {
                                    // No business change -> skip this row
                                    continue;
                                }
                            }

                            var upSig = $"{entity.TableName}||{string.Join("|", updatable)}||{keyColumn}||{(hasCrc ? "withCrc" : "noCrc")}";
                            if (!updateCmdCache.TryGetValue(upSig, out var upd))
                            {
                                var setList = string.Join(", ", updatable.Select((c, i) => $"[{c}] = @p{i}"));
                                var sql = hasCrc
                                    ? $"UPDATE [{entity.TableName}] SET {setList} WHERE [{keyColumn}] = @key AND ([CRC] <> @crc OR [CRC] IS NULL OR @crc IS NULL)"
                                    : $"UPDATE [{entity.TableName}] SET {setList} WHERE [{keyColumn}] = @key";
                                var cmd = new OleDbCommand(sql, connection, tx);
                                var typeMap = colTypeCache.TryGetValue(entity.TableName, out var tm)
                                    ? tm
                                    : (colTypeCache[entity.TableName] = await GetColumnTypesAsync(connection, entity.TableName));
                                for (int i = 0; i < updatable.Count; i++)
                                {
                                    var colName = updatable[i];
                                    if (!typeMap.TryGetValue(colName, out var t)) t = InferOleDbTypeFromValue(entity.Properties.ContainsKey(colName) ? entity.Properties[colName] : null);
                                    var p = new OleDbParameter($"@p{i}", t) { Value = DBNull.Value };
                                    cmd.Parameters.Add(p);
                                }
                                // key parameter at the end (typed)
                                {
                                    var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                                    var pKey = new OleDbParameter("@key", keyType) { Value = DBNull.Value };
                                    cmd.Parameters.Add(pKey);
                                }
                                if (hasCrc)
                                {
                                    var crcType = typeMap.TryGetValue("CRC", out var ct) ? ct : OleDbType.Integer;
                                    var pCrc = new OleDbParameter("@crc", crcType) { Value = DBNull.Value };
                                    cmd.Parameters.Add(pCrc);
                                }
                                updateCmdCache[upSig] = (cmd, updatable.ToList(), updatable.Count);
                                upd = updateCmdCache[upSig];
                            }
                            // Set parameters for this row
                            for (int i = 0; i < upd.Cols.Count; i++)
                            {
                                var p = upd.Cmd.Parameters[i];
                                p.Value = CoerceValueForOleDb(entity.Properties[upd.Cols[i]], p.OleDbType);
                            }
                            {
                                var pKey = upd.Cmd.Parameters[upd.KeyIndex];
                                pKey.Value = CoerceValueForOleDb(keyValue, pKey.OleDbType);
                            }
                            if (hasCrc)
                            {
                                // last parameter is @crc
                                var pCrc = upd.Cmd.Parameters[upd.KeyIndex + 1];
                                pCrc.Value = crcValue.HasValue ? (object)CoerceValueForOleDb(crcValue.Value, pCrc.OleDbType) : DBNull.Value;
                            }
                            var affected = await upd.Cmd.ExecuteNonQueryAsync();
                            if (affected > 0 && !suppressChangeLog)
                            {
                                string chKey = keyValue?.ToString();
                                // Encode the exact columns updated so the sync push constructs a partial update payload
                                var opColumns = updatable ?? new List<string>();
                                var opType = $"UPDATE({string.Join(",", opColumns)})";
                                changeTuples.Add((entity.TableName, chKey, opType));
                            }
                        }

                        // ARCHIVES (logical delete)
                        foreach (var entity in toArchive)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            var hasIsDeleted = cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase);
                            var hasDeleteDate = cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase);

                            // Determine key
                            string keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                            if (!entity.Properties.ContainsKey(keyColumn))
                                throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                            object keyValue = entity.Properties[keyColumn];

                            if (hasIsDeleted || hasDeleteDate)
                            {
                                // Reuse prepared soft-delete command per table to reduce command creation overhead
                                if (!archiveCmdCache.TryGetValue(entity.TableName, out var cmd))
                                {
                                    var setParts = new List<string>();
                                    var paramNames = new List<string>();
                                    setParts.Add($"[{_syncConfig.IsDeletedColumn}] = true");
                                    if (hasDeleteDate) { setParts.Add("[DeleteDate] = @p0"); paramNames.Add("@p0"); }
                                    if (lastModCol != null) { setParts.Add($"[{lastModCol}] = @p1"); paramNames.Add("@p1"); }
                                    var sql = $"UPDATE [{entity.TableName}] SET {string.Join(", ", setParts)} WHERE [{keyColumn}] = @key";
                                    cmd = new OleDbCommand(sql, connection, tx);
                                    var typeMap = colTypeCache.TryGetValue(entity.TableName, out var tm)
                                        ? tm
                                        : (colTypeCache[entity.TableName] = await GetColumnTypesAsync(connection, entity.TableName));
                                    // Prepare parameters in fixed order if present (typed)
                                    if (hasDeleteDate)
                                    {
                                        var t = typeMap.TryGetValue("DeleteDate", out var dt) ? dt : OleDbType.Date;
                                        var p0 = new OleDbParameter("@p0", t) { Value = CoerceValueForOleDb(nowUtc, t) };
                                        cmd.Parameters.Add(p0);
                                    }
                                    if (lastModCol != null)
                                    {
                                        var t = typeMap.TryGetValue(lastModCol, out var lt) ? lt : OleDbType.Date;
                                        var p1 = new OleDbParameter("@p1", t) { Value = CoerceValueForOleDb(nowUtc, t) };
                                        cmd.Parameters.Add(p1);
                                    }
                                    {
                                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                                        var pk = new OleDbParameter("@key", keyType) { Value = DBNull.Value };
                                        cmd.Parameters.Add(pk);
                                    }
                                    archiveCmdCache[entity.TableName] = cmd;
                                }
                                // Update parameter values per row
                                int baseIndex = 0;
                                if (hasDeleteDate)
                                {
                                    var p0 = cmd.Parameters[baseIndex++];
                                    p0.Value = CoerceValueForOleDb(nowUtc, p0.OleDbType);
                                }
                                if (lastModCol != null)
                                {
                                    var p1 = cmd.Parameters[baseIndex++];
                                    p1.Value = CoerceValueForOleDb(nowUtc, p1.OleDbType);
                                }
                                var pkParam = cmd.Parameters[baseIndex];
                                pkParam.Value = CoerceValueForOleDb(keyValue, pkParam.OleDbType); // @key
                                await cmd.ExecuteNonQueryAsync();
                            }
                            else
                            {
                                var sql = $"DELETE FROM [{entity.TableName}] WHERE [{keyColumn}] = @key";
                                using (var cmd = new OleDbCommand(sql, connection, tx))
                                {
                                    var typeMap = colTypeCache.TryGetValue(entity.TableName, out var tm)
                                        ? tm
                                        : (colTypeCache[entity.TableName] = await GetColumnTypesAsync(connection, entity.TableName));
                                    var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                                    var p = new OleDbParameter("@key", keyType) { Value = CoerceValueForOleDb(keyValue, keyType) };
                                    cmd.Parameters.Add(p);
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                            string chKey = keyValue?.ToString();
                            if (!suppressChangeLog)
                                changeTuples.Add((entity.TableName, chKey, "DELETE"));
                        }

                        tx.Commit();

                        // Record change logs (local or control DB depending on flag)
                        if (!suppressChangeLog && changeTuples.Count > 0)
                        {
                            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId));
                            await tracker.RecordChangesAsync(changeTuples);
                        }
                        return true;
                    }
                    catch (Exception ex)
                    {
                        try { tx.Rollback(); } catch { }
                        throw;
                    }
                    finally
                    {
                        // Dispose cached commands
                        foreach (var kv in archiveCmdCache)
                        {
                            try { kv.Value?.Dispose(); } catch { }
                        }
                        foreach (var kv in insertCmdCache)
                        {
                            try { kv.Value.Cmd?.Dispose(); } catch { }
                        }
                        foreach (var kv in updateCmdCache)
                        {
                            try { kv.Value.Cmd?.Dispose(); } catch { }
                        }
                    }
                }
            }
        }

        private async Task EnsureSyncLocksTableExistsAsync(OleDbConnection connection)
        {
            var restrictions = new string[4];
            restrictions[2] = "SyncLocks";
            DataTable table = connection.GetSchema("Tables", restrictions);
            if (table.Rows.Count == 0)
            {
                using (var cmd = new OleDbCommand(
                    "CREATE TABLE SyncLocks (" +
                    "LockID TEXT(36) PRIMARY KEY, " +
                    "Reason TEXT, " +
                    "CreatedAt DATETIME, " +
                    "ExpiresAt DATETIME, " +
                    "MachineName TEXT, " +
                    "ProcessId INTEGER, " +
                    "SyncStatus TEXT(50))", connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            // Ensure SyncStatus column exists in any case (upgrade path)
            var colRestrictions = new string[4];
            colRestrictions[2] = "SyncLocks";
            DataTable columns = connection.GetSchema("Columns", colRestrictions);
            bool hasSyncStatus = false;
            foreach (DataRow r in columns.Rows)
            {
                var colName = r["COLUMN_NAME"]?.ToString();
                if (string.Equals(colName, "SyncStatus", StringComparison.OrdinalIgnoreCase))
                {
                    hasSyncStatus = true;
                    break;
                }
            }
            if (!hasSyncStatus)
            {
                try
                {
                    using (var alter = new OleDbCommand("ALTER TABLE SyncLocks ADD COLUMN SyncStatus TEXT(50)", connection))
                    {
                        await alter.ExecuteNonQueryAsync();
                    }
                }
                catch { /* best-effort upgrade */ }
            }
        }

        private sealed class GlobalLockHandle : IDisposable
        {
            private readonly string _connStr;
            private readonly string _lockId;
            private readonly int _expirySeconds;
            private bool _released;
            private System.Timers.Timer _heartbeat;
            private int _hbRunning; // 0 = idle, 1 = executing

            public GlobalLockHandle(string connStr, string lockId, int expirySeconds)
            {
                _connStr = connStr;
                _lockId = lockId;
                _expirySeconds = Math.Max(30, expirySeconds);
                StartHeartbeat();
            }

            private void StartHeartbeat()
            {
                try
                {
                    // Renew at half the expiry, bounded between 15s and 120s
                    int periodSec = Math.Max(15, Math.Min(120, _expirySeconds / 2));
                    _heartbeat = new System.Timers.Timer(periodSec * 1000);
                    _heartbeat.AutoReset = true;
                    _heartbeat.Elapsed += (s, e) =>
                    {
                        // Prevent overlapping ticks or post-release renewals
                        if (Volatile.Read(ref _released)) return;
                        if (Interlocked.Exchange(ref _hbRunning, 1) == 1) return;
                        try
                        {
                            var newExpiry = DateTime.UtcNow.AddSeconds(_expirySeconds);
                            using (var conn = new OleDbConnection(_connStr))
                            {
                                conn.Open();
                                using (var cmd = new OleDbCommand("UPDATE SyncLocks SET ExpiresAt = ? WHERE LockID = ?", conn))
                                {
                                    var pExpires = new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = CoerceValueForOleDb(newExpiry, OleDbType.Date) };
                                    var pLock = new OleDbParameter("@LockID", OleDbType.VarWChar, 36) { Value = (object)_lockId ?? DBNull.Value };
                                    cmd.Parameters.Add(pExpires);
                                    cmd.Parameters.Add(pLock);
                                    var corr = Guid.NewGuid();
                                    LogOleDbCommand("GlobalLockHandle.Heartbeat", cmd, corr);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                        catch { /* best-effort */ }
                        finally { Interlocked.Exchange(ref _hbRunning, 0); }
                    };
                    _heartbeat.Start();
                }
                catch { /* best-effort */ }
            }

            public void Dispose()
            {
                if (Volatile.Read(ref _released)) return;
                try
                {
                    try { _heartbeat?.Stop(); _heartbeat?.Dispose(); } catch { }
                    using (var conn = new OleDbConnection(_connStr))
                    {
                        conn.Open();
                        using (var cmd = new OleDbCommand("DELETE FROM SyncLocks WHERE LockID = ?", conn))
                        {
                            var pLock = new OleDbParameter("@LockID", OleDbType.VarWChar, 36) { Value = (object)_lockId ?? DBNull.Value };
                            cmd.Parameters.Add(pLock);
                            var corr = Guid.NewGuid();
                            LogOleDbCommand("GlobalLockHandle.Dispose", cmd, corr);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch { /* best-effort */ }
                finally { Volatile.Write(ref _released, true); }
            }
        }

        // Lightweight diagnostic helper for OleDb commands
        private static void LogOleDbCommand(string where, OleDbCommand cmd, Guid correlationId)
        {
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine($"[{correlationId}] {where} SQL: {cmd?.CommandText}");
                if (cmd != null)
                {
                    foreach (OleDbParameter p in cmd.Parameters)
                    {
                        var v = p?.Value;
                        var vs = v == null ? "<null>" : (v == DBNull.Value ? "<DBNULL>" : v.ToString());
                        sb.AppendLine($"[{correlationId}]   {p?.ParameterName} ({p?.OleDbType}): {vs}");
                    }
                }
                System.Diagnostics.Debug.WriteLine(sb.ToString());
            }
            catch { /* logging must never throw */ }
        }

        private async Task<IDisposable> AcquireGlobalLockInternalAsync(string identifier, string reason, int timeoutSeconds, CancellationToken token)
        {
            if (string.IsNullOrEmpty(_currentCountryId))
                throw new InvalidOperationException("Aucun pays courant n'est initialisé");

            // Two semantics:
            // - Wait budget: we will wait up to timeoutSeconds to acquire the lock (0 => no wait, fail fast)
            // - Expiry: to avoid deadlocks, we always set an expiration; if timeoutSeconds==0, default to 3 minutes
            int waitBudgetSeconds = Math.Max(0, timeoutSeconds);
            int expirySeconds = timeoutSeconds > 0 ? timeoutSeconds : 180; // 3 min safety default

            string connStr = GetRemoteLockConnectionString(_currentCountryId);
            DateTime deadline = DateTime.UtcNow.AddSeconds(waitBudgetSeconds);
            Exception lastError = null;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                string lockId = Guid.NewGuid().ToString();
                var now = DateTime.UtcNow;
                var expiresAt = now.AddSeconds(expirySeconds);

                try
                {
                    using (var connection = new OleDbConnection(connStr))
                    {
                        await connection.OpenAsync(token);
                        await EnsureSyncLocksTableExistsAsync(connection);

                        // Cleanup expired locks
                        using (var cleanup = new OleDbCommand("DELETE FROM SyncLocks WHERE ExpiresAt IS NOT NULL AND ExpiresAt < ?", connection))
                        {
                            cleanup.Parameters.Add(new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = now });
                            await cleanup.ExecuteNonQueryAsync();
                        }

                        // Purge stale self-locks: same machine, process no longer alive
                        try
                        {
                            using (var selectSelf = new OleDbCommand("SELECT LockID, ProcessId FROM SyncLocks WHERE (ExpiresAt IS NULL OR ExpiresAt > ?) AND MachineName = ?", connection))
                            {
                                selectSelf.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                                selectSelf.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                                using (var reader = await selectSelf.ExecuteReaderAsync())
                                {
                                    var staleIds = new List<string>();
                                    while (await reader.ReadAsync())
                                    {
                                        string id = reader["LockID"]?.ToString();
                                        int pid = 0;
                                        try { pid = Convert.ToInt32(reader["ProcessId"]); } catch { pid = 0; }
                                        bool alive = false;
                                        if (pid > 0)
                                        {
                                            try { var p = System.Diagnostics.Process.GetProcessById(pid); alive = (p != null && !p.HasExited); } catch { alive = false; }
                                        }
                                        if (!alive && !string.IsNullOrEmpty(id)) staleIds.Add(id);
                                    }
                                    reader.Close();

                                    foreach (var id in staleIds)
                                    {
                                        using (var del = new OleDbCommand("DELETE FROM SyncLocks WHERE LockID = ?", connection))
                                        {
                                            del.Parameters.Add(new OleDbParameter("@LockID", OleDbType.VarWChar, 36) { Value = (object)id ?? DBNull.Value });
                                            await del.ExecuteNonQueryAsync();
                                        }
                                    }
                                }
                            }
                        }
                        catch { /* best-effort cleanup */ }

                        // Check if a global lock is already held
                        using (var check = new OleDbCommand("SELECT COUNT(*) FROM SyncLocks WHERE ExpiresAt IS NULL OR ExpiresAt > ?", connection))
                        {
                            check.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                            var countObj = await check.ExecuteScalarAsync();
                            int active = 0;
                            if (countObj != null && countObj != DBNull.Value)
                                active = Convert.ToInt32(countObj);

                            if (active == 0)
                            {
                                // Try to acquire the lock
                                using (var command = new OleDbCommand("INSERT INTO SyncLocks (LockID, Reason, CreatedAt, ExpiresAt, MachineName, ProcessId) VALUES (?, ?, ?, ?, ?, ?)", connection))
                                {
                                    command.Parameters.Add(new OleDbParameter("@LockID", OleDbType.VarWChar, 36) { Value = lockId });
                                    command.Parameters.Add(new OleDbParameter("@Reason", OleDbType.VarWChar, 255) { Value = (object)(reason ?? "Global") ?? DBNull.Value });
                                    command.Parameters.Add(new OleDbParameter("@CreatedAt", OleDbType.Date) { Value = now });
                                    command.Parameters.Add(new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = expiresAt });
                                    command.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                                    command.Parameters.Add(new OleDbParameter("@ProcessId", OleDbType.Integer) { Value = System.Diagnostics.Process.GetCurrentProcess().Id });

                                    await command.ExecuteNonQueryAsync();
                                }

                                // Best-effort initial status
                                try
                                {
                                    using (var set = new OleDbCommand("UPDATE SyncLocks SET SyncStatus = ? WHERE LockID = ?", connection))
                                    {
                                        set.Parameters.Add(new OleDbParameter("@SyncStatus", OleDbType.VarWChar, 50) { Value = "Acquired" });
                                        set.Parameters.Add(new OleDbParameter("@LockID", OleDbType.VarWChar, 36) { Value = lockId });
                                        await set.ExecuteNonQueryAsync();
                                    }
                                }
                                catch { /* column may not exist yet */ }

                                return new GlobalLockHandle(connStr, lockId, expirySeconds);
                            }
                        }
                    }

                    // If we reach here, a lock is already held
                    if (waitBudgetSeconds == 0 || DateTime.UtcNow >= deadline)
                        throw new TimeoutException("Impossible d'acquérir le verrou global dans le délai imparti.");

                    await Task.Delay(TimeSpan.FromMilliseconds(300), token);
                    continue;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Could be race condition on insert or transient DB issue; honor wait budget
                    lastError = ex;
                    if (waitBudgetSeconds == 0 || DateTime.UtcNow >= deadline)
                        throw new InvalidOperationException("Echec de l'acquisition du verrou global.", lastError);

                    await Task.Delay(TimeSpan.FromMilliseconds(300), token);
                    continue;
                }
            }
        }

        #endregion

        /// <summary>
        /// Vérifie si un verrou global est actuellement actif pour le pays courant.
        /// Utilisé par l'IHM pour éviter des opérations réseau concurrentes (ex: import) lors d'une synchronisation.
        /// </summary>
        public async Task<bool> IsGlobalLockActiveAsync(CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(_currentCountryId))
                return false;

            try
            {
                string connStr = GetRemoteLockConnectionString(_currentCountryId);
                var now = DateTime.UtcNow;
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync(token);
                    await EnsureSyncLocksTableExistsAsync(connection);

                    // Nettoyer les verrous expirés puis vérifier l'existence d'un verrou actif
                    using (var cleanup = new OleDbCommand("DELETE FROM SyncLocks WHERE ExpiresAt IS NOT NULL AND ExpiresAt < ?", connection))
                    {
                        cleanup.Parameters.Add(new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = now });
                        await cleanup.ExecuteNonQueryAsync();
                    }

                    using (var check = new OleDbCommand("SELECT COUNT(*) FROM SyncLocks WHERE ExpiresAt IS NULL OR ExpiresAt > ?", connection))
                    {
                        check.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                        var countObj = await check.ExecuteScalarAsync();
                        int active = 0;
                        if (countObj != null && countObj != DBNull.Value)
                            active = Convert.ToInt32(countObj);
                        return active > 0;
                    }
                }
            }
            catch
            {
                // En cas d'erreur d'accès réseau, considérer qu'aucun verrou ne bloque l'IHM
                return false;
            }
        }

        /// <summary>
        /// Attend (polling) jusqu'à la libération d'un verrou global ou expiration du délai.
        /// </summary>
        public async Task<bool> WaitForGlobalLockReleaseAsync(TimeSpan pollInterval, TimeSpan timeout, CancellationToken token = default)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            while (DateTime.UtcNow < deadline)
            {
                token.ThrowIfCancellationRequested();
                var locked = await IsGlobalLockActiveAsync(token);
                if (!locked) return true;
                await Task.Delay(pollInterval, token);
            }
            return false;
        }

        /// <summary>
        /// Met à jour le champ SyncStatus pour le verrou global actif détenu par ce processus.
        /// </summary>
        public async Task SetSyncStatusAsync(string status, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(_currentCountryId)) return;
            try
            {
                string connStr = GetRemoteLockConnectionString(_currentCountryId);
                var now = DateTime.UtcNow;
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync(token);
                    await EnsureSyncLocksTableExistsAsync(connection);
                    using (var cmd = new OleDbCommand("UPDATE SyncLocks SET SyncStatus = ? WHERE MachineName = ? AND ProcessId = ? AND (ExpiresAt IS NULL OR ExpiresAt > ?)", connection))
                    {
                        cmd.Parameters.Add(new OleDbParameter("@SyncStatus", OleDbType.VarWChar, 50) { Value = (object)(status ?? "Unknown") ?? DBNull.Value });
                        cmd.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                        cmd.Parameters.Add(new OleDbParameter("@ProcessId", OleDbType.Integer) { Value = System.Diagnostics.Process.GetCurrentProcess().Id });
                        cmd.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
            }
            catch { /* best-effort */ }
        }

        /// <summary>
        /// Récupère le SyncStatus courant pour le verrou global actif de ce processus, s'il existe.
        /// </summary>
        public async Task<string> GetCurrentSyncStatusAsync(CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(_currentCountryId)) return null;
            try
            {
                string connStr = GetRemoteLockConnectionString(_currentCountryId);
                var now = DateTime.UtcNow;
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync(token);
                    await EnsureSyncLocksTableExistsAsync(connection);
                    using (var cmd = new OleDbCommand("SELECT TOP 1 SyncStatus FROM SyncLocks WHERE MachineName = ? AND ProcessId = ? AND (ExpiresAt IS NULL OR ExpiresAt > ?) ORDER BY CreatedAt DESC", connection))
                    {
                        cmd.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                        cmd.Parameters.Add(new OleDbParameter("@ProcessId", OleDbType.Integer) { Value = System.Diagnostics.Process.GetCurrentProcess().Id });
                        cmd.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                        var obj = await cmd.ExecuteScalarAsync();
                        return obj == null || obj == DBNull.Value ? null : obj.ToString();
                    }
                }
            }
            catch { return null; }
        }

        #region Configuration Properties

        // Configuration centralisée dans T_Param - plus de propriétés redondantes
        // Utilisation directe de GetParameter() pour tous les paramètres applicatifs

        #endregion

        #region Fields and Properties

        private string _ReferentialDatabasePath;

        // Nouvelle approche README: service de synchro et configuration
        private SynchronizationService _syncService;
        private SyncConfiguration _syncConfig;
        private readonly ConcurrentDictionary<string, DateTime> _lastSyncTimes;
        private readonly object _lockObject = new object();
        private bool _isInitialized = false;
        private bool _disposed = false;
        
        // Gestion d'une seule country à la fois
        private Country _currentCountry;
        private string _currentCountryId;
        private readonly string _currentUser;
        private readonly ConcurrentDictionary<string, object> _countrySyncLocks = new ConcurrentDictionary<string, object>(); // Added per-country synchronization gate

        public string ReferentialDatabasePath => _ReferentialDatabasePath;

        /// <summary>
        /// Utilisateur actuel pour le verrouillage des enregistrements
        /// </summary>
        public string CurrentUser => _currentUser;

        /// <summary>
        /// Indique si le service est initialisé
        /// </summary>
        public bool IsInitialized => _isInitialized;

        /// <summary>
        /// Indique si la synchronisation réseau est disponible (basé sur la config et l'accès au fichier distant)
        /// </summary>
        public bool IsNetworkSyncAvailable
        {
            get
            {
                try
                {
                    var remotePath = _syncConfig?.RemoteDatabasePath;
                    return !string.IsNullOrWhiteSpace(remotePath) && File.Exists(remotePath);
                }
                catch { return false; }
            }
        }

        /// <summary>
        /// Pays actuellement sélectionné (lecture seule)
        /// </summary>
        public Country CurrentCountry => _currentCountry;

        /// <summary>
        /// Identifiant du pays actuellement sélectionné (lecture seule)
        /// </summary>
        public string CurrentCountryId => _currentCountryId;

        /// <summary>
        /// Liste des pays depuis les référentiels (copie pour immutabilité côté appelant)
        /// </summary>
        public List<Country> Countries
        {
            get
            {
                lock (_referentialLock)
                {
                    return new List<Country>(_countries);
                }
            }
        }

        #endregion

        #region Constructor

        /// <summary>
        /// Constructeur par défaut
        /// </summary>
        public OfflineFirstService()
        {
            _currentUser = Environment.UserName;
            _lastSyncTimes = new ConcurrentDictionary<string, DateTime>();
            
            // Charger la configuration depuis App.config
            LoadConfiguration();
            
            // Charger les référentiels en mémoire au premier accès
            _ = LoadReferentialsAsync();

            // Feature flag from configuration (T_Param). Any non-empty, non-"false" value enables it
            var flag = GetCentralConfig("UseLocalChangeLog") ?? GetParameter("UseLocalChangeLog");
            if (!string.IsNullOrWhiteSpace(flag) && !string.Equals(flag.Trim(), "false", StringComparison.OrdinalIgnoreCase))
                _useLocalChangeLog = true;
        }

        /// <summary>
        /// Charge toutes les tables référentielles en mémoire une seule fois, de manière thread-safe.
        /// </summary>
        public async Task LoadReferentialsAsync()
        {
            // Double-checked locking pour éviter les rechargements inutiles
            if (_referentialsLoaded) return;

            List<AmbreImportField> ambreImportFields = new List<AmbreImportField>();
            List<AmbreTransactionCode> ambreTransactionCodes = new List<AmbreTransactionCode>();
            List<AmbreTransform> ambreTransforms = new List<AmbreTransform>();
            List<Country> countries = new List<Country>();
            List<UserField> userFields = new List<UserField>();
            List<UserFilter> userFilters = new List<UserFilter>();
            List<Param> parameters = new List<Param>();

            try
            {
                using (var connection = new OleDbConnection(ReferentialDatabasePath))
                {
                    await connection.OpenAsync();

                    // Helpers locaux
                    async Task LoadListAsync<T>(string sql, Func<IDataReader, T> map, List<T> target)
                    {
                        using (var cmd = new OleDbCommand(sql, connection))
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                                target.Add(map(reader));
                        }
                    }

                    await LoadListAsync(
                        "SELECT AMB_Source, AMB_Destination FROM T_Ref_Ambre_ImportFields ORDER BY AMB_Source",
                        r => new AmbreImportField
                        {
                            AMB_Source = r["AMB_Source"]?.ToString(),
                            AMB_Destination = r["AMB_Destination"]?.ToString()
                        },
                        ambreImportFields);

                    await LoadListAsync(
                        "SELECT ATC_ID, ATC_CODE, ATC_TAG FROM T_Ref_Ambre_TransactionCodes ORDER BY ATC_ID",
                        r => new AmbreTransactionCode
                        {
                            ATC_ID = Convert.ToInt32(r["ATC_ID"]),
                            ATC_CODE = r["ATC_CODE"]?.ToString(),
                            ATC_TAG = r["ATC_TAG"]?.ToString()
                        },
                        ambreTransactionCodes);

                    await LoadListAsync(
                        "SELECT AMB_Source, AMB_Destination, AMB_TransformationFunction, AMB_Description FROM T_Ref_Ambre_Transform",
                        r => new AmbreTransform
                        {
                            AMB_Source = r["AMB_Source"]?.ToString(),
                            AMB_Destination = r["AMB_Destination"]?.ToString(),
                            AMB_TransformationFunction = r["AMB_TransformationFunction"]?.ToString(),
                            AMB_Description = r["AMB_Description"]?.ToString()
                        },
                        ambreTransforms);

                    await LoadListAsync(
                        "SELECT CNT_Id, CNT_Name, CNT_AmbrePivotCountryId, CNT_AmbrePivot, CNT_AmbreReceivable, CNT_AmbreReceivableCountryId, CNT_ServiceCode, CNT_BIC FROM T_Ref_Country ORDER BY CNT_Name",
                        r => new Country
                        {
                            CNT_Id = r["CNT_Id"]?.ToString(),
                            CNT_Name = r["CNT_Name"]?.ToString(),
                            CNT_AmbrePivotCountryId = SafeInt(r["CNT_AmbrePivotCountryId"]),
                            CNT_AmbrePivot = r["CNT_AmbrePivot"]?.ToString(),
                            CNT_AmbreReceivable = r["CNT_AmbreReceivable"]?.ToString(),
                            CNT_AmbreReceivableCountryId = SafeInt(r["CNT_AmbreReceivableCountryId"]),
                            CNT_ServiceCode = r["CNT_ServiceCode"]?.ToString(),
                            CNT_BIC = r["CNT_BIC"]?.ToString()
                        },
                        countries);

                    await LoadListAsync(
                        "SELECT USR_ID, USR_Category, USR_FieldName, USR_FieldDescription, USR_Pivot, USR_Receivable, USR_Color FROM T_Ref_User_Fields ORDER BY USR_Category, USR_FieldName",
                        r => new UserField
                        {
                            USR_ID = SafeInt(r["USR_ID"]),
                            USR_Category = r["USR_Category"]?.ToString(),
                            USR_FieldName = r["USR_FieldName"]?.ToString(),
                            USR_FieldDescription = r["USR_FieldDescription"]?.ToString(),
                            USR_Pivot = SafeBool(r["USR_Pivot"]),
                            USR_Receivable = SafeBool(r["USR_Receivable"]),
                            USR_Color = r["USR_Color"]?.ToString()
                        },
                        userFields);

                    await LoadListAsync(
                        "SELECT UFI_id, UFI_Name, UFI_SQL, UFI_CreatedBy FROM T_Ref_User_Filter ORDER BY UFI_Name",
                        r => new UserFilter
                        {
                            UFI_id = SafeInt(r["UFI_id"]),
                            UFI_Name = r["UFI_Name"]?.ToString(),
                            UFI_SQL = r["UFI_SQL"]?.ToString(),
                            UFI_CreatedBy = r["UFI_CreatedBy"]?.ToString()
                        },
                        userFilters);

                    await LoadListAsync(
                        "SELECT PAR_Key, PAR_Value, PAR_Description FROM T_Param",
                        r => new Param
                        {
                            PAR_Key = r["PAR_Key"]?.ToString(),
                            PAR_Value = r["PAR_Value"]?.ToString(),
                            PAR_Description = r["PAR_Description"]?.ToString()
                        },
                        parameters);
                }

                lock (_referentialLock)
                {
                    if (_referentialsLoaded) return; // quelqu'un a déjà chargé
                    _ambreImportFields = ambreImportFields;
                    _ambreTransactionCodes = ambreTransactionCodes;
                    _ambreTransforms = ambreTransforms;
                    _countries = countries;
                    _userFields = userFields;
                    _userFilters = userFilters;
                    _params = parameters;
                    _referentialsLoaded = true;
                    _referentialsLoadTime = DateTime.UtcNow;
                }

                // Initialiser des propriétés dépendantes des paramètres (ajoute valeurs défaut si nécessaires)
                InitializePropertiesFromParams();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[REF LOAD] Erreur lors du chargement des référentiels: {ex.Message}");
                throw;
            }

            // Helpers locaux pour mapping
            static int SafeInt(object o)
            {
                try { return o == null || o == DBNull.Value ? 0 : Convert.ToInt32(o); } catch { return 0; }
            }
            static bool SafeBool(object o)
            {
                try { return o != null && o != DBNull.Value && Convert.ToBoolean(o); } catch { return false; }
            }
        }

        /// <summary>
        /// Valide l'existence des colonnes métadonnées requises sur les tables à synchroniser
        /// </summary>
        /// <param name="localDbPath">Chemin de la base locale</param>
        /// <param name="tables">Tables à vérifier</param>
        private async Task ValidateSyncTablesAsync(string localDbPath, IEnumerable<string> tables)
        {
            try
            {
                if (tables == null) return;
                using (var connection = new OleDbConnection(GetLocalConnectionString()))
                {
                    await connection.OpenAsync();
                    foreach (var table in tables)
                    {
                        if (string.IsNullOrWhiteSpace(table)) continue;
                        var required = new[] { "LastModified", "IsDeleted" };
                        var missing = new List<string>();

                        using (var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, table, null }))
                        {
                            if (schema == null)
                            {
                                System.Diagnostics.Debug.WriteLine($"[SYNC VALIDATION] Table introuvable: {table}");
                                continue;
                            }
                            var cols = new HashSet<string>(schema.Rows.Cast<System.Data.DataRow>().Select(r => r["COLUMN_NAME"].ToString()), StringComparer.OrdinalIgnoreCase);
                            foreach (var col in required)
                                if (!cols.Contains(col)) missing.Add(col);
                        }

                        if (missing.Count > 0)
                        {
                            System.Diagnostics.Debug.WriteLine($"[SYNC VALIDATION] Table {table}: colonnes manquantes -> {string.Join(", ", missing)}");
                        }
                        else
                        {
                            System.Diagnostics.Debug.WriteLine($"[SYNC VALIDATION] Table {table}: OK");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC VALIDATION] Erreur: {ex.Message}");
            }
        }
        
        // Ensure the dedicated local ChangeLog database exists and has the proper schema
        private async Task EnsureLocalChangeLogSchemaAsync(string countryId)
        {
            if (!_useLocalChangeLog) return;
            if (string.IsNullOrWhiteSpace(countryId)) return;
            try
            {
                var changeLogDbPath = GetLocalChangeLogDbPath(countryId);

                // Ensure directory exists
                var dir = Path.GetDirectoryName(changeLogDbPath);
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);

                // If DB missing, create with a minimal schema containing only ChangeLog
                if (!File.Exists(changeLogDbPath))
                {
                    await DatabaseTemplateGenerator.CreateCustomTemplateAsync(changeLogDbPath, config =>
                    {
                        config.Tables.Clear();
                        var changeLogTable = new TableConfiguration
                        {
                            Name = "ChangeLog",
                            PrimaryKeyColumn = "ChangeID",
                            PrimaryKeyType = typeof(long),
                            LastModifiedColumn = null,
                            CreateTableSql = @"CREATE TABLE [ChangeLog] (
    [ChangeID] COUNTER PRIMARY KEY,
    [TableName] TEXT(128) NOT NULL,
    [RecordID] TEXT(255),
    [Operation] TEXT(16) NOT NULL,
    [Timestamp] DATETIME NOT NULL,
    [Synchronized] BIT NOT NULL
)"
                        };
                        changeLogTable.Columns.Add(new ColumnDefinition("ChangeID", typeof(long), "LONG", false, true, true));
                        changeLogTable.Columns.Add(new ColumnDefinition("TableName", typeof(string), "TEXT(128)", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("RecordID", typeof(string), "TEXT(255)", true));
                        changeLogTable.Columns.Add(new ColumnDefinition("Operation", typeof(string), "TEXT(16)", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("Timestamp", typeof(DateTime), "DATETIME", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("Synchronized", typeof(bool), "BIT", false));
                        config.Tables.Add(changeLogTable);
                    });
                }

                // Open the dedicated ChangeLog DB and ensure columns exist (best-effort schema repair)
                using (var connection = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={changeLogDbPath};"))
                {
                    await connection.OpenAsync();

                    // Check if ChangeLog table exists
                    bool tableExists = false;
                    using (var tblSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" }))
                    {
                        if (tblSchema != null)
                        {
                            foreach (System.Data.DataRow row in tblSchema.Rows)
                            {
                                var name = row["TABLE_NAME"]?.ToString();
                                if (string.Equals(name, "ChangeLog", StringComparison.OrdinalIgnoreCase))
                                {
                                    tableExists = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (!tableExists)
                    {
                        var createSql = @"CREATE TABLE [ChangeLog] (
    [ChangeID] COUNTER PRIMARY KEY,
    [TableName] TEXT(128) NOT NULL,
    [RecordID] TEXT(255),
    [Operation] TEXT(16) NOT NULL,
    [Timestamp] DATETIME NOT NULL,
    [Synchronized] BIT NOT NULL
)";
                        using (var cmd = new OleDbCommand(createSql, connection))
                        {
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    // Ensure required columns exist (best-effort). No type migrations; deployment guarantees fresh DBs.
                    var requiredCols = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        { "ChangeID", "COUNTER" },
                        { "TableName", "TEXT(128)" },
                        { "RecordID", "TEXT(255)" },
                        { "Operation", "TEXT(16)" },
                        { "Timestamp", "DATETIME" },
                        { "Synchronized", "BIT" }
                    };

                    var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    using (var colSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, "ChangeLog", null }))
                    {
                        if (colSchema != null)
                        {
                            foreach (System.Data.DataRow row in colSchema.Rows)
                            {
                                var colName = row["COLUMN_NAME"]?.ToString();
                                if (!string.IsNullOrWhiteSpace(colName))
                                {
                                    existingCols.Add(colName);
                                }
                            }
                        }
                    }

                    foreach (var kv in requiredCols)
                    {
                        if (!existingCols.Contains(kv.Key))
                        {
                            var alter = $"ALTER TABLE [ChangeLog] ADD COLUMN [{kv.Key}] {kv.Value}";
                            try { using (var cmd = new OleDbCommand(alter, connection)) { await cmd.ExecuteNonQueryAsync(); } }
                            catch { }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[MIGRATION] EnsureLocalChangeLogSchemaAsync failed for {countryId}: {ex.Message}");
            }
        }

        /// <summary>
        /// Supprime une entité
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="entity">Entité à supprimer</param>
        /// <returns>True si supprimé avec succès</returns>
        public async Task<bool> DeleteEntityAsync(string identifier, Entity entity)
        {
            return await DeleteEntityAsync(identifier, entity, null);
        }

        /// <summary>
        /// Supprime une entité (avec session de change-log optionnelle)
        /// </summary>
        public async Task<bool> DeleteEntityAsync(string identifier, Entity entity, OfflineFirstAccess.ChangeTracking.IChangeLogSession changeLogSession)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var tableCols = await GetTableColumnsAsync(connection, entity.TableName);
                var lastModCol = tableCols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (tableCols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                var hasIsDeleted = tableCols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase);
                var hasDeleteDate = tableCols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase);

                // Determine key
                string keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                if (!entity.Properties.ContainsKey(keyColumn))
                    throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                object keyValue = entity.Properties[keyColumn];

                if (hasIsDeleted || hasDeleteDate)
                {
                    var setParts = new List<string>();
                    var parameters = new List<object>();
                    if (hasIsDeleted) { setParts.Add($"[{_syncConfig.IsDeletedColumn}] = true"); }
                    if (hasDeleteDate) { setParts.Add("[DeleteDate] = @p0"); parameters.Add(DateTime.UtcNow); }
                    if (lastModCol != null) { setParts.Add($"[{lastModCol}] = @p1"); parameters.Add(DateTime.UtcNow); }
                    var sql = $"UPDATE [{entity.TableName}] SET {string.Join(", ", setParts)} WHERE [{keyColumn}] = @key";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        var typeMap = await GetColumnTypesAsync(connection, entity.TableName);
                        if (hasDeleteDate)
                        {
                            var t = typeMap.TryGetValue("DeleteDate", out var dt) ? dt : OleDbType.Date;
                            var p0 = new OleDbParameter("@p0", t) { Value = CoerceValueForOleDb(DateTime.UtcNow, t) };
                            cmd.Parameters.Add(p0);
                        }
                        if (lastModCol != null)
                        {
                            var t = typeMap.TryGetValue(lastModCol, out var lt) ? lt : OleDbType.Date;
                            var p1 = new OleDbParameter("@p1", t) { Value = CoerceValueForOleDb(DateTime.UtcNow, t) };
                            cmd.Parameters.Add(p1);
                        }
                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                        var pKey = new OleDbParameter("@key", keyType) { Value = CoerceValueForOleDb(keyValue, keyType) };
                        cmd.Parameters.Add(pKey);
                        var affected = await cmd.ExecuteNonQueryAsync();
                        string changeKey = keyValue?.ToString();
                        if (changeLogSession != null)
                            await changeLogSession.AddAsync(entity.TableName, changeKey, "DELETE");
                        else
                            await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "DELETE");
                        return affected > 0;
                    }
                }
                else
                {
                    // Hard delete fallback
                    var sql = $"DELETE FROM [{entity.TableName}] WHERE [{keyColumn}] = @key";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        var typeMap = await GetColumnTypesAsync(connection, entity.TableName);
                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                        var pKey = new OleDbParameter("@key", keyType) { Value = CoerceValueForOleDb(keyValue, keyType) };
                        cmd.Parameters.Add(pKey);
                        var affected = await cmd.ExecuteNonQueryAsync();
                        string changeKey = keyValue?.ToString();
                        if (changeLogSession != null)
                            await changeLogSession.AddAsync(entity.TableName, changeKey, "DELETE");
                        else
                            await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "DELETE");
                        return affected > 0;
                    }
                }
            }
        }

        /// <summary>
        /// Exécute une requête SQL personnalisée
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="sql">Requête SQL</param>
        /// <param name="parameters">Paramètres</param>
        /// <returns>Résultats de la requête</returns>
        public async Task<List<Entity>> ExecuteQueryAsync(string identifier, string sql, Dictionary<string, object> parameters = null)
        {
            EnsureInitialized();
            var results = new List<Entity>();
            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    if (parameters != null)
                    {
                        int i = 0;
                        foreach (var kv in parameters)
                        {
                            var val = kv.Value;
                            if (val == null)
                            {
                                var p = new OleDbParameter($"@p{i}", OleDbType.Variant) { Value = DBNull.Value };
                                cmd.Parameters.Add(p);
                            }
                            else
                            {
                                var t = InferOleDbTypeFromValue(val);
                                var p = new OleDbParameter($"@p{i}", t) { Value = CoerceValueForOleDb(val, t) };
                                cmd.Parameters.Add(p);
                            }
                            i++;
                        }
                    }
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        // If the query selects from a single table, caller should map appropriately; we'll return row-shaped Entities
                        while (await reader.ReadAsync())
                        {
                            var ent = new Entity { TableName = "RESULT" };
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var col = reader.GetName(i);
                                var val = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                ent.Properties[col] = val;
                            }
                            results.Add(ent);
                        }
                    }
                }
            }
            return results;
        }

        /// <summary>
        /// Opens a change-log session against the remote lock database for the specified country.
        /// Call CommitAsync() to commit, otherwise Dispose() will rollback.
        /// </summary>
        public async Task<OfflineFirstAccess.ChangeTracking.IChangeLogSession> BeginChangeLogSessionAsync(string countryId)
        {
            // S'assurer que le schéma ChangeLog local existe et que Timestamp est DATETIME (migration si besoin)
            try { await EnsureLocalChangeLogSchemaAsync(countryId); } catch { /* best-effort */ }
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));
            return await tracker.BeginSessionAsync();
        }

        /// <summary>
        /// Returns the number of unsynchronized change-log entries for the specified country
        /// from the remote lock database (where ChangeLog resides).
        /// </summary>
        public async Task<int> GetUnsyncedChangeCountAsync(string countryId)
        {
            // Pas besoin d'initialisation complète: ne dépend que de la base de lock distante
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));
            var entries = await tracker.GetUnsyncedChangesAsync();
            return entries?.Count() ?? 0;
        }

        /// <summary>
        /// Acquiert un verrou global pour empêcher la synchronisation pendant des opérations critiques (TimeSpan overload).
        /// </summary>
        public async Task<IDisposable> AcquireGlobalLockAsync(string identifier, string reason, TimeSpan timeout, CancellationToken token = default)
        {
            int timeoutSeconds = (int)Math.Max(0, timeout.TotalSeconds);
            return await AcquireGlobalLockInternalAsync(identifier, reason, timeoutSeconds, token);
        }

        private void EnsureInitialized()
        {
            if (!_isInitialized || _syncConfig == null)
                throw new InvalidOperationException("OfflineFirstService non initialisé. Appelez SetCurrentCountryAsync d'abord.");
        }

        private string GetLocalConnectionString()
        {
            if (_syncConfig == null || string.IsNullOrWhiteSpace(_syncConfig.LocalDatabasePath))
                throw new InvalidOperationException("Configuration locale invalide");
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={_syncConfig.LocalDatabasePath};";
        }

        /// <summary>
        /// Expose publiquement la chaîne de connexion locale courante (lecture seule)
        /// </summary>
        public string GetCurrentLocalConnectionString()
        {
            EnsureInitialized();
            return GetLocalConnectionString();
        }

        private async Task<HashSet<string>> GetTableColumnsAsync(OleDbConnection connection, string tableName)
        {
            using (var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null }))
            {
                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (schema != null)
                {
                    foreach (System.Data.DataRow row in schema.Rows)
                    {
                        set.Add(row["COLUMN_NAME"].ToString());
                    }
                }
                return await Task.FromResult(set);
            }
        }

        /// <summary>
        /// Returns a map of column name -> OleDbType for the given table by reading the schema.
        /// Opens the connection if needed and closes it if it was opened here.
        /// </summary>
        private async Task<Dictionary<string, OleDbType>> GetColumnTypesAsync(OleDbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required", nameof(tableName));

            bool openedHere = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
                openedHere = true;
            }

            try
            {
                var map = new Dictionary<string, OleDbType>(StringComparer.OrdinalIgnoreCase);
                using (var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null }))
                {
                    if (schema != null)
                    {
                        foreach (System.Data.DataRow row in schema.Rows)
                        {
                            var colName = Convert.ToString(row["COLUMN_NAME"]);
                            if (string.IsNullOrWhiteSpace(colName)) continue;
                            try
                            {
                                var typeCode = Convert.ToInt32(row["DATA_TYPE"]);
                                map[colName] = (OleDbType)typeCode;
                            }
                            catch
                            {
                                map[colName] = OleDbType.Variant;
                            }
                        }
                    }
                }
                return await Task.FromResult(map);
            }
            finally
            {
                if (openedHere)
                {
                    try { connection.Close(); } catch { }
                }
            }
        }

        /// <summary>
        /// Best-effort inference of OleDbType from a CLR value when schema metadata is unavailable.
        /// </summary>
        private OleDbType InferOleDbTypeFromValue(object value)
        {
            if (value == null || value == DBNull.Value) return OleDbType.Variant;

            var t = value.GetType();
            if (t == typeof(Guid)) return OleDbType.Guid;
            if (t == typeof(byte[])) return OleDbType.Binary;
            if (t == typeof(TimeSpan)) return OleDbType.Double; // store as OADate seconds if coerced to string/number

            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Boolean: return OleDbType.Boolean;
                case TypeCode.Byte: return OleDbType.UnsignedTinyInt;
                case TypeCode.SByte: return OleDbType.TinyInt;
                case TypeCode.Int16: return OleDbType.SmallInt;
                case TypeCode.UInt16: return OleDbType.Integer; // closest available (Access has no unsigned 16)
                case TypeCode.Int32: return OleDbType.Integer;
                case TypeCode.UInt32: return OleDbType.BigInt;
                case TypeCode.Int64: return OleDbType.BigInt;
                case TypeCode.UInt64: return OleDbType.Decimal;
                case TypeCode.Single: return OleDbType.Single;
                case TypeCode.Double: return OleDbType.Double;
                case TypeCode.Decimal: return OleDbType.Decimal;
                case TypeCode.DateTime: return OleDbType.Date;
                case TypeCode.Char: return OleDbType.WChar;
                case TypeCode.String: return OleDbType.VarWChar;
                default: return OleDbType.Variant;
            }
        }

        /// <summary>
        /// Coerces application values into OleDb-compatible values based on the target OleDbType.
        /// Returns DBNull.Value for null/empty where appropriate.
        /// </summary>
        private static object CoerceValueForOleDb(object value, OleDbType targetType)
        {
            if (value == null || value == DBNull.Value) return DBNull.Value;

            try
            {
                switch (targetType)
                {
                    case OleDbType.Boolean:
                        if (value is bool b) return b;
                        if (value is string bs)
                        {
                            if (bool.TryParse(bs, out var bb)) return bb;
                            if (int.TryParse(bs, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bi)) return bi != 0;
                            return DBNull.Value;
                        }
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);

                    case OleDbType.TinyInt:
                        if (value is sbyte sb) return sb;
                        return Convert.ToSByte(value, CultureInfo.InvariantCulture);

                    case OleDbType.UnsignedTinyInt:
                        if (value is byte by) return by;
                        return Convert.ToByte(value, CultureInfo.InvariantCulture);

                    case OleDbType.SmallInt:
                        if (value is short s) return s;
                        return Convert.ToInt16(value, CultureInfo.InvariantCulture);

                    case OleDbType.Integer:
                        if (value is int i32) return i32;
                        return Convert.ToInt32(value, CultureInfo.InvariantCulture);

                    case OleDbType.BigInt:
                        if (value is long i64) return i64;
                        return Convert.ToInt64(value, CultureInfo.InvariantCulture);

                    case OleDbType.Single:
                        if (value is float f) return f;
                        return Convert.ToSingle(value, CultureInfo.InvariantCulture);

                    case OleDbType.Double:
                        if (value is double d) return d;
                        return Convert.ToDouble(value, CultureInfo.InvariantCulture);

                    case OleDbType.Decimal:
                    case OleDbType.Numeric:
                    case OleDbType.VarNumeric:
                    case OleDbType.Currency:
                        if (value is decimal dec) return dec;
                        if (value is string ds)
                        {
                            if (decimal.TryParse(ds, NumberStyles.Any, CultureInfo.InvariantCulture, out var dd)) return dd;
                            return DBNull.Value;
                        }
                        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);

                    case OleDbType.Date:
                        if (value is DateTime dt) return dt;
                        if (value is string dts)
                        {
                            if (DateTime.TryParse(dts, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed)) return parsed;
                            return DBNull.Value;
                        }
                        if (value is double oa) return DateTime.FromOADate(oa);
                        if (value is float oaf) return DateTime.FromOADate(oaf);
                        if (value is decimal oad) return DateTime.FromOADate((double)oad);
                        return Convert.ToDateTime(value, CultureInfo.InvariantCulture);

                    case OleDbType.Guid:
                        if (value is Guid g) return g;
                        if (value is string gs) { return Guid.TryParse(gs, out var gg) ? gg : (object)DBNull.Value; }
                        return DBNull.Value;

                    case OleDbType.Binary:
                    case OleDbType.VarBinary:
                    case OleDbType.LongVarBinary:
                        if (value is byte[] bytes) return bytes;
                        return DBNull.Value;

                    case OleDbType.Char:
                    case OleDbType.VarChar:
                    case OleDbType.LongVarChar:
                    case OleDbType.WChar:
                    case OleDbType.VarWChar:
                    case OleDbType.LongVarWChar:
                        var sVal = value.ToString();
                        return sVal;

                    default:
                        return value;
                }
            }
            catch
            {
                return DBNull.Value;
            }
        }

        private async Task<string> GetPrimaryKeyColumnAsync(OleDbConnection connection, string tableName)
        {
            // 1) Try explicit Primary_Keys schema
            using (var pkSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, tableName }))
            {
                if (pkSchema != null && pkSchema.Rows.Count > 0)
                {
                    string first = null;
                    foreach (System.Data.DataRow row in pkSchema.Rows)
                    {
                        var name = row["COLUMN_NAME"]?.ToString();
                        if (string.Equals(name, "ID", StringComparison.OrdinalIgnoreCase))
                            return "ID";
                        if (first == null && !string.IsNullOrWhiteSpace(name))
                            first = name;
                    }
                    if (!string.IsNullOrWhiteSpace(first)) return first;
                }
            }

            // 2) Fallback: check Indexes schema for PRIMARY_KEY
            using (var idxSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes, new object[] { null, null, null, null, tableName }))
            {
                if (idxSchema != null)
                {
                    var rows = idxSchema.Select("PRIMARY_KEY = true");
                    if (rows != null && rows.Length > 0)
                    {
                        var name = rows[0]["COLUMN_NAME"]?.ToString();
                        if (!string.IsNullOrWhiteSpace(name)) return name;
                    }
                }
            }

            // 3) Heuristics: prefer an ID column if present, else first column name
            var cols = await GetTableColumnsAsync(connection, tableName);
            if (cols.Contains("ID", StringComparer.OrdinalIgnoreCase)) return "ID";
            if (cols.Count > 0) return cols.First();
            return null;
        }

        #region Configuration Methods
        
        /// <summary>
        /// Charge la configuration depuis App.config et initialise les paramètres
        /// </summary>
        private void LoadConfiguration()
        {
            try
            {
                // Récupérer le chemin de la base référentielle depuis App.config
                _ReferentialDatabasePath = Properties.Settings.Default.ReferentialDB;
                
                if (string.IsNullOrEmpty(ReferentialDatabasePath))
                {
                    throw new InvalidOperationException("Le chemin de la base référentielle n'est pas configuré dans App.config (clé: ReferentialDB)");
                }
                
                // Construire la chaîne de connexion OLE DB
                _ReferentialDatabasePath = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ReferentialDatabasePath};";
                
                System.Diagnostics.Debug.WriteLine($"Configuration chargée. Base référentielle: {ReferentialDatabasePath}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors du chargement de la configuration: {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// Récupère un paramètre depuis la table T_Param chargée en mémoire
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <returns>Valeur du paramètre ou null si non trouvé</returns>
        public string GetParameter(string key)
        {
            lock (_referentialLock)
            {
                var param = _params.FirstOrDefault(p => p.PAR_Key.Equals(key, StringComparison.OrdinalIgnoreCase));
                return param?.PAR_Value;
            }
        }
        
        /// <summary>
        /// Récupère tous les paramètres
        /// </summary>
        /// <returns>Dictionnaire des paramètres</returns>
        public Dictionary<string, string> GetAllParameters()
        {
            lock (_referentialLock)
            {
                return _params.ToDictionary(
                    p => p.PAR_Key,
                    p => p.PAR_Value,
                    StringComparer.OrdinalIgnoreCase
                );
            }
        }
        
        /// <summary>
        /// Retourne la liste des pays référentiels (copie). Charge les référentiels si nécessaire.
        /// </summary>
        public async Task<List<Country>> GetCountries()
        {
            if (!_referentialsLoaded)
                await LoadReferentialsAsync();
            lock (_referentialLock)
            {
                return new List<Country>(_countries);
            }
        }
        
        /// <summary>
        /// Initialise les paramètres depuis T_Param et charge la dernière country utilisée
        /// </summary>
        private void InitializePropertiesFromParams()
        {
            // Vérifier et définir les valeurs par défaut si nécessaires
            
            // Répertoire de données local pour offline-first
            string dataDirectory = GetParameter("DataDirectory");
            if (string.IsNullOrEmpty(dataDirectory))
            {
                // Valeur par défaut si non définie
                string defaultDataDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "RecoTool", "Data");
                // Ajouter le paramètre par défaut (note: SetParameter n'existe pas encore, on l'ajoute au cache)
                lock (_referentialLock)
                {
                    var existingParam = _params.FirstOrDefault(p => p.PAR_Key == "DataDirectory");
                    if (existingParam == null)
                    {
                        _params.Add(new Param { PAR_Key = "DataDirectory", PAR_Value = defaultDataDir });
                    }
                }
            }
            
            // Préfixe des bases country
            string countryPrefix = GetParameter("CountryDatabasePrefix");
            if (string.IsNullOrEmpty(countryPrefix))
            {
                // Essayer avec d'autres noms possibles ou défaut
                countryPrefix = GetParameter("CountryDBPrefix") ?? GetParameter("CountryPrefix") ?? "DB_";
                // Ajouter le paramètre par défaut au cache
                lock (_referentialLock)
                {
                    var existingParam = _params.FirstOrDefault(p => p.PAR_Key == "CountryDatabasePrefix");
                    if (existingParam == null)
                    {
                        _params.Add(new Param { PAR_Key = "CountryDatabasePrefix", PAR_Value = countryPrefix });
                    }
                }
            }
            
            System.Diagnostics.Debug.WriteLine($"Paramètres T_Param vérifiés:");
            System.Diagnostics.Debug.WriteLine($"  - DataDirectory: {GetParameter("DataDirectory")}");
            System.Diagnostics.Debug.WriteLine($"  - CountryDatabaseDirectory: {GetParameter("CountryDatabaseDirectory")}");
            System.Diagnostics.Debug.WriteLine($"  - CountryDatabasePrefix: {GetParameter("CountryDatabasePrefix")}");

            // Ne pas initialiser automatiquement un pays au démarrage.
            // Le pays sera défini explicitement par l'UI (sélection utilisateur),
            // afin d'éviter toute copie réseau->local avant choix explicite.
        }
        
        /// <summary>
        /// Recharge la configuration et les paramètres
        /// </summary>
        public async Task RefreshConfigurationAsync()
        {
            // Recharger la configuration de base
            LoadConfiguration();
            
            // Forcer le rechargement des référentiels
            lock (_referentialLock)
            {
                _referentialsLoaded = false;
            }
            
            await LoadReferentialsAsync();
        }
        
        #endregion
        
        #region Database Connection Methods
        
        /// <summary>
        /// Construit la chaîne de connexion pour la base DW
        /// </summary>
        /// <returns>Chaîne de connexion OLE DB pour la base DW</returns>
        public string GetDWConnectionString()
        {
            string localDwPath = GetLocalDWDatabasePath();
            if (string.IsNullOrEmpty(localDwPath))
            {
                throw new InvalidOperationException("Chemin local de la base DW introuvable (vérifier DataDirectory, CountryDatabaseDirectory et DWDatabasePrefix, et qu'un pays est sélectionné)");
            }
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={localDwPath};";
        }

        /// <summary>
        /// Retourne le chemin local attendu pour la base DW (même nom de fichier que la base réseau, dans DataDirectory)
        /// </summary>
        public string GetLocalDWDatabasePath()
        {
            // Réutilise la logique unifiée: DW est par pays via CountryDatabaseDirectory + DWDatabasePrefix
            // Si aucun pays courant n'est défini, on ne peut pas résoudre le chemin local DW
            if (string.IsNullOrEmpty(_currentCountryId)) return null;
            try { return GetLocalDwDbPath(_currentCountryId); } catch { return null; }
        }

        /// <summary>
        /// Copie la base DW du réseau vers le local si nécessaire (nouvelle ou plus récente)
        /// </summary>
        /// <param name="onProgress">Callback progression (0-100, message)</param>
        public async Task EnsureLocalDWCopyAsync(Action<int, string> onProgress = null)
        {
            // Nouvelle implémentation: s'appuie sur la copie unifiée réseau->local pour le pays courant
            if (string.IsNullOrEmpty(_currentCountryId))
                throw new InvalidOperationException("Aucun pays sélectionné: impossible de préparer la base DW.");
            try
            {
                onProgress?.Invoke(10, "Préparation DW...");
                await CopyNetworkToLocalDwAsync(_currentCountryId);
                onProgress?.Invoke(100, "DW prêt");
            }
            catch (Exception ex)
            {
                onProgress?.Invoke(0, $"Erreur copie DW: {ex.Message}");
                throw;
            }
        }
        
        
        /// <summary>
        /// Exécute une requête sur la base DW (lecture seule)
        /// </summary>
        /// <typeparam name="T">Type de données à retourner</typeparam>
        /// <param name="query">Requête SQL</param>
        /// <param name="mapper">Fonction pour mapper les résultats</param>
        /// <returns>Liste des résultats</returns>
        public async Task<List<T>> QueryDWDatabaseAsync<T>(string query, Func<IDataReader, T> mapper)
        {
            var results = new List<T>();
            
            try
            {
                using (var connection = new OleDbConnection(GetDWConnectionString()))
                {
                    await connection.OpenAsync();
                    using (var command = new OleDbCommand(query, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            results.Add(mapper(reader));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la requête sur la base DW: {ex.Message}");
                throw;
            }
            
            return results;
        }
        
        /// <summary>
        /// Vérifie si la base country existe pour un pays donné
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        /// <returns>True si la base existe</returns>
        public bool CountryDatabaseExists(string countryId)
        {
            string countryDatabaseDirectory = GetParameter("CountryDatabaseDirectory");
            if (string.IsNullOrEmpty(countryDatabaseDirectory) || string.IsNullOrEmpty(countryId))
                return false;
                
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string databaseFileName = $"{countryDatabasePrefix}{countryId}.accdb";
            string databasePath = Path.Combine(countryDatabaseDirectory, databaseFileName);
            
            return File.Exists(databasePath);
        }
        
        /// <summary>
        /// Récupère une entité par clé primaire depuis la base locale du pays courant.
        /// </summary>
        public async Task<Entity> GetEntityByIdAsync(string countryId, string tableName, string keyColumn, object keyValue)
        {
            EnsureInitialized();
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Le pays demandé ne correspond pas au pays courant initialisé.");

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var sql = $"SELECT * FROM [{tableName}] WHERE [{keyColumn}] = @key";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    var typeMap = await GetColumnTypesAsync(connection, tableName);
                    var kType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                    var p = new OleDbParameter("@key", kType) { Value = CoerceValueForOleDb(keyValue, kType) };
                    cmd.Parameters.Add(p);
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            var ent = new Entity { TableName = tableName };
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var col = reader.GetName(i);
                                var val = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                ent.Properties[col] = val;
                            }
                            return ent;
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Récupère les clés existantes pour une table et une liste de valeurs de clé primaire en une ou plusieurs requêtes.
        /// Optimisé pour éviter un aller-retour par enregistrement lors des imports massifs.
        /// </summary>
        /// <param name="countryId">Pays courant</param>
        /// <param name="tableName">Nom de la table</param>
        /// <param name="keyColumn">Colonne de clé primaire</param>
        /// <param name="keys">Ensemble de valeurs à vérifier</param>
        /// <returns>Ensemble des clés trouvées dans la base locale</returns>
        public async Task<HashSet<string>> GetExistingKeysAsync(string countryId, string tableName, string keyColumn, IEnumerable<string> keys)
        {
            EnsureInitialized();
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Le pays demandé ne correspond pas au pays courant initialisé.");

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (keys == null) return result;

            var distinctKeys = keys.Where(k => !string.IsNullOrWhiteSpace(k)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (distinctKeys.Count == 0) return result;

            // Limiter le nombre de paramètres par requête (sécurité pour OleDb/Access).
            const int batchSize = 200; // prudent pour Access

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var typeMap = await GetColumnTypesAsync(connection, tableName);

                for (int i = 0; i < distinctKeys.Count; i += batchSize)
                {
                    var chunk = distinctKeys.Skip(i).Take(batchSize).ToList();
                    var placeholders = string.Join(", ", chunk.Select((_, idx) => $"@p{idx}"));
                    var sql = $"SELECT [{keyColumn}] FROM [{tableName}] WHERE [{keyColumn}] IN ({placeholders})";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        foreach (var (val, idx) in chunk.Select((v, idx) => (v, idx)))
                        {
                            var t = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(val);
                            var p = new OleDbParameter($"@p{idx}", t) { Value = CoerceValueForOleDb(val, t) };
                            cmd.Parameters.Add(p);
                        }
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var val = reader.IsDBNull(0) ? null : reader.GetValue(0)?.ToString();
                                if (!string.IsNullOrEmpty(val)) result.Add(val);
                            }
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Récupère les versions existantes (colonne Version par défaut) pour une liste de clés d'une table donnée.
        /// </summary>
        /// <param name="countryId">Pays courant</param>
        /// <param name="tableName">Nom de la table</param>
        /// <param name="keyColumn">Nom de la colonne clé primaire</param>
        /// <param name="keys">Liste des clés à interroger</param>
        /// <param name="versionColumn">Nom de la colonne de version (par défaut: "Version")</param>
        /// <returns>Dictionnaire ID -> Version (nullable si absent)</returns>
        public async Task<Dictionary<string, int?>> GetExistingVersionsAsync(string countryId, string tableName, string keyColumn, IEnumerable<string> keys, string versionColumn = "Version")
        {
            EnsureInitialized();
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Le pays demandé ne correspond pas au pays courant initialisé.");

            var map = new Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase);
            if (keys == null) return map;

            var list = keys.Where(k => !string.IsNullOrWhiteSpace(k)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (list.Count == 0) return map;

            const int batchSize = 200;
            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                // Check table schema to avoid "No value given for one or more required parameters" when a column name is wrong/missing
                var cols = await GetTableColumnsAsync(connection, tableName);
                var typeMap = await GetColumnTypesAsync(connection, tableName);
                // Validate key column; fallback to ID if available
                if (!cols.Contains(keyColumn, StringComparer.OrdinalIgnoreCase))
                {
                    if (cols.Contains("ID", StringComparer.OrdinalIgnoreCase))
                        keyColumn = "ID";
                    else
                        throw new InvalidOperationException($"Colonne clé '{keyColumn}' introuvable dans la table {tableName}.");
                }
                bool hasVersion = cols.Contains(versionColumn, StringComparer.OrdinalIgnoreCase);

                for (int i = 0; i < list.Count; i += batchSize)
                {
                    var chunk = list.Skip(i).Take(batchSize).ToList();
                    var placeholders = string.Join(", ", chunk.Select((_, idx) => $"@p{idx}"));
                    var sql = hasVersion
                        ? $"SELECT [{keyColumn}], [{versionColumn}] FROM [{tableName}] WHERE [{keyColumn}] IN ({placeholders})"
                        : $"SELECT [{keyColumn}] FROM [{tableName}] WHERE [{keyColumn}] IN ({placeholders})";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        foreach (var (val, idx) in chunk.Select((v, idx) => (v, idx)))
                        {
                            var t = typeMap.TryGetValue(keyColumn, out var kt) ? kt : InferOleDbTypeFromValue(val);
                            var p = new OleDbParameter($"@p{idx}", t) { Value = CoerceValueForOleDb(val, t) };
                            cmd.Parameters.Add(p);
                        }
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var id = reader.IsDBNull(0) ? null : reader.GetValue(0)?.ToString();
                                int? ver = null;
                                if (hasVersion)
                                {
                                    if (!reader.IsDBNull(1))
                                    {
                                        try { ver = Convert.ToInt32(reader.GetValue(1)); }
                                        catch { ver = null; }
                                    }
                                }
                                if (!string.IsNullOrEmpty(id)) map[id] = ver;
                            }
                        }
                    }
                }
            }

            return map;
        }

        /// <summary>
        /// Ajoute une entité (avec session de change-log optionnelle)
        /// </summary>
        public async Task<bool> AddEntityAsync(string countryId, Entity entity, OfflineFirstAccess.ChangeTracking.IChangeLogSession changeLogSession)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Le pays demandé ne correspond pas au pays courant initialisé.");

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var cols = await GetTableColumnsAsync(connection, entity.TableName);
                var nowUtc = DateTime.UtcNow;
                var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                var isDeletedCol = cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.IsDeletedColumn : (cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase) ? "DeleteDate" : null);

                if (lastModCol != null) entity.Properties[lastModCol] = nowUtc;
                if (isDeletedCol != null)
                {
                    if (isDeletedCol.Equals(_syncConfig.IsDeletedColumn, StringComparison.OrdinalIgnoreCase))
                        entity.Properties[isDeletedCol] = false;
                    else if (isDeletedCol.Equals("DeleteDate", StringComparison.OrdinalIgnoreCase))
                        entity.Properties[isDeletedCol] = DBNull.Value;
                }

                var validCols = entity.Properties.Keys.Where(k => cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                if (validCols.Count == 0) return false;

                var colList = string.Join(", ", validCols.Select(c => $"[{c}]"));
                var paramList = string.Join(", ", validCols.Select((c, i) => $"@p{i}"));
                var sql = $"INSERT INTO [{entity.TableName}] ({colList}) VALUES ({paramList})";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    var typeMap = await GetColumnTypesAsync(connection, entity.TableName);
                    for (int i = 0; i < validCols.Count; i++)
                    {
                        var colName = validCols[i];
                        var t = typeMap.TryGetValue(colName, out var mapped) ? mapped : InferOleDbTypeFromValue(entity.Properties[colName]);
                        var p = new OleDbParameter($"@p{i}", t) { Value = CoerceValueForOleDb(entity.Properties[colName], t) };
                        cmd.Parameters.Add(p);
                    }
                    var affected = await cmd.ExecuteNonQueryAsync();

                    // Determine PK value for change logging
                    var pkColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                    object keyVal = entity.Properties.ContainsKey(pkColumn) ? entity.Properties[pkColumn] : null;
                    if (keyVal == null)
                    {
                        using (var idCmd = new OleDbCommand("SELECT @@IDENTITY", connection))
                        {
                            keyVal = await idCmd.ExecuteScalarAsync();
                        }
                    }
                    string changeKey = keyVal?.ToString();
                    if (changeLogSession != null)
                        await changeLogSession.AddAsync(entity.TableName, changeKey, "INSERT");
                    else
                        await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "INSERT");

                    return affected > 0;
                }
            }
        }

        /// <summary>
        /// Ajoute une entité (surcharge sans session de change-log)
        /// </summary>
        public async Task<bool> AddEntityAsync(string countryId, Entity entity)
        {
            return await AddEntityAsync(countryId, entity, null);
        }

        /// <summary>
        /// Met à jour une entité (avec session de change-log optionnelle)
        /// </summary> 
        public async Task<bool> UpdateEntityAsync(string countryId, Entity entity, OfflineFirstAccess.ChangeTracking.IChangeLogSession changeLogSession)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Le pays demandé ne correspond pas au pays courant initialisé.");

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var cols = await GetTableColumnsAsync(connection, entity.TableName);
                var nowUtc = DateTime.UtcNow;
                var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                if (lastModCol != null) entity.Properties[lastModCol] = nowUtc;

                var pkColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                if (!entity.Properties.ContainsKey(pkColumn))
                    throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {pkColumn})");
                object keyValue = entity.Properties[pkColumn];

                var updatable = entity.Properties.Keys.Where(k => !k.Equals(pkColumn, StringComparison.OrdinalIgnoreCase) && cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                if (updatable.Count == 0) return false;

                var setList = string.Join(", ", updatable.Select((c, i) => $"[{c}] = @p{i}"));
                var sql = $"UPDATE [{entity.TableName}] SET {setList} WHERE [{pkColumn}] = @key";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    var typeMap = await GetColumnTypesAsync(connection, entity.TableName);
                    for (int i = 0; i < updatable.Count; i++)
                    {
                        var col = updatable[i];
                        var t = typeMap.TryGetValue(col, out var mapped) ? mapped : InferOleDbTypeFromValue(entity.Properties[col]);
                        var p = new OleDbParameter($"@p{i}", t) { Value = CoerceValueForOleDb(entity.Properties[col], t) };
                        cmd.Parameters.Add(p);
                    }
                    var keyType = typeMap.TryGetValue(pkColumn, out var kt) ? kt : InferOleDbTypeFromValue(keyValue);
                    var pKey = new OleDbParameter("@key", keyType) { Value = CoerceValueForOleDb(keyValue, keyType) };
                    cmd.Parameters.Add(pKey);
                    var affected = await cmd.ExecuteNonQueryAsync();

                    string changeKey = keyValue?.ToString();
                    // Encode the exact columns updated so the sync push constructs a partial update payload
                    var opColumns = updatable ?? new List<string>();
                    var opType = $"UPDATE({string.Join(",", opColumns)})";
                    if (changeLogSession != null)
                        await changeLogSession.AddAsync(entity.TableName, changeKey, opType);
                    else
                        await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, opType);

                    return affected > 0;
                }
            }
        }

        /// <summary>
        /// Met à jour une entité (surcharge sans session de change-log)
        /// </summary>
        public async Task<bool> UpdateEntityAsync(string countryId, Entity entity)
        {
            return await UpdateEntityAsync(countryId, entity, null);
        }
        /// <returns>SyncConfiguration prête pour SynchronizationService</returns>
        private SyncConfiguration BuildSyncConfiguration(string countryId)
        {
            // Récupérer chemins et préfixe
            string dataDirectory = GetParameter("DataDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string remoteDir = GetParameter("CountryDatabaseDirectory");

            // Construire chemins local et distant
            string localDbPath = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb");
            string remoteDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb");

            // Tables à synchroniser (depuis T_Param, sinon défaut)
            var tables = new List<string>();
            string syncTables = GetParameter("SyncTables");
            if (!string.IsNullOrWhiteSpace(syncTables))
            {
                foreach (var t in syncTables.Split(','))
                {
                    var name = t?.Trim();
                    if (!string.IsNullOrEmpty(name)) tables.Add(name);
                }
            }
            if (tables.Count == 0)
            {
                tables.Add("T_Reconciliation");
            }

            return new SyncConfiguration
            {
                LocalDatabasePath = localDbPath,
                RemoteDatabasePath = remoteDbPath,
                LockDatabasePath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb"),
                TablesToSync = tables
            };
        }

        /// <summary>
        /// Construit une configuration de synchro pour la base AMBRE (tables AMBRE uniquement)
        /// </summary>
        [Obsolete("La base AMBRE est publiée via l'import (CopyLocalToNetworkAsync). Aucune synchro incrémentale n'est effectuée ici.")]
        private SyncConfiguration BuildAmbreSyncConfiguration(string countryId, List<string> ambreTables)
        {
            if (ambreTables == null) ambreTables = new List<string>();
            if (ambreTables.Count == 0) ambreTables.Add("T_Data_Ambre");

            string localDbPath = GetLocalAmbreDbPath(countryId);
            string remoteDbPath = GetNetworkAmbreDbPath(countryId);

            // Utilise le même lock DB par pays
            string remoteDir = Path.GetDirectoryName(remoteDbPath);
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";

            return new SyncConfiguration
            {
                LocalDatabasePath = localDbPath,
                RemoteDatabasePath = remoteDbPath,
                LockDatabasePath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb"),
                TablesToSync = ambreTables
            };
        }

        /// <summary>
        /// Construit une configuration de synchro pour la base RECONCILIATION (tables RECON uniquement)
        /// </summary>
        private SyncConfiguration BuildReconciliationSyncConfiguration(string countryId, List<string> reconTables)
        {
            if (reconTables == null) reconTables = new List<string>();
            if (reconTables.Count == 0) reconTables.Add("T_Reconciliation");

            string localDbPath = GetLocalReconciliationDbPath(countryId);
            string remoteDbPath = GetNetworkReconciliationDbPath(countryId);

            // Utilise le même lock DB par pays
            string remoteDir = Path.GetDirectoryName(remoteDbPath);
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";

            return new SyncConfiguration
            {
                LocalDatabasePath = localDbPath,
                RemoteDatabasePath = remoteDbPath,
                LockDatabasePath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb"),
                ChangeLogConnectionString = GetChangeLogConnectionString(countryId),
                TablesToSync = reconTables
            };
        }

        /// <summary>
        /// S'assure que les tables de la base de lock existent avec la structure unifiée
        /// </summary>
        /// <param name="lockDbPath">Chemin de la base de lock</param>
        private void EnsureLockTablesExist(string lockDbPath)
        {
            try
            {
                using (var connection = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={lockDbPath};"))
                {
                    connection.Open();

                    bool HasTable(string tableName)
                    {
                        DataTable schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, tableName, "TABLE" });
                        return schema != null && schema.Rows.Count > 0;
                    }

                    void Exec(string sql)
                    {
                        using (var cmd = new OleDbCommand(sql, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    if (!HasTable("SyncLocks"))
                    {
                        Exec(@"CREATE TABLE SyncLocks (
                LockID TEXT(50) PRIMARY KEY,
                Reason TEXT(255),
                CreatedAt DATETIME,
                ExpiresAt DATETIME,
                MachineName TEXT(50),
                ProcessId LONG
            )");
                    }

                    if (!HasTable("Sessions"))
                    {
                        Exec(@"CREATE TABLE Sessions (
                        SessionID TEXT(255) PRIMARY KEY,
                        UserID TEXT(50) NOT NULL,
                        MachineName TEXT(100) NOT NULL,
                        StartTime DATETIME NOT NULL,
                        LastActivity DATETIME NOT NULL,
                        IsActive YESNO DEFAULT 1
                    )");
                    }

                    if (!HasTable("ChangeLog"))
                    {
                        Exec(@"CREATE TABLE ChangeLog (
                        ChangeID COUNTER PRIMARY KEY,
                        TableName TEXT(100) NOT NULL,
                        RecordID TEXT(255) NOT NULL,
                        Operation TEXT(20) NOT NULL,
                        [Timestamp] DATETIME NOT NULL,
                        Synchronized BIT NOT NULL DEFAULT 0
                    )");
                    }

                    if (!HasTable("SyncLog"))
                    {
                        Exec(@"CREATE TABLE SyncLog (
                        ID COUNTER PRIMARY KEY,
                        Operation TEXT(50),
                        Status TEXT(50),
                        Details TEXT(255),
                        [Timestamp] DATETIME
                    )");
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur EnsureLockTablesExist: {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// Crée la base de données de lock avec les tables nécessaires
        /// </summary>
        /// <param name="lockDbPath">Chemin de la base de lock à créer</param>
        private void CreateLockDatabase(string lockDbPath)
        {
            try
            {
                // S'assurer que le répertoire existe
                string directory = Path.GetDirectoryName(lockDbPath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                
                // Créer une nouvelle base Access via Microsoft Access Application
                Type accessType = Type.GetTypeFromProgID("Access.Application");
                dynamic accessApp = Activator.CreateInstance(accessType);
                accessApp.Visible = false;
                
                try
                {
                    // Créer la nouvelle base de données
                    accessApp.NewCurrentDatabase(lockDbPath);
                    
                    // Créer les tables de lock nécessaires via SQL
                    var db = accessApp.CurrentDb();
                    
                    // Créer la table SyncLocks (structure harmonisée avec GenericAccessService)
            string createSyncLocksTable = @"CREATE TABLE SyncLocks (
                LockID TEXT(50) PRIMARY KEY,
                Reason TEXT(255),
                CreatedAt DATETIME,
                ExpiresAt DATETIME,
                MachineName TEXT(50),
                ProcessId LONG
            )";
            db.Execute(createSyncLocksTable);
                    
                    // Créer la table Sessions (structure harmonisée avec GenericAccessService)
                    string createSessionsTable = @"CREATE TABLE Sessions (
                        SessionID TEXT(255) PRIMARY KEY,
                        UserID TEXT(50) NOT NULL,
                        MachineName TEXT(100) NOT NULL,
                        StartTime DATETIME NOT NULL,
                        LastActivity DATETIME NOT NULL,
                        IsActive YESNO DEFAULT 1
                    )";
                    db.Execute(createSessionsTable);
                    
                    // CORRECTION : Créer aussi la table ChangeLog (structure harmonisée avec GenericAccessService)
                    string createChangeLogTable = @"CREATE TABLE ChangeLog (
                        ChangeID COUNTER PRIMARY KEY,
                        TableName TEXT(100) NOT NULL,
                        RecordID TEXT(255) NOT NULL,
                        Operation TEXT(20) NOT NULL,
                        [Timestamp] DATETIME NOT NULL,
                        Synchronized BIT NOT NULL DEFAULT 0
                    )";
                    db.Execute(createChangeLogTable);
                    
                    // CORRECTION : Créer aussi la table SyncLog (structure harmonisée avec GenericAccessService)
                    string createSyncLogTable = @"CREATE TABLE SyncLog (
                        ID COUNTER PRIMARY KEY,
                        Operation TEXT(50),
                        Status TEXT(50),
                        Details TEXT(255),
                        [Timestamp] DATETIME
                    )";
                    db.Execute(createSyncLogTable);
                    
                    System.Diagnostics.Debug.WriteLine($"Base de lock créée avec TOUTES les tables système : {lockDbPath}");
                }
                finally
                {
                    // Fermer et libérer Access
                    accessApp.CloseCurrentDatabase();
                    accessApp.Quit();
                    //System.Runtime.InteropServices.Marshal.ReleaseComObject(accessApp);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la création de la base de lock : {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Initialise la base de données locale pour un pays
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        /// <returns>True si l'initialisation a réussi</returns>
        private async Task<bool> InitializeLocalDatabaseAsync(string countryId)
        {
            try
            {
                string countryDatabaseDirectory = GetParameter("CountryDatabaseDirectory");
                string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
                string dataDirectory = GetParameter("DataDirectory");
                
                string networkDbPath = Path.Combine(countryDatabaseDirectory, $"{countryDatabasePrefix}{countryId}.accdb");
                string localDbPath = Path.Combine(dataDirectory, $"{countryDatabasePrefix}{countryId}.accdb");
                
                // S'assurer que le répertoire local existe
                if (!Directory.Exists(dataDirectory))
                {
                    Directory.CreateDirectory(dataDirectory);
                }
                
                // Vérifier si la base locale existe
                if (!File.Exists(localDbPath))
                {
                    System.Diagnostics.Debug.WriteLine($"Base locale inexistante pour {countryId}, tentative de copie depuis le réseau");
                    
                    // Vérifier que la base réseau existe
                    if (!File.Exists(networkDbPath))
                    {
                        System.Diagnostics.Debug.WriteLine($"Base réseau inexistante pour {countryId}: {networkDbPath}");
                        return false;
                    }
                    
                    // Vérifier si la base réseau n'est pas verrouillée (import en cours)
                    if (await IsDatabaseLockedAsync(networkDbPath))
                    {
                        System.Diagnostics.Debug.WriteLine($"Base réseau verrouillée pour {countryId} (import en cours ?)");
                        return false;
                    }
                    
                    // Copier la base réseau vers le local
                    File.Copy(networkDbPath, localDbPath, true);
                    System.Diagnostics.Debug.WriteLine($"Base réseau copiée vers le local pour {countryId}");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Base locale existante pour {countryId}, synchronisation nécessaire");
                    // La synchronisation sera gérée par le DatabaseService via InitializeLocalDatabaseAsync
                }
                
                // NOTE: ne pas dupliquer la base de lock en local. Les verrous doivent rester sur le réseau.
                // Ensure local ChangeLog schema exists if feature enabled
                if (_useLocalChangeLog)
                {
                    try { await EnsureLocalChangeLogSchemaAsync(countryId); } catch { }
                }

                // Marquer le service comme initialisé pour ce pays
                _isInitialized = true;
                _currentCountryId = countryId;

                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de l'initialisation de la base locale pour {countryId}: {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// Définit le pays courant et prépare l'environnement local.
        /// Règle: s'il existe des changements locaux non synchronisés, on les pousse d'abord vers le réseau,
        /// puis on recopie toutes les bases pertinentes (Reconciliation, Ambre, DW) du réseau vers le local.
        /// </summary>
        public Task<bool> SetCurrentCountryAsync(string countryId, bool suppressPush = false)
        {
            return SetCurrentCountryAsync(countryId, suppressPush, null);
        }

        /// <summary>
        /// Variante avec reporting de progression.
        /// </summary>
        public async Task<bool> SetCurrentCountryAsync(string countryId, bool suppressPush, Action<int, string> onProgress)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                return false;

            onProgress?.Invoke(0, "Initialisation du pays...");

            // 1) Initialiser/assurer la base locale principale (et positionner _currentCountryId)
            onProgress?.Invoke(10, "Préparation de la base locale...");
            var initialized = await InitializeLocalDatabaseAsync(countryId);
            if (!initialized)
                return false;

            // 1.a) Construire et enregistrer la configuration de synchronisation (utile pour GetLocalConnectionString et EnsureInitialized)
            _syncConfig = BuildSyncConfiguration(countryId);
            try { EnsureLockTablesExist(_syncConfig.LockDatabasePath); } catch { }
            // Préparer le service de synchronisation
            if (_syncService == null)
            {
                _syncService = new SynchronizationService();
            }
            try { await _syncService.InitializeAsync(_syncConfig); } catch { }
            onProgress?.Invoke(30, "Configuration de synchronisation initialisée");

            // 1.b) Positionner également l'objet Country courant depuis les référentiels
            try
            {
                _currentCountry = await GetCountryByIdAsync(countryId);
            }
            catch
            {
                _currentCountry = null; // si échec de chargement, rester prudent
            }
            onProgress?.Invoke(35, "Référentiels pays chargés");

            // 2) Synchronisation complète (PUSH puis PULL) des tables configurées (ici: T_Reconciliation)
            //    Évite tout push fire-and-forget et garantit que la base locale est alignée proprement.
            if (!suppressPush)
            {
                try
                {
                    onProgress?.Invoke(40, "Synchronisation des changements (push + pull)...");
                    var syncResult = await _syncService.SynchronizeAsync((pct, msg) =>
                    {
                        try { onProgress?.Invoke(Math.Min(49, Math.Max(41, pct)), msg); } catch { }
                    });
                    if (!(syncResult?.Success ?? false))
                    {
                        System.Diagnostics.Debug.WriteLine($"[{nameof(SetCurrentCountryAsync)}] Synchronization failed for {countryId}: {syncResult?.ErrorDetails}");
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[{nameof(SetCurrentCountryAsync)}] Error during synchronization for {countryId}: {ex.Message}");
                }
            }

            // 3) Après le push, rafraîchir toutes les bases locales depuis le réseau (best-effort)
            // 3) Ne plus recouvrir la base locale de rapprochement juste après la sync.
            //    La synchronisation a déjà aligné local et réseau pour T_Reconciliation.
            onProgress?.Invoke(50, "Vérifications post-synchronisation pour RECON...");

            try
            {
                onProgress?.Invoke(60, "Mise à jour locale: AMBRE...");
                await CopyNetworkToLocalAmbreAsync(countryId);
            }
            catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"AMBRE: échec copie réseau->local pour {countryId}: {ex.Message}"); }

            try
            {
                onProgress?.Invoke(70, "Mise à jour locale: DW...");
                await CopyNetworkToLocalDwAsync(countryId);
            }
            catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"DW: échec copie réseau->local pour {countryId}: {ex.Message}"); }

            // 3.b) Optional: schema verification (Immediate Window only)
            try
            {
                if (RecoTool.Properties.Settings.Default.EnableSchemaVerification)
                {
                    await VerifyDatabaseSchemaAsync(countryId);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] Unexpected error for {countryId}: {ex.Message}");
            }

            onProgress?.Invoke(80, "Initialisation pays terminée");
            return true;
        }
        
        /// <summary>
        /// Vérifie si une base de données est verrouillée (import en cours)
        /// </summary>
        /// <param name="databasePath">Chemin de la base à vérifier</param>
        /// <returns>True si la base est verrouillée</returns>
        private async Task<bool> IsDatabaseLockedAsync(string databasePath)
        {
            try
            {
                // Essayer d'ouvrir la base en mode exclusif pour vérifier si elle est verrouillée
                using (var connection = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={databasePath};Mode=Share Exclusive;"))
                {
                    await connection.OpenAsync();
                    // Si on arrive ici, la base n'est pas verrouillée
                    return false;
                }
            }
            catch (OleDbException ex)
            {
                // Si l'erreur indique que la base est déjà ouverte, elle est verrouillée
                if (ex.Message.Contains("already opened exclusively") || 
                    ex.Message.Contains("could not use") ||
                    ex.Message.Contains("installable ISAM"))
                {
                    return true;
                }
                throw;
            }
            catch (Exception)
            {
                // En cas d'autre erreur, considérer la base comme verrouillée par précaution
                return true;
            }
        }

        /// <summary>
        /// Vérifie les schémas des bases locales (DWINGS, AMBRE, RECONCILIATION) pour un pays donné.
        /// Compare uniquement la présence des colonnes attendues et loggue les manquants dans la fenêtre Immediate.
        /// Aucune interaction UI; non bloquant autant que possible.
        /// </summary>
        /// <param name="countryId">Code pays (ex: "ES")</param>
        private async Task VerifyDatabaseSchemaAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;

            System.Diagnostics.Debug.WriteLine($"[SchemaVerification] Start for {countryId}");

            // Helpers locaux
            bool TableExists(OleDbConnection conn, string tableName)
            {
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    return schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), tableName, StringComparison.OrdinalIgnoreCase));
                }
                catch { return false; }
            }

            HashSet<string> GetActualColumns(OleDbConnection conn, string tableName)
            {
                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                    if (schema != null)
                    {
                        foreach (System.Data.DataRow row in schema.Rows)
                        {
                            var colName = Convert.ToString(row["COLUMN_NAME"]);
                            if (!string.IsNullOrEmpty(colName)) set.Add(colName);
                        }
                    }
                }
                catch { }
                return set;
            }

            // Construit dynamiquement les schémas attendus via les configureActions existantes
            Dictionary<string, HashSet<string>> BuildExpected(OfflineFirstAccess.Helpers.DatabaseTemplateBuilder builder, params string[] onlyTheseTables)
            {
                var dict = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                var cfg = builder.GetConfiguration();
                var filter = (onlyTheseTables != null && onlyTheseTables.Length > 0);
                foreach (var table in cfg.Tables)
                {
                    if (filter && !onlyTheseTables.Contains(table.Name, StringComparer.OrdinalIgnoreCase))
                        continue;
                    var cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (table.Columns != null)
                    {
                        foreach (var c in table.Columns)
                            if (!string.IsNullOrEmpty(c.Name)) cols.Add(c.Name);
                    }
                    dict[table.Name] = cols;
                }
                return dict;
            }

            Func<string> newTmp = () => Path.Combine(Path.GetTempPath(), $"schema_{Guid.NewGuid():N}.accdb");

            // DWINGS: la configuration retire certaines tables système; ne vérifier que les tables métier
            var dwBuilder = new OfflineFirstAccess.Helpers.DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureDwings(dwBuilder);
            var expectedDw = BuildExpected(dwBuilder, "T_DW_Guarantee", "T_DW_Data");

            // AMBRE: filtrer uniquement la table métier
            var ambreBuilder = new OfflineFirstAccess.Helpers.DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureAmbre(ambreBuilder);
            var expectedAmbre = BuildExpected(ambreBuilder, "T_Data_Ambre");

            // RECONCILIATION: filtrer uniquement la table métier
            var reconBuilder = new OfflineFirstAccess.Helpers.DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureReconciliation(reconBuilder);
            var expectedRecon = BuildExpected(reconBuilder, "T_Reconciliation");

            async Task VerifyOneAsync(string dbLabel, string dbPath, Dictionary<string, HashSet<string>> expected)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath))
                    {
                        System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: local DB not found -> {dbPath}");
                        return;
                    }

                    using (var conn = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={dbPath};"))
                    {
                        await conn.OpenAsync();

                        foreach (var kvp in expected)
                        {
                            var table = kvp.Key;
                            var expectedCols = kvp.Value;

                            if (!TableExists(conn, table))
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: missing table '{table}'");
                                continue;
                            }

                            var actualCols = GetActualColumns(conn, table);
                            var missing = expectedCols.Except(actualCols, StringComparer.OrdinalIgnoreCase).ToList();
                            if (missing.Count > 0)
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: table '{table}' missing {missing.Count} column(s): {string.Join(", ", missing)}");
                            }
                            else
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: table '{table}' OK");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: error -> {ex.Message}");
                }
            }

            // DWINGS
            await VerifyOneAsync("DWINGS", GetLocalDwDbPath(countryId), expectedDw);
            // AMBRE
            await VerifyOneAsync("AMBRE", GetLocalAmbreDbPath(countryId), expectedAmbre);
            // RECONCILIATION
            await VerifyOneAsync("RECONCILIATION", GetLocalReconciliationDbPath(countryId), expectedRecon);

            System.Diagnostics.Debug.WriteLine($"[SchemaVerification] End for {countryId}");
        }

        /// <summary>
        /// Copie la base locale du pays vers l'emplacement réseau de manière atomique.
        /// Suppose que le verrou global a été acquis en amont pour éviter les accès concurrents.
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        public async Task CopyLocalToNetworkAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId est requis", nameof(countryId));

            // Récupérer chemins
            string dataDirectory = GetParameter("DataDirectory");
            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";

            if (string.IsNullOrWhiteSpace(dataDirectory) || string.IsNullOrWhiteSpace(remoteDir))
                throw new InvalidOperationException("Paramètres DataDirectory ou CountryDatabaseDirectory manquants (T_Param)");

            string localDbPath = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb");
            string networkDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb");

            if (!File.Exists(localDbPath))
                throw new FileNotFoundException($"Base locale introuvable pour {countryId}", localDbPath);

            // S'assurer que le répertoire réseau existe
            if (!Directory.Exists(remoteDir))
            {
                Directory.CreateDirectory(remoteDir);
            }

            // Vérifier si la base réseau est verrouillée (meilleure robustesse)
            // Normalement inutile si le verrou global applicatif est respecté
            try
            {
                if (File.Exists(networkDbPath))
                {
                    bool locked = await IsDatabaseLockedAsync(networkDbPath);
                    if (locked)
                    {
                        throw new IOException($"La base réseau est verrouillée: {networkDbPath}");
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Avertissement: impossible de vérifier le verrou de la base réseau ({ex.Message}). Poursuite de la copie.");
            }

            // Chemins temporaires et sauvegarde
            string tempPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb.tmp_{Guid.NewGuid():N}");

            // Compact & Repair local DB to a temporary file before publishing (best-effort)
            string sourceForPublish = localDbPath;
            string compactTempLocal = null;
            try
            {
                compactTempLocal = await TryCompactAccessDatabaseAsync(localDbPath);
                if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal))
                {
                    sourceForPublish = compactTempLocal;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Compact/Repair échec ({ex.Message}). Publication avec la base locale telle quelle.");
            }

            // Copier vers un fichier temporaire sur le même volume réseau
            File.Copy(sourceForPublish, tempPath, true);

            // Remplacer le fichier réseau sans créer de backup réseau
            if (File.Exists(networkDbPath))
            {
                try { File.Delete(networkDbPath); } catch { }
            }
            File.Move(tempPath, networkDbPath);

            System.Diagnostics.Debug.WriteLine($"Base locale publiée vers le réseau pour {countryId} -> {networkDbPath}");

            // Nettoyer le fichier compact temporaire s'il existe
            try { if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal)) File.Delete(compactTempLocal); } catch { }
        }

        /// <summary>
        /// Crée une sauvegarde locale de la base RECON avant un import (fichier copié dans un dossier SavedLocal/ avec horodatage).
        /// Best-effort: nève pas d'exception si la sauvegarde échoue.
        /// </summary>
        public async Task CreateLocalReconciliationBackupAsync(string countryId, string label = null)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(countryId)) return;
                string localDbPath = GetLocalReconciliationDbPath(countryId);
                if (!File.Exists(localDbPath)) return;

                string dir = Path.GetDirectoryName(localDbPath);
                if (string.IsNullOrWhiteSpace(dir)) return;
                string savedDir = Path.Combine(dir, "SavedLocal");
                if (!Directory.Exists(savedDir)) Directory.CreateDirectory(savedDir);

                string baseName = Path.GetFileNameWithoutExtension(localDbPath);
                string timeStamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                string suffix = string.IsNullOrWhiteSpace(label) ? "PreImport" : label.Trim();
                string backupPath = Path.Combine(savedDir, $"{baseName}_{suffix}_{timeStamp}.accdb");

                // Copier de manière asynchrone
                await CopyFileAsync(localDbPath, backupPath, overwrite: true).ConfigureAwait(false);
                System.Diagnostics.Debug.WriteLine($"RECON: sauvegarde locale créée: {backupPath}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"RECON: échec sauvegarde locale (non bloquant): {ex.Message}");
            }
        }
        
        /// <summary>
        /// Marque toutes les entrées non synchronisées du ChangeLog comme synchronisées.
        /// À utiliser immédiatement après publication locale->réseau (la base réseau reflète déjà les changements).
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        public async Task MarkAllLocalChangesAsSyncedAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId est requis", nameof(countryId));

            // Le ChangeLog est stocké dans la base de lock côté réseau (voir DatabaseTemplateBuilder commentaire)
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));
            var unsynced = await tracker.GetUnsyncedChangesAsync();
            if (unsynced == null) return;
            var ids = unsynced.Select(c => c.Id).ToList();
            if (ids.Count == 0) return;
            await tracker.MarkChangesAsSyncedAsync(ids);
        }

        /// <summary>
        /// Copie la base réseau du pays vers la base locale de manière atomique (sur le volume local).
        /// Crée un fichier temporaire dans le répertoire local puis remplace atomiquement la base locale.
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        public async Task CopyNetworkToLocalAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId est requis", nameof(countryId));

            string dataDirectory = GetParameter("DataDirectory");
            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";

            if (string.IsNullOrWhiteSpace(dataDirectory) || string.IsNullOrWhiteSpace(remoteDir))
                throw new InvalidOperationException("Paramètres DataDirectory ou CountryDatabaseDirectory manquants (T_Param)");

            string localDbPath = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb");
            string networkDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb");

            if (!File.Exists(networkDbPath))
                throw new FileNotFoundException($"Base réseau introuvable pour {countryId}", networkDbPath);

            // Vérifier que la base réseau n'est pas verrouillée (par précaution)
            if (await IsDatabaseLockedAsync(networkDbPath))
                throw new IOException($"La base réseau est verrouillée: {networkDbPath}");

            // S'assurer que le répertoire local existe
            if (!Directory.Exists(dataDirectory))
            {
                Directory.CreateDirectory(dataDirectory);
            }

            // Copier vers un fichier temporaire local, puis remplacer atomiquement la base locale
            string tempLocal = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb.tmp_{Guid.NewGuid():N}");
            string backupLocal = Path.Combine(dataDirectory, $"{countryPrefix}{countryId}.accdb.bak");

            File.Copy(networkDbPath, tempLocal, true);

            if (File.Exists(localDbPath))
            {
                await FileReplaceWithRetriesAsync(tempLocal, localDbPath, backupLocal, maxAttempts: 5, initialDelayMs: 400);
            }
            else
            {
                File.Move(tempLocal, localDbPath);
            }

            System.Diagnostics.Debug.WriteLine($"Base réseau copiée vers le local pour {countryId} -> {localDbPath}");
            // Après un rafraîchissement complet local <- réseau, initialiser/mettre à jour l'ancre de synchronisation
            try { await SetLastSyncAnchorAsync(countryId, DateTime.UtcNow); }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC][WARN] Echec SetLastSyncAnchorAsync après copie réseau->local ({countryId}): {ex.Message}");
            }
        }

        /// <summary>
        /// Copie la base AMBRE réseau vers la base AMBRE locale (atomique côté local).
        /// Utilise les paramètres Ambre* si présents, sinon retombe sur Country*.
        /// </summary>
        public async Task CopyNetworkToLocalAmbreAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // Guard: prevent overwriting local AMBRE DB if unsynced local changes exist
            if (await HasUnsyncedLocalChangesAsync(countryId))
                throw new InvalidOperationException($"Rafraîchissement AMBRE bloqué: des changements locaux non synchronisés existent pour {countryId}.");

            string localDbPath = GetLocalAmbreDbPath(countryId);
            string dataDirectory = Path.GetDirectoryName(localDbPath);
            if (string.IsNullOrWhiteSpace(dataDirectory)) throw new InvalidOperationException("DataDirectory invalide");

            // 0) Si un ZIP AMBRE est présent côté réseau pour ce pays, on le préfère et on ne copie que s'il est différent
            try
            {
                string networkZipPath = GetNetworkAmbreZipPath(countryId);
                if (!string.IsNullOrWhiteSpace(networkZipPath) && File.Exists(networkZipPath))
                {
                    if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);
                    string localZipPath = GetLocalAmbreZipCachePath(countryId);

                    var netFi = new FileInfo(networkZipPath);
                    var locFi = new FileInfo(localZipPath);
                    bool needZipCopy = !locFi.Exists || !FilesAreEqual(locFi, netFi);

                    if (needZipCopy)
                    {
                        string tmpZip = localZipPath + ".tmp_copy";
                        await CopyFileAsync(networkZipPath, tmpZip, overwrite: true).ConfigureAwait(false);
                        try { await FileReplaceWithRetriesAsync(tmpZip, localZipPath, localZipPath + ".bak", maxAttempts: 5, initialDelayMs: 250); }
                        catch
                        {
                            try { if (File.Exists(localZipPath)) File.Delete(localZipPath); } catch { }
                            await Task.Run(() => File.Move(tmpZip, localZipPath));
                        }
                        try { var bak = localZipPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }

                        await ExtractAmbreZipToLocalAsync(countryId, localZipPath, localDbPath);
                    }
                    else if (!File.Exists(localDbPath))
                    {
                        await ExtractAmbreZipToLocalAsync(countryId, localZipPath, localDbPath);
                    }

                    System.Diagnostics.Debug.WriteLine($"AMBRE: ZIP réseau synchronisé vers local pour {countryId} -> {localDbPath}");
                    try { await SetLastSyncAnchorAsync(countryId, DateTime.UtcNow); } catch { }
                    return;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"AMBRE: échec gestion ZIP ({ex.Message}). Bascule sur copie réseau .accdb.");
            }

            // 1) Fallback: copie brute .accdb réseau -> local
            string networkDbPath = GetNetworkAmbreDbPath(countryId);
            if (!File.Exists(networkDbPath))
                throw new FileNotFoundException($"Base AMBRE réseau introuvable pour {countryId}", networkDbPath);

            if (await IsDatabaseLockedAsync(networkDbPath))
                throw new IOException($"La base AMBRE réseau est verrouillée: {networkDbPath}");

            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);

            string baseName = Path.GetFileNameWithoutExtension(networkDbPath);
            string tempLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.tmp_{Guid.NewGuid():N}");
            string backupLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.bak");

            await CopyFileAsync(networkDbPath, tempLocal, overwrite: true).ConfigureAwait(false);
            if (File.Exists(localDbPath))
                await FileReplaceWithRetriesAsync(tempLocal, localDbPath, backupLocal, maxAttempts: 5, initialDelayMs: 300);
            else
                await Task.Run(() => File.Move(tempLocal, localDbPath));

            System.Diagnostics.Debug.WriteLine($"AMBRE: base réseau copiée vers local pour {countryId} -> {localDbPath}");
            try { await SetLastSyncAnchorAsync(countryId, DateTime.UtcNow); }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC][WARN] Echec SetLastSyncAnchorAsync après copie réseau->local AMBRE ({countryId}): {ex.Message}");
            }
        }

        /// <summary>
        /// Copie la base RECONCILIATION réseau vers la base locale (atomique côté local).
        /// Utilise les paramètres Reconciliation* si présents, sinon retombe sur Country*.
        /// </summary>
        public async Task CopyNetworkToLocalReconciliationAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // Guard: prevent overwriting local RECON DB if unsynced local changes exist
            if (await HasUnsyncedLocalChangesAsync(countryId))
                throw new InvalidOperationException($"Rafraîchissement RECON bloqué: des changements locaux non synchronisés existent pour {countryId}.");

            string localDbPath = GetLocalReconciliationDbPath(countryId);
            string networkDbPath = GetNetworkReconciliationDbPath(countryId);
            string dataDirectory = Path.GetDirectoryName(localDbPath);
            if (string.IsNullOrWhiteSpace(dataDirectory)) throw new InvalidOperationException("DataDirectory invalide");

            if (!File.Exists(networkDbPath))
                throw new FileNotFoundException($"Base RECON réseau introuvable pour {countryId}", networkDbPath);

            if (await IsDatabaseLockedAsync(networkDbPath))
                throw new IOException($"La base RECON réseau est verrouillée: {networkDbPath}");

            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);

            string baseName = Path.GetFileNameWithoutExtension(networkDbPath);
            string tempLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.tmp_{Guid.NewGuid():N}");
            string backupLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.bak");

            await CopyFileAsync(networkDbPath, tempLocal, overwrite: true).ConfigureAwait(false);
            if (File.Exists(localDbPath))
                await FileReplaceWithRetriesAsync(tempLocal, localDbPath, backupLocal, maxAttempts: 5, initialDelayMs: 300);
            else
                await Task.Run(() => File.Move(tempLocal, localDbPath));

            System.Diagnostics.Debug.WriteLine($"RECON: base réseau copiée vers local pour {countryId} -> {localDbPath}");
            // Après un rafraîchissement complet local <- réseau, initialiser/mettre à jour l'ancre de synchronisation
            try { await SetLastSyncAnchorAsync(countryId, DateTime.UtcNow); }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC][WARN] Echec SetLastSyncAnchorAsync après copie réseau->local RECON ({countryId}): {ex.Message}");
            }
        }

        /// <summary>
        /// Copie la base DWINGS réseau vers la base locale (atomique côté local).
        /// </summary>
        public async Task CopyNetworkToLocalDwAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // Guard: prevent overwriting local DW DB if unsynced local changes exist
            if (await HasUnsyncedLocalChangesAsync(countryId))
                throw new InvalidOperationException($"Rafraîchissement DW bloqué: des changements locaux non synchronisés existent pour {countryId}.");

            string localDbPath = GetLocalDwDbPath(countryId);
            string networkDbPath = GetNetworkDwDbPath(countryId);
            string dataDirectory = Path.GetDirectoryName(localDbPath);
            if (string.IsNullOrWhiteSpace(dataDirectory)) throw new InvalidOperationException("DataDirectory invalide");

            // 0) Si un ZIP DWINGS est présent côté réseau pour ce pays, on l'extrait en priorité via le cache local
            try
            {
                var netDwZip = GetNetworkDwZipPath(countryId);
                if (!string.IsNullOrWhiteSpace(netDwZip) && File.Exists(netDwZip))
                {
                    var locZip = GetLocalDwZipCachePath(countryId);
                    try
                    {
                        await CopyZipIfDifferentAsync(netDwZip, locZip);
                        await ExtractDwZipToLocalAsync(countryId, locZip, localDbPath);
                        System.Diagnostics.Debug.WriteLine($"DW: ZIP extrait pour {countryId} depuis {netDwZip} -> {localDbPath}");
                        return;
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"DW: échec extraction ZIP ({ex.Message}). Bascule sur copie réseau.");
                    }
                }
            }
            catch { }

            if (!File.Exists(networkDbPath))
                throw new FileNotFoundException($"Base DW réseau introuvable pour {countryId}", networkDbPath);

            if (await IsDatabaseLockedAsync(networkDbPath))
                throw new IOException($"La base DW réseau est verrouillée: {networkDbPath}");

            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);

            string baseName = Path.GetFileNameWithoutExtension(networkDbPath);
            string tempLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.tmp_{Guid.NewGuid():N}");
            string backupLocal = Path.Combine(dataDirectory, $"{baseName}.accdb.bak");

            await CopyFileAsync(networkDbPath, tempLocal, overwrite: true).ConfigureAwait(false);
            if (File.Exists(localDbPath))
                await FileReplaceWithRetriesAsync(tempLocal, localDbPath, backupLocal, maxAttempts: 5, initialDelayMs: 300);
            else
                await Task.Run(() => File.Move(tempLocal, localDbPath));

            System.Diagnostics.Debug.WriteLine($"DW: base réseau copiée vers local pour {countryId} -> {localDbPath}");
        }

        /// <summary>
        /// Publie la base AMBRE locale vers le réseau.
        /// </summary>
        public async Task CopyLocalToNetworkAmbreAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            string localDbPath = GetLocalAmbreDbPath(countryId);
            if (!File.Exists(localDbPath)) throw new FileNotFoundException($"Base AMBRE locale introuvable pour {countryId}", localDbPath);

            string networkZipPath = GetNetworkAmbreZipPath(countryId);
            string remoteDir = Path.GetDirectoryName(networkZipPath);
            if (string.IsNullOrWhiteSpace(remoteDir)) throw new InvalidOperationException("Répertoire réseau AMBRE invalide");
            if (!Directory.Exists(remoteDir)) Directory.CreateDirectory(remoteDir);

            // Pas de sauvegarde réseau

            // Compact local puis créer un ZIP temporaire local
            string sourceForZip = localDbPath;
            string compactTempLocal = null;
            try
            {
                compactTempLocal = await TryCompactAccessDatabaseAsync(localDbPath);
                if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal)) sourceForZip = compactTempLocal;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"AMBRE: Compact/Repair échec ({ex.Message}). Compression du fichier courant.");
            }

            string localTempZip = Path.Combine(Path.GetDirectoryName(localDbPath) ?? Path.GetTempPath(), $"{Path.GetFileNameWithoutExtension(localDbPath)}_AMBRE.zip.tmp_{Guid.NewGuid():N}");
            try
            {
                using (var archive = ZipFile.Open(localTempZip, ZipArchiveMode.Create))
                {
                    string entryName = Path.GetFileName(localDbPath);
                    archive.CreateEntryFromFile(sourceForZip, entryName, CompressionLevel.Optimal);
                }
            }
            catch
            {
                try { if (File.Exists(localTempZip)) File.Delete(localTempZip); } catch { }
                throw;
            }

            // Copier le ZIP temporaire vers le réseau de façon atomique
            string tempRemote = Path.Combine(remoteDir, $"{Path.GetFileNameWithoutExtension(networkZipPath)}.tmp_{Guid.NewGuid():N}.zip");
            File.Copy(localTempZip, tempRemote, true);
            if (File.Exists(networkZipPath)) { try { File.Delete(networkZipPath); } catch { } }
            File.Move(tempRemote, networkZipPath);
            System.Diagnostics.Debug.WriteLine($"AMBRE: archive ZIP publiée vers réseau pour {countryId} -> {networkZipPath}");

            // Nettoyage local temporaire
            try { if (File.Exists(localTempZip)) File.Delete(localTempZip); } catch { }
            try { if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal)) File.Delete(compactTempLocal); } catch { }
        }

        /// <summary>
        /// Publie la base RECONCILIATION locale vers le réseau.
        /// </summary>
        public async Task CopyLocalToNetworkReconciliationAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            string localDbPath = GetLocalReconciliationDbPath(countryId);
            string networkDbPath = GetNetworkReconciliationDbPath(countryId);
            if (!File.Exists(localDbPath)) throw new FileNotFoundException($"Base RECON locale introuvable pour {countryId}", localDbPath);

            string remoteDir = Path.GetDirectoryName(networkDbPath);
            if (string.IsNullOrWhiteSpace(remoteDir)) throw new InvalidOperationException("Répertoire réseau RECON invalide");
            if (!Directory.Exists(remoteDir)) Directory.CreateDirectory(remoteDir);

            try
            {
                if (File.Exists(networkDbPath))
                {
                    bool locked = await IsDatabaseLockedAsync(networkDbPath);
                    if (locked) throw new IOException($"La base RECON réseau est verrouillée: {networkDbPath}");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"RECON: avertissement vérification verrou échouée ({ex.Message}). Poursuite.");
            }

            // Pas de sauvegarde réseau

            // Compact & Replace atomique
            string baseNameForTemp = Path.GetFileNameWithoutExtension(networkDbPath);
            string tempPath = Path.Combine(remoteDir, $"{baseNameForTemp}.accdb.tmp_{Guid.NewGuid():N}");

            string sourceForPublish = localDbPath;
            string compactTempLocal = null;
            try
            {
                compactTempLocal = await TryCompactAccessDatabaseAsync(localDbPath);
                if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal)) sourceForPublish = compactTempLocal;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"RECON: Compact/Repair échec ({ex.Message}). Publication avec la base locale.");
            }

            File.Copy(sourceForPublish, tempPath, true);
            if (File.Exists(networkDbPath)) { try { File.Delete(networkDbPath); } catch { } }
            File.Move(tempPath, networkDbPath);
            System.Diagnostics.Debug.WriteLine($"RECON: base locale publiée vers réseau pour {countryId} -> {networkDbPath}");
            try { if (!string.IsNullOrEmpty(compactTempLocal) && File.Exists(compactTempLocal)) File.Delete(compactTempLocal); } catch { }
        }
        
        /// <summary>
        /// À exécuter sur les autres clients après la publication réseau d'un import.
        /// 1) Pousse les modifications locales en attente (si présentes) vers le réseau sous verrou global.
        /// 2) Rafraîchit la base locale à partir du réseau (copie atomique).
        /// Retourne le nombre de changements locaux poussés.
        /// </summary>
        public async Task<int> PostPublishReconcileAsync(string countryId, System.Threading.CancellationToken token = default)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId est requis", nameof(countryId));

            // 1) Pousser les pending locaux (acquiert un verrou global si nécessaire)
            int pushed = 0;
            try
            {
                var preloaded = (await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId)).GetUnsyncedChangesAsync())?.ToList();
                if (preloaded != null && preloaded.Any())
                {
                    pushed = await PushPendingChangesToNetworkAsync(countryId, assumeLockHeld: false, token: token, source: "ServiceEntry", preloadedUnsynced: preloaded);
                }
                else
                {
                    pushed = 0;
                }
            }
            catch (Exception ex)
            {
                // Journaliser mais continuer: on veut tout de même réaligner le local sur le réseau
                System.Diagnostics.Debug.WriteLine($"PostPublishReconcile: erreur lors du push des pending pour {countryId}: {ex.Message}");
            }

            // 2) Rafraîchir la base locale à partir du réseau
            await CopyNetworkToLocalAsync(countryId);

            return pushed;
        }
        
        /// <summary>
        /// Construit la chaîne de connexion vers la base réseau d'un pays donné.
        /// </summary>
        private string GetNetworkCountryConnectionString(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            if (string.IsNullOrWhiteSpace(remoteDir))
                throw new InvalidOperationException("Paramètre CountryDatabaseDirectory manquant (T_Param)");
            string networkDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb");
            // Enable row-level locking and avoid share-deny write to reduce locking conflicts
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={networkDbPath};Jet OLEDB:Database Locking Mode=1;Mode=Share Deny None;";
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
                if (diag)
                {
                    var src = string.IsNullOrWhiteSpace(source) ? "-" : source;
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Another push is already running. Skipping this invocation. src={src}");
                    try { LogManager.Info($"[PUSH][{countryId}] Skip: push already running (src={src})"); } catch { }
                }
                return 0; // best-effort: let the ongoing push finish; caller proceeds without blocking
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
                    await OpenWithTimeoutAsync(localConn, openTimeout, token, IsDiagSyncEnabled() ? countryId : null, isNetwork:false);
                    if (diag) { System.Diagnostics.Debug.WriteLine($"[PUSH][{countryId}] Local DB opened."); try { LogManager.Info($"[PUSH][{countryId}] Local DB opened"); } catch { } }
                    _pushStages[countryId] = "OpenNetwork";
                    await OpenWithTimeoutAsync(netConn, openTimeout, token, IsDiagSyncEnabled() ? countryId : null, isNetwork:true);
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

                                // Coerce common problematic types
                                if (t.HasValue && (t.Value == OleDbType.DBDate || t.Value == OleDbType.DBTime || t.Value == OleDbType.DBTimeStamp || t.Value == OleDbType.Date))
                                {
                                    if (v is double d) v = DateTime.FromOADate(d);
                                    else if (v is float f) v = DateTime.FromOADate(f);
                                    else if (v is string s)
                                    {
                                        if (double.TryParse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var od)) v = DateTime.FromOADate(od);
                                        else if (DateTime.TryParse(s, out var dt2)) v = dt2;
                                    }
                                    else if (v is DateTimeOffset dto) v = dto.UtcDateTime;
                                }

                                if (t.HasValue)
                                {
                                    var p = cmd.Parameters.Add(name, t.Value);
                                    p.Value = v;
                                }
                                else
                                {
                                    // Fallback: infer from value and coerce
                                    var it = InferOleDbTypeFromValue(v);
                                    var p = new OleDbParameter(name, it) { Value = CoerceValueForOleDb(v, it) };
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
                                    var localTypeMap = await GetColumnTypesAsync(localConn, table);
                                    var kType = localTypeMap.TryGetValue(pkCol, out var ktt) ? ktt : InferOleDbTypeFromValue(localPkVal);
                                    var pK = new OleDbParameter("@k", kType) { Value = CoerceValueForOleDb(localPkVal, kType) };
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
                                        for (int i = 0; i < allCols.Count; i++) setParts.Add($"[{allCols[i]}] = @p{i}");
                                        using (var up = new OleDbCommand($"UPDATE [{table}] SET {string.Join(", ", setParts)} WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            for (int i = 0; i < allCols.Count; i++) addParam(up, $"@p{i}", localValues[allCols[i]], allCols[i], typeMap);
                                            addParam(up, "@key", localPkVal, pkCol, typeMap);
                                            await execWithRetryAsync(up);
                                        }
                                    }
                                    else
                                    {
                                        // INSERT
                                        var insertCols = localValues.Keys.ToList();
                                        var ph = string.Join(", ", insertCols.Select((c, i) => $"@p{i}"));
                                        var colList = string.Join(", ", insertCols.Select(c => $"[{c}]"));
                                        using (var ins = new OleDbCommand($"INSERT INTO [{table}] ({colList}) VALUES ({ph})", netConn, tx))
                                        {
                                            for (int i = 0; i < insertCols.Count; i++) addParam(ins, $"@p{i}", localValues[insertCols[i]], insertCols[i], typeMap);
                                            await execWithRetryAsync(ins);
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
        
        /// <summary>
        /// Récupère les données DWINGS depuis la base DW
        /// </summary>
        /// <returns>Liste des données DWINGS</returns>
        public async Task<List<DWData>> GetDWDataAsync()
        {
            string query = "SELECT * FROM T_DW_Data ORDER BY INVOICE_ID";
            
            return await QueryDWDatabaseAsync(query, reader => new DWData
            {
                INVOICE_ID = reader["INVOICE_ID"]?.ToString(),
                BOOKING = reader["BOOKING"]?.ToString(),
                REQUESTED_INVOICE_AMOUNT = reader["REQUESTED_INVOICE_AMOUNT"]?.ToString(),
                SENDER_NAME = reader["SENDER_NAME"]?.ToString(),
                RECEIVER_NAME = reader["RECEIVER_NAME"]?.ToString(),
                SENDER_REFERENCE = reader["SENDER_REFERENCE"]?.ToString(),
                RECEIVER_REFERENCE = reader["RECEIVER_REFERENCE"]?.ToString(),
                T_INVOICE_STATUS = reader["T_INVOICE_STATUS"]?.ToString(),
                BILLING_AMOUNT = reader["BILLING_AMOUNT"]?.ToString(),
                BILLING_CURRENCY = reader["BILLING_CURRENCY"]?.ToString(),
                START_DATE = reader["START_DATE"]?.ToString(),
                END_DATE = reader["END_DATE"]?.ToString(),
                FINAL_AMOUNT = reader["FINAL_AMOUNT"]?.ToString(),
                // DWINGS schema update: column renamed to T_COMMISSION_PERIOD_STATUS
                T_COMMISSION_PERIOD_STAT = reader["T_COMMISSION_PERIOD_STATUS"]?.ToString(),
                BUSINESS_CASE_REFERENCE = reader["BUSINESS_CASE_REFERENCE"]?.ToString(),
                BUSINESS_CASE_ID = reader["BUSINESS_CASE_ID"]?.ToString(),
                POSTING_PERIODICITY = reader["POSTING_PERIODICITY"]?.ToString(),
                EVENT_ID = reader["EVENT_ID"]?.ToString(),
                COMMENTS = reader["COMMENTS"]?.ToString(),
                SENDER_ACCOUNT_NUMBER = reader["SENDER_ACCOUNT_NUMBER"]?.ToString(),
                SENDER_ACCOUNT_BIC = reader["SENDER_ACCOUNT_BIC"]?.ToString(),
                RECEIVER_ACCOUNT_NUMBER = reader["RECEIVER_ACCOUNT_NUMBER"]?.ToString(),
                RECEIVER_ACCOUNT_BIC = reader["RECEIVER_ACCOUNT_BIC"]?.ToString(),
                REQUESTED_AMOUNT = reader["REQUESTED_AMOUNT"]?.ToString(),
                EXECUTED_AMOUNT = reader["EXECUTED_AMOUNT"]?.ToString(),
                REQUESTED_EXECUTION_DATE = reader["REQUESTED_EXECUTION_DATE"]?.ToString(),
                T_PAYMENT_REQUEST_STATUS = reader["T_PAYMENT_REQUEST_STATUS"]?.ToString(),
                BGPMT = reader["BGPMT"]?.ToString(),
                DEBTOR_ACCOUNT_ID = reader["DEBTOR_ACCOUNT_ID"]?.ToString(),
                CREDITOR_ACCOUNT_ID = reader["CREDITOR_ACCOUNT_ID"]?.ToString(),
                // DWINGS schema update: COMMISSION_ID removed from T_DW_Data
                COMMISSION_ID = null,
                DEBTOR_PARTY_ID = reader["DEBTOR_PARTY_ID"]?.ToString(),
                DEBTOR_PARTY_NAME = reader["DEBTOR_PARTY_NAME"]?.ToString(),
                DEBTOR_ACCOUNT_NUMBER = reader["DEBTOR_ACCOUNT_NUMBER"]?.ToString(),
                CREDITOR_PARTY_ID = reader["CREDITOR_PARTY_ID"]?.ToString(),
                CREDITOR_PARTY_NAME = reader["CREDITOR_PARTY_NAME"]?.ToString(),
                CREDITOR_ACCOUNT_NUMBER = reader["CREDITOR_ACCOUNT_NUMBER"]?.ToString()
            });
        }
        
        /// <summary>
        /// Récupère une garantie DWINGS spécifique par son ID
        /// </summary>
        /// <param name="guaranteeId">ID de la garantie</param>
        /// <returns>La garantie ou null si non trouvée</returns>
        public async Task<DWGuarantee> GetDWGuaranteeByIdAsync(string guaranteeId)
        {
            if (string.IsNullOrEmpty(guaranteeId))
                return null;
                
            string query = $"SELECT * FROM T_DW_Guarantee WHERE GUARANTEE_ID = '{guaranteeId.Replace("'", "''")}'";  // Protection contre l'injection SQL
            var results = await QueryDWDatabaseAsync(query, reader => new DWGuarantee
            {
                GUARANTEE_ID = reader["GUARANTEE_ID"]?.ToString(),
                // DWINGS schema update: SYNDICATE not present anymore
                SYNDICATE = null,
                // Map new schema fields to legacy properties for compatibility
                CURRENCY = reader["CURRENCYNAME"]?.ToString(),
                AMOUNT = reader["OUTSTANDING_AMOUNT"]?.ToString(),
                OfficialID = reader["OFFICIALREF"]?.ToString(),
                // Obsolete/unknown fields in new schema -> keep nulls to preserve stability
                GuaranteeType = null,
                Client = null,
                _791Sent = null,
                InvoiceStatus = reader["GUARANTEE_STATUS"]?.ToString(),
                TriggerDate = null,
                FXRate = null,
                RMPM = null,
                GroupName = reader["BRANCH_NAME"]?.ToString()
            });
            
            return results.FirstOrDefault();
        }
        
        
        
        #endregion
        
        #region Public Accessors for Referential Data
        
        /// <summary>
        /// Récupère tous les champs d'import Ambre
        /// </summary>
        /// <returns>Liste des champs d'import</returns>
        public List<AmbreImportField> GetAmbreImportFields()
        {
            lock (_referentialLock)
            {
                return new List<AmbreImportField>(_ambreImportFields);
            }
        }
        
        /// <summary>
        /// Récupère toutes les transformations Ambre
        /// </summary>
        /// <returns>Liste des transformations</returns>
        public List<AmbreTransform> GetAmbreTransforms()
        {
            lock (_referentialLock)
            {
                return new List<AmbreTransform>(_ambreTransforms);
            }
        }
        
        /// <summary>
        /// Récupère tous les codes de transaction Ambre
        /// </summary>
        /// <returns>Liste des codes de transaction</returns>
        public List<AmbreTransactionCode> GetAmbreTransactionCodes()
        {
            lock (_referentialLock)
            {
                return new List<AmbreTransactionCode>(_ambreTransactionCodes);
            }
        }
        
        /// <summary>
        /// Expose les champs utilisateur en mémoire (copie pour immutabilité côté appelant)
        /// </summary>
        public List<UserField> UserFields
        {
            get
            {
                lock (_referentialLock)
                {
                    return new List<UserField>(_userFields);
                }
            }
        }
        
        /// <summary>
        /// Récupère la liste des filtres utilisateur en mémoire (copie)
        /// </summary>
        public Task<List<UserFilter>> GetUserFilters()
        {
            lock (_referentialLock)
            {
                return Task.FromResult(new List<UserFilter>(_userFilters));
            }
        }

        /// <summary>
        /// Exécute une requête sur la base de données d'une country
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        /// <param name="query">Requête SQL</param>
        /// <param name="parameters">Paramètres de la requête</param>
        /// <param name="readerAction">Action à exécuter avec le DataReader</param>

        #endregion

        #region Private Helper Methods

        /// <summary>
        /// Prépare une valeur .NET pour l'envoyer à OLE DB (gère les null/DBNull/DateTime/bool/etc.)
        /// </summary>
        private bool IsDiagSyncEnabled()
        {
            try
            {
                var flag = GetParameter("DiagSyncLog");
                if (string.Equals(flag, "1", StringComparison.OrdinalIgnoreCase)) return true;
            }
            catch { }
            try
            {
                var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) ?? string.Empty, "RecoTool");
                var marker = Path.Combine(dir, "diag_sync.on");
                return File.Exists(marker);
            }
            catch { }
            return false;
        }

        /// <summary>
        /// Prépare une valeur .NET pour l'envoyer à OLE DB (gère les null/DBNull/DateTime/bool/etc.)
        /// </summary>
        private object PrepareValueForDatabase(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return DBNull.Value;
            }

            if (value is DateTime dt)
            {
                // Convertit la date en un double universellement compris par Access
                return dt.ToOADate();
            }

            if (value is bool b)
            {
                // Convertit le booléen en entier (1 pour true, 0 pour false)
                return b ? 1 : 0;
            }

            // Pour tous les autres types (string, int, etc.), on retourne la valeur telle quelle
            return value;
        }

        /// <summary>
        /// Ouvre une connexion OleDb avec un timeout. En cas de dépassement, log et lève TimeoutException.
        /// </summary>
        private async Task OpenWithTimeoutAsync(OleDbConnection connection, TimeSpan timeout, CancellationToken token, string diagCountryId, bool isNetwork)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            var openTask = connection.OpenAsync();
            var completed = await Task.WhenAny(openTask, Task.Delay(timeout, token)) == openTask;
            if (!completed)
            {
                var kind = isNetwork ? "network" : "local";
                if (IsDiagSyncEnabled())
                {
                    System.Diagnostics.Debug.WriteLine($"[PUSH][{diagCountryId}] Timeout opening {kind} DB after {timeout.TotalSeconds}s");
                    try { LogManager.Info($"[PUSH][{diagCountryId}] Timeout opening {kind} DB after {timeout.TotalSeconds}s"); } catch { }
                }
                try { connection.Close(); } catch { }
                try { connection.Dispose(); } catch { }
                throw new TimeoutException($"Timeout opening {kind} database connection after {timeout.TotalSeconds}s");
            }
            // Propagate any exception
            await openTask;
        }

        /// <summary>
        /// Calcule un CRC32 stable à partir des colonnes ordonnées (normalisées) d'une entité.
        /// </summary>
        private static uint ComputeCrc32ForEntity(Entity entity, List<string> orderedCols)
        {
            uint crc = 0u;
            var enc = Encoding.UTF8;
            var sep = new byte[] { 0x1F }; // Unit Separator pour délimiter
            bool first = true;
            foreach (var col in orderedCols)
            {
                if (!first)
                    crc = Crc32Append(crc, sep, 0, sep.Length);
                first = false;
                entity.Properties.TryGetValue(col, out var val);
                var norm = NormalizeForCrc(val);
                var bytes = enc.GetBytes(norm);
                crc = Crc32Append(crc, bytes, 0, bytes.Length);
            }
            return crc;
        }

        private static string NormalizeForCrc(object value)
        {
            if (value == null || value == DBNull.Value) return string.Empty;
            switch (value)
            {
                case string s:
                    return s?.Trim() ?? string.Empty;
                case DateTime dt:
                    return dt.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture);
                case bool b:
                    return b ? "1" : "0";
                case decimal dec:
                    return dec.ToString(CultureInfo.InvariantCulture);
                case double d:
                    return d.ToString("G17", CultureInfo.InvariantCulture);
                case float f:
                    return f.ToString("G9", CultureInfo.InvariantCulture);
                default:
                    return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            }
        }

        private static readonly uint[] _crc32Table = BuildCrc32Table();

        private static uint Crc32Append(uint crc, byte[] buffer, int offset, int count)
        {
            for (int i = 0; i < count; i++)
            {
                byte b = buffer[offset + i];
                uint idx = (crc ^ b) & 0xFFu;
                crc = (crc >> 8) ^ _crc32Table[idx];
            }
            return crc;
        }

        private static uint[] BuildCrc32Table()
        {
            const uint poly = 0xEDB88320u;
            var table = new uint[256];
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int j = 0; j < 8; j++)
                {
                    if ((c & 1) != 0)
                        c = poly ^ (c >> 1);
                    else
                        c >>= 1;
                }
                table[i] = c;
            }
            return table;
        }

        /// <summary>
        /// Récupère un pays par son identifiant depuis le cache. Charge les référentiels si nécessaire.
        /// </summary>
        public async Task<Country> GetCountryByIdAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return null;
            if (!_referentialsLoaded)
                await LoadReferentialsAsync();
            lock (_referentialLock)
            {
                return _countries.FirstOrDefault(c => string.Equals(c?.CNT_Id, countryId, StringComparison.OrdinalIgnoreCase));
            }
        }
        
        /// <summary>
        /// Récupère toutes les lignes d'une table en entités, depuis la base locale du pays spécifié.
        /// </summary>
        public async Task<List<Entity>> GetEntitiesAsync(string countryId, string tableName)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName est requis", nameof(tableName));

            // Si nécessaire, basculer sur le bon pays
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                var ok = await SetCurrentCountryAsync(countryId, suppressPush: true);
                if (!ok) throw new InvalidOperationException($"Impossible d'initialiser la base locale pour {countryId}");
            }
            EnsureInitialized();

            var list = new List<Entity>();
            // Choisir la base locale cible en fonction de la table
            string targetDbPath;
            if (string.Equals(tableName, "T_Data_Ambre", StringComparison.OrdinalIgnoreCase))
            {
                targetDbPath = GetLocalAmbreDbPath(_currentCountryId);
            }
            else
            {
                targetDbPath = GetLocalReconciliationDbPath(_currentCountryId);
            }

            if (string.IsNullOrWhiteSpace(targetDbPath) || !File.Exists(targetDbPath))
                throw new FileNotFoundException($"Base locale introuvable pour la table '{tableName}'", targetDbPath ?? "<null>");

            var connStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={targetDbPath};";
            using (var connection = new OleDbConnection(connStr))
            {
                await connection.OpenAsync();
                using (var cmd = new OleDbCommand($"SELECT * FROM [{tableName}]", connection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var ent = new Entity { TableName = tableName };
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var col = reader.GetName(i);
                            var val = reader.IsDBNull(i) ? null : reader.GetValue(i);
                            ent.Properties[col] = val;
                        }
                        list.Add(ent);
                    }
                }
            }
            return list;
        }
        
        /// <summary>
        /// Lance une synchronisation avec retour de résultat et progression.
        /// </summary>
        public async Task<SyncResult> SynchronizeAsync(string countryId, CancellationToken? cancellationToken = null, Action<int, string> onProgress = null)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // Coalesce: if a sync for this country is already running, return the same Task
            if (_activeSyncs.TryGetValue(countryId, out var existing))
            {
                return await existing;
            }

            async Task<SyncResult> RunSyncAsync()
            {
                // Per-country sync semaphore to serialize syncs (safety net)
                var sem = _syncSemaphores.GetOrAdd(countryId, _ => new SemaphoreSlim(1, 1));
                if (!await sem.WaitAsync(0))
                {
                    return new SyncResult { Success = false, Message = "Synchronization already in progress for this country" };
                }
                try
                {
                    // S'assurer que la configuration est prête pour le pays demandé
                    if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
                    {
                        var ok = await SetCurrentCountryAsync(countryId, suppressPush: true);
                        if (!ok)
                            return new SyncResult { Success = false, Message = $"Initialisation impossible pour {countryId}" };
                    }

                    // Pause automatique si verrou d'import actif
                    try
                    {
                        var lockActive = await IsGlobalLockActiveAsync();
                        if (lockActive)
                        {
                            onProgress?.Invoke(0, "Synchronisation en pause: verrou d'import actif");
                            return new SyncResult { Success = false, Message = "Import lock active - sync paused" };
                        }
                    }
                    catch { /* ignorer les erreurs lors de la vérification du verrou */ }

                    // Push local pending changes first (best-effort)
                    if (_useLocalChangeLog)
                    {
                        try
                        {
                            onProgress?.Invoke(1, "Poussée des changements locaux en attente...");
                            await PushReconciliationIfPendingAsync(_currentCountryId);
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"[SYNC] Pre-pull push failed: {ex.Message}");
                            // continue with reconcile; entries remain pending locally
                        }
                    }

                    // Fast-path: comparer uniquement la base de réconciliation (AMBRE est géré par import)
                    try
                    {
                        var localPath = GetLocalReconciliationDbPath(_currentCountryId);
                        var remotePath = GetNetworkReconciliationDbPath(_currentCountryId);
                        if (!string.IsNullOrWhiteSpace(localPath) && !string.IsNullOrWhiteSpace(remotePath)
                            && File.Exists(localPath) && File.Exists(remotePath))
                        {
                            var lfi = new FileInfo(localPath);
                            var rfi = new FileInfo(remotePath);
                            bool filesLikelyIdentical = FilesAreEqual(lfi, rfi);

                            if (filesLikelyIdentical)
                            {
                                // Vérifier les changements en attente dans la base de lock
                                var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(_currentCountryId));
                                var unsynced = await tracker.GetUnsyncedChangesAsync();
                                if (unsynced == null || !unsynced.Any())
                                {
                                    onProgress?.Invoke(100, "Bases identiques (RECON) - aucune synchronisation nécessaire.");
                                    return new SyncResult { Success = true, Message = "No-op (identique)" };
                                }
                            }
                        }
                    }
                    catch { /* best-effort, en cas d'erreur on continue avec la synchro normale */ }

                    // Déterminer les tables de réconciliation à synchroniser
                    var reconTables = new List<string>();
                    var syncTables = GetParameter("SyncTables");
                    if (!string.IsNullOrWhiteSpace(syncTables))
                    {
                        foreach (var t in syncTables.Split(','))
                        {
                            var name = t?.Trim();
                            if (!string.IsNullOrEmpty(name) && name.StartsWith("T_Reconciliation", StringComparison.OrdinalIgnoreCase))
                                reconTables.Add(name);
                        }
                    }
                    if (reconTables.Count == 0)
                    {
                        reconTables.Add("T_Reconciliation");
                    }

                    onProgress?.Invoke(0, "Initialisation synchro RECON...");
                    var reconCfg = BuildReconciliationSyncConfiguration(_currentCountryId, reconTables);
                    if (_syncService == null) _syncService = new SynchronizationService();
                    await _syncService.InitializeAsync(reconCfg);
                    onProgress?.Invoke(5, "Démarrage RECON...");
                    var reconRes = await _syncService.SynchronizeAsync(onProgress);

                    if (reconRes != null && reconRes.Success)
                    {
                        _lastSyncTimes[_currentCountryId] = DateTime.UtcNow;
                        return reconRes;
                    }
                    return reconRes ?? new SyncResult { Success = false, Message = "Résultat RECON nul" };
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[SYNC] Erreur: {ex.Message}");
                    return new SyncResult { Success = false, Message = ex.Message };
                }
                finally
                {
                    try { sem.Release(); } catch { }
                }
            }

            var runner = RunSyncAsync();
            if (!_activeSyncs.TryAdd(countryId, runner))
            {
                // Race: another runner just got added
                if (_activeSyncs.TryGetValue(countryId, out var current)) return await current;
            }
            try
            {
                return await runner;
            }
            finally
            {
                _activeSyncs.TryRemove(countryId, out _);
            }
        }
        
        /// <summary>
        /// Lance une synchronisation simple sans progression. Retourne true si succès.
        /// </summary>
        public async Task<bool> SynchronizeData()
        {
            if (string.IsNullOrEmpty(_currentCountryId)) return false;
            var res = await SynchronizeAsync(_currentCountryId, null, null);
            return res != null && res.Success;
        }
        
        #endregion
        
        #region IDisposable Implementation

        /// <summary>
        /// Libère les ressources utilisées par le service
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Libère les ressources utilisées par le service
        /// </summary>
        /// <param name="disposing">True si appelé depuis Dispose()</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Arrêter la surveillance des changements
                    try
                    {
                        //StopWatching();
                    }
                    catch
                    {
                        // Ignorer les erreurs lors de la libération
                    }

                    // Rien à libérer concernant l'ancien service de base de données (supprimé)
                }

                _disposed = true;
            }
        }

        #endregion
    }

}
#endregion
