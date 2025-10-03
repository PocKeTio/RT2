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
using RecoTool.Helpers;
using RecoTool.Services.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Service de gestion des accès offline-first aux bases de données Access
    /// Gère deux types de bases :
    /// - Référentielles : chargées en mémoire une seule fois (lecture seule)
    /// - Par country : synchronisation offline-first avec OfflineFirstAccess.dll
    /// </summary>
    public partial class OfflineFirstService : IDisposable
    {
        // Controls whether background pushes are allowed by SyncMonitorService and other periodic mechanisms
        // Defaults to false to honor the "no auto sync" policy; pages can opt-in temporarily if needed
        public bool AllowBackgroundPushes { get; set; } = false;

        // Ensure configuration is loaded as soon as the service is constructed so that
        // referential connection string is available at startup before any LoadReferentialsAsync calls
        public OfflineFirstService()
        {
            try
            {
                LoadConfiguration();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[Startup] LoadConfiguration failed: {ex.Message}");
                throw;
            }
        }

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

        // Ambre import scope counter (supports nested scopes)
        private int _ambreImportScopeCount;

        public IDisposable BeginAmbreImportScope()
        {
            Interlocked.Increment(ref _ambreImportScopeCount);
            return new AmbreImportScope(this);
        }

        private sealed class AmbreImportScope : IDisposable
        {
            private OfflineFirstService _svc;
            private int _disposed;
            public AmbreImportScope(OfflineFirstService svc) { _svc = svc; }
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
                if (_svc != null)
                {
                    Interlocked.Decrement(ref _svc._ambreImportScopeCount);
                    _svc = null;
                }
            }
        }

        /// <summary>
        /// Returns true if a global lock is currently active AND held by another process (not this MachineName+ProcessId).
        /// Ignores expired rows and performs a best-effort cleanup of expired entries.
        /// </summary>
        public async Task<bool> IsGlobalLockActiveByOthersAsync(CancellationToken token = default)
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

                    // Cleanup expired locks
                    using (var cleanup = new OleDbCommand("DELETE FROM SyncLocks WHERE ExpiresAt IS NOT NULL AND ExpiresAt < ?", connection))
                    {
                        cleanup.Parameters.Add(new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = now });
                        await cleanup.ExecuteNonQueryAsync();
                    }

                    // Check if an active lock exists held by OTHER processes
                    using (var check = new OleDbCommand("SELECT COUNT(*) FROM SyncLocks WHERE (ExpiresAt IS NULL OR ExpiresAt > ?) AND NOT (MachineName = ? AND ProcessId = ?)", connection))
                    {
                        check.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                        check.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                        check.Parameters.Add(new OleDbParameter("@ProcessId", OleDbType.Integer) { Value = System.Diagnostics.Process.GetCurrentProcess().Id });
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
                // On error, do not block caller: assume no foreign lock
                return false;
            }
        }

        // No-op handle used when a re-entrant acquisition detects an active lock already held by this process.
        private sealed class NoopLockHandle : IDisposable
        {
            public static readonly NoopLockHandle Instance = new NoopLockHandle();
            private NoopLockHandle() { }
            public void Dispose() { /* nothing */ }
        }
        
        #endregion

        #region Background Push Control
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.Push.cs */
        #endregion

        #region Per-country Sync Gate
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.SyncGates.cs */
        #endregion

        #region Background Sync Scheduler
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.SyncGates.cs (ScheduleSyncIfNeededAsync) */
        #endregion

        #region Lock Helpers

        private static bool IsAccessLockException(OleDbException ex)
        {
            // Common Access/Jet/ACE locking and sharing violation error codes
            // 3218: Could not update; currently locked by another session/user
            // 3260: Couldn't lock table; already in use
            // 3050: Couldn't lock file
            // 3188/3197: Couldn't update; currently locked
            // 3704: Operation not allowed when the object is closed (may follow a lock)
            var codes = new HashSet<int> { 3218, 3260, 3050, 3188, 3197 };
            try
            {
                foreach (OleDbError err in ex.Errors)
                {
                    if (codes.Contains(err.NativeError)) return true;
                }
            }
            catch { }
            return false;
        }

        /* moved to partial: Services/OfflineFirst/OfflineFirstService.SyncOperations.cs (PushReconciliationIfPendingAsync) */

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

            string file = $"{prefix}{countryId}_lock.accdb";
            return Path.Combine(dir, file);
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
            // Normalize destination timestamp to source (UTC) to avoid false mismatches across clients
            try { File.SetLastWriteTimeUtc(localZipPath, netFi.LastWriteTimeUtc); } catch { }
            return true;
        }

        private static bool FilesAreEqual(FileInfo first, FileInfo second)
        {
            try
            {
                if (!first.Exists || !second.Exists) return false;
                if (first.Length != second.Length) return false;
                // Compare UTC timestamps with tolerance to absorb FS/ZIP/SMB rounding and DST issues
                var dt1 = first.LastWriteTimeUtc;
                var dt2 = second.LastWriteTimeUtc;
                var diff = dt1 > dt2 ? (dt1 - dt2) : (dt2 - dt1);
                return diff <= TimeSpan.FromSeconds(5);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Retourne le chemin du ZIP DW réseau le plus pertinent pour un pays (le plus récent contenant le pays et "DW/DWINGS"). Peut renvoyer null.
        /// </summary>
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.Paths.cs (GetNetworkDwZipPath) */

        /// <summary>
        /// Retourne le chemin du cache local ZIP DW (nom stable).
        /// </summary>
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.Paths.cs (GetLocalDwZipCachePath) */

        /// <summary>
        /// Vérifie si le ZIP DW local correspond au ZIP DW réseau (taille/contenu). True si pas de ZIP réseau ou si identiques.
        /// </summary>
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.Paths.cs (IsLocalDwZipInSyncWithNetworkAsync) */
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.Diagnostics.cs (GetAmbreZipDiagnostics, GetDwZipDiagnostics) */

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
            return AceConn(localDbPath);
        }

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
                selectedConnStr = AceConn(ambrePath);
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
                                    : (colTypeCache[entity.TableName] = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName));
                                for (int i = 0; i < validCols.Count; i++)
                                {
                                    var colName = validCols[i];
                                    if (!typeMap.TryGetValue(colName, out var t))
                                    {
                                        t = OleDbSchemaHelper.InferOleDbTypeFromValue(entity.Properties[colName]);
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
                                p.Value = OleDbSchemaHelper.CoerceValueForOleDb(entity.Properties[tup.Cols[i]], p.OleDbType);
                            }
                            // Retry on transient Access lock errors (e.g., 3218/3260)
                            {
                                int attempts = 0;
                                while (true)
                                {
                                    try
                                    {
                                        await tup.Cmd.ExecuteNonQueryAsync();
                                        break;
                                    }
                                    catch (OleDbException ex) when (IsAccessLockException(ex) && attempts < 4)
                                    {
                                        attempts++;
                                        await Task.Delay(100 * attempts);
                                    }
                                }
                            }
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
                                    : (colTypeCache[entity.TableName] = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName));
                                for (int i = 0; i < updatable.Count; i++)
                                {
                                    var colName = updatable[i];
                                    if (!typeMap.TryGetValue(colName, out var t)) t = OleDbSchemaHelper.InferOleDbTypeFromValue(entity.Properties.ContainsKey(colName) ? entity.Properties[colName] : null);
                                    var p = new OleDbParameter($"@p{i}", t) { Value = DBNull.Value };
                                    cmd.Parameters.Add(p);
                                }
                                // key parameter at the end (typed)
                                {
                                    var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : OleDbSchemaHelper.InferOleDbTypeFromValue(keyValue);
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
                                p.Value = OleDbSchemaHelper.CoerceValueForOleDb(entity.Properties[upd.Cols[i]], p.OleDbType);
                            }
                            {
                                var pKey = upd.Cmd.Parameters[upd.KeyIndex];
                                pKey.Value = OleDbSchemaHelper.CoerceValueForOleDb(keyValue, pKey.OleDbType);
                            }
                            if (hasCrc)
                            {
                                // last parameter is @crc
                                var pCrc = upd.Cmd.Parameters[upd.KeyIndex + 1];
                                pCrc.Value = crcValue.HasValue ? (object)OleDbSchemaHelper.CoerceValueForOleDb(crcValue.Value, pCrc.OleDbType) : DBNull.Value;
                            }
                            int affected;
                            // Retry on transient Access lock errors (e.g., 3218/3260)
                            {
                                int attempts = 0;
                                while (true)
                                {
                                    try
                                    {
                                        affected = await upd.Cmd.ExecuteNonQueryAsync();
                                        break;
                                    }
                                    catch (OleDbException ex) when (IsAccessLockException(ex) && attempts < 4)
                                    {
                                        attempts++;
                                        await Task.Delay(100 * attempts);
                                    }
                                }
                            }
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
                                        : (colTypeCache[entity.TableName] = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName));
                                    // Prepare parameters in fixed order if present (typed)
                                    if (hasDeleteDate)
                                    {
                                        var t = typeMap.TryGetValue("DeleteDate", out var dt) ? dt : OleDbType.Date;
                                        var p0 = new OleDbParameter("@p0", t) { Value = OleDbSchemaHelper.CoerceValueForOleDb(nowUtc, t) };
                                        cmd.Parameters.Add(p0);
                                    }
                                    if (lastModCol != null)
                                    {
                                        var t = typeMap.TryGetValue(lastModCol, out var lt) ? lt : OleDbType.Date;
                                        var p1 = new OleDbParameter("@p1", t) { Value = OleDbSchemaHelper.CoerceValueForOleDb(nowUtc, t) };
                                        cmd.Parameters.Add(p1);
                                    }
                                    {
                                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : OleDbSchemaHelper.InferOleDbTypeFromValue(keyValue);
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
                                    p0.Value = OleDbSchemaHelper.CoerceValueForOleDb(nowUtc, p0.OleDbType);
                                }
                                if (lastModCol != null)
                                {
                                    var p1 = cmd.Parameters[baseIndex++];
                                    p1.Value = OleDbSchemaHelper.CoerceValueForOleDb(nowUtc, p1.OleDbType);
                                }
                                var pkParam = cmd.Parameters[baseIndex];
                                pkParam.Value = OleDbSchemaHelper.CoerceValueForOleDb(keyValue, pkParam.OleDbType); // @key
                                await cmd.ExecuteNonQueryAsync();
                            }
                            else
                            {
                                var sql = $"DELETE FROM [{entity.TableName}] WHERE [{keyColumn}] = @key";
                                using (var cmd = new OleDbCommand(sql, connection, tx))
                                {
                                    var typeMap = colTypeCache.TryGetValue(entity.TableName, out var tm)
                                        ? tm
                                        : (colTypeCache[entity.TableName] = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName));
                                    var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : OleDbSchemaHelper.InferOleDbTypeFromValue(keyValue);
                                    var p = new OleDbParameter("@key", keyType) { Value = OleDbSchemaHelper.CoerceValueForOleDb(keyValue, keyType) };
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
                                    var pExpires = new OleDbParameter("@ExpiresAt", OleDbType.Date) { Value = newExpiry };
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

        // Wrapper that ensures our in-process gate is released when the underlying DB lock is disposed
        private sealed class ProcessGateLockHandle : IDisposable
        {
            private readonly IDisposable _inner;
            private readonly SemaphoreSlim _gate;
            private int _released;

            public ProcessGateLockHandle(IDisposable inner, SemaphoreSlim gate)
            {
                _inner = inner;
                _gate = gate;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _released, 1) == 1) return;
                try { _inner?.Dispose(); } catch { }
                try { _gate?.Release(); } catch { }
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
                            else
                            {
                                // Re-entrancy: if the active lock is ours (same process), allow nested acquisition without waiting
                                using (var self = new OleDbCommand("SELECT TOP 1 LockID FROM SyncLocks WHERE (ExpiresAt IS NULL OR ExpiresAt > ?) AND MachineName = ? AND ProcessId = ? ORDER BY CreatedAt DESC", connection))
                                {
                                    self.Parameters.Add(new OleDbParameter("@Now", OleDbType.Date) { Value = now });
                                    self.Parameters.Add(new OleDbParameter("@MachineName", OleDbType.VarWChar, 255) { Value = (object)Environment.MachineName ?? DBNull.Value });
                                    self.Parameters.Add(new OleDbParameter("@ProcessId", OleDbType.Integer) { Value = System.Diagnostics.Process.GetCurrentProcess().Id });
                                    var obj = await self.ExecuteScalarAsync();
                                    if (obj != null && obj != DBNull.Value)
                                    {
                                        // Return a no-op handle; original holder will release the DB row
                                        return NoopLockHandle.Instance;
                                    }
                                }
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
        // In-process gate: ensure only one AcquireGlobalLockAsync is executing per process at any time.
        private static readonly SemaphoreSlim _acquireGlobalProcessGate = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Returns true if a synchronization is currently in progress for the specified country.
        /// This uses the internal semaphore to detect if the sync lock is held.
        /// </summary>
        public bool IsSynchronizationInProgress(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return false;
            var sem = _syncSemaphores.GetOrAdd(countryId, _ => new SemaphoreSlim(1, 1));
            // Try to acquire without waiting: if we can, then no sync is currently running.
            if (sem.Wait(0))
            {
                sem.Release();
                return false;
            }
            return true;
        }

        /// <summary>
        /// Wait until the current synchronization (if any) completes for the specified country.
        /// Returns immediately if no synchronization is running.
        /// </summary>
        public async Task WaitForSynchronizationAsync(string countryId, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;
            var sem = _syncSemaphores.GetOrAdd(countryId, _ => new SemaphoreSlim(1, 1));
            // Wait to acquire; if already free, this returns immediately. Then release so others can proceed.
            await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
            sem.Release();
        }
        /* moved to partial: Services/OfflineFirst/OfflineFirstService.CountryContext.cs (state fields and properties) */

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
        /// Charge toutes les tables référentielles en mémoire une seule fois, de manière thread-safe.
        /// </summary>
        public async Task LoadReferentialsAsync()
        {
            // Defensive: if configuration hasn't been loaded yet, load it now
            if (string.IsNullOrWhiteSpace(_ReferentialDatabasePath))
            {
                LoadConfiguration();
            }

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
                using (var connection = new OleDbConnection(ReferentialConnectionString))
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
    [Operation] TEXT(255) NOT NULL,
    [Timestamp] DATETIME NOT NULL,
    [Synchronized] BIT NOT NULL
)"
                        };
                        changeLogTable.Columns.Add(new ColumnDefinition("ChangeID", typeof(long), "LONG", false, true, true));
                        changeLogTable.Columns.Add(new ColumnDefinition("TableName", typeof(string), "TEXT(128)", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("RecordID", typeof(string), "TEXT(255)", true));
                        changeLogTable.Columns.Add(new ColumnDefinition("Operation", typeof(string), "TEXT(255)", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("Timestamp", typeof(DateTime), "DATETIME", false));
                        changeLogTable.Columns.Add(new ColumnDefinition("Synchronized", typeof(bool), "BIT", false));
                        config.Tables.Add(changeLogTable);
                    });
                }

                // Open the dedicated ChangeLog DB and ensure columns exist (best-effort schema repair)
                using (var connection = new OleDbConnection(AceConn(changeLogDbPath)))
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
    [Operation] TEXT(255) NOT NULL,
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
                        { "Operation", "TEXT(255)" },
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
                        var typeMap = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName);
                        if (hasDeleteDate)
                        {
                            var t = typeMap.TryGetValue("DeleteDate", out var dt) ? dt : OleDbType.Date;
                            var p0 = new OleDbParameter("@p0", t) { Value = OleDbSchemaHelper.CoerceValueForOleDb(DateTime.UtcNow, t) };
                            cmd.Parameters.Add(p0);
                        }
                        if (lastModCol != null)
                        {
                            var t = typeMap.TryGetValue(lastModCol, out var lt) ? lt : OleDbType.Date;
                            var p1 = new OleDbParameter("@p1", t) { Value = OleDbSchemaHelper.CoerceValueForOleDb(DateTime.UtcNow, t) };
                            cmd.Parameters.Add(p1);
                        }
                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : OleDbSchemaHelper.InferOleDbTypeFromValue(keyValue);
                        var pKey = new OleDbParameter("@key", keyType) { Value = OleDbSchemaHelper.CoerceValueForOleDb(keyValue, keyType) };
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
                        var typeMap = await OleDbSchemaHelper.GetColumnTypesAsync(connection, entity.TableName);
                        var keyType = typeMap.TryGetValue(keyColumn, out var kt) ? kt : OleDbSchemaHelper.InferOleDbTypeFromValue(keyValue);
                        var pKey = new OleDbParameter("@key", keyType) { Value = OleDbSchemaHelper.CoerceValueForOleDb(keyValue, keyType) };
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
            // Serialize acquisition attempts within this process and keep the gate held for the lock lifetime
            await _acquireGlobalProcessGate.WaitAsync(token);
            try
            {
                var inner = await AcquireGlobalLockInternalAsync(identifier, reason, timeoutSeconds, token);
                return new ProcessGateLockHandle(inner, _acquireGlobalProcessGate);
            }
            catch
            {
                try { _acquireGlobalProcessGate.Release(); } catch { }
                throw;
            }
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
            return AceConn(_syncConfig.LocalDatabasePath);
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
                
                if (string.IsNullOrEmpty(_ReferentialDatabasePath))
                {
                    throw new InvalidOperationException("Le chemin de la base référentielle n'est pas configuré dans App.config (clé: ReferentialDB)");
                }
                
                System.Diagnostics.Debug.WriteLine($"Configuration chargée. Base référentielle (path): {_ReferentialDatabasePath}");
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

            // Dynamic toggle from T_Param: EnableSyncLog or SYNCLOG (true/false)
            bool enableSyncLog = false;
            try
            {
                var p = GetParameter("EnableSyncLog");
                if (string.IsNullOrWhiteSpace(p)) p = GetParameter("SYNCLOG");
                if (!string.IsNullOrWhiteSpace(p)) bool.TryParse(p.Trim(), out enableSyncLog);
            }
            catch { }

            return new SyncConfiguration
            {
                LocalDatabasePath = localDbPath,
                RemoteDatabasePath = remoteDbPath,
                LockDatabasePath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb"),
                TablesToSync = tables,
                EnableSyncLog = enableSyncLog
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

            bool enableSyncLogRecon = false;
            try
            {
                var p = GetParameter("EnableSyncLog");
                if (string.IsNullOrWhiteSpace(p)) p = GetParameter("SYNCLOG");
                if (!string.IsNullOrWhiteSpace(p)) bool.TryParse(p.Trim(), out enableSyncLogRecon);
            }
            catch { }

            return new SyncConfiguration
            {
                LocalDatabasePath = localDbPath,
                RemoteDatabasePath = remoteDbPath,
                LockDatabasePath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb"),
                ChangeLogConnectionString = GetChangeLogConnectionString(countryId),
                TablesToSync = reconTables,
                EnableSyncLog = enableSyncLogRecon
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
                using (var connection = new OleDbConnection(AceConn(lockDbPath)))
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
                        Operation TEXT(255) NOT NULL,
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

                // Invalider le cache DWINGS du pays précédent (si existant)
                try
                {
                    var prevDwPath = GetLocalDWDatabasePath(_currentCountryId);
                    DwingsService.InvalidateSharedCacheForPath(prevDwPath);
                }
                catch { }

                // Changer le pays courant
                _currentCountryId = countryId;

                // Précharger les données DWINGS en mémoire pour accélérer les réconciliations
                try
                {
                    var dwSvc = new DwingsService(this);
                    await dwSvc.PrimeCachesAsync().ConfigureAwait(false);
                }
                catch { }

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

            // 0) Provisionner les bases RÉSEAU depuis des modèles si au moins une est absente (Reconciliation, AMBRE, Lock)
            try
            {
                bool needProvision = false;
                try
                {
                    var reconPath = GetNetworkReconciliationDbPath(countryId);
                    needProvision = !string.IsNullOrWhiteSpace(reconPath) && !File.Exists(reconPath);
                }
                catch { needProvision = true; }

                if (needProvision)
                {
                    onProgress?.Invoke(5, "Vérification des modèles réseau...");
                    await ProvisionNetworkFromTemplatesAsync(countryId);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[Template] Provision réseau ignorée: {ex.Message}");
            }

            // 1) Initialiser/assurer la base locale principale (et positionner _currentCountryId)
            onProgress?.Invoke(10, "Préparation de la base locale...");
            // Detect cold-local to avoid incremental UPDATE flood
            string dataDirectory_cold = GetParameter("DataDirectory");
            string countryDatabasePrefix_cold = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string localDbPath_cold = Path.Combine(dataDirectory_cold, $"{countryDatabasePrefix_cold}{countryId}.accdb");
            bool wasColdLocal = !File.Exists(localDbPath_cold);

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

            // Augment cold-local if table exists but is empty (fresh DB): skip sync as well
            if (!wasColdLocal)
            {
                try
                {
                    if (await IsLocalReconciliationEmptyAsync(countryId))
                    {
                        wasColdLocal = true;
                    }
                }
                catch { }
            }

            // 2) Synchronisation complète (PUSH puis PULL) des tables configurées (ici: T_Reconciliation)
            //    Évite tout push fire-and-forget et garantit que la base locale est alignée proprement.
            //    IMPORTANT: si la base locale était absente (cold local), ne PAS faire d'incrémental, on vient de copier depuis le réseau.
            if (!suppressPush && !wasColdLocal)
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
            else
            {
                if (wasColdLocal)
                {
                    // Just hydrated local DB from network copy; skip incremental updates.
                    onProgress?.Invoke(45, "Synchronisation ignorée (base locale initialisée depuis le réseau)");
                }
            }

            // 3) Après le push+pull, ne plus recouvrir Reconciliation local depuis réseau.
            //    La synchronisation a déjà aligné local et réseau pour T_Reconciliation.
            //    Invalider le cache UI pour forcer un rechargement frais lors du prochain Refresh().
            try { ReconciliationService.InvalidateReconciliationViewCache(countryId); } catch { }
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
        /// S'il existe un répertoire de modèles (paramètre 'Template' ou 'TemplateDirectory'),
        /// copie les fichiers contenant 'XX' en remplaçant par le code pays vers le répertoire réseau
        /// (CountryDatabaseDirectory) sans écraser les fichiers existants.
        /// Exemple attendu: DB_XX.accdb, DB_XX_lock.accdb, AMBRE_XX.accdb, AMBRE_XX.zip, etc.
        /// </summary>
        private async Task ProvisionNetworkFromTemplatesAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;
            string remoteDir = null;
            string templateDir = null;
            try
            {
                remoteDir = GetParameter("CountryDatabaseDirectory");
                templateDir = GetParameter("Template");
                if (string.IsNullOrWhiteSpace(templateDir)) templateDir = GetCentralConfig("Template");
                if (string.IsNullOrWhiteSpace(templateDir)) templateDir = GetParameter("TemplateDirectory");
            }
            catch { }

            if (string.IsNullOrWhiteSpace(remoteDir) || string.IsNullOrWhiteSpace(templateDir)) return;
            if (!Directory.Exists(templateDir)) return;

            try { Directory.CreateDirectory(remoteDir); } catch { }

            // Copier tous les fichiers contenant 'XX' (accdb/zip), en remplaçant par le code pays
            string[] patterns = new[] { "*XX*.accdb", "*XX*.zip" };
            foreach (var pattern in patterns)
            {
                string[] files = Array.Empty<string>();
                try { files = Directory.GetFiles(templateDir, pattern, SearchOption.TopDirectoryOnly); } catch { }
                foreach (var src in files)
                {
                    try
                    {
                        var destName = Path.GetFileName(src).Replace("XX", countryId);
                        var destPath = Path.Combine(remoteDir, destName);
                        if (File.Exists(destPath)) continue; // ne pas écraser
                        // copie asynchrone best-effort
                        await CopyFileAsync(src, destPath, overwrite: false);
                    }
                    catch { }
                }
            }
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
                using (var connection = new OleDbConnection(AceConn(databasePath)))
                {
                    await connection.OpenAsync();
                    return false; // ouverture exclusive OK => non verrouillée
                }
            }
            catch
            {
                // Toute exception lors de l'ouverture exclusive => considérer comme verrouillée
                return true;
            }
        }

        /// <summary>
        /// Indique si la table locale T_Reconciliation est vide pour le pays donné.
        /// Utilisé pour traiter un cas "cold-local" même si le fichier existe (DB fraîche).
        /// </summary>
        private async Task<bool> IsLocalReconciliationEmptyAsync(string countryId)
        {
            try
            {
                var connStr = GetCountryConnectionString(countryId);
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync();
                    // Vérifier l'existence de la table
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool hasTable = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), "T_Reconciliation", StringComparison.OrdinalIgnoreCase));
                    if (!hasTable) return true; // considérer vide si la table n'existe pas

                    using (var cmd = new OleDbCommand("SELECT COUNT(*) FROM [T_Reconciliation]", connection))
                    {
                        var obj = await cmd.ExecuteScalarAsync();
                        int count = 0;
                        if (obj != null && obj != DBNull.Value)
                            count = Convert.ToInt32(obj, System.Globalization.CultureInfo.InvariantCulture);
                        return count == 0;
                    }
                }
            }
            catch
            {
                // En cas d'erreur, rester conservateur et retourner false pour ne pas masquer d'autres problèmes
                return false;
            }
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

            // AMBRE est un instantané lecture seule côté client. Même s'il y a des changements locaux en attente
            // (liés à la table de réconciliation), on doit toujours rafraîchir AMBRE depuis le réseau.
            // On retire donc le blocage ici.

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

                    var netZipFi = new FileInfo(networkZipPath);
                    var locZipFi = new FileInfo(localZipPath);
                    bool needZipCopy = !locZipFi.Exists || !FilesAreEqual(locZipFi, netZipFi);

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
                        // Normalize destination timestamp to source (UTC) to prevent false mismatch
                        try { File.SetLastWriteTimeUtc(localZipPath, netZipFi.LastWriteTimeUtc); } catch { }

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
                System.Diagnostics.Debug.WriteLine($"AMBRE: échec gestion ZIP ({ex.Message}). Bascule sur copie réseau.");
            }

            // 1) Fallback: copie brute .accdb réseau -> local
            string networkDbPath = GetNetworkAmbreDbPath(countryId);
            if (!File.Exists(networkDbPath))
                throw new FileNotFoundException($"Base AMBRE réseau introuvable pour {countryId}", networkDbPath);

            if (await IsDatabaseLockedAsync(networkDbPath))
                throw new IOException($"La base AMBRE réseau est verrouillée: {networkDbPath}");

            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);

            // Copier uniquement si le fichier réseau diffère du local
            var netFi = new FileInfo(networkDbPath);
            var locFi = new FileInfo(localDbPath);
            bool needCopy = !locFi.Exists || !FilesAreEqual(locFi, netFi);
            if (!needCopy)
            {
                System.Diagnostics.Debug.WriteLine($"AMBRE: local à jour pour {countryId} (aucune copie nécessaire)");
                return;
            }

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
            // Previous behavior threw an exception here; we now log and return silently to avoid noisy errors
            // during country initialization. The full sync path (push+pull) already aligns reconciliation data.
            if (await HasUnsyncedLocalChangesAsync(countryId))
            {
                try { System.Diagnostics.Debug.WriteLine($"RECON: refresh skipped due to pending local changes for {countryId}"); } catch { }
                return;
            }

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

            // DWINGS est un instantané lecture seule côté client. On rafraîchit toujours depuis le réseau
            // quand le ZIP (ou l'accdb) réseau diffère de la version locale.

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

            var connStr = AceConn(targetDbPath);
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

                    // Detect remote changes since last sync to avoid false no-op (prefer Version watermark; fallback to LastModified)
                    bool remoteHasChanges = false;
                    try
                    {
                        var remotePath = GetNetworkReconciliationDbPath(_currentCountryId);
                        if (!string.IsNullOrWhiteSpace(remotePath) && File.Exists(remotePath))
                        {
                            // Read watermarks from local control DB (_SyncConfig)
                            DateTime lastSync = DateTime.MinValue;
                            long lastSyncVersion = -1;
                            try
                            {
                                using (var ctrl = new OleDbConnection(GetControlConnectionString()))
                                {
                                    await ctrl.OpenAsync();
                                    // Ensure _SyncConfig exists
                                    var schema = ctrl.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                                    bool has = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), "_SyncConfig", StringComparison.OrdinalIgnoreCase));
                                    if (!has)
                                    {
                                        using (var create = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", ctrl))
                                            await create.ExecuteNonQueryAsync();
                                    }
                                    // Try version first
                                    try
                                    {
                                        using (var verCmd = new OleDbCommand("SELECT ConfigValue FROM _SyncConfig WHERE ConfigKey = 'LastSyncVersion'", ctrl))
                                        using (var r = await verCmd.ExecuteReaderAsync())
                                        {
                                            if (await r.ReadAsync())
                                            {
                                                var val = r.IsDBNull(0) ? null : r.GetString(0);
                                                if (!string.IsNullOrWhiteSpace(val)) long.TryParse(val, out lastSyncVersion);
                                            }
                                        }
                                    }
                                    catch { }
                                    using (var cmd = new OleDbCommand("SELECT ConfigValue FROM _SyncConfig WHERE ConfigKey = 'LastSyncTimestamp'", ctrl))
                                    using (var reader = await cmd.ExecuteReaderAsync())
                                    {
                                        if (await reader.ReadAsync())
                                        {
                                            var val = reader.IsDBNull(0) ? null : reader.GetString(0);
                                            if (!string.IsNullOrWhiteSpace(val))
                                            {
                                                DateTime parsed;
                                                if (DateTime.TryParse(val, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out parsed))
                                                    lastSync = parsed.ToUniversalTime();
                                            }
                                        }
                                    }
                                }
                            }
                            catch { }

                            // Quick count on remote using Version watermark if available; else LastModified timestamp
                            try
                            {
                                string conn = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={remotePath};Persist Security Info=False;";
                                using (var remote = new OleDbConnection(conn))
                                {
                                    await remote.OpenAsync();
                                    if (lastSyncVersion >= 0)
                                    {
                                        using (var countCmd = new OleDbCommand("SELECT COUNT(*) FROM T_Reconciliation WHERE Version > ?", remote))
                                        {
                                            // Access/ACE is sensitive to parameter types. Use explicit Integer for Version
                                            var p = new OleDbParameter("@p1", OleDbType.Integer) { Value = lastSyncVersion };
                                            countCmd.Parameters.Add(p);
                                            var obj = await countCmd.ExecuteScalarAsync();
                                            int cnt;
                                            if (obj != null && int.TryParse(Convert.ToString(obj), out cnt))
                                                remoteHasChanges = cnt > 0;
                                        }
                                    }
                                    else
                                    {
                                        using (var countCmd = new OleDbCommand("SELECT COUNT(*) FROM T_Reconciliation WHERE LastModified > ?", remote))
                                        {
                                            // Access/ACE: use explicit Date type for DateTime parameters
                                            // Use Access lower bound (1899-12-30) when no lastSync available
                                            var lowerBound = new DateTime(1899, 12, 30);
                                            var dt = lastSync == DateTime.MinValue ? lowerBound : lastSync;
                                            var p = new OleDbParameter("@p1", OleDbType.Date) { Value = dt };
                                            countCmd.Parameters.Add(p);
                                            var obj = await countCmd.ExecuteScalarAsync();
                                            int cnt;
                                            if (obj != null && int.TryParse(Convert.ToString(obj), out cnt))
                                                remoteHasChanges = cnt > 0;
                                        }
                                    }
                                }
                            }
                            catch { }
                        }
                    }
                    catch { }

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
                                // If files are identical and there are no local pending changes, we can safely skip sync regardless of remoteHasChanges
                                // This avoids redundant row-level reapplication after a fresh local copy or after restart
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
                        // Persist LastSyncTimestamp in _SyncConfig (ISO UTC)
                        try
                        {
                            var iso = _lastSyncTimes[_currentCountryId].ToString("o");
                            using (var connection = new OleDbConnection(GetControlConnectionString()))
                            {
                                await connection.OpenAsync();
                                // Ensure _SyncConfig exists locally
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

                                // Also persist LastSyncVersion if the remote table supports it
                                try
                                {
                                    var remotePath = GetNetworkReconciliationDbPath(_currentCountryId);
                                    if (!string.IsNullOrWhiteSpace(remotePath) && File.Exists(remotePath))
                                    {
                                        var connStr = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={remotePath};Persist Security Info=False;";
                                        long maxVer = -1;
                                        using (var remote = new OleDbConnection(connStr))
                                        {
                                            await remote.OpenAsync();
                                            maxVer = await OleDbUtils.GetMaxVersionAsync(remote, "T_Reconciliation");
                                        }

                                        if (maxVer >= 0)
                                        {
                                            using (var up = new OleDbCommand("UPDATE _SyncConfig SET ConfigValue = @v WHERE ConfigKey = 'LastSyncVersion'", connection))
                                            {
                                                up.Parameters.Add(new OleDbParameter("@v", OleDbType.LongVarWChar) { Value = maxVer.ToString() });
                                                int rows = await up.ExecuteNonQueryAsync();
                                                if (rows == 0)
                                                {
                                                    using (var ins = new OleDbCommand("INSERT INTO _SyncConfig (ConfigKey, ConfigValue) VALUES ('LastSyncVersion', @v)", connection))
                                                    {
                                                        ins.Parameters.Add(new OleDbParameter("@v", OleDbType.LongVarWChar) { Value = maxVer.ToString() });
                                                        await ins.ExecuteNonQueryAsync();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                catch { }
                            }
                        }
                        catch { }
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
