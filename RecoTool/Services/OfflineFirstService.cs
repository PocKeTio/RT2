using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Models;
using OfflineFirstAccess.Models;
using OfflineFirstAccess.Synchronization;
using System.Threading;
using System.Collections.Concurrent;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Timers;

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

        #region Lock Helpers

        private string GetRemoteLockConnectionString(string countryId)
        {
            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string lockPath = Path.Combine(remoteDir, $"{countryDatabasePrefix}{countryId}_lock.accdb");
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={lockPath};";
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
        /// Applique en lot des ajouts/mises à jour/archivages dans une seule connexion et transaction.
        /// Réduit drastiquement le coût des imports volumineux.
        /// </summary>
        public async Task<bool> ApplyEntitiesBatchAsync(string identifier, List<Entity> toAdd, List<Entity> toUpdate, List<Entity> toArchive)
        {
            EnsureInitialized();
            toAdd = toAdd ?? new List<Entity>();
            toUpdate = toUpdate ?? new List<Entity>();
            toArchive = toArchive ?? new List<Entity>();

            if (toAdd.Count == 0 && toUpdate.Count == 0 && toArchive.Count == 0)
                return true;

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                using (var tx = connection.BeginTransaction())
                {
                    var changeTuples = new List<(string TableName, string RecordId, string OperationType)>();
                    // Caches must be declared outside try to be visible in finally
                    var tableColsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
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

                            var validCols = entity.Properties.Keys.Where(k => cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                            if (validCols.Count == 0) continue;
                            var sig = $"{entity.TableName}||{string.Join("|", validCols)}";
                            if (!insertCmdCache.TryGetValue(sig, out var tup))
                            {
                                var colList = string.Join(", ", validCols.Select(c => $"[{c}]"));
                                var paramList = string.Join(", ", validCols.Select((c, i) => $"@p{i}"));
                                var sql = $"INSERT INTO [{entity.TableName}] ({colList}) VALUES ({paramList})";
                                var cmd = new OleDbCommand(sql, connection, tx);
                                for (int i = 0; i < validCols.Count; i++)
                                {
                                    // Initialize parameters once
                                    cmd.Parameters.AddWithValue($"@p{i}", DBNull.Value);
                                }
                                insertCmdCache[sig] = (cmd, validCols.ToList());
                                tup = insertCmdCache[sig];
                            }
                            // Set parameter values for this row
                            for (int i = 0; i < tup.Cols.Count; i++)
                            {
                                var prepared = PrepareValueForDatabase(entity.Properties[tup.Cols[i]]);
                                tup.Cmd.Parameters[i].Value = prepared;
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
                            changeTuples.Add((entity.TableName, chKey, "INSERT"));
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

                            var upSig = $"{entity.TableName}||{string.Join("|", updatable)}||{keyColumn}";
                            if (!updateCmdCache.TryGetValue(upSig, out var upd))
                            {
                                var setList = string.Join(", ", updatable.Select((c, i) => $"[{c}] = @p{i}"));
                                var sql = $"UPDATE [{entity.TableName}] SET {setList} WHERE [{keyColumn}] = @key";
                                var cmd = new OleDbCommand(sql, connection, tx);
                                for (int i = 0; i < updatable.Count; i++)
                                {
                                    cmd.Parameters.AddWithValue($"@p{i}", DBNull.Value);
                                }
                                // key parameter at the end
                                cmd.Parameters.AddWithValue("@key", DBNull.Value);
                                updateCmdCache[upSig] = (cmd, updatable.ToList(), updatable.Count);
                                upd = updateCmdCache[upSig];
                            }
                            // Set parameters for this row
                            for (int i = 0; i < upd.Cols.Count; i++)
                            {
                                var prepared = PrepareValueForDatabase(entity.Properties[upd.Cols[i]]);
                                upd.Cmd.Parameters[i].Value = prepared;
                            }
                            upd.Cmd.Parameters[upd.KeyIndex].Value = keyValue ?? DBNull.Value;
                            await upd.Cmd.ExecuteNonQueryAsync();
                            string chKey = keyValue?.ToString();
                            changeTuples.Add((entity.TableName, chKey, "UPDATE"));
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
                                    // Prepare parameters in fixed order if present
                                    if (hasDeleteDate) cmd.Parameters.AddWithValue("@p0", PrepareValueForDatabase(nowUtc));
                                    if (lastModCol != null) cmd.Parameters.AddWithValue("@p1", PrepareValueForDatabase(nowUtc));
                                    cmd.Parameters.AddWithValue("@key", DBNull.Value); // placeholder, set per row
                                    archiveCmdCache[entity.TableName] = cmd;
                                }
                                // Update parameter values per row
                                int baseIndex = 0;
                                if (hasDeleteDate)
                                {
                                    cmd.Parameters[baseIndex++].Value = PrepareValueForDatabase(nowUtc);
                                }
                                if (lastModCol != null)
                                {
                                    cmd.Parameters[baseIndex++].Value = PrepareValueForDatabase(nowUtc);
                                }
                                cmd.Parameters[baseIndex].Value = keyValue ?? DBNull.Value; // @key
                                await cmd.ExecuteNonQueryAsync();
                            }
                            else
                            {
                                var sql = $"DELETE FROM [{entity.TableName}] WHERE [{keyColumn}] = @key";
                                using (var cmd = new OleDbCommand(sql, connection, tx))
                                {
                                    cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                            string chKey = keyValue?.ToString();
                            changeTuples.Add((entity.TableName, chKey, "DELETE"));
                        }

                        tx.Commit();

                        // Batch-change tracking (use same target as existing per-row calls for consistency)
                        var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId));
                        await tracker.RecordChangesAsync(changeTuples);
                        return true;
                    }
                    catch
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
                        try
                        {
                            var newExpiry = DateTime.UtcNow.AddSeconds(_expirySeconds);
                            using (var conn = new OleDbConnection(_connStr))
                            {
                                conn.Open();
                                using (var cmd = new OleDbCommand("UPDATE SyncLocks SET ExpiresAt = ? WHERE LockID = ?", conn))
                                {
                                    cmd.Parameters.AddWithValue("@ExpiresAt", newExpiry.ToOADate());
                                    cmd.Parameters.AddWithValue("@LockID", _lockId);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                        catch { /* best-effort */ }
                    };
                    _heartbeat.Start();
                }
                catch { /* best-effort */ }
            }

            public void Dispose()
            {
                if (_released) return;
                try
                {
                    try { _heartbeat?.Stop(); _heartbeat?.Dispose(); } catch { }
                    using (var conn = new OleDbConnection(_connStr))
                    {
                        conn.Open();
                        using (var cmd = new OleDbCommand("DELETE FROM SyncLocks WHERE LockID = ?", conn))
                        {
                            cmd.Parameters.AddWithValue("@LockID", _lockId);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch { /* best-effort */ }
                finally { _released = true; }
            }
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
                            cleanup.Parameters.AddWithValue("@ExpiresAt", now.ToOADate());
                            await cleanup.ExecuteNonQueryAsync();
                        }

                        // Purge stale self-locks: same machine, process no longer alive
                        try
                        {
                            using (var selectSelf = new OleDbCommand("SELECT LockID, ProcessId FROM SyncLocks WHERE (ExpiresAt IS NULL OR ExpiresAt > ?) AND MachineName = ?", connection))
                            {
                                selectSelf.Parameters.AddWithValue("@Now", now.ToOADate());
                                selectSelf.Parameters.AddWithValue("@MachineName", Environment.MachineName);
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
                                            del.Parameters.AddWithValue("@LockID", id);
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
                            check.Parameters.AddWithValue("@Now", now.ToOADate());
                            var countObj = await check.ExecuteScalarAsync();
                            int active = 0;
                            if (countObj != null && countObj != DBNull.Value)
                                active = Convert.ToInt32(countObj);

                            if (active == 0)
                            {
                                // Try to acquire the lock
                                using (var command = new OleDbCommand("INSERT INTO SyncLocks (LockID, Reason, CreatedAt, ExpiresAt, MachineName, ProcessId) VALUES (?, ?, ?, ?, ?, ?)", connection))
                                {
                                    command.Parameters.AddWithValue("@LockID", lockId);
                                    command.Parameters.AddWithValue("@Reason", reason ?? "Global");
                                    command.Parameters.AddWithValue("@CreatedAt", now.ToOADate());
                                    command.Parameters.AddWithValue("@ExpiresAt", expiresAt.ToOADate());
                                    command.Parameters.AddWithValue("@MachineName", Environment.MachineName);
                                    command.Parameters.AddWithValue("@ProcessId", System.Diagnostics.Process.GetCurrentProcess().Id);

                                    await command.ExecuteNonQueryAsync();
                                }

                                // Best-effort initial status
                                try
                                {
                                    using (var set = new OleDbCommand("UPDATE SyncLocks SET SyncStatus = ? WHERE LockID = ?", connection))
                                    {
                                        set.Parameters.AddWithValue("@SyncStatus", "Acquired");
                                        set.Parameters.AddWithValue("@LockID", lockId);
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
                        cleanup.Parameters.AddWithValue("@ExpiresAt", now.ToOADate());
                        await cleanup.ExecuteNonQueryAsync();
                    }

                    using (var check = new OleDbCommand("SELECT COUNT(*) FROM SyncLocks WHERE ExpiresAt IS NULL OR ExpiresAt > ?", connection))
                    {
                        check.Parameters.AddWithValue("@Now", now.ToOADate());
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
                        cmd.Parameters.AddWithValue("@SyncStatus", status ?? "Unknown");
                        cmd.Parameters.AddWithValue("@MachineName", Environment.MachineName);
                        cmd.Parameters.AddWithValue("@ProcessId", System.Diagnostics.Process.GetCurrentProcess().Id);
                        cmd.Parameters.AddWithValue("@Now", now.ToOADate());
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
                        cmd.Parameters.AddWithValue("@MachineName", Environment.MachineName);
                        cmd.Parameters.AddWithValue("@ProcessId", System.Diagnostics.Process.GetCurrentProcess().Id);
                        cmd.Parameters.AddWithValue("@Now", now.ToOADate());
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
        private bool _isWatching = false;

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
                        int pi = 0;
                        foreach (var p in parameters)
                        {
                            var prepared = PrepareValueForDatabase(p);
                            cmd.Parameters.AddWithValue($"@p{pi++}", prepared);
                        }
                        cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                        var affected = await cmd.ExecuteNonQueryAsync();
                        string changeKey = keyValue?.ToString();
                        if (changeLogSession != null)
                            await changeLogSession.AddAsync(entity.TableName, changeKey, "DELETE");
                        else
                            await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "DELETE");
                        return affected > 0;
                    }
                }
                else
                {
                    // Hard delete fallback
                    var sql = $"DELETE FROM [{entity.TableName}] WHERE [{keyColumn}] = @key";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                        var affected = await cmd.ExecuteNonQueryAsync();
                        string changeKey = keyValue?.ToString();
                        if (changeLogSession != null)
                            await changeLogSession.AddAsync(entity.TableName, changeKey, "DELETE");
                        else
                            await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "DELETE");
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
                            cmd.Parameters.AddWithValue($"@p{i++}", kv.Value ?? DBNull.Value);
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
            EnsureInitialized();
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(countryId));
            return await tracker.BeginSessionAsync();
        }

        /// <summary>
        /// Returns the number of unsynchronized change-log entries for the specified country
        /// from the remote lock database (where ChangeLog resides).
        /// </summary>
        public async Task<int> GetUnsyncedChangeCountAsync(string countryId)
        {
            EnsureInitialized();
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(countryId));
            var entries = await tracker.GetUnsyncedChangesAsync();
            return entries?.Count() ?? 0;
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

        private async Task<string> GetPrimaryKeyColumnAsync(OleDbConnection connection, string tableName)
        {
            using (var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, tableName }))
            {
                if (schema != null && schema.Rows.Count > 0)
                {
                    // Assume single-column PK
                    return await Task.FromResult(schema.Rows[0]["COLUMN_NAME"].ToString());
                }
            }
            return await Task.FromResult<string>(null);
        }

        private async Task<string> GetPrimaryKeyValueAsync(OleDbConnection connection, Entity entity)
        {
            var pk = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
            if (entity.Properties.ContainsKey(pk)) return entity.Properties[pk]?.ToString();
            return null;
        }

        /// <summary>
        /// Démarre la surveillance des changements
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <returns>True si démarré avec succès</returns>
        public bool StartWatching(string identifier)
        {
            // Implémentation minimale: active un flag de surveillance
            // La logique avancée peut être branchée sur SynchronizationService si nécessaire
            _isWatching = true;
            return true;
        }

        /// <summary>
        /// Arrête la surveillance des changements
        /// </summary>
        /// <returns>True si arrêté avec succès</returns>
        public bool StopWatching()
        {
            _isWatching = false;
            return true;
        }

        /// <summary>
        /// Obtient la date de dernière synchronisation
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <returns>Date de dernière synchronisation ou null</returns>
        public DateTime? GetLastSyncTime(string identifier)
        {
            return _lastSyncTimes.TryGetValue(identifier, out DateTime lastSync) ? lastSync : (DateTime?)null;
        }

        /// <summary>
        /// Acquiert un verrou global pour empêcher la synchronisation pendant des opérations critiques
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="reason">Raison du verrouillage</param>
        /// <param name="timeout">Délai d'attente</param>
        /// <param name="token">Token d'annulation</param>
        /// <returns>Un objet IDisposable qui libère le verrou lorsqu'il est disposé</returns>
        public async Task<IDisposable> AcquireGlobalLockAsync(string identifier, string reason, TimeSpan timeout, CancellationToken token = default)
        {
            int timeoutSeconds = (int)timeout.TotalSeconds;
            return await AcquireGlobalLockInternalAsync(identifier, reason, timeoutSeconds, token);
        }

        /// <summary>
        /// Acquiert un verrou global pour empêcher la synchronisation pendant des opérations critiques
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="reason">Raison du verrouillage</param>
        /// <param name="timeoutSeconds">Délai d'attente en secondes (0 pour pas d'expiration)</param>
        /// <param name="token">Token d'annulation</param>
        /// <returns>Un objet IDisposable qui libère le verrou lorsqu'il est disposé</returns>
        public async Task<IDisposable> AcquireGlobalLockAsync(string identifier, string reason, int timeoutSeconds = 0, CancellationToken token = default)
        {
            return await AcquireGlobalLockInternalAsync(identifier, reason, timeoutSeconds, token);
        }

        #endregion
        
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
            System.Diagnostics.Debug.WriteLine($"  - DWDatabasePath: {GetParameter("DWDatabasePath")}");
            System.Diagnostics.Debug.WriteLine($"  - CountryDatabaseDirectory: {GetParameter("CountryDatabaseDirectory")}");
            System.Diagnostics.Debug.WriteLine($"  - CountryDatabasePrefix: {GetParameter("CountryDatabasePrefix")}");

            // Récupérer et charger la dernière country utilisée
            SetCurrentCountryAsync(GetParameter("LastCountryUsed")).Wait();
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
                throw new InvalidOperationException("Chemin local de la base DW introuvable (vérifier T_Param: DataDirectory et DWDatabasePath)");
            }
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={localDwPath};";
        }

        /// <summary>
        /// Retourne le chemin local attendu pour la base DW (même nom de fichier que la base réseau, dans DataDirectory)
        /// </summary>
        public string GetLocalDWDatabasePath()
        {
            string dwDatabasePath = GetParameter("DWDatabasePath");
            string dataDirectory = GetParameter("DataDirectory");
            if (string.IsNullOrEmpty(dwDatabasePath) || string.IsNullOrEmpty(dataDirectory))
            {
                return null;
            }
            var fileName = System.IO.Path.GetFileName(dwDatabasePath);
            return System.IO.Path.Combine(dataDirectory, fileName);
        }

        /// <summary>
        /// Copie la base DW du réseau vers le local si nécessaire (nouvelle ou plus récente)
        /// </summary>
        /// <param name="onProgress">Callback progression (0-100, message)</param>
        public async Task EnsureLocalDWCopyAsync(Action<int, string> onProgress = null)
        {
            try
            {
                string remotePath = GetParameter("DWDatabasePath");
                string localPath = GetLocalDWDatabasePath();
                if (string.IsNullOrEmpty(remotePath) || string.IsNullOrEmpty(localPath))
                    throw new InvalidOperationException("DWDatabasePath ou DataDirectory non configuré.");

                if (!File.Exists(remotePath))
                    throw new FileNotFoundException($"Base DW distante introuvable: {remotePath}");

                var remoteInfo = new FileInfo(remotePath);
                var needCopy = !File.Exists(localPath);

                if (!needCopy)
                {
                    var localInfo = new FileInfo(localPath);
                    needCopy = remoteInfo.LastWriteTimeUtc > localInfo.LastWriteTimeUtc;
                }

                if (!needCopy)
                {
                    onProgress?.Invoke(100, "Base DW locale à jour");
                    return;
                }

                Directory.CreateDirectory(Path.GetDirectoryName(localPath));
                onProgress?.Invoke(0, "Préparation de la copie de la base DW...");

                const int bufferSize = 1024 * 1024; // 1 MB
                long totalBytes = remoteInfo.Length;
                long copied = 0;

                using (var source = new FileStream(remotePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var dest = new FileStream(localPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    var buffer = new byte[bufferSize];
                    int read;
                    while ((read = await source.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        await dest.WriteAsync(buffer, 0, read);
                        copied += read;
                        int percent = totalBytes > 0 ? (int)(copied * 100 / totalBytes) : 0;
                        onProgress?.Invoke(percent, $"Copie de la base DW... {percent}%");
                    }
                }

                // Align last write time
                File.SetLastWriteTimeUtc(localPath, remoteInfo.LastWriteTimeUtc);
                onProgress?.Invoke(100, "Copie DW terminée");
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
                    cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
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

                for (int i = 0; i < distinctKeys.Count; i += batchSize)
                {
                    var chunk = distinctKeys.Skip(i).Take(batchSize).ToList();
                    var placeholders = string.Join(", ", chunk.Select((_, idx) => $"@p{idx}"));
                    var sql = $"SELECT [{keyColumn}] FROM [{tableName}] WHERE [{keyColumn}] IN ({placeholders})";
                    using (var cmd = new OleDbCommand(sql, connection))
                    {
                        foreach (var (val, idx) in chunk.Select((v, idx) => (v, idx)))
                        {
                            cmd.Parameters.AddWithValue($"@p{idx}", val);
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
                            cmd.Parameters.AddWithValue($"@p{idx}", val);
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
                    for (int i = 0; i < validCols.Count; i++)
                    {
                        var prepared = PrepareValueForDatabase(entity.Properties[validCols[i]]);
                        cmd.Parameters.AddWithValue($"@p{i}", prepared);
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
                        await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "INSERT");

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
                    for (int i = 0; i < updatable.Count; i++)
                    {
                        var prepared = PrepareValueForDatabase(entity.Properties[updatable[i]]);
                        cmd.Parameters.AddWithValue($"@p{i}", prepared);
                    }
                    cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                    var affected = await cmd.ExecuteNonQueryAsync();

                    string changeKey = keyValue?.ToString();
                    if (changeLogSession != null)
                        await changeLogSession.AddAsync(entity.TableName, changeKey, "UPDATE");
                    else
                        await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "UPDATE");

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

        /// <summary>
        /// Définit le pays courant et initialise la configuration/synchronisation locale si nécessaire.
        /// </summary>
        public async Task<bool> SetCurrentCountryAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId est requis", nameof(countryId));

            // Mettre à jour le pays courant depuis le référentiel si disponible
            lock (_referentialLock)
            {
                _currentCountry = _countries?.FirstOrDefault(c => string.Equals(c?.CNT_Id, countryId, StringComparison.OrdinalIgnoreCase))
                                  ?? _currentCountry; // conserver si non trouvé
            }

            // Construire la configuration et initialiser les couches locales
            _syncConfig = BuildSyncConfiguration(countryId);
            _syncService = new SynchronizationService();

            var initialized = await InitializeLocalDatabaseAsync(countryId);
            if (initialized)
            {
                _currentCountryId = countryId;

                // Au démarrage/changement de pays, pousser d'éventuels changements en attente
                try
                {
                    var pending = await GetUnsyncedChangeCountAsync(countryId);
                    if (pending > 0)
                    {
                        System.Diagnostics.Debug.WriteLine($"Pending changes detected ({pending}) for {countryId} on startup. Pushing to network...");
                        await PushPendingChangesToNetworkAsync(countryId);
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Erreur lors du push des changements en attente pour {countryId} au démarrage: {ex.Message}");
                    // Ne pas interrompre l'initialisation si le push échoue; l'utilisateur pourra réessayer manuellement
                }
            }
            return initialized;
        }

        /// <summary>
        /// Construit la SyncConfiguration à partir des paramètres T_Param et d'un pays donné
        /// </summary>
        /// <param name="countryId">ID du pays</param>
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
                tables.Add("T_Data_Ambre");
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
            string backupPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb.bak");

            // Copier vers un fichier temporaire sur le même volume réseau
            File.Copy(localDbPath, tempPath, true);

            // Remplacer atomiquement la base réseau (ou déplacer si inexistant)
            if (File.Exists(networkDbPath))
            {
                // File.Replace est atomique et crée une sauvegarde
                File.Replace(tempPath, networkDbPath, backupPath);
            }
            else
            {
                File.Move(tempPath, networkDbPath);
            }

            System.Diagnostics.Debug.WriteLine($"Base locale publiée vers le réseau pour {countryId} -> {networkDbPath}");
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
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(countryId));
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
                File.Replace(tempLocal, localDbPath, backupLocal);
            }
            else
            {
                File.Move(tempLocal, localDbPath);
            }

            System.Diagnostics.Debug.WriteLine($"Base réseau copiée vers le local pour {countryId} -> {localDbPath}");
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
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={networkDbPath};";
        }

        /// <summary>
        /// Pousse de manière robuste les changements locaux en attente vers la base réseau sous verrou global.
        /// Applique INSERT/UPDATE/DELETE sur la base réseau à partir de l'état local pour chaque ChangeLog non synchronisé trouvé,
        /// puis marque uniquement ces entrées comme synchronisées. Ignore les entrées qui ne correspondent pas à une ligne locale.
        /// </summary>
        public async Task<int> PushPendingChangesToNetworkAsync(string countryId, bool assumeLockHeld = false, CancellationToken token = default)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // S'assurer que le service est positionné sur le bon pays (AcquireGlobalLockAsync utilise _currentCountryId)
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                var ok = await SetCurrentCountryAsync(countryId);
                if (!ok) throw new InvalidOperationException($"Impossible d'initialiser la base locale pour {countryId}");
            }

            // Récupérer les entrées non synchronisées depuis la base de lock
            var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(countryId));
            var unsynced = (await tracker.GetUnsyncedChangesAsync())?.ToList() ?? new List<OfflineFirstAccess.Models.ChangeLogEntry>();
            if (unsynced.Count == 0) return 0;

            // Acquérir le verrou global si non détenu par l'appelant
            IDisposable globalLock = null;
            if (!assumeLockHeld)
            {
                globalLock = await AcquireGlobalLockAsync(countryId, "PushPendingChanges", TimeSpan.FromMinutes(5), token);
                if (globalLock == null)
                    throw new InvalidOperationException($"Impossible d'acquérir le verrou global pour {countryId} (PushPendingChanges)");
            }
            try
            {

                var appliedIds = new List<long>();

                // Préparer connexions
                using (var localConn = new OleDbConnection(GetCountryConnectionString(countryId)))
                using (var netConn = new OleDbConnection(GetNetworkCountryConnectionString(countryId)))
                {
                    await localConn.OpenAsync();
                    await netConn.OpenAsync();

                    using (var tx = netConn.BeginTransaction())
                    {
                        try
                        {
                            // Caches de schéma
                            var tableColsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                            var pkColCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

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

                            foreach (var entry in unsynced)
                            {
                                if (token.IsCancellationRequested) break;
                                if (string.IsNullOrWhiteSpace(entry?.TableName) || string.IsNullOrWhiteSpace(entry?.RecordId)) continue;

                                var table = entry.TableName;
                                var op = (entry.OperationType ?? string.Empty).Trim().ToUpperInvariant();

                                var cols = await getColsAsync(table);
                                if (cols == null || cols.Count == 0) continue;
                                var pkCol = await getPkAsync(table);

                                // 1) Lire la ligne locale (si elle existe)
                                object localPkVal = entry.RecordId;
                                object Prepare(object v) => v ?? DBNull.Value;

                                Dictionary<string, object> localValues = null;
                                using (var lcCmd = new OleDbCommand($"SELECT * FROM [{table}] WHERE [{pkCol}] = @k", localConn))
                                {
                                    lcCmd.Parameters.AddWithValue("@k", localPkVal);
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
                                        var parameters = new List<object>();
                                        if (hasIsDeleted) setParts.Add($"[{_syncConfig.IsDeletedColumn}] = true");
                                        if (hasDeleteDate) { setParts.Add("[DeleteDate] = @p0"); parameters.Add(DateTime.UtcNow); }
                                        if (hasLastMod)
                                        {
                                            var col = cols.Contains(_syncConfig.LastModifiedColumn) ? _syncConfig.LastModifiedColumn : "LastModified";
                                            setParts.Add($"[{col}] = @p1"); parameters.Add(DateTime.UtcNow);
                                        }
                                        using (var cmd = new OleDbCommand($"UPDATE [{table}] SET {string.Join(", ", setParts)} WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            for (int i = 0; i < parameters.Count; i++) cmd.Parameters.AddWithValue($"@p{i}", Prepare(parameters[i]));
                                            cmd.Parameters.AddWithValue("@key", Prepare(localPkVal));
                                            await cmd.ExecuteNonQueryAsync();
                                        }
                                    }
                                    else
                                    {
                                        using (var cmd = new OleDbCommand($"DELETE FROM [{table}] WHERE [{pkCol}] = @key", netConn, tx))
                                        {
                                            cmd.Parameters.AddWithValue("@key", Prepare(localPkVal));
                                            await cmd.ExecuteNonQueryAsync();
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
                                        exCmd.Parameters.AddWithValue("@key", Prepare(localPkVal));
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
                                            for (int i = 0; i < allCols.Count; i++) up.Parameters.AddWithValue($"@p{i}", Prepare(localValues[allCols[i]]));
                                            up.Parameters.AddWithValue("@key", Prepare(localPkVal));
                                            await up.ExecuteNonQueryAsync();
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
                                            for (int i = 0; i < insertCols.Count; i++) ins.Parameters.AddWithValue($"@p{i}", Prepare(localValues[insertCols[i]]));
                                            await ins.ExecuteNonQueryAsync();
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
                            throw;
                        }
                    }
                }

                // Marquer uniquement les id appliqués
                if (appliedIds.Count > 0)
                {
                    await tracker.MarkChangesAsSyncedAsync(appliedIds);
                    // Rafraîchir local pour refléter l'état réseau après application des pending
                    await CopyNetworkToLocalAsync(countryId);
                }

                return appliedIds.Count;
            }
            finally
            {
                try { globalLock?.Dispose(); } catch { }
            }
        }
        
        /// <summary>
        /// Récupère les garanties DWINGS depuis la base DW
        /// </summary>
        /// <returns>Liste des garanties DWINGS</returns>
        public async Task<List<DWGuarantee>> GetDWGuaranteesAsync()
        {
            string query = "SELECT * FROM T_DW_Guarantee ORDER BY GUARANTEE_ID";
            
            return await QueryDWDatabaseAsync(query, reader => new DWGuarantee
            {
                GUARANTEE_ID = reader["GUARANTEE_ID"]?.ToString(),
                SYNDICATE = reader["SYNDICATE"]?.ToString(),
                CURRENCY = reader["CURRENCY"]?.ToString(),
                AMOUNT = reader["AMOUNT"]?.ToString(),
                OfficialID = reader["OfficialID"]?.ToString(),
                GuaranteeType = reader["GuaranteeType"]?.ToString(),
                Client = reader["Client"]?.ToString(),
                _791Sent = reader["791Sent"]?.ToString(),
                InvoiceStatus = reader["InvoiceStatus"]?.ToString(),
                TriggerDate = reader["TriggerDate"]?.ToString(),
                FXRate = reader["FXRate"]?.ToString(),
                RMPM = reader["RMPM"]?.ToString(),
                GroupName = reader["GroupName"]?.ToString()
            });
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
                T_COMMISSION_PERIOD_STAT = reader["T_COMMISSION_PERIOD_STAT"]?.ToString(),
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
                COMMISSION_ID = reader["COMMISSION_ID"]?.ToString(),
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
                SYNDICATE = reader["SYNDICATE"]?.ToString(),
                CURRENCY = reader["CURRENCY"]?.ToString(),
                AMOUNT = reader["AMOUNT"]?.ToString(),
                OfficialID = reader["OfficialID"]?.ToString(),
                GuaranteeType = reader["GuaranteeType"]?.ToString(),
                Client = reader["Client"]?.ToString(),
                _791Sent = reader["791Sent"]?.ToString(),
                InvoiceStatus = reader["InvoiceStatus"]?.ToString(),
                TriggerDate = reader["TriggerDate"]?.ToString(),
                FXRate = reader["FXRate"]?.ToString(),
                RMPM = reader["RMPM"]?.ToString(),
                GroupName = reader["GroupName"]?.ToString()
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
                var ok = await SetCurrentCountryAsync(countryId);
                if (!ok) throw new InvalidOperationException($"Impossible d'initialiser la base locale pour {countryId}");
            }
            EnsureInitialized();

            var list = new List<Entity>();
            using (var connection = new OleDbConnection(GetLocalConnectionString()))
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

            // S'assurer que la configuration est prête pour le pays demandé
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                var ok = await SetCurrentCountryAsync(countryId);
                if (!ok)
                    return new SyncResult { Success = false, Message = $"Initialisation impossible pour {countryId}" };
            }

            // Pause automatique de la synchronisation si un verrou d'import global est actif
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

            try
            {
                onProgress?.Invoke(0, "Initialisation de la synchronisation...");
                await _syncService.InitializeAsync(_syncConfig);

                var token = cancellationToken ?? default;
                onProgress?.Invoke(5, "Démarrage...");
                var result = await _syncService.SynchronizeAsync(onProgress);
                if (result != null && result.Success)
                {
                    _lastSyncTimes[_currentCountryId] = DateTime.UtcNow;
                }
                return result;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SYNC] Erreur: {ex.Message}");
                return new SyncResult { Success = false, Message = ex.Message };
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
                        StopWatching();
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
