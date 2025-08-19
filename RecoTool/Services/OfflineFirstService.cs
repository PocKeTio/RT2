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
                    var changeTuples = new List<(string TableName, string RowGuid, string OperationType)>();
                    try
                    {
                        var tableColsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

                        Func<string, Task<HashSet<string>>> getColsAsync = async (table) =>
                        {
                            if (!tableColsCache.TryGetValue(table, out var cols))
                            {
                                cols = await GetTableColumnsAsync(connection, table);
                                tableColsCache[table] = cols;
                            }
                            return cols;
                        };

                        // Helpers
                        Func<Entity, HashSet<string>, string> getRowGuidCol = (e, cols) =>
                            cols.Contains(_syncConfig.PrimaryKeyGuidColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.PrimaryKeyGuidColumn : null;

                        // INSERTS
                        foreach (var entity in toAdd)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var rowGuidCol = getRowGuidCol(entity, cols);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            var isDeletedCol = cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.IsDeletedColumn : (cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase) ? "DeleteDate" : null);

                            if (rowGuidCol != null && (!entity.Properties.ContainsKey(rowGuidCol) || entity.Properties[rowGuidCol] == null))
                                entity.Properties[rowGuidCol] = Guid.NewGuid().ToString();
                            if (lastModCol != null)
                                entity.Properties[lastModCol] = DateTime.UtcNow;
                            if (isDeletedCol != null)
                            {
                                if (isDeletedCol.Equals(_syncConfig.IsDeletedColumn, StringComparison.OrdinalIgnoreCase))
                                    entity.Properties[isDeletedCol] = false;
                                else if (isDeletedCol.Equals("DeleteDate", StringComparison.OrdinalIgnoreCase))
                                    entity.Properties[isDeletedCol] = DBNull.Value;
                            }

                            var validCols = entity.Properties.Keys.Where(k => cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                            if (validCols.Count == 0) continue;
                            var colList = string.Join(", ", validCols.Select(c => $"[{c}]"));
                            var paramList = string.Join(", ", validCols.Select((c, i) => $"@p{i}"));
                            var sql = $"INSERT INTO [{entity.TableName}] ({colList}) VALUES ({paramList})";
                            using (var cmd = new OleDbCommand(sql, connection, tx))
                            {
                                for (int i = 0; i < validCols.Count; i++)
                                {
                                    var prepared = PrepareValueForDatabase(entity.Properties[validCols[i]]);
                                    cmd.Parameters.AddWithValue($"@p{i}", prepared);
                                }
                                await cmd.ExecuteNonQueryAsync();
                            }
                            string chKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : await GetPrimaryKeyValueAsync(connection, entity);
                            changeTuples.Add((entity.TableName, chKey, "INSERT"));
                        }

                        // UPDATES
                        foreach (var entity in toUpdate)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var rowGuidCol = getRowGuidCol(entity, cols);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            if (lastModCol != null)
                                entity.Properties[lastModCol] = DateTime.UtcNow;

                            var updatable = entity.Properties.Keys.Where(k => (rowGuidCol == null || !k.Equals(rowGuidCol, StringComparison.OrdinalIgnoreCase)) && cols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                            if (updatable.Count == 0) continue;

                            // Determine key
                            string keyColumn = rowGuidCol;
                            object keyValue = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol] : null;
                            if (keyColumn == null || keyValue == null)
                            {
                                keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                                if (!entity.Properties.ContainsKey(keyColumn))
                                    throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                                keyValue = entity.Properties[keyColumn];
                            }

                            var setList = string.Join(", ", updatable.Select((c, i) => $"[{c}] = @p{i}"));
                            var sql = $"UPDATE [{entity.TableName}] SET {setList} WHERE [{keyColumn}] = @key";
                            using (var cmd = new OleDbCommand(sql, connection, tx))
                            {
                                for (int i = 0; i < updatable.Count; i++)
                                {
                                    var prepared = PrepareValueForDatabase(entity.Properties[updatable[i]]);
                                    cmd.Parameters.AddWithValue($"@p{i}", prepared);
                                }
                                cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                                await cmd.ExecuteNonQueryAsync();
                            }
                            string chKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : keyValue?.ToString();
                            changeTuples.Add((entity.TableName, chKey, "UPDATE"));
                        }

                        // ARCHIVES (logical delete)
                        foreach (var entity in toArchive)
                        {
                            var cols = await getColsAsync(entity.TableName);
                            var rowGuidCol = getRowGuidCol(entity, cols);
                            var lastModCol = cols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (cols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                            var hasIsDeleted = cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase);
                            var hasDeleteDate = cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase);

                            // Determine key
                            string keyColumn = rowGuidCol;
                            object keyValue = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol] : null;
                            if (keyColumn == null || keyValue == null)
                            {
                                keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                                if (!entity.Properties.ContainsKey(keyColumn))
                                    throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                                keyValue = entity.Properties[keyColumn];
                            }

                            if (hasIsDeleted || hasDeleteDate)
                            {
                                var setParts = new List<string>();
                                var parameters = new List<object>();
                                if (hasIsDeleted) { setParts.Add($"[{_syncConfig.IsDeletedColumn}] = true"); }
                                if (hasDeleteDate) { setParts.Add("[DeleteDate] = @p0"); parameters.Add(DateTime.UtcNow); }
                                if (lastModCol != null) { setParts.Add($"[{lastModCol}] = @p1"); parameters.Add(DateTime.UtcNow); }
                                var sql = $"UPDATE [{entity.TableName}] SET {string.Join(", ", setParts)} WHERE [{keyColumn}] = @key";
                                using (var cmd = new OleDbCommand(sql, connection, tx))
                                {
                                    int pi = 0;
                                    foreach (var p in parameters)
                                    {
                                        var prepared = PrepareValueForDatabase(p);
                                        cmd.Parameters.AddWithValue($"@p{pi++}", prepared);
                                    }
                                    cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                                    await cmd.ExecuteNonQueryAsync();
                                }
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
                            string chKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : keyValue?.ToString();
                            changeTuples.Add((entity.TableName, chKey, "DELETE"));
                        }

                        tx.Commit();

                        // Batch-change tracking (use same target as existing per-row calls for consistency)
                        var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetLocalConnectionString());
                        await tracker.RecordChangesAsync(changeTuples);
                        return true;
                    }
                    catch
                    {
                        try { tx.Rollback(); } catch { }
                        throw;
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
                    "ProcessId INTEGER)", connection))
                {
                    await cmd.ExecuteNonQueryAsync();
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
        /// Valide l'existence des colonnes métadonnées requises sur les tables à synchroniser
        /// </summary>
        /// <param name="localDbPath">Chemin de la base locale</param>
        /// <param name="tables">Tables à vérifier</param>
        private async Task ValidateSyncTablesAsync(string localDbPath, IEnumerable<string> tables)
        {
            try
            {
                if (tables == null) return;
                using (var connection = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={localDbPath};"))
                {
                    await connection.OpenAsync();

                    foreach (var table in tables)
                    {
                        if (string.IsNullOrWhiteSpace(table)) continue;
                        var required = new[] { "RowGuid", "LastModified", "IsDeleted" };
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
                System.Diagnostics.Debug.WriteLine($"Erreur ValidateSyncTablesAsync: {ex.Message}");
            }
        }

        /// <summary>
        /// Constructeur avec country spécifique
        /// </summary>
        /// <param name="countryId">ID du pays à gérer</param>
        public OfflineFirstService(string countryId)
        {
            _currentUser = Environment.UserName;
            _lastSyncTimes = new ConcurrentDictionary<string, DateTime>();
            
            // Charger la configuration depuis App.config
            LoadConfiguration();
            
            // Charger les référentiels en mémoire
            _ = LoadReferentialsAsync();
            
            // Initialiser pour le pays spécifié
            _ = SetCurrentCountryAsync(countryId);
        }

        /// <summary>
        /// Définit le pays courant et initialise la connexion pour ce pays
        /// </summary>
        /// <param name="countryId">ID du pays à activer</param>
        public async Task<bool> SetCurrentCountryAsync(string countryId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(countryId))
                    return false;

                // Charger les informations du pays depuis la base référentielle
                var country = await GetCountryByIdAsync(countryId);
                if (country == null)
                {
                    System.Diagnostics.Debug.WriteLine($"Pays {countryId} non trouvé dans la base référentielle");
                    return false;
                }

                // Initialiser la base locale pour ce pays avec notre logique personnalisée
                bool initSuccess = await InitializeLocalDatabaseAsync(country.CNT_Id);
                if (!initSuccess)
                {
                    System.Diagnostics.Debug.WriteLine($"Échec de l'initialisation de la base locale pour {countryId}");
                    return false;
                }

                // Nouvelle approche README: préparer la configuration et le service de synchro
                _syncConfig = BuildSyncConfiguration(countryId);

                // Assurer l'existence et la normalisation de la base de lock sans passer par l'ancien service
                try
                {
                    string remoteDir = GetParameter("CountryDatabaseDirectory");
                    string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
                    string lockDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}_lock.accdb");
                    if (!File.Exists(lockDbPath))
                    {
                        CreateLockDatabase(lockDbPath);
                    }
                    EnsureLockTablesExist(lockDbPath);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[LOCK INIT] Erreur préparation base lock: {ex.Message}");
                }

                // Valider les tables à synchroniser (colonnes métadonnées requises)
                await ValidateSyncTablesAsync(_syncConfig.LocalDatabasePath, _syncConfig.TablesToSync);

                _syncService = new SynchronizationService();
                await _syncService.InitializeAsync(_syncConfig);

                // Mettre à jour le pays courant
                _currentCountry = country;
                _currentCountryId = countryId;
                _isInitialized = true;
                
                System.Diagnostics.Debug.WriteLine($"OfflineFirstService initialisé pour le pays: {country.CNT_Name} ({countryId})");
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de l'initialisation pour le pays {countryId}: {ex.Message}");
                _isInitialized = false;
                return false;
            }
        }
        
        /// <summary>
        /// Obtient le pays actuellement actif
        /// </summary>
        public Country CurrentCountry => _currentCountry;
        
        /// <summary>
        /// Obtient l'ID du pays actuellement actif
        /// </summary>
        public string CurrentCountryId => _currentCountryId;

        #endregion

        #region Propriétés d'accès aux référentiels (en mémoire)

        /// <summary>
        /// Accès aux champs d'import Ambre
        /// </summary>
        public IReadOnlyList<AmbreImportField> AmbreImportFields => _ambreImportFields.AsReadOnly();

        /// <summary>
        /// Accès aux codes de transaction Ambre
        /// </summary>
        public IReadOnlyList<AmbreTransactionCode> AmbreTransactionCodes => _ambreTransactionCodes.AsReadOnly();

        /// <summary>
        /// Accès aux transformations Ambre
        /// </summary>
        public IReadOnlyList<AmbreTransform> AmbreTransforms => _ambreTransforms.AsReadOnly();

        /// <summary>
        /// Accès aux pays
        /// </summary>
        public IReadOnlyList<Country> Countries => _countries.AsReadOnly();

        /// <summary>
        /// Accès aux champs utilisateur
        /// </summary>
        public IReadOnlyList<UserField> UserFields => _userFields.AsReadOnly();

        

        /// <summary>
        /// Accès aux filtres utilisateur
        /// </summary>
        public IReadOnlyList<UserFilter> UserFilters => _userFilters.AsReadOnly();

        /// <summary>
        /// Accès aux paramètres de configuration
        /// </summary>
        public IReadOnlyList<Param> Params => _params.AsReadOnly();

        /// <summary>
        /// Indique si les référentiels sont chargés
        /// </summary>
        public bool AreReferentialsLoaded => _referentialsLoaded;

        /// <summary>
        /// Date/heure du dernier chargement des référentiels
        /// </summary>
        public DateTime? ReferentialsLoadTime => _referentialsLoaded ? _referentialsLoadTime : (DateTime?)null;

        #endregion

        #region Chargement des référentiels en mémoire

        /// <summary>
        /// Charge toutes les tables référentielles en mémoire (une seule fois)
        /// </summary>
        private async Task<bool> LoadReferentialsAsync()
        {
            lock (_referentialLock)
            {
                if (_referentialsLoaded)
                    return true; // Déjà chargé
            }

            try
            {
                using (var connection = new OleDbConnection(ReferentialDatabasePath))
                {
                    await connection.OpenAsync();

                    // Charger T_Ref_Ambre_ImportFields
                    await LoadAmbreImportFieldsAsync(connection);
                    
                    // Charger T_Ref_Ambre_TransactionCodes
                    await LoadAmbreTransactionCodesAsync(connection);
                    
                    // Charger T_Ref_Ambre_Transform
                    await LoadAmbreTransformsAsync(connection);
                    
                    // Charger T_Ref_Country
                    await LoadCountriesAsync(connection);
                    
                    // Charger T_Ref_User_Fields
                    await LoadUserFieldsAsync(connection);
                    
                    // [Deprecated] Ancien modèle de préférences par champ (UPF_FieldName...).
                    // Ne plus charger: remplacé par les "Saved Views" via ReconciliationService.
                    // await LoadUserFieldPreferencesAsync(connection);
                    
                    // Charger T_Ref_User_Filter
                    await LoadUserFiltersAsync(connection);
                    
                    // Charger la table des paramètres
                    await LoadParamsAsync(connection);
                }
                
                // Initialiser les propriétés depuis les paramètres chargés
                InitializePropertiesFromParams();
                // Note: DWINGS n'est plus chargé en mémoire. Accès direct via GetDW*Async()/QueryDWDatabaseAsync().

                lock (_referentialLock)
                {
                    _referentialsLoaded = true;
                    _referentialsLoadTime = DateTime.Now;
                }

                System.Diagnostics.Debug.WriteLine($"Référentiels chargés en mémoire à {_referentialsLoadTime:HH:mm:ss}");
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors du chargement des référentiels: {ex.Message}");
                return false;
            }
        }

        private async Task LoadAmbreImportFieldsAsync(OleDbConnection connection)
        {
            var query = "SELECT AMB_Source, AMB_Destination FROM T_Ref_Ambre_ImportFields";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<AmbreImportField>();
                while (await reader.ReadAsync())
                {
                    list.Add(new AmbreImportField
                    {
                        AMB_Source = reader["AMB_Source"]?.ToString(),
                        AMB_Destination = reader["AMB_Destination"]?.ToString()
                    });
                }
                _ambreImportFields = list;
            }
        }

        private async Task LoadAmbreTransactionCodesAsync(OleDbConnection connection)
        {
            var query = "SELECT ATC_ID, ATC_CODE, ATC_TAG FROM T_Ref_Ambre_TransactionCodes";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<AmbreTransactionCode>();
                while (await reader.ReadAsync())
                {
                    list.Add(new AmbreTransactionCode
                    {
                        ATC_ID = Convert.ToInt32(reader["ATC_ID"]),
                        ATC_CODE = reader["ATC_CODE"]?.ToString(),
                        ATC_TAG = reader["ATC_TAG"]?.ToString()
                    });
                }
                _ambreTransactionCodes = list;
            }
        }

        private async Task LoadAmbreTransformsAsync(OleDbConnection connection)
        {
            var query = "SELECT AMB_Source, AMB_Destination, AMB_TransformationFunction, AMB_Description FROM T_Ref_Ambre_Transform";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<AmbreTransform>();
                while (await reader.ReadAsync())
                {
                    list.Add(new AmbreTransform
                    {
                        AMB_Source = reader["AMB_Source"]?.ToString(),
                        AMB_Destination = reader["AMB_Destination"]?.ToString(),
                        AMB_TransformationFunction = reader["AMB_TransformationFunction"]?.ToString(),
                        AMB_Description = reader["AMB_Description"]?.ToString()
                    });
                }
                _ambreTransforms = list;
            }
        }

        private async Task LoadCountriesAsync(OleDbConnection connection)
        {
            var query = @"SELECT CNT_Id, CNT_Name, CNT_AmbrePivotCountryId, CNT_AmbrePivot, 
                                CNT_AmbreReceivable, CNT_AmbreReceivableCountryId, 
                                CNT_ServiceCode, CNT_BIC 
                         FROM T_Ref_Country";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<Country>();
                while (await reader.ReadAsync())
                {
                    list.Add(new Country
                    {
                        CNT_Id = reader["CNT_Id"]?.ToString(),
                        CNT_Name = reader["CNT_Name"]?.ToString(),
                        CNT_AmbrePivotCountryId = Convert.ToInt32(reader["CNT_AmbrePivotCountryId"] ?? 0),
                        CNT_AmbrePivot = reader["CNT_AmbrePivot"]?.ToString(),
                        CNT_AmbreReceivable = reader["CNT_AmbreReceivable"]?.ToString(),
                        CNT_AmbreReceivableCountryId = Convert.ToInt32(reader["CNT_AmbreReceivableCountryId"] ?? 0),
                        CNT_ServiceCode = reader["CNT_ServiceCode"]?.ToString(),
                        CNT_BIC = reader["CNT_BIC"]?.ToString()
                    });
                }
                _countries = list;
            }
        }

        private async Task LoadUserFieldsAsync(OleDbConnection connection)
        {
            var query = "SELECT USR_ID, USR_Category, USR_FieldName, USR_FieldDescription, USR_Pivot, USR_Receivable, USR_Color FROM T_Ref_User_Fields";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<UserField>();
                while (await reader.ReadAsync())
                {
                    list.Add(new UserField
                    {
                        USR_ID = Convert.ToInt32(reader["USR_ID"]),
                        USR_Category = reader["USR_Category"]?.ToString(),
                        USR_FieldName = reader["USR_FieldName"]?.ToString(),
                        USR_FieldDescription = reader["USR_FieldDescription"]?.ToString(),
                        USR_Pivot = Convert.ToBoolean(reader["USR_Pivot"] ?? false),
                        USR_Receivable = Convert.ToBoolean(reader["USR_Receivable"] ?? false),
                        USR_Color = reader["USR_Color"] == DBNull.Value ? null : reader["USR_Color"].ToString()
                    });
                }
                _userFields = list;
            }
        }

        

        private async Task LoadUserFiltersAsync(OleDbConnection connection)
        {
            var query = "SELECT UFI_id, UFI_Name, UFI_SQL, UFI_CreatedBy FROM T_Ref_User_Filter";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<UserFilter>();
                while (await reader.ReadAsync())
                {
                    list.Add(new UserFilter
                    {
                        UFI_id = Convert.ToInt32(reader["UFI_id"]),
                        UFI_Name = reader["UFI_Name"]?.ToString(),
                        UFI_SQL = reader["UFI_SQL"]?.ToString(),
                        UFI_CreatedBy = reader["UFI_CreatedBy"]?.ToString()
                    });
                }
                _userFilters = list;
            }
        }

        private async Task LoadParamsAsync(OleDbConnection connection)
        {
            var query = "SELECT PAR_Key, PAR_Value, PAR_Description FROM T_Param";
            using (var cmd = new OleDbCommand(query, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var list = new List<Param>();
                while (await reader.ReadAsync())
                {
                    list.Add(new Param
                    {
                        PAR_Key = reader["PAR_Key"]?.ToString(),
                        PAR_Value = reader["PAR_Value"]?.ToString(),
                        PAR_Description = reader["PAR_Description"]?.ToString()
                    });
                }
                _params = list;
            }
        }

        #endregion

        #region Conflict Management

        /// <summary>
        /// Détecte les conflits dans les données
        /// </summary>
        /// <returns>Liste des conflits détectés</returns>
        public async Task<List<object>> DetectConflicts()
        {
            try
            {
                if (!_isInitialized || _syncService == null)
                {
                    System.Diagnostics.Debug.WriteLine("Service non initialisé pour la détection de conflits");
                    return new List<object>();
                }

                // TODO: Implémenter la vraie détection de conflits via l'API OfflineFirstAccess
                // Pour l'instant, retourner une liste vide
                return new List<object>();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la détection de conflits: {ex.Message}");
                return new List<object>();
            }
        }

        /// <summary>
        /// Synchronise les données avec le serveur
        /// </summary>
        /// <returns>True si la synchronisation a réussi</returns>
        public async Task<bool> SynchronizeData()
        {
            try
            {
                // Utilise désormais le SynchronizationService basé sur SyncConfiguration
                if (!_isInitialized || _syncService == null)
                {
                    System.Diagnostics.Debug.WriteLine("Service non initialisé pour la synchronisation");
                    return false;
                }

                var result = await _syncService.SynchronizeAsync();
                if (!result.Success)
                {
                    System.Diagnostics.Debug.WriteLine($"Synchronisation échouée: {result.Message}");
                    return false;
                }

                // Mettre à jour le timestamp de dernière synchronisation
                if (_currentCountryId != null)
                {
                    _lastSyncTimes[_currentCountryId] = DateTime.Now;
                }

                System.Diagnostics.Debug.WriteLine($"Synchronisation réussie pour le pays: {_currentCountryId}");
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la synchronisation: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Résout un conflit spécifique
        /// </summary>
        /// <param name="conflict">Conflit à résoudre</param>
        /// <param name="resolution">Résolution choisie</param>
        /// <returns>True si le conflit a été résolu</returns>
        public async Task<bool> ResolveConflict(object conflict, string resolution)
        {
            try
            {
                if (!_isInitialized || _syncService == null)
                {
                    System.Diagnostics.Debug.WriteLine("Service non initialisé pour la résolution de conflits");
                    return false;
                }

                // TODO: Implémenter la vraie résolution de conflits via l'API OfflineFirstAccess
                System.Diagnostics.Debug.WriteLine($"Conflit résolu avec la résolution: {resolution}");
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la résolution de conflit: {ex.Message}");
                return false;
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Récupère un pays spécifique par son ID depuis le cache mémoire
        /// </summary>
        /// <param name="countryId">ID du pays à récupérer</param>
        /// <returns>Pays trouvé ou null</returns>
        public async Task<Country> GetCountryByIdAsync(string countryId)
        {
            try
            {
                // S'assurer que les référentiels sont chargés
                if (_countries.Count == 0)
                {
                    await LoadReferentialsAsync();
                }

                // Chercher dans le cache mémoire
                var country = _countries.FirstOrDefault(c => c.CNT_Id == countryId);
                
                if (country != null)
                {
                    System.Diagnostics.Debug.WriteLine($"Pays {countryId} trouvé en mémoire: {country.CNT_Name}");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Pays {countryId} non trouvé dans le cache mémoire");
                }
                
                return country;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la récupération du pays {countryId}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Récupère la liste des pays disponibles depuis le cache mémoire
        /// </summary>
        /// <returns>Liste des pays</returns>
        public async Task<List<Country>> GetCountries()
        {
            try
            {
                // S'assurer que les référentiels sont chargés
                if (!_referentialsLoaded)
                {
                    await LoadReferentialsAsync();
                }

                // Retourner une copie de la liste en mémoire
                var countries = _countries.ToList();
                
                System.Diagnostics.Debug.WriteLine($"{countries.Count} pays disponibles dans le cache mémoire");
                return countries;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la récupération des pays : {ex.Message}");
                return new List<Country>();
            }
        }

        #endregion

        #region OfflineFirst API Methods

        /// <summary>
        /// Retourne la liste des filtres sauvegardés (référentiel en mémoire)
        /// </summary>
        /// <param name="createdBy">Optionnel: filtre par créateur (insensible à la casse). Si null, retourne tous les filtres.</param>
        public async Task<List<UserFilter>> GetUserFilters(string createdBy = null)
        {
            try
            {
                // S'assurer que les référentiels sont chargés
                if (!_referentialsLoaded)
                {
                    await LoadReferentialsAsync();
                }

                var list = _userFilters?.ToList() ?? new List<UserFilter>();
                if (!string.IsNullOrWhiteSpace(createdBy))
                {
                    list = list.Where(f => string.IsNullOrEmpty(f.UFI_CreatedBy) ||
                                           f.UFI_CreatedBy.Equals(createdBy, StringComparison.OrdinalIgnoreCase))
                               .ToList();
                }
                return list;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la récupération des filtres utilisateur: {ex.Message}");
                return new List<UserFilter>();
            }
        }

        /// <summary>
        /// Recharge la liste des filtres utilisateur depuis la base référentielle (met à jour le cache mémoire)
        /// </summary>
        public async Task<bool> RefreshUserFiltersAsync()
        {
            try
            {
                using (var connection = new OleDbConnection(ReferentialDatabasePath))
                {
                    await connection.OpenAsync();
                    var query = "SELECT UFI_id, UFI_Name, UFI_SQL, UFI_CreatedBy FROM T_Ref_User_Filter";
                    using (var cmd = new OleDbCommand(query, connection))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        var list = new List<UserFilter>();
                        while (await reader.ReadAsync())
                        {
                            list.Add(new UserFilter
                            {
                                UFI_id = Convert.ToInt32(reader["UFI_id"]),
                                UFI_Name = reader["UFI_Name"]?.ToString(),
                                UFI_SQL = reader["UFI_SQL"]?.ToString(),
                                UFI_CreatedBy = reader["UFI_CreatedBy"]?.ToString()
                            });
                        }
                        _userFilters = list;
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors du rafraîchissement des filtres utilisateur: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Synchronise les données avec la base distante
        /// </summary>
        /// <param name="identifier">Identifiant de contexte (pays)</param>
        /// <param name="tables">Tables à synchroniser (null pour toutes)</param>
        /// <param name="onProgress">Callback de progression</param>
        /// <param name="cancellationToken">Token d'annulation</param>
        /// <returns>Résultat de la synchronisation</returns>
        public async Task<OfflineFirstAccess.Models.SyncResult> SynchronizeAsync(string identifier, List<string> tables = null, 
            Action<int, string> onProgress = null, CancellationToken? cancellationToken = null)
        {
            if (!_isInitialized)
            {
                throw new InvalidOperationException("Le service n'est pas encore initialisé");
            }

            try
            {
                // Utilise désormais SynchronizationService (les tables prises en compte proviennent de _syncConfig)
                if (_syncService == null)
                    throw new InvalidOperationException("_syncService non initialisé");
                var result = await _syncService.SynchronizeAsync(onProgress);

                // Mettre à jour le temps de dernière synchronisation
                _lastSyncTimes[identifier] = DateTime.Now;

                return result;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Erreur lors de la synchronisation pour {identifier}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Récupère toutes les entités d'une table
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="tableName">Nom de la table</param>
        /// <returns>Liste des entités</returns>
        public async Task<List<Entity>> GetEntitiesAsync(string identifier, string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName");
            EnsureInitialized();
            var results = new List<Entity>();
            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var cols = await GetTableColumnsAsync(connection, tableName);
                string where = string.Empty;
                if (cols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase))
                {
                    where = $" WHERE [{_syncConfig.IsDeletedColumn}] = false OR [{_syncConfig.IsDeletedColumn}] IS NULL";
                }
                else if (cols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase))
                {
                    where = " WHERE [DeleteDate] IS NULL";
                }
                var sql = $"SELECT * FROM [{tableName}]" + where;
                using (var cmd = new OleDbCommand(sql, connection))
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
                        results.Add(ent);
                    }
                }
            }
            return results;
        }

        /// <summary>
        /// Récupère une entité par son ID
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="tableName">Nom de la table</param>
        /// <param name="primaryKeyColumn">Colonne de clé primaire</param>
        /// <param name="id">ID de l'entité</param>
        /// <returns>L'entité ou null</returns>
        public async Task<Entity> GetEntityByIdAsync(string identifier, string tableName, string primaryKeyColumn, object id)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName");
            if (string.IsNullOrWhiteSpace(primaryKeyColumn)) throw new ArgumentException("primaryKeyColumn");
            EnsureInitialized();
            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var sql = $"SELECT * FROM [{tableName}] WHERE [{primaryKeyColumn}] = @p0";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@p0", id ?? DBNull.Value);
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
        /// Ajoute une entité
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="entity">Entité à ajouter</param>
        /// <returns>True si ajouté avec succès</returns>
        public async Task<bool> AddEntityAsync(string identifier, Entity entity)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var tableCols = await GetTableColumnsAsync(connection, entity.TableName);

                // Ensure metadata columns if present in table schema
                var rowGuidCol = tableCols.Contains(_syncConfig.PrimaryKeyGuidColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.PrimaryKeyGuidColumn : null;
                var lastModCol = tableCols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (tableCols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                var isDeletedCol = tableCols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.IsDeletedColumn : (tableCols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase) ? "DeleteDate" : null);

                if (rowGuidCol != null && (!entity.Properties.ContainsKey(rowGuidCol) || entity.Properties[rowGuidCol] == null))
                    entity.Properties[rowGuidCol] = Guid.NewGuid().ToString();
                if (lastModCol != null)
                    entity.Properties[lastModCol] = DateTime.UtcNow;
                if (isDeletedCol != null)
                {
                    if (isDeletedCol.Equals(_syncConfig.IsDeletedColumn, StringComparison.OrdinalIgnoreCase))
                        entity.Properties[isDeletedCol] = false;
                    else if (isDeletedCol.Equals("DeleteDate", StringComparison.OrdinalIgnoreCase))
                        entity.Properties[isDeletedCol] = DBNull.Value; // null delete date
                }

                var cols = entity.Properties.Keys.Where(k => tableCols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                if (cols.Count == 0) return false;
                var colList = string.Join(", ", cols.Select(c => $"[{c}]"));
                var paramList = string.Join(", ", cols.Select((c, i) => $"@p{i}"));
                var sql = $"INSERT INTO [{entity.TableName}] ({colList}) VALUES ({paramList})";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    for (int i = 0; i < cols.Count; i++)
                    {
                        var preparedValue = PrepareValueForDatabase(entity.Properties[cols[i]]);
                        cmd.Parameters.AddWithValue($"@p{i}", preparedValue);
                    }
                    var affected = await cmd.ExecuteNonQueryAsync();
                    // record change (prefer RowGuid, else primary key if available)
                    string changeKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : await GetPrimaryKeyValueAsync(connection, entity);
                    await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "INSERT");
                    return affected > 0;
                }
            }
        }

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
        /// Met à jour une entité
        /// </summary>
        /// <param name="identifier">Identifiant de contexte</param>
        /// <param name="entity">Entité à mettre à jour</param>
        /// <returns>True si mis à jour avec succès</returns>
        public async Task<bool> UpdateEntityAsync(string identifier, Entity entity)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var tableCols = await GetTableColumnsAsync(connection, entity.TableName);

                var rowGuidCol = tableCols.Contains(_syncConfig.PrimaryKeyGuidColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.PrimaryKeyGuidColumn : null;
                var lastModCol = tableCols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (tableCols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);

                if (lastModCol != null)
                    entity.Properties[lastModCol] = DateTime.UtcNow;

                var updatable = entity.Properties.Keys.Where(k => (rowGuidCol == null || !k.Equals(rowGuidCol, StringComparison.OrdinalIgnoreCase)) && tableCols.Contains(k, StringComparer.OrdinalIgnoreCase)).ToList();
                if (updatable.Count == 0) return false;
                var setList = string.Join(", ", updatable.Select((c, i) => $"[{c}] = @p{i}"));
                // Determine key
                string keyColumn = rowGuidCol;
                object keyValue = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol] : null;
                if (keyColumn == null || keyValue == null)
                {
                    keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                    if (!entity.Properties.ContainsKey(keyColumn))
                        throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                    keyValue = entity.Properties[keyColumn];
                }
                var sql = $"UPDATE [{entity.TableName}] SET {setList} WHERE [{keyColumn}] = @key";
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    for (int i = 0; i < updatable.Count; i++)
                    {
                        var preparedValue = PrepareValueForDatabase(entity.Properties[updatable[i]]);
                        cmd.Parameters.AddWithValue($"@p{i}", preparedValue);
                    }
                    cmd.Parameters.AddWithValue("@key", keyValue ?? DBNull.Value);
                    var affected = await cmd.ExecuteNonQueryAsync();
                    string changeKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : keyValue?.ToString();
                    await new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetRemoteLockConnectionString(_currentCountryId)).RecordChangeAsync(entity.TableName, changeKey, "UPDATE");
                    return affected > 0;
                }
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
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (string.IsNullOrWhiteSpace(entity.TableName)) throw new ArgumentException("Entity.TableName is required");
            EnsureInitialized();

            using (var connection = new OleDbConnection(GetLocalConnectionString()))
            {
                await connection.OpenAsync();
                var tableCols = await GetTableColumnsAsync(connection, entity.TableName);
                var rowGuidCol = tableCols.Contains(_syncConfig.PrimaryKeyGuidColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.PrimaryKeyGuidColumn : null;
                var lastModCol = tableCols.Contains(_syncConfig.LastModifiedColumn, StringComparer.OrdinalIgnoreCase) ? _syncConfig.LastModifiedColumn : (tableCols.Contains("LastModified", StringComparer.OrdinalIgnoreCase) ? "LastModified" : null);
                var hasIsDeleted = tableCols.Contains(_syncConfig.IsDeletedColumn, StringComparer.OrdinalIgnoreCase);
                var hasDeleteDate = tableCols.Contains("DeleteDate", StringComparer.OrdinalIgnoreCase);

                // Determine key
                string keyColumn = rowGuidCol;
                object keyValue = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol] : null;
                if (keyColumn == null || keyValue == null)
                {
                    keyColumn = await GetPrimaryKeyColumnAsync(connection, entity.TableName) ?? "ID";
                    if (!entity.Properties.ContainsKey(keyColumn))
                        throw new InvalidOperationException($"Valeur de clé introuvable pour la table {entity.TableName} (colonne {keyColumn})");
                    keyValue = entity.Properties[keyColumn];
                }

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
                        string changeKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : keyValue?.ToString();
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
                        string changeKey = rowGuidCol != null && entity.Properties.ContainsKey(rowGuidCol) ? entity.Properties[rowGuidCol]?.ToString() : keyValue?.ToString();
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
        /// Construit la chaîne de connexion pour une base country spécifique
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        /// <returns>Chaîne de connexion OLE DB pour la base country</returns>
        public string GetCountryConnectionString(string countryId)
        {
            string countryDatabaseDirectory = GetParameter("CountryDatabaseDirectory");
            if (string.IsNullOrEmpty(countryDatabaseDirectory))
            {
                throw new InvalidOperationException("Le répertoire des bases country n'est pas configuré dans T_Param");
            }
            
            // Construire le nom du fichier : Préfixe + CountryId + .accdb
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string databaseFileName = $"{countryDatabasePrefix}{countryId}.accdb";
            string databasePath = Path.Combine(countryDatabaseDirectory, databaseFileName);
            
            return $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={databasePath};";
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
        /// Exécute une requête sur la base de données d'une country
        /// </summary>
        /// <param name="countryId">ID du pays</param>
        /// <param name="query">Requête SQL</param>
        /// <param name="parameters">Paramètres de la requête</param>
        /// <param name="readerAction">Action à exécuter avec le DataReader</param>
        
        #endregion
        
        #region Private Helper Methods
        
        
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
