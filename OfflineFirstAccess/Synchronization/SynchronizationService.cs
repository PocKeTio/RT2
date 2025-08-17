using System;
using System.Threading.Tasks;
using System.Data.OleDb;
using OfflineFirstAccess.Conflicts;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Synchronization
{
    public class SynchronizationService : ISynchronizationService
    {
        private SyncConfiguration _config;
        private SyncOrchestrator _orchestrator;
        private string LockConnStr => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={_config.LockDatabasePath};";

        public async Task InitializeAsync(SyncConfiguration config)
        {
            _config = config;

            // 1. Create Data Providers for local and remote databases (requires full OleDb connection strings)
            if (string.IsNullOrWhiteSpace(config.LocalDatabasePath))
                throw new InvalidOperationException("LocalDatabasePath must be provided in SyncConfiguration.");
            if (string.IsNullOrWhiteSpace(config.RemoteDatabasePath))
                throw new InvalidOperationException("RemoteDatabasePath must be provided in SyncConfiguration.");
            var localConnStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={config.LocalDatabasePath};";
            var remoteConnStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={config.RemoteDatabasePath};";
            var localProvider = await Data.AccessDataProvider.CreateAsync(localConnStr, config);
            var remoteProvider = await Data.AccessDataProvider.CreateAsync(remoteConnStr, config);

            // 2. Create the Change Tracker targeting the network Lock database
            if (string.IsNullOrWhiteSpace(config.LockDatabasePath))
                throw new InvalidOperationException("LockDatabasePath must be set in SyncConfiguration.");

            string lockConnStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={config.LockDatabasePath};";
            var changeTracker = new ChangeTracking.ChangeTracker(lockConnStr);

            // 3. Create the Conflict Resolver
            var conflictResolver = new ManualConflictResolver(config);

            // 4. Assemble the Orchestrator
            _orchestrator = new SyncOrchestrator(localProvider, remoteProvider, changeTracker, conflictResolver, _config);

            // 5. Ensure SyncLog table exists in Lock DB
            await EnsureSyncLogTableExistsAsync();
            // Optionally check for interrupted syncs
            await CheckAndResumeInterruptedSyncAsync();
        }

        public Task<SyncResult> SynchronizeAsync(Action<int, string> onProgress = null)
        {
            if (_orchestrator == null)
            {
                throw new InvalidOperationException("Service must be initialized with a valid configuration before synchronization.");
            }

            return RunWithLoggingAsync(onProgress);
        }

        private async Task<SyncResult> RunWithLoggingAsync(Action<int, string> onProgress)
        {
            await EnsureSyncLogTableExistsAsync();
            await LogSyncOperationAsync("Sync", "Started", "Starting synchronization");
            try
            {
                var result = await _orchestrator.SynchronizeAsync(onProgress);
                var details = result.Success
                    ? $"Completed. UnresolvedConflicts={result.UnresolvedConflicts?.Count ?? 0}"
                    : $"Completed with errors: {result.ErrorDetails}";
                await LogSyncOperationAsync("Sync", result.Success ? "Completed" : "Failed", details);
                return result;
            }
            catch (Exception ex)
            {
                await LogSyncOperationAsync("Sync", "Failed", ex.Message);
                return new SyncResult { Success = false, ErrorDetails = ex.Message, Message = ex.Message };
            }
        }

        private async Task EnsureSyncLogTableExistsAsync()
        {
            using (var connection = new OleDbConnection(LockConnStr))
            {
                await connection.OpenAsync();
                bool HasTable(string tableName)
                {
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, tableName, "TABLE" });
                    return schema != null && schema.Rows.Count > 0;
                }

                if (!HasTable("SyncLog"))
                {
                    string createSql = @"CREATE TABLE SyncLog (
                        ID COUNTER PRIMARY KEY,
                        Operation TEXT(50),
                        Status TEXT(50),
                        Details TEXT(255),
                        [Timestamp] DATETIME
                    )";
                    using (var cmd = new OleDbCommand(createSql, connection))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
            }
        }

        public async Task CheckAndResumeInterruptedSyncAsync()
        {
            // Simple heuristic: if last entry is a Started without a subsequent Completed/Failed, log Resuming.
            using (var connection = new OleDbConnection(LockConnStr))
            {
                await connection.OpenAsync();
                string selectSql = "SELECT TOP 1 Operation, Status, [Timestamp] FROM SyncLog ORDER BY [Timestamp] DESC";
                using (var cmd = new OleDbCommand(selectSql, connection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var status = reader["Status"]?.ToString();
                        if (string.Equals(status, "Started", StringComparison.OrdinalIgnoreCase))
                        {
                            await LogSyncOperationAsync("Sync", "Resuming", "Detected previously started sync without completion.");
                        }
                    }
                }
            }
        }

        private async Task LogSyncOperationAsync(string operation, string status, string details)
        {
            using (var connection = new OleDbConnection(LockConnStr))
            {
                await connection.OpenAsync();
                string insertSql = "INSERT INTO SyncLog (Operation, Status, Details, [Timestamp]) VALUES (@op, @st, @de, @ts)";
                using (var cmd = new OleDbCommand(insertSql, connection))
                {
                    cmd.Parameters.AddWithValue("@op", operation);
                    cmd.Parameters.AddWithValue("@st", status);
                    cmd.Parameters.AddWithValue("@de", details ?? string.Empty);
                    cmd.Parameters.AddWithValue("@ts", DateTime.UtcNow.ToOADate());
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
