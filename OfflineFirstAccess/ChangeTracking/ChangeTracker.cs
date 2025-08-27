using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OfflineFirstAccess.Data;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.ChangeTracking
{
    public class ChangeTracker : IChangeTracker
    {
        private readonly string _localConnectionString;
        private const string ChangeLogTableName = "ChangeLog";

        public ChangeTracker(string localConnectionString)
        {
            _localConnectionString = localConnectionString;
        }

        public async Task RecordChangeAsync(string tableName, string recordId, string operationType)
        {
            var query = $"INSERT INTO [{ChangeLogTableName}] ([TableName], [RecordID], [Operation], [Timestamp], [Synchronized]) VALUES (@TableName, @RecordID, @Operation, @Timestamp, @Synchronized)";

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                await OpenWithTimeoutAsync(connection, TimeSpan.FromSeconds(20));
                using (var command = new OleDbCommand(query, connection))
                {
                    command.CommandTimeout = 20;
                    var pTable = command.Parameters.Add("@TableName", System.Data.OleDb.OleDbType.VarWChar);
                    var pRecord = command.Parameters.Add("@RecordID", System.Data.OleDb.OleDbType.VarWChar);
                    var pOp = command.Parameters.Add("@Operation", System.Data.OleDb.OleDbType.VarWChar);
                    var pTs = command.Parameters.Add("@Timestamp", System.Data.OleDb.OleDbType.Date);
                    var pSync = command.Parameters.Add("@Synchronized", System.Data.OleDb.OleDbType.Boolean);

                    pTable.Value = tableName ?? (object)DBNull.Value;
                    pRecord.Value = recordId ?? (object)DBNull.Value;
                    pOp.Value = operationType ?? (object)DBNull.Value;
                    pTs.Value = DateTime.UtcNow;
                    pSync.Value = false;

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        // Enforce hard timeouts for commands since OleDb may ignore CommandTimeout under some circumstances
        private static async Task<DbDataReader> ExecuteReaderWithTimeoutAsync(OleDbCommand command, TimeSpan timeout)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            var conn = command.Connection;
            if (conn == null) throw new InvalidOperationException("Command has no connection");
            var op = command.ExecuteReaderAsync();
            var completed = await Task.WhenAny(op, Task.Delay(timeout)) == op;
            if (!completed)
            {
                Debug.WriteLine($"[ChangeTracker] Timeout reading after {timeout.TotalSeconds}s. Disposing connection.");
                try { conn.Close(); } catch { }
                try { conn.Dispose(); } catch { }
                throw new TimeoutException($"Timeout reading from database after {timeout.TotalSeconds}s");
            }
            return await op; // propagate exceptions
        }

        /// <summary>
        /// Enregistre plusieurs changements en une seule fois en utilisant une connexion et une transaction.
        /// </summary>
        /// <param name="changes">Séquence (TableName, RecordId, OperationType)</param>
        public async Task RecordChangesAsync(IEnumerable<(string TableName, string RecordId, string OperationType)> changes)
        {
            if (changes == null)
                return;

            var list = changes.ToList();
            if (list.Count == 0)
                return;

            var insertSql = $"INSERT INTO [{ChangeLogTableName}] ([TableName], [RecordID], [Operation], [Timestamp], [Synchronized]) VALUES (@TableName, @RecordID, @Operation, @Timestamp, @Synchronized)";

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                await OpenWithTimeoutAsync(connection, TimeSpan.FromSeconds(20));
                using (var tx = connection.BeginTransaction())
                {
                    try
                    {
                        using (var cmd = new OleDbCommand(insertSql, connection, tx))
                        {
                            cmd.CommandTimeout = 20;
                            // Prepare parameters once and reuse
                            var pTable = cmd.Parameters.Add("@TableName", System.Data.OleDb.OleDbType.VarWChar);
                            var pRecord = cmd.Parameters.Add("@RecordID", System.Data.OleDb.OleDbType.VarWChar);
                            var pOp = cmd.Parameters.Add("@Operation", System.Data.OleDb.OleDbType.VarWChar);
                            var pTs = cmd.Parameters.Add("@Timestamp", System.Data.OleDb.OleDbType.Date);
                            var pSync = cmd.Parameters.Add("@Synchronized", System.Data.OleDb.OleDbType.Boolean);

                            foreach (var (table, recordId, op) in list)
                            {
                                pTable.Value = table ?? (object)DBNull.Value;
                                pRecord.Value = recordId ?? (object)DBNull.Value;
                                pOp.Value = op ?? (object)DBNull.Value;
                                pTs.Value = DateTime.UtcNow;
                                pSync.Value = false;
                                await cmd.ExecuteNonQueryAsync();
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
        }

        /// <summary>
        /// Démarre une session pour enregistrer de nombreux changements en réutilisant une seule connexion/transaction.
        /// </summary>
        public async Task<IChangeLogSession> BeginSessionAsync()
        {
            var connection = new OleDbConnection(_localConnectionString);
            await OpenWithTimeoutAsync(connection, TimeSpan.FromSeconds(20));
            var tx = connection.BeginTransaction();

            var insertSql = $"INSERT INTO [{ChangeLogTableName}] ([TableName], [RecordID], [Operation], [Timestamp], [Synchronized]) VALUES (@TableName, @RecordID, @Operation, @Timestamp, @Synchronized)";
            var cmd = new OleDbCommand(insertSql, connection, tx);
            cmd.CommandTimeout = 20;
            var pTable = cmd.Parameters.Add("@TableName", System.Data.OleDb.OleDbType.VarWChar);
            var pRecord = cmd.Parameters.Add("@RecordID", System.Data.OleDb.OleDbType.VarWChar);
            var pOp = cmd.Parameters.Add("@Operation", System.Data.OleDb.OleDbType.VarWChar);
            var pTs = cmd.Parameters.Add("@Timestamp", System.Data.OleDb.OleDbType.Date);
            var pSync = cmd.Parameters.Add("@Synchronized", System.Data.OleDb.OleDbType.Boolean);

            return new ChangeLogSession(connection, tx, cmd, pTable, pRecord, pOp, pTs, pSync);
        }

        private sealed class ChangeLogSession : IChangeLogSession
        {
            private readonly OleDbConnection _connection;
            private readonly OleDbTransaction _tx;
            private readonly OleDbCommand _cmd;
            private readonly OleDbParameter _pTable;
            private readonly OleDbParameter _pRecord;
            private readonly OleDbParameter _pOp;
            private readonly OleDbParameter _pTs;
            private readonly OleDbParameter _pSync;
            private bool _committed;

            public ChangeLogSession(OleDbConnection connection, OleDbTransaction tx, OleDbCommand cmd,
                OleDbParameter pTable, OleDbParameter pRecord, OleDbParameter pOp, OleDbParameter pTs, OleDbParameter pSync)
            {
                _connection = connection;
                _tx = tx;
                _cmd = cmd;
                _pTable = pTable;
                _pRecord = pRecord;
                _pOp = pOp;
                _pTs = pTs;
                _pSync = pSync;
            }

            public async Task AddAsync(string tableName, string recordId, string operationType)
            {
                _pTable.Value = tableName ?? (object)DBNull.Value;
                _pRecord.Value = recordId ?? (object)DBNull.Value;
                _pOp.Value = operationType ?? (object)DBNull.Value;
                _pTs.Value = DateTime.UtcNow;
                _pSync.Value = false;
                await _cmd.ExecuteNonQueryAsync();
            }

            public async Task CommitAsync()
            {
                if (_committed) return;
                _tx.Commit();
                _committed = true;
                await Task.CompletedTask;
            }

            public void Dispose()
            {
                try
                {
                    if (!_committed)
                    {
                        try { _tx.Rollback(); } catch { }
                    }
                }
                finally
                {
                    try { _cmd.Dispose(); } catch { }
                    try { _tx.Dispose(); } catch { }
                    try { _connection.Close(); } catch { }
                    try { _connection.Dispose(); } catch { }
                }
            }
        }

        public async Task<IEnumerable<ChangeLogEntry>> GetUnsyncedChangesAsync()
        {
            var entries = new List<ChangeLogEntry>();
            var query = $"SELECT ChangeID AS ID, TableName, RecordID AS RecordId, Operation AS OperationType, [Timestamp] AS TimestampUTC FROM [{ChangeLogTableName}] WHERE Synchronized = false ORDER BY [Timestamp]";

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                Debug.WriteLine("[ChangeTracker] GetUnsyncedChangesAsync: opening connection...");
                await OpenWithTimeoutAsync(connection, TimeSpan.FromSeconds(20));
                Debug.WriteLine("[ChangeTracker] GetUnsyncedChangesAsync: connection opened.");
                using (var command = new OleDbCommand(query, connection))
                {
                    command.CommandTimeout = 20;
                    Debug.WriteLine("[ChangeTracker] GetUnsyncedChangesAsync: executing reader...");
                    using (var reader = await ExecuteReaderWithTimeoutAsync(command, TimeSpan.FromSeconds(15)))
                    {
                        Debug.WriteLine("[ChangeTracker] GetUnsyncedChangesAsync: reader acquired, iterating...");
                        int cnt = 0;
                        while (await reader.ReadAsync())
                        {
                            entries.Add(new ChangeLogEntry
                            {
                                Id = Convert.ToInt64(reader["ID"]),
                                TableName = reader["TableName"].ToString(),
                                RecordId = reader["RecordId"].ToString(),
                                OperationType = reader["OperationType"].ToString(),
                                TimestampUTC = reader["TimestampUTC"] is double d
                                    ? DateTime.FromOADate(d)
                                    : Convert.ToDateTime(reader["TimestampUTC"]).ToUniversalTime()
                            });
                            cnt++;
                            if (cnt % 50 == 0) Debug.WriteLine($"[ChangeTracker] GetUnsyncedChangesAsync: {cnt} rows read...");
                        }
                        Debug.WriteLine($"[ChangeTracker] GetUnsyncedChangesAsync: completed, {cnt} rows read.");
                    }
                }
            }

            return entries;
        }

        public async Task MarkChangesAsSyncedAsync(IEnumerable<long> changeIds)
        {
            if (changeIds == null)
                return;

            // Sanitize: remove duplicates and non-positive IDs (Access COUNTER starts at 1)
            var ids = changeIds.Where(id => id > 0).Distinct().ToList();
            if (ids.Count == 0)
                return;

            const int batchSize = 200; // keep parameter count safe

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                await OpenWithTimeoutAsync(connection, TimeSpan.FromSeconds(20));

                for (int offset = 0; offset < ids.Count; offset += batchSize)
                {
                    var batch = ids.Skip(offset).Take(batchSize).ToList();
                    var placeholders = string.Join(",", batch.Select((_, i) => $"@p{i}"));
                    var query = $"UPDATE [{ChangeLogTableName}] SET Synchronized = true WHERE ChangeID IN ({placeholders})";

                    using (var command = new OleDbCommand(query, connection))
                    {
                        command.CommandTimeout = 20;
                        for (int i = 0; i < batch.Count; i++)
                        {
                            var p = command.Parameters.Add($"@p{i}", System.Data.OleDb.OleDbType.Integer);
                            // Access Long Integer (INTEGER) is 32-bit, safe cast
                            p.Value = unchecked((int)batch[i]);
                        }
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }

        // Helper: open with timeout to avoid hangs when the file is locked or share unavailable
        private static async Task OpenWithTimeoutAsync(OleDbConnection connection, TimeSpan timeout)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            var openTask = connection.OpenAsync();
            var completed = await Task.WhenAny(openTask, Task.Delay(timeout, CancellationToken.None)) == openTask;
            if (!completed)
            {
                Debug.WriteLine($"[ChangeTracker] Timeout opening DB after {timeout.TotalSeconds}s");
                try { connection.Close(); } catch { }
                try { connection.Dispose(); } catch { }
                throw new TimeoutException($"Timeout opening database connection after {timeout.TotalSeconds}s");
            }
            await openTask; // propagate exceptions if any
        }
    }
}

