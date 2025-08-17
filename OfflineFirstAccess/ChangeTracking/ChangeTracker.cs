using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;
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

        public async Task RecordChangeAsync(string tableName, string rowGuid, string operationType)
        {
            var query = $"INSERT INTO [{ChangeLogTableName}] (TableName, RecordID, Operation, [Timestamp], Synchronized) VALUES (@TableName, @RecordID, @Operation, @Timestamp, @Synchronized)";

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableName", tableName);
                    command.Parameters.AddWithValue("@RecordID", rowGuid);
                    command.Parameters.AddWithValue("@Operation", operationType);
                    command.Parameters.AddWithValue("@Timestamp", DateTime.UtcNow.ToOADate());
                    command.Parameters.AddWithValue("@Synchronized", false);
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<IEnumerable<ChangeLogEntry>> GetUnsyncedChangesAsync()
        {
            var entries = new List<ChangeLogEntry>();
            var query = $"SELECT ChangeID AS ID, TableName, RecordID AS RowGuid, Operation AS OperationType, [Timestamp] AS TimestampUTC FROM [{ChangeLogTableName}] WHERE Synchronized = false ORDER BY [Timestamp]";

            using (var connection = new OleDbConnection(_localConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            entries.Add(new ChangeLogEntry
                            {
                                Id = Convert.ToInt64(reader["ID"]),
                                TableName = reader["TableName"].ToString(),
                                RowGuid = reader["RowGuid"].ToString(),
                                OperationType = reader["OperationType"].ToString(),
                                TimestampUTC = Convert.ToDateTime(reader["TimestampUTC"])
                            });
                        }
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
                await connection.OpenAsync();

                for (int offset = 0; offset < ids.Count; offset += batchSize)
                {
                    var batch = ids.Skip(offset).Take(batchSize).ToList();
                    var placeholders = string.Join(",", batch.Select((_, i) => $"@p{i}"));
                    var query = $"UPDATE [{ChangeLogTableName}] SET Synchronized = true WHERE ChangeID IN ({placeholders})";

                    using (var command = new OleDbCommand(query, connection))
                    {
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
    }
}
