using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

using GenericRecord = System.Collections.Generic.Dictionary<string, object>;

namespace OfflineFirstAccess.Data
{
    public class AccessDataProvider : IDataProvider
    {
        private readonly string _connectionString;
        private readonly SyncConfiguration _config;

        private AccessDataProvider(string connectionString, SyncConfiguration config)
        {
            _connectionString = connectionString;
            _config = config;
        }

        // Normalize values to what Access via OleDb expects
        private static object PrepareValueForDatabase(object value)
        {
            if (value == null)
                return DBNull.Value;

            // Unwrap DBNull
            if (value is DBNull)
                return value;

            // Handle DateTime (and boxed nullable DateTime)
            if (value is DateTime dt)
                return dt.ToOADate();

            //// If value came already as DateTime? boxed as Nullable<DateTime>
            //if (value is DateTime? ndt)
            //    return ndt.HasValue ? ndt.Value.ToOADate() : (object)DBNull.Value;

            return value;
        }

        // On crée une "fabrique" publique et asynchrone
        public static async Task<AccessDataProvider> CreateAsync(string connectionString, SyncConfiguration config)
        {
            var provider = new AccessDataProvider(connectionString, config);
            await provider.EnsureConfigTableExistsAsync(); // Appel asynchrone correct
            return provider;
        }

        public async Task<IEnumerable<GenericRecord>> GetChangesAsync(string tableName, DateTime? since)
        {
            var records = new List<GenericRecord>();
            var query = $"SELECT * FROM [{tableName}] WHERE [{_config.LastModifiedColumn}] > @lastSync";

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    // Access expects dates as OLE Automation Date (double)
                    var lastSyncValue = since.HasValue ? since.Value.ToOADate() : DateTime.MinValue.ToOADate();
                    command.Parameters.AddWithValue("@lastSync", lastSyncValue);
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var record = new GenericRecord();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                record[reader.GetName(i)] = reader.GetValue(i);
                            }
                            records.Add(record);
                        }
                    }
                }
            }
            return records;
        }

        public async Task ApplyChangesAsync(string tableName, IEnumerable<GenericRecord> changesToApply)
        {
            if (changesToApply == null || !changesToApply.Any())
            {
                return;
            }

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (var record in changesToApply)
                        {
                            var isDeleted = record.ContainsKey(_config.IsDeletedColumn) && (bool)record[_config.IsDeletedColumn];
                            var guid = record[_config.PrimaryKeyGuidColumn].ToString();

                            if (isDeleted)
                            {
                                var deleteSql = $"DELETE FROM [{tableName}] WHERE [{_config.PrimaryKeyGuidColumn}] = @guid";
                                using (var command = new OleDbCommand(deleteSql, connection, transaction))
                                {
                                    command.Parameters.AddWithValue("@guid", guid);
                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                            else
                            {
                                // Optimized Upsert logic: Try UPDATE first, then INSERT.
                                var updateSetClause = string.Join(", ", record.Keys.Where(k => k != _config.PrimaryKeyGuidColumn).Select(k => $"[{k}] = @{k}"));
                                var updateSql = $"UPDATE [{tableName}] SET {updateSetClause} WHERE [{_config.PrimaryKeyGuidColumn}] = @guid";
                                int rowsAffected;

                                using (var updateCommand = new OleDbCommand(updateSql, connection, transaction))
                                {
                                    foreach (var key in record.Keys.Where(k => k != _config.PrimaryKeyGuidColumn))
                                    {
                                        updateCommand.Parameters.AddWithValue($"@{key}", PrepareValueForDatabase(record[key]));
                                    }
                                    updateCommand.Parameters.AddWithValue("@guid", guid);
                                    rowsAffected = await updateCommand.ExecuteNonQueryAsync();
                                }

                                if (rowsAffected == 0)
                                {
                                    // If no rows were updated, the record doesn't exist. Insert it.
                                    var insertColumns = string.Join(", ", record.Keys.Select(k => $"[{k}]"));
                                    var insertValues = string.Join(", ", record.Keys.Select(k => $"@{k}"));
                                    var insertSql = $"INSERT INTO [{tableName}] ({insertColumns}) VALUES ({insertValues})";
                                    using (var insertCommand = new OleDbCommand(insertSql, connection, transaction))
                                    {
                                        foreach (var key in record.Keys)
                                        {
                                            insertCommand.Parameters.AddWithValue($"@{key}", PrepareValueForDatabase(record[key]));
                                        }
                                        await insertCommand.ExecuteNonQueryAsync();
                                    }
                                }
                            }
                        }
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }

        public async Task<IEnumerable<GenericRecord>> GetRecordsByGuid(string tableName, IEnumerable<string> guids)
        {
            var records = new List<GenericRecord>();
            if (guids == null)
                return records;

            // Sanitize GUID list: remove null/empty and duplicates
            var guidList = guids.Where(g => !string.IsNullOrWhiteSpace(g)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (guidList.Count == 0)
                return records;

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();

                // Validate table exists
                var tableSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                bool tableExists = tableSchema != null && tableSchema.Rows.OfType<DataRow>()
                    .Any(r => string.Equals(r["TABLE_NAME"].ToString(), tableName, StringComparison.OrdinalIgnoreCase));
                if (!tableExists)
                {
                    throw new InvalidOperationException($"Table '{tableName}' introuvable dans la base. Disponible(s): " +
                        string.Join(", ", tableSchema?.Rows.OfType<DataRow>().Select(r => r["TABLE_NAME"].ToString()) ?? Array.Empty<string>()));
                }

                // Validate column exists
                var colSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                bool columnExists = colSchema != null && colSchema.Rows.OfType<DataRow>()
                    .Any(r => string.Equals(r["COLUMN_NAME"].ToString(), _config.PrimaryKeyGuidColumn, StringComparison.OrdinalIgnoreCase));
                if (!columnExists)
                {
                    throw new InvalidOperationException($"Colonne '{_config.PrimaryKeyGuidColumn}' introuvable dans la table '{tableName}'. Colonnes: " +
                        string.Join(", ", colSchema?.Rows.OfType<DataRow>().Select(r => r["COLUMN_NAME"].ToString()) ?? Array.Empty<string>()));
                }

                // Build query and parameters
                var parameterPlaceholders = string.Join(",", guidList.Select((_, i) => $"@p{i}"));
                var query = $"SELECT * FROM [{tableName}] WHERE [{_config.PrimaryKeyGuidColumn}] IN ({parameterPlaceholders})";

                using (var command = new OleDbCommand(query, connection))
                {
                    for (int i = 0; i < guidList.Count; i++)
                    {
                        command.Parameters.AddWithValue($"@p{i}", guidList[i]);
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var record = new GenericRecord();
                            for (int j = 0; j < reader.FieldCount; j++)
                            {
                                record[reader.GetName(j)] = reader.GetValue(j);
                            }
                            records.Add(record);
                        }
                    }
                }
            }
            return records;
        }
        public async Task<string> GetParameterAsync(string key)
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                var command = new OleDbCommand("SELECT ConfigValue FROM _SyncConfig WHERE ConfigKey = @Key", connection);
                command.Parameters.AddWithValue("@Key", key);
                var result = await command.ExecuteScalarAsync();
                return result?.ToString();
            }
        }

        public async Task SetParameterAsync(string key, string value)
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                
                var updateCommand = new OleDbCommand("UPDATE _SyncConfig SET ConfigValue = @Value WHERE ConfigKey = @Key", connection);
                updateCommand.Parameters.AddWithValue("@Value", value);
                updateCommand.Parameters.AddWithValue("@Key", key);
                int rowsAffected = await updateCommand.ExecuteNonQueryAsync();

                if (rowsAffected == 0)
                {
                    var insertCommand = new OleDbCommand("INSERT INTO _SyncConfig (ConfigKey, ConfigValue) VALUES (@Key, @Value)", connection);
                    insertCommand.Parameters.AddWithValue("@Key", key);
                    insertCommand.Parameters.AddWithValue("@Value", value);
                    await insertCommand.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task EnsureConfigTableExistsAsync()
        {
            try
            {
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool tableExists = schema.Rows.OfType<DataRow>().Any(r => r["TABLE_NAME"].ToString().Equals("_SyncConfig", StringComparison.OrdinalIgnoreCase));

                    if (!tableExists)
                    {
                        var createCommand = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection);
                        await createCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error ensuring _SyncConfig table exists: {ex.Message}");
                // In a real app, this should be logged to a proper logging framework.
            }
        }
    }
}