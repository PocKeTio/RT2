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

        // Create explicitly-typed parameters to avoid Access type guessing problems
        private static OleDbParameter CreateParameter(string name, object value)
        {
            if (value == null || value is DBNull)
            {
                var p = new OleDbParameter(name, OleDbType.Variant);
                p.Value = DBNull.Value;
                return p;
            }

            switch (value)
            {
                case DateTime dt:
                    {
                        var p = new OleDbParameter(name, OleDbType.DBTimeStamp);
                        p.Value = dt;
                        return p;
                    }
                case bool b:
                    {
                        var p = new OleDbParameter(name, OleDbType.Boolean);
                        p.Value = b;
                        return p;
                    }
                case string s:
                    {
                        var p = new OleDbParameter(name, OleDbType.VarWChar);
                        p.Value = s;
                        return p;
                    }
                case byte[] bytes:
                    {
                        var p = new OleDbParameter(name, OleDbType.Binary);
                        p.Value = bytes;
                        return p;
                    }
            }

            // Numerics and others: let OleDb infer, but avoid OADate conversions
            var def = new OleDbParameter(name, value);
            return def;
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
                    var lastSyncValue = since.HasValue ? since.Value : DateTime.MinValue;
                    command.Parameters.Add(CreateParameter("@lastSync", lastSyncValue.ToOADate()));
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
                            var id = record[_config.PrimaryKeyColumn].ToString();

                            if (isDeleted)
                            {
                                var deleteSql = $"DELETE FROM [{tableName}] WHERE [{_config.PrimaryKeyColumn}] = @id";
                                using (var command = new OleDbCommand(deleteSql, connection, transaction))
                                {
                                    command.Parameters.Add(CreateParameter("@id", id));
                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                            else
                            {
                                // Optimized Upsert logic: Try UPDATE first, then INSERT.
                                var updateSetClause = string.Join(", ", record.Keys.Where(k => k != _config.PrimaryKeyColumn).Select(k => $"[{k}] = @{k}"));
                                var updateSql = $"UPDATE [{tableName}] SET {updateSetClause} WHERE [{_config.PrimaryKeyColumn}] = @id";
                                int rowsAffected;

                                using (var updateCommand = new OleDbCommand(updateSql, connection, transaction))
                                {
                                    foreach (var key in record.Keys.Where(k => k != _config.PrimaryKeyColumn))
                                    {
                                        updateCommand.Parameters.Add(CreateParameter($"@{key}", record[key]));
                                    }
                                    updateCommand.Parameters.Add(CreateParameter("@id", id));
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
                                            insertCommand.Parameters.Add(CreateParameter($"@{key}", record[key]));
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

        public async Task<IEnumerable<GenericRecord>> GetRecordsByIds(string tableName, IEnumerable<string> ids)
        {
            var records = new List<GenericRecord>();
            if (ids == null)
                return records;

            // Sanitize ID list: remove null/empty and duplicates
            var idList = ids.Where(g => !string.IsNullOrWhiteSpace(g)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (idList.Count == 0)
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
                    .Any(r => string.Equals(r["COLUMN_NAME"].ToString(), _config.PrimaryKeyColumn, StringComparison.OrdinalIgnoreCase));
                if (!columnExists)
                {
                    throw new InvalidOperationException($"Colonne '{_config.PrimaryKeyColumn}' introuvable dans la table '{tableName}'. Colonnes: " +
                        string.Join(", ", colSchema?.Rows.OfType<DataRow>().Select(r => r["COLUMN_NAME"].ToString()) ?? Array.Empty<string>()));
                }

                // Build query and parameters
                var parameterPlaceholders = string.Join(",", idList.Select((_, i) => $"@p{i}"));
                var query = $"SELECT * FROM [{tableName}] WHERE [{_config.PrimaryKeyColumn}] IN ({parameterPlaceholders})";

                using (var command = new OleDbCommand(query, connection))
                {
                    for (int i = 0; i < idList.Count; i++)
                    {
                        command.Parameters.Add(CreateParameter($"@p{i}", idList[i]));
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