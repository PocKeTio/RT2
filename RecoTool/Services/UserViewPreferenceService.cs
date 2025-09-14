using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Manages Saved Views (T_Ref_User_Fields_Preference): list, get, upsert, delete.
    /// Extracted from ReconciliationService to reduce responsibilities.
    /// </summary>
    public sealed class UserViewPreferenceService
    {
        private readonly string _connString;
        private readonly string _currentUser;

        public UserViewPreferenceService(string referentialConnectionStringOrPath, string currentUser)
        {
            if (string.IsNullOrWhiteSpace(referentialConnectionStringOrPath))
                throw new ArgumentNullException(nameof(referentialConnectionStringOrPath));

            // Accept file path or full connection string
            if (!referentialConnectionStringOrPath.Contains("=") && File.Exists(referentialConnectionStringOrPath))
            {
                var ext = Path.GetExtension(referentialConnectionStringOrPath)?.ToLowerInvariant();
                if (ext == ".accdb")
                    _connString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
                else if (ext == ".mdb")
                    _connString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
                else
                    _connString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
            }
            else
            {
                _connString = referentialConnectionStringOrPath;
            }

            _currentUser = string.IsNullOrWhiteSpace(currentUser) ? Environment.UserName : currentUser;
        }

        public async Task<List<UserFieldsPreference>> GetAllAsync()
        {
            var list = new List<UserFieldsPreference>();
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"SELECT UPF_id, UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths FROM T_Ref_User_Fields_Preference ORDER BY UPF_Name", connection);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        list.Add(new UserFieldsPreference
                        {
                            UPF_id = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
                            UPF_Name = reader.IsDBNull(1) ? null : reader.GetString(1),
                            UPF_user = reader.IsDBNull(2) ? null : reader.GetString(2),
                            UPF_SQL = reader.IsDBNull(3) ? null : reader.GetString(3),
                            UPF_ColumnWidths = reader.IsDBNull(4) ? null : reader.GetString(4)
                        });
                    }
                }
            }
            return list;
        }

        public async Task<int> InsertAsync(string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"INSERT INTO T_Ref_User_Fields_Preference (UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths) VALUES (?, ?, ?, ?)", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p3", sql ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p4", columnsJson ?? (object)DBNull.Value);
                var n = await cmd.ExecuteNonQueryAsync();
                if (n > 0)
                {
                    var idCmd = new OleDbCommand(@"SELECT @@IDENTITY", connection);
                    var obj = await idCmd.ExecuteScalarAsync();
                    return obj == null || obj == DBNull.Value ? 0 : Convert.ToInt32(obj);
                }
                return 0;
            }
        }

        public async Task<bool> UpdateAsync(int id, string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"UPDATE T_Ref_User_Fields_Preference SET UPF_Name = ?, UPF_user = ?, UPF_SQL = ?, UPF_ColumnWidths = ? WHERE UPF_id = ?", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p3", sql ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p4", columnsJson ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p5", id);
                return await cmd.ExecuteNonQueryAsync() > 0;
            }
        }

        public async Task<int> UpsertAsync(string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                int? existingId = null;
                var check = new OleDbCommand(@"SELECT TOP 1 UPF_id FROM T_Ref_User_Fields_Preference WHERE UPF_Name = ? AND UPF_user = ?", connection);
                check.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                check.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                var obj = await check.ExecuteScalarAsync();
                if (obj != null && obj != DBNull.Value)
                    existingId = Convert.ToInt32(obj);

                if (existingId.HasValue)
                {
                    await UpdateAsync(existingId.Value, name, sql, columnsJson);
                    return existingId.Value;
                }
                else
                {
                    return await InsertAsync(name, sql, columnsJson);
                }
            }
        }

        public async Task<UserFieldsPreference> GetByNameAsync(string name)
        {
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"SELECT TOP 1 UPF_id, UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths FROM T_Ref_User_Fields_Preference WHERE UPF_Name = ?", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        return new UserFieldsPreference
                        {
                            UPF_id = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
                            UPF_Name = reader.IsDBNull(1) ? null : reader.GetString(1),
                            UPF_user = reader.IsDBNull(2) ? null : reader.GetString(2),
                            UPF_SQL = reader.IsDBNull(3) ? null : reader.GetString(3),
                            UPF_ColumnWidths = reader.IsDBNull(4) ? null : reader.GetString(4)
                        };
                    }
                }
            }
            return null;
        }

        public async Task<List<string>> ListNamesAsync(string contains = null)
        {
            var result = new List<string>();
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                OleDbCommand cmd;
                if (string.IsNullOrWhiteSpace(contains))
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                }
                else
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? AND UPF_Name LIKE ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@p2", "%" + contains + "%");
                }
                using (var rd = await cmd.ExecuteReaderAsync())
                {
                    while (await rd.ReadAsync())
                    {
                        if (!rd.IsDBNull(0)) result.Add(rd.GetString(0));
                    }
                }
            }
            return result;
        }

        public async Task<List<(string Name, string Creator)>> ListDetailedAsync(string contains = null)
        {
            var list = new List<(string, string)>();
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                OleDbCommand cmd;
                if (string.IsNullOrWhiteSpace(contains))
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name, UPF_user FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                }
                else
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name, UPF_user FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? AND UPF_Name LIKE ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@p2", "%" + contains + "%");
                }
                using (var rd = await cmd.ExecuteReaderAsync())
                {
                    while (await rd.ReadAsync())
                    {
                        var name = rd.IsDBNull(0) ? string.Empty : rd.GetString(0);
                        var creator = rd.IsDBNull(1) ? string.Empty : rd.GetString(1);
                        list.Add((name, creator));
                    }
                }
            }
            return list;
        }

        public async Task<bool> DeleteByNameAsync(string name)
        {
            using (var connection = new OleDbConnection(_connString))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"DELETE FROM T_Ref_User_Fields_Preference WHERE UPF_Name = ? AND UPF_user = ?", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                return await cmd.ExecuteNonQueryAsync() > 0;
            }
        }
    }
}
