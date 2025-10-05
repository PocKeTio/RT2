using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Manages shared/global ToDo list items stored in the referential DB (T_Ref_TodoList).
    /// </summary>
    public sealed class UserTodoListService
    {
        private readonly string _connString;

        public UserTodoListService(string referentialConnectionStringOrPath)
        {
            if (string.IsNullOrWhiteSpace(referentialConnectionStringOrPath))
                throw new ArgumentNullException(nameof(referentialConnectionStringOrPath));

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
        }

        public async Task<bool> EnsureTableAsync()
        {
            const string table = "T_Ref_TodoList";
            using (var conn = new OleDbConnection(_connString))
            {
                await conn.OpenAsync().ConfigureAwait(false);
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, table, "TABLE" });
                    if (schema == null || schema.Rows.Count == 0)
                    {
                        // Create table
                        var ddl = $@"CREATE TABLE [{table}] (
TDL_id AUTOINCREMENT PRIMARY KEY,
TDL_Name TEXT(100),
TDL_FilterName TEXT(100),
TDL_ViewName TEXT(100),
TDL_Account TEXT(50),
TDL_Order INTEGER,
TDL_Active YESNO,
TDL_CountryId TEXT(20)
)";
                        using (var cmd = new OleDbCommand(ddl, conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                        try
                        {
                            using (var idx = new OleDbCommand($"CREATE UNIQUE INDEX UX_{table}_NameCountry ON [{table}] (TDL_Name, TDL_CountryId)", conn))
                            {
                                await idx.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }
                        }
                        catch { /* best-effort index */ }
                        return true;
                    }
                    else
                    {
                        await EnsureMissingColumnsAsync(conn, table).ConfigureAwait(false);
                        return true;
                    }
                }
                catch
                {
                    return false;
                }
            }
        }

        private static async Task EnsureMissingColumnsAsync(OleDbConnection conn, string table)
        {
            var cols = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, table, null });
            async Task Ensure(string name, string ddl)
            {
                bool exists = cols != null && cols.Rows.Cast<DataRow>().Any(r => string.Equals(Convert.ToString(r["COLUMN_NAME"]), name, StringComparison.OrdinalIgnoreCase));
                if (!exists)
                {
                    try
                    {
                        using (var cmd = new OleDbCommand($"ALTER TABLE [{table}] ADD COLUMN [{name}] {ddl}", conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                    catch { }
                }
            }
            await Ensure("TDL_Name", "TEXT(100)");
            await Ensure("TDL_FilterName", "TEXT(100)");
            await Ensure("TDL_ViewName", "TEXT(100)");
            await Ensure("TDL_Account", "TEXT(50)");
            await Ensure("TDL_Order", "INTEGER");
            await Ensure("TDL_CountryId", "TEXT(20)"); // Kept for backward compatibility but not used in filters
        }

        public async Task<List<TodoListItem>> ListAsync(string countryId)
        {
            var list = new List<TodoListItem>();    
            using (var conn = new OleDbConnection(_connString))
            {
                await conn.OpenAsync().ConfigureAwait(false);
                // No filter on TDL_CountryId: all todos are shared across countries
                var cmd = new OleDbCommand($"SELECT TDL_id, TDL_Name, TDL_FilterName, TDL_ViewName, TDL_Account, TDL_Order, TDL_Active, TDL_CountryId FROM T_Ref_TodoList WHERE (TDL_Active <> 0 OR TDL_Active IS NULL) ORDER BY TDL_Order, TDL_Name", conn);
                using (var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await rdr.ReadAsync().ConfigureAwait(false))
                    {
                        list.Add(new TodoListItem
                        {
                            TDL_id = rdr.IsDBNull(0) ? 0 : Convert.ToInt32(rdr.GetValue(0)),
                            TDL_Name = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                            TDL_FilterName = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                            TDL_ViewName = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                            TDL_Account = rdr.IsDBNull(4) ? null : rdr.GetString(4),
                            TDL_Order = rdr.IsDBNull(5) ? (int?)null : Convert.ToInt32(rdr.GetValue(5)),
                            TDL_Active = rdr.IsDBNull(6) ? true : Convert.ToBoolean(rdr.GetValue(6)),
                            TDL_CountryId = rdr.IsDBNull(7) ? null : rdr.GetString(7)
                        });
                    }
                }
            }
            return list;
        }

        public async Task<int> UpsertAsync(TodoListItem item)
        {
            if (item == null || string.IsNullOrWhiteSpace(item.TDL_Name)) return 0;
            using (var conn = new OleDbConnection(_connString))
            {
                await conn.OpenAsync().ConfigureAwait(false);

                // Check by name only (no country filter)
                int? existingId = null;
                using (var check = new OleDbCommand("SELECT TOP 1 TDL_id FROM T_Ref_TodoList WHERE TDL_Name = ?", conn))
                {
                    check.Parameters.AddWithValue("@p1", item.TDL_Name);
                    var obj = await check.ExecuteScalarAsync().ConfigureAwait(false);
                    if (obj != null && obj != DBNull.Value) existingId = Convert.ToInt32(obj);
                }

                if (existingId.HasValue)
                {
                    using (var cmd = new OleDbCommand(@"UPDATE T_Ref_TodoList SET 
TDL_FilterName = ?, TDL_ViewName = ?, TDL_Account = ?, TDL_Order = ?, TDL_Active = ?, TDL_CountryId = ? 
WHERE TDL_id = ?", conn))
                    {
                        cmd.Parameters.AddWithValue("@p1", (object)item.TDL_FilterName ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p2", (object)item.TDL_ViewName ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p3", (object)item.TDL_Account ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p4", (object)(item.TDL_Order.HasValue ? item.TDL_Order.Value : (int?)null) ?? DBNull.Value);
                        cmd.Parameters.Add("@p5", OleDbType.Boolean).Value = item.TDL_Active;
                        cmd.Parameters.AddWithValue("@p6", (object)item.TDL_CountryId ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p7", existingId.Value);
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        return existingId.Value;
                    }
                }
                else
                {
                    using (var cmd = new OleDbCommand(@"INSERT INTO T_Ref_TodoList (TDL_Name, TDL_FilterName, TDL_ViewName, TDL_Account, TDL_Order, TDL_Active, TDL_CountryId) VALUES (?, ?, ?, ?, ?, ?, ?)", conn))
                    {
                        cmd.Parameters.AddWithValue("@p1", (object)item.TDL_Name ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p2", (object)item.TDL_FilterName ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p3", (object)item.TDL_ViewName ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p4", (object)item.TDL_Account ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p5", (object)(item.TDL_Order.HasValue ? item.TDL_Order.Value : (int?)null) ?? DBNull.Value);
                        cmd.Parameters.Add("@p6", OleDbType.Boolean).Value = item.TDL_Active;
                        cmd.Parameters.AddWithValue("@p7", (object)item.TDL_CountryId ?? DBNull.Value);
                        var n = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        if (n > 0)
                        {
                            using (var idCmd = new OleDbCommand("SELECT @@IDENTITY", conn))
                            {
                                var obj = await idCmd.ExecuteScalarAsync().ConfigureAwait(false);
                                return obj == null || obj == DBNull.Value ? 0 : Convert.ToInt32(obj);
                            }
                        }
                        return 0;
                    }
                }
            }
        }

        public async Task<bool> DeleteAsync(int id)
        {
            using (var conn = new OleDbConnection(_connString))
            {
                await conn.OpenAsync().ConfigureAwait(false);
                using (var cmd = new OleDbCommand("DELETE FROM T_Ref_TodoList WHERE TDL_id = ?", conn))
                {
                    cmd.Parameters.AddWithValue("@p1", id);
                    var n = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    return n > 0;
                }
            }
        }
    }
}
