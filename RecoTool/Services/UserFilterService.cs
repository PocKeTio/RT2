using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;

namespace RecoTool.Services
{
    public class UserFilterService
    {
        private readonly string _connString;
        private readonly string _currentUser;

        public UserFilterService(string referentialConnectionStringOrPath, string currentUser)
        {
            if (referentialConnectionStringOrPath == null) throw new ArgumentNullException(nameof(referentialConnectionStringOrPath));

            // If the value looks like a file path (no '=' typically found in conn strings),
            // build a proper OleDb connection string for Access.
            if (!referentialConnectionStringOrPath.Contains("=") && File.Exists(referentialConnectionStringOrPath))
            {
                var ext = Path.GetExtension(referentialConnectionStringOrPath)?.ToLowerInvariant();
                if (ext == ".accdb")
                {
                    _connString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
                }
                else if (ext == ".mdb")
                {
                    _connString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
                }
                else
                {
                    // Default to ACE for unknown extensions but existing file
                    _connString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={referentialConnectionStringOrPath};Persist Security Info=False;";
                }
            }
            else
            {
                _connString = referentialConnectionStringOrPath;
            }

            _currentUser = string.IsNullOrWhiteSpace(currentUser) ? Environment.UserName : currentUser;
        }

        public void SaveUserFilter(string name, string whereClause)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Filter name is required", nameof(name));
            if (string.IsNullOrWhiteSpace(whereClause)) whereClause = string.Empty;

            using (var conn = new OleDbConnection(_connString))
            {
                conn.Open();
                int? existingId = null;
                using (var cmd = new OleDbCommand("SELECT UFI_id FROM T_Ref_User_Filter WHERE UFI_Name = ?", conn))
                {
                    cmd.Parameters.AddWithValue("@p1", name);
                    var obj = cmd.ExecuteScalar();
                    if (obj != null && obj != DBNull.Value)
                        existingId = Convert.ToInt32(obj);
                }

                if (existingId.HasValue)
                {
                    using (var cmd = new OleDbCommand("UPDATE T_Ref_User_Filter SET UFI_SQL = ?, UFI_CreatedBy = ? WHERE UFI_id = ?", conn))
                    {
                        cmd.Parameters.AddWithValue("@p1", whereClause);
                        cmd.Parameters.AddWithValue("@p2", _currentUser);
                        cmd.Parameters.AddWithValue("@p3", existingId.Value);
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    using (var cmd = new OleDbCommand("INSERT INTO T_Ref_User_Filter (UFI_Name, UFI_SQL, UFI_CreatedBy) VALUES (?, ?, ?)", conn))
                    {
                        cmd.Parameters.AddWithValue("@p1", name);
                        cmd.Parameters.AddWithValue("@p2", whereClause);
                        cmd.Parameters.AddWithValue("@p3", _currentUser);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public string LoadUserFilterWhere(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Filter name is required", nameof(name));

            using (var conn = new OleDbConnection(_connString))
            using (var cmd = new OleDbCommand("SELECT UFI_SQL FROM T_Ref_User_Filter WHERE UFI_Name = ?", conn))
            {
                conn.Open();
                cmd.Parameters.AddWithValue("@p1", name);
                var obj = cmd.ExecuteScalar();
                return obj == null || obj == DBNull.Value ? null : obj.ToString();
            }
        }

        public IList<string> ListUserFilterNames()
        {
            var names = new List<string>();
            using (var conn = new OleDbConnection(_connString))
            using (var cmd = new OleDbCommand("SELECT UFI_Name FROM T_Ref_User_Filter ORDER BY UFI_Name", conn))
            {
                conn.Open();
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        names.Add(rdr.GetString(0));
                    }
                }
            }
            return names;
        }

        public IList<string> ListUserFilterNames(string contains)
        {
            if (string.IsNullOrWhiteSpace(contains)) return ListUserFilterNames();
            var names = new List<string>();
            using (var conn = new OleDbConnection(_connString))
            using (var cmd = new OleDbCommand("SELECT UFI_Name FROM T_Ref_User_Filter WHERE UFI_Name LIKE ? ORDER BY UFI_Name", conn))
            {
                conn.Open();
                cmd.Parameters.AddWithValue("@p1", "%" + contains + "%");
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        names.Add(rdr.GetString(0));
                    }
                }
            }
            return names;
        }

        /// <summary>
        /// Return list of (Name, CreatedBy) for filters, optionally filtered by substring on name.
        /// </summary>
        public IList<(string Name, string CreatedBy)> ListUserFiltersDetailed(string contains = null)
        {
            var list = new List<(string, string)>();
            using (var conn = new OleDbConnection(_connString))
            using (var cmd = string.IsNullOrWhiteSpace(contains)
                ? new OleDbCommand("SELECT UFI_Name, UFI_CreatedBy FROM T_Ref_User_Filter ORDER BY UFI_Name", conn)
                : new OleDbCommand("SELECT UFI_Name, UFI_CreatedBy FROM T_Ref_User_Filter WHERE UFI_Name LIKE ? ORDER BY UFI_Name", conn))
            {
                conn.Open();
                if (!string.IsNullOrWhiteSpace(contains))
                {
                    cmd.Parameters.AddWithValue("@p1", "%" + contains + "%");
                }
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        var name = rdr.IsDBNull(0) ? string.Empty : rdr.GetString(0);
                        var creator = rdr.IsDBNull(1) ? string.Empty : rdr.GetString(1);
                        list.Add((name, creator));
                    }
                }
            }
            return list;
        }

        public bool DeleteUserFilter(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            using (var conn = new OleDbConnection(_connString))
            using (var cmd = new OleDbCommand("DELETE FROM T_Ref_User_Filter WHERE UFI_Name = ?", conn))
            {
                conn.Open();
                cmd.Parameters.AddWithValue("@p1", name);
                return cmd.ExecuteNonQuery() > 0;
            }
        }
    }
}
