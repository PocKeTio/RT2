using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Text.RegularExpressions;
using RecoTool.Services.Cache;

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

            // DO NOT sanitize during save - the WHERE clause is already clean from GenerateWhereClause
            // which excludes AccountId. SanitizeWhereClause is only for loading legacy filters.
            // whereClause = SanitizeWhereClause(whereClause);

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
            
            // OPTIMIZATION: Invalidate cache when filter is saved
            CacheService.Instance.Invalidate($"UserFilter_Where_{name}");
            CacheService.Instance.InvalidateByPrefix("UserFilter_Names_");
        }

        public static string SanitizeWhereClause(string where)
        {
            if (string.IsNullOrWhiteSpace(where)) return string.Empty;
            var s = where.Trim();

            // 1) Extract optional leading JSON comment block starting with /*JSON:...*/ (preserve it)
            string jsonHeader = string.Empty;
            var jsonMatch = Regex.Match(s, @"^\s*(/\*JSON:.*?\*/\s*)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
            if (jsonMatch.Success)
            {
                jsonHeader = jsonMatch.Groups[1].Value;
                s = s.Substring(jsonMatch.Length);
            }

            // 2) Define regexes for predicates to remove
            // Account: Account_ID = '...'
            var rxAccount = new Regex(@"(?i)(\bAND\b|\bWHERE\b)?\s*\bAccount_ID\b\s*=\s*'[^']*'\s*", RegexOptions.IgnoreCase);
            // Status: [a.]DeleteDate IS [NOT] NULL, with optional brackets/alias
            var rxStatus = new Regex(@"(?i)(\bAND\b|\bWHERE\b)?\s*(?:\b[aA]\.\s*)?\[?DeleteDate\]?\s+IS\s+(?:NOT\s+)?NULL\s*", RegexOptions.IgnoreCase);

            // Remove all occurrences iteratively to handle multiple conditions
            string prev;
            do { prev = s; s = rxAccount.Replace(s, " "); } while (prev != s);
            do { prev = s; s = rxStatus.Replace(s, " "); } while (prev != s);

            // 3) Clean dangling WHERE/AND/OR and extra spaces/parentheses
            // Remove leading WHERE if nothing follows
            s = Regex.Replace(s, @"(?i)^\s*WHERE\s*($|\))", string.Empty);
            // Remove leading AND/OR
            s = Regex.Replace(s, @"(?i)^\s*(AND|OR)\s+", string.Empty);
            // Remove trailing AND/OR
            s = Regex.Replace(s, @"(?i)\s+(AND|OR)\s*$", string.Empty);
            // Collapse repeated spaces
            s = Regex.Replace(s, "\u00A0", " "); // non-breaking spaces
            s = Regex.Replace(s, @"\s+", " ").Trim();

            // If result is just WHERE or empty parentheses, normalize to empty
            if (string.Equals(s, "WHERE", StringComparison.OrdinalIgnoreCase) || s == "()") s = string.Empty;

            // 4) Reattach preserved JSON header (if any), keeping a single space between header and SQL
            if (!string.IsNullOrWhiteSpace(jsonHeader))
            {
                if (string.IsNullOrWhiteSpace(s)) return jsonHeader.Trim() + " ";
                return jsonHeader + s;
            }
            return s;
        }

        public string LoadUserFilterWhere(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Filter name is required", nameof(name));

            // OPTIMIZATION: Cache filter SQL (filters rarely change)
            var cacheKey = $"UserFilter_Where_{name}";
            return CacheService.Instance.GetOrLoad(cacheKey, () =>
            {
                using (var conn = new OleDbConnection(_connString))
                using (var cmd = new OleDbCommand("SELECT UFI_SQL FROM T_Ref_User_Filter WHERE UFI_Name = ?", conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@p1", name);
                    var obj = cmd.ExecuteScalar();
                    return obj == null || obj == DBNull.Value ? null : obj.ToString();
                }
            }, TimeSpan.FromHours(1)); // Cache for 1 hour (filters rarely change)
        }

        public IList<string> ListUserFilterNames()
        {
            return ListUserFilterNames(null);
        }

        public IList<string> ListUserFilterNames(string contains = null)
        {
            // OPTIMIZATION: Cache filter names list (filters rarely change)
            var cacheKey = $"UserFilter_Names_{contains ?? "all"}";
            return CacheService.Instance.GetOrLoad(cacheKey, () =>
            {
                var names = new List<string>();
                using (var conn = new OleDbConnection(_connString))
                using (var cmd = string.IsNullOrWhiteSpace(contains)
                    ? new OleDbCommand("SELECT UFI_Name FROM T_Ref_User_Filter ORDER BY UFI_Name", conn)
                    : new OleDbCommand("SELECT UFI_Name FROM T_Ref_User_Filter WHERE UFI_Name LIKE ? ORDER BY UFI_Name", conn))
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
                            names.Add(rdr.GetString(0));
                        }
                    }
                }
                return (IList<string>)names;
            }, TimeSpan.FromHours(1)); // Cache for 1 hour
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
                var deleted = cmd.ExecuteNonQuery() > 0;
                
                // OPTIMIZATION: Invalidate cache when filter is deleted
                if (deleted)
                {
                    CacheService.Instance.Invalidate($"UserFilter_Where_{name}");
                    CacheService.Instance.InvalidateByPrefix("UserFilter_Names_");
                }
                
                return deleted;
            }
        }
    }
}
