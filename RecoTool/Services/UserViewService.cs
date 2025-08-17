using System;
using System.Collections.Generic;
using System.Data.OleDb;

namespace RecoTool.Services
{
    /// <summary>
    /// Synchronous helper service for Saved Views (T_Ref_User_Fields_Preference)
    /// used by UI elements like FilterPickerWindow. Uses the referential DB.
    /// </summary>
    public class UserViewService
    {
        private readonly string _referentialConnectionString;
        private readonly string _currentUser;

        public UserViewService(string referentialConnectionString, string currentUser)
        {
            _referentialConnectionString = referentialConnectionString ?? throw new ArgumentNullException(nameof(referentialConnectionString));
            _currentUser = currentUser ?? Environment.UserName;
        }

        /// <summary>
        /// Returns distinct saved view names for the current user, optionally filtered by contains.
        /// </summary>
        public IEnumerable<string> ListViewNames(string contains = null)
        {
            var names = new List<string>();
            using (var conn = new OleDbConnection(_referentialConnectionString))
            {
                conn.Open();
                string sql = "SELECT DISTINCT UPF_Name FROM T_Ref_User_Fields_Preference WHERE UPF_user = ?";
                if (!string.IsNullOrWhiteSpace(contains))
                {
                    sql += " AND UPF_Name LIKE ?";
                }
                sql += " ORDER BY UPF_Name";
                using (var cmd = new OleDbCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@p1", _currentUser);
                    if (!string.IsNullOrWhiteSpace(contains))
                        cmd.Parameters.AddWithValue("@p2", "%" + contains + "%");
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var val = reader[0]?.ToString();
                            if (!string.IsNullOrWhiteSpace(val)) names.Add(val);
                        }
                    }
                }
            }
            return names;
        }

        /// <summary>
        /// Deletes all entries for the given view name for the current user.
        /// Returns true if at least one row was deleted.
        /// </summary>
        public bool DeleteView(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            using (var conn = new OleDbConnection(_referentialConnectionString))
            {
                conn.Open();
                using (var cmd = new OleDbCommand("DELETE FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? AND UPF_Name = ?", conn))
                {
                    cmd.Parameters.AddWithValue("@p1", _currentUser);
                    cmd.Parameters.AddWithValue("@p2", name);
                    var n = cmd.ExecuteNonQuery();
                    return n > 0;
                }
            }
        }
    }
}
