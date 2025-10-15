using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Linq;

namespace RecoTool.Domain.Filters
{
    /// <summary>
    /// Utilities to embed/extract JSON snapshot comments and to sanitize backend WHERE clauses.
    /// </summary>
    public static class FilterSqlHelper
    {
        // Regex for Account_ID with optional alias/brackets: [Alias.]?[Account_ID] = '...'
        private static readonly Regex AccountPredicateRegex = new Regex(@"(?i)(\b[\w\[\]]+\.)?\[?Account_ID\]?\s*=\s*'[^']*'", RegexOptions.Compiled);

        /// <summary>
        /// Removes any Account_ID predicate from a WHERE or SQL fragment. Preserves other predicates and WHERE keyword when present.
        /// </summary>
        public static string StripAccount(string whereOrSql)
        {
            if (string.IsNullOrWhiteSpace(whereOrSql)) return whereOrSql;
            try
            {
                var s = whereOrSql.Trim();
                var hasWhere = s.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase);
                if (hasWhere) s = s.Substring(6).Trim();

                string RemovePredicate(string input)
                {
                    if (string.IsNullOrWhiteSpace(input)) return input;
                    var text = input;

                    // 1) Predicate at start followed by AND/OR
                    text = Regex.Replace(text, @"^(\s*\(*\s*)" + AccountPredicateRegex + @"(\s*\)*\s*(AND|OR)\s*)", string.Empty, RegexOptions.IgnoreCase);
                    // 2) Predicate at end preceded by AND/OR
                    text = Regex.Replace(text, @"(\s*(AND|OR)\s*\(*\s*)" + AccountPredicateRegex + @"(\s*\)*\s*)$", string.Empty, RegexOptions.IgnoreCase);
                    // 3) Predicate in the middle with AND neighbors -> collapse to single AND/OR left side
                    text = Regex.Replace(text, @"(\s*(AND|OR)\s*)" + AccountPredicateRegex + @"(\s*(AND|OR)\s*)", m => m.Groups[1].Value, RegexOptions.IgnoreCase);
                    // 4) Bare predicate alone
                    text = Regex.Replace(text, @"^\s*" + AccountPredicateRegex + @"\s*$", string.Empty, RegexOptions.IgnoreCase);

                    // Cleanup doubled spaces and trim parentheses
                    text = Regex.Replace(text, @"\s{2,}", " ").Trim();
                    text = Regex.Replace(text, @"^\((.*)\)$", "$1");
                    text = text.Trim();
                    return text;
                }

                var cleaned = RemovePredicate(s);
                if (string.IsNullOrWhiteSpace(cleaned)) return string.Empty;
                return hasWhere ? ("WHERE " + cleaned) : cleaned;
            }
            catch { return whereOrSql; }
        }

        /// <summary>
        /// Builds a SQL string that embeds a JSON snapshot in a comment prefix.
        /// Only non-null values are serialized to keep the JSON minimal.
        /// </summary>
        public static string BuildSqlWithJson(object preset, string whereClause)
        {
            try
            {
                var options = new JsonSerializerOptions 
                { 
                    WriteIndented = false,
                    DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
                };
                var json = JsonSerializer.Serialize(preset, options);
                return $"/*JSON:{json}*/ " + (whereClause ?? string.Empty);
            }
            catch
            {
                return whereClause ?? string.Empty;
            }
        }

        /// <summary>
        /// Extracts an embedded JSON snapshot (if any) and returns the pure WHERE fragment.
        /// </summary>
        public static bool TryExtractPreset(string sql, out string json, out string pureWhere)
        {
            json = null;
            pureWhere = sql ?? string.Empty;
            if (string.IsNullOrWhiteSpace(sql)) return false;

            try
            {
                var m = Regex.Match(sql, @"^/\*JSON:(.*?)\*/\s*(.*)$", RegexOptions.Singleline);
                if (m.Success)
                {
                    json = m.Groups[1].Value;
                    pureWhere = m.Groups[2].Value?.Trim();
                    return true;
                }
            }
            catch { }
            return false;
        }

        // New: extract PotentialDuplicates flag from JSON prefix (if present)
        public static bool TryExtractPotentialDuplicatesFlag(string filterSql)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(filterSql)) return false;
                var mDup = Regex.Match(filterSql, @"^/\*JSON:(.*?)\*/", RegexOptions.Singleline);
                if (!mDup.Success) return false;
                var json = mDup.Groups[1].Value;
                var preset = JsonSerializer.Deserialize<FilterPreset>(json);
                return preset?.PotentialDuplicates == true;
            }
            catch { return false; }
        }

        // New: normalize/sanitize a predicate fragment suitable to append after AND (...)
        public static string ExtractNormalizedPredicate(string filterSql)
        {
            if (string.IsNullOrWhiteSpace(filterSql)) return null;
            var cond = filterSql.Trim();

            // Strip optional embedded JSON snapshot prefix
            var m = Regex.Match(cond, @"^/\*JSON:(.*?)\*/\s*(.*)$", RegexOptions.Singleline);
            if (m.Success)
                cond = m.Groups[2].Value?.Trim();

            // Unwrap single outer parentheses repeatedly
            while (!string.IsNullOrEmpty(cond) && cond.StartsWith("(") && cond.EndsWith(")"))
            {
                cond = cond.Substring(1, cond.Length - 2).Trim();
            }

            // Strip leading WHERE
            if (cond.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                cond = cond.Substring(6).Trim();

            // Minimal safety gate
            if (!string.IsNullOrEmpty(cond))
            {
                var lower = cond.ToLowerInvariant();
                string[] banned = { " union ", " select ", " insert ", " delete ", " update ", " drop ", " alter ", " exec ", ";" };
                bool hasBanned = banned.Any(k => lower.Contains(k));
                if (hasBanned)
                    return null;

                return cond;
            }

            return null;
        }
    }
}
