using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Globalization;
using RecoTool.Domain.Filters;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    // Partial: Saved filter preset snapshot + SQL bridge helpers
    public partial class ReconciliationView
    {
        // ---- Generic saved filter snapshot support ----
        // OPTIMIZATION: Only capture non-empty/non-default fields to keep saved filters minimal
        // IMPORTANT: Never save AccountId or Country - these are managed by page/todolist context
        private FilterPreset GetCurrentFilterPreset()
        {
            var f = VM?.CurrentFilter ?? new RecoTool.Domain.Filters.FilterState();
            var preset = new FilterPreset();
            
            // Only set fields that have actual values (not null, not empty, not default)
            // NEVER save AccountId or Country - managed externally
            
            if (!string.IsNullOrWhiteSpace(f.Currency))
                preset.Currency = f.Currency;
            
            if (f.MinAmount.HasValue)
                preset.MinAmount = f.MinAmount;
            
            if (f.MaxAmount.HasValue)
                preset.MaxAmount = f.MaxAmount;
            
            if (f.FromDate.HasValue)
                preset.FromDate = f.FromDate;
            
            if (f.ToDate.HasValue)
                preset.ToDate = f.ToDate;
            
            if (f.ActionId.HasValue)
                preset.Action = f.ActionId;
            
            if (f.KpiId.HasValue)
                preset.KPI = f.KpiId;
            
            if (f.IncidentTypeId.HasValue)
                preset.IncidentType = f.IncidentTypeId;
            
            if (!string.IsNullOrWhiteSpace(f.Status))
                preset.Status = f.Status;
            
            if (!string.IsNullOrWhiteSpace(f.ReconciliationNum))
                preset.ReconciliationNum = f.ReconciliationNum;
            
            if (!string.IsNullOrWhiteSpace(f.RawLabel))
                preset.RawLabel = f.RawLabel;
            
            if (!string.IsNullOrWhiteSpace(f.EventNum))
                preset.EventNum = f.EventNum;
            
            if (!string.IsNullOrWhiteSpace(f.DwGuaranteeId))
                preset.DwGuaranteeId = f.DwGuaranteeId;
            
            if (!string.IsNullOrWhiteSpace(f.DwCommissionId))
                preset.DwCommissionId = f.DwCommissionId;
            
            if (!string.IsNullOrWhiteSpace(f.GuaranteeType))
                preset.GuaranteeType = f.GuaranteeType;
            
            if (!string.IsNullOrWhiteSpace(f.Comments))
                preset.Comments = f.Comments;
            
            if (f.PotentialDuplicates.HasValue && f.PotentialDuplicates.Value)
                preset.PotentialDuplicates = f.PotentialDuplicates;
            
            if (f.Unmatched.HasValue && f.Unmatched.Value)
                preset.Unmatched = f.Unmatched;
            
            if (f.NewLines.HasValue && f.NewLines.Value)
                preset.NewLines = f.NewLines;
            
            if (f.ActionDone.HasValue)
                preset.ActionDone = f.ActionDone;
            
            if (f.ActionDateFrom.HasValue)
                preset.ActionDateFrom = f.ActionDateFrom;
            
            if (f.ActionDateTo.HasValue)
                preset.ActionDateTo = f.ActionDateTo;
            
            return preset;
        }

        private void ApplyFilterPreset(FilterPreset p)
        {
            if (p == null) return;
            try
            {
                // Ne pas appliquer le compte depuis un preset de vue/filtre (compte géré en dehors)
                // FilterAccountId = p.AccountId;
                FilterCurrency = p.Currency;
                _filterCountry = p.Country; // informational
                FilterMinAmount = p.MinAmount;
                FilterMaxAmount = p.MaxAmount;
                FilterFromDate = p.FromDate;
                FilterToDate = p.ToDate;
                // Prefer ID-based restore
                FilterActionId = p.Action;
                FilterKpiId = p.KPI;
                FilterIncidentTypeId = p.IncidentType;
                FilterStatus = p.Status;
                FilterReconciliationNum = p.ReconciliationNum;
                FilterRawLabel = p.RawLabel;
                FilterEventNum = p.EventNum;
                FilterDwGuaranteeId = p.DwGuaranteeId;
                FilterDwCommissionId = p.DwCommissionId;
                FilterGuaranteeType = p.GuaranteeType;
                FilterComments = p.Comments;
                // Default to false if not present in legacy presets
                FilterPotentialDuplicates = p.PotentialDuplicates ?? false;
                FilterUnmatched = p.Unmatched ?? false;
                FilterNewLines = p.NewLines ?? false;
                // New
                FilterActionDone = p.ActionDone;
                FilterActionDateFrom = p.ActionDateFrom;
                FilterActionDateTo = p.ActionDateTo;
            }
            catch { }
        }

        // Removes any Account_ID = '...' predicate from a WHERE or full SQL fragment.
        // Preserves other predicates and keeps/strips the WHERE keyword appropriately.
        private string StripAccountFromWhere(string whereOrSql)
        {
            return FilterSqlHelper.StripAccount(whereOrSql);
        }

        private string BuildSqlWithJsonComment(FilterPreset preset, string whereClause)
        {
            return FilterSqlHelper.BuildSqlWithJson(preset, whereClause);
        }

        private bool TryExtractPresetFromSql(string sql, out FilterPreset preset, out string pureWhere)
        {
            preset = null;
            pureWhere = sql ?? string.Empty;
            if (string.IsNullOrWhiteSpace(sql)) return false;
            try
            {
                if (FilterSqlHelper.TryExtractPreset(sql, out var json, out var where))
                {
                    pureWhere = where;
                    if (!string.IsNullOrWhiteSpace(json))
                        preset = JsonSerializer.Deserialize<FilterPreset>(json);
                    return preset != null;
                }
            }
            catch { }
            return false;
        }

        // Build an Access SQL WHERE clause from current bound filters
        // IMPORTANT: Never include AccountId - it's managed by page/todolist context
        private string GenerateWhereClause()
        {
            var f = VM?.CurrentFilter ?? new RecoTool.Domain.Filters.FilterState();
            
            // DEBUG: Log filter state before building WHERE
            System.Diagnostics.Debug.WriteLine($"[GenerateWhereClause] ActionId={f.ActionId}, KpiId={f.KpiId}, Status={f.Status}");
            
            var state = new RecoTool.Domain.Filters.FilterState
            {
                // AccountId excluded - managed by page/todolist
                Currency = f.Currency,
                MinAmount = f.MinAmount,
                MaxAmount = f.MaxAmount,
                FromDate = f.FromDate,
                ToDate = f.ToDate,
                DeletedDate = f.DeletedDate,
                ReconciliationNum = f.ReconciliationNum,
                RawLabel = f.RawLabel,
                EventNum = f.EventNum,
                TransactionTypeId = f.TransactionTypeId,
                TransactionType = f.TransactionType,
                GuaranteeStatus = f.GuaranteeStatus,
                GuaranteeType = f.GuaranteeType,
                Comments = f.Comments,
                DwGuaranteeId = f.DwGuaranteeId,
                DwCommissionId = f.DwCommissionId,
                Status = f.Status,
                ActionId = f.ActionId,
                KpiId = f.KpiId,
                IncidentTypeId = f.IncidentTypeId,
                ActionDone = f.ActionDone,
                ActionDateFrom = f.ActionDateFrom,
                ActionDateTo = f.ActionDateTo,
            };
            var result = RecoTool.Domain.Filters.FilterBuilder.BuildWhere(state);
            System.Diagnostics.Debug.WriteLine($"[GenerateWhereClause] Generated SQL: {result}");
            return result;
        }

        // Parse the WHERE clause we generate and set bound properties accordingly
        private void ApplyWhereClause(string where)
        {
            if (string.IsNullOrWhiteSpace(where)) return;
            string s = where.Trim();
            if (s.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase)) s = s.Substring(6);

            string GetString(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                return m.Success ? m.Groups[1].Value.Replace("''", "'") : null;
            }
            decimal? GetDecimal(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                if (!m.Success) return null;
                return decimal.TryParse(m.Groups[1].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : (decimal?)null;
            }
            DateTime? GetDate(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                if (!m.Success) return null;
                var v = m.Groups[1].Value; // yyyy-MM-dd
                return DateTime.TryParse(v, out var dt) ? dt : (DateTime?)null;
            }

            FilterAccountId = GetString(@"Account_ID\s*=\s*'([^']*)'");
            FilterCurrency = GetString(@"CCY\s*=\s*'([^']*)'");
            FilterMinAmount = GetDecimal(@"SignedAmount\s*>=\s*([0-9]+(?:\.[0-9]+)?)");
            FilterMaxAmount = GetDecimal(@"SignedAmount\s*<=\s*([0-9]+(?:\.[0-9]+)?)");
            var d1 = GetDate(@"Operation_Date\s*>=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#");
            var d2 = GetDate(@"Operation_Date\s*<=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#");
            FilterFromDate = d1;
            FilterToDate = d2;
            FilterReconciliationNum = GetString(@"Reconciliation_Num\s+LIKE\s+'%([^']*)%'");
            FilterRawLabel = GetString(@"RawLabel\s+LIKE\s+'%([^']*)%'");
            FilterEventNum = GetString(@"Event_Num\s+LIKE\s+'%([^']*)%'");
            FilterComments = GetString(@"r\.Comments\s+LIKE\s+'%([^']*)%'");
            // Restore Transaction Type if present (from either field condition)
            var trn1 = GetString(@"Pivot_TransactionCodesFromLabel\s+LIKE\s+'%([^']*)%'");
            var trn2 = GetString(@"Pivot_TRNFromLabel\s+LIKE\s+'%([^']*)%'");
            if (!string.IsNullOrWhiteSpace(trn1)) FilterTransactionType = trn1;
            else if (!string.IsNullOrWhiteSpace(trn2)) FilterTransactionType = trn2;
            // Restore Guarantee Status
            var gs = GetString(@"GUARANTEE_STATUS\s+LIKE\s+'%([^']*)%'");
            if (!string.IsNullOrWhiteSpace(gs)) FilterGuaranteeStatus = gs;
            // Restore Guarantee Type (exact match)
            string MapDbToUi(string s2)
            {
                switch ((s2 ?? string.Empty).Trim().ToUpperInvariant())
                {
                    case "REISSU": return "REISSUANCE";
                    case "ISSU": return "ISSUANCE";
                    case "NOTIF": return "ADVISING";
                    default: return s2;
                }
            }
            var gt = GetString(@"GUARANTEE_TYPE\s*=\s*'([^']*)'");
            if (!string.IsNullOrWhiteSpace(gt)) FilterGuaranteeType = MapDbToUi(gt);
            var hasMatched = Regex.IsMatch(s, @"\(\(DWINGS_GuaranteeID\s+Is\s+Not\s+Null\s+AND\s+DWINGS_GuaranteeID\s+<>\s+''\)\s+OR\s+\(DWINGS_BGPMT\s+Is\s+Not\s+Null\s+AND\s+DWINGS_BGPMT\s+<>\s+''\)\)", RegexOptions.IgnoreCase);
            var hasUnmatched = Regex.IsMatch(s, @"\(\(DWINGS_GuaranteeID\s+Is\s+Null\s+OR\s+DWINGS_GuaranteeID\s+=\s+''\)\s+AND\s+\(DWINGS_BGPMT\s+Is\s+Null\s+OR\s+DWINGS_BGPMT\s+=\s+''\)\)", RegexOptions.IgnoreCase);
            FilterStatus = hasMatched ? "Matched" : hasUnmatched ? "Unmatched" : FilterStatus;
            FilterDwGuaranteeId = GetString(@"DWINGS_GuaranteeID.*LIKE\s+'%([^']*)%'");
            FilterDwCommissionId = GetString(@"DWINGS_BGPMT.*LIKE\s+'%([^']*)%'");

            // Restore DeletedDate single-day filter if present (expects pattern a.DeleteDate >= #YYYY-MM-DD# AND a.DeleteDate < #YYYY-MM-DD#)
            try
            {
                var m1 = Regex.Match(s, @"a\.DeleteDate\s*>=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#", RegexOptions.IgnoreCase);
                if (m1.Success)
                {
                    if (DateTime.TryParse(m1.Groups[1].Value, out var dd))
                        FilterDeletedDate = dd.Date;
                }
            }
            catch { }

            // Restore Live/Archived from DeleteDate presence conditions
            try
            {
                if (Regex.IsMatch(s, @"a\.DeleteDate\s+is\s+null", RegexOptions.IgnoreCase))
                    FilterStatus = "Live";
                else if (Regex.IsMatch(s, @"a\.DeleteDate\s+is\s+not\s+null", RegexOptions.IgnoreCase))
                    FilterStatus = "Archived";
            }
            catch { }
        }
    }
}
