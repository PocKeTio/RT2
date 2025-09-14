using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace RecoTool.Domain.Filters
{
    /// <summary>
    /// Builds Access-compatible WHERE clauses from a FilterState snapshot.
    /// </summary>
    public static class FilterBuilder
    {
        private static string Esc(string s) => string.IsNullOrEmpty(s) ? s : s.Replace("'", "''");
        private static string DateLit(DateTime d) => "#" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "#"; // Access date literal

        // UI -> DB mapping for a few enumerated values (keep compatible with current behavior)
        private static string MapUiToDb(string s)
        {
            switch ((s ?? string.Empty).Trim().ToUpperInvariant())
            {
                case "REISSUANCE": return "REISSU";
                case "ISSUANCE": return "ISSU";
                case "ADVISING": return "NOTIF";
                default: return s;
            }
        }

        public static string BuildWhere(FilterState f)
        {
            if (f == null) return string.Empty;

            var parts = new List<string>();

            if (!string.IsNullOrWhiteSpace(f.AccountId)) parts.Add($"Account_ID = '{Esc(f.AccountId)}'");
            if (!string.IsNullOrWhiteSpace(f.Currency)) parts.Add($"CCY = '{Esc(f.Currency)}'");
            if (f.MinAmount.HasValue) parts.Add($"SignedAmount >= {f.MinAmount.Value.ToString(CultureInfo.InvariantCulture)}");
            if (f.MaxAmount.HasValue) parts.Add($"SignedAmount <= {f.MaxAmount.Value.ToString(CultureInfo.InvariantCulture)}");
            if (f.FromDate.HasValue) parts.Add($"Operation_Date >= {DateLit(f.FromDate.Value)}");
            if (f.ToDate.HasValue) parts.Add($"Operation_Date <= {DateLit(f.ToDate.Value)}");

            if (f.DeletedDate.HasValue)
            {
                var d = f.DeletedDate.Value.Date;
                var next = d.AddDays(1);
                parts.Add($"a.DeleteDate >= {DateLit(d)} AND a.DeleteDate < {DateLit(next)}");
            }

            if (!string.IsNullOrWhiteSpace(f.ReconciliationNum)) parts.Add($"a.Reconciliation_Num LIKE '%{Esc(f.ReconciliationNum)}%'");
            if (!string.IsNullOrWhiteSpace(f.RawLabel)) parts.Add($"RawLabel LIKE '%{Esc(f.RawLabel)}%'");
            if (!string.IsNullOrWhiteSpace(f.EventNum)) parts.Add($"Event_Num LIKE '%{Esc(f.EventNum)}%'");

            if (!string.IsNullOrWhiteSpace(f.TransactionType))
            {
                var t = Esc(f.TransactionType);
                parts.Add($"(Pivot_TransactionCodesFromLabel LIKE '%{t}%' OR Pivot_TRNFromLabel LIKE '%{t}%')");
            }

            if (!string.IsNullOrWhiteSpace(f.GuaranteeStatus))
            {
                var gs = Esc(f.GuaranteeStatus);
                parts.Add($"GUARANTEE_STATUS LIKE '%{gs}%'");
            }

            if (!string.IsNullOrWhiteSpace(f.GuaranteeType))
            {
                var gt = Esc(MapUiToDb(f.GuaranteeType));
                parts.Add($"GUARANTEE_TYPE = '{gt}'");
            }

            if (!string.IsNullOrWhiteSpace(f.DwGuaranteeId)) parts.Add($"DWINGS_GuaranteeID LIKE '%{Esc(f.DwGuaranteeId)}%'");
            if (!string.IsNullOrWhiteSpace(f.DwCommissionId)) parts.Add($"DWINGS_CommissionID LIKE '%{Esc(f.DwCommissionId)}%'");

            if (!string.IsNullOrWhiteSpace(f.Status))
            {
                var status = f.Status.Trim();
                if (string.Equals(status, "Matched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Not Null AND DWINGS_GuaranteeID <> '') OR (DWINGS_CommissionID Is Not Null AND DWINGS_CommissionID <> ''))");
                else if (string.Equals(status, "Unmatched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Null OR DWINGS_GuaranteeID = '') AND (DWINGS_CommissionID Is Null OR DWINGS_CommissionID = ''))");
                else if (string.Equals(status, "Live", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NULL");
                else if (string.Equals(status, "Archived", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NOT NULL");
            }

            if (f.ActionDone.HasValue)
                parts.Add(f.ActionDone.Value ? "r.ActionStatus = TRUE" : "(r.ActionStatus = FALSE OR r.ActionStatus IS NULL)");

            if (f.ActionDateFrom.HasValue) parts.Add($"r.ActionDate >= {DateLit(f.ActionDateFrom.Value)}");
            if (f.ActionDateTo.HasValue) parts.Add($"r.ActionDate <= {DateLit(f.ActionDateTo.Value)}");

            // Comments contains (on reconciliation side)
            if (!string.IsNullOrWhiteSpace(f.Comments))
            {
                var c = Esc(f.Comments);
                parts.Add($"r.Comments LIKE '%{c}%'");
            }

            return parts.Count == 0 ? string.Empty : ("WHERE " + string.Join(" AND ", parts));
        }
    }
}
