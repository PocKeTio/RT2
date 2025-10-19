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
            
            // Amount filter with optional tolerance
            if (f.Amount.HasValue)
            {
                if (f.AmountWithTolerance)
                {
                    var minAmount = f.Amount.Value - 1m;
                    var maxAmount = f.Amount.Value + 1m;
                    parts.Add($"SignedAmount >= {minAmount.ToString(CultureInfo.InvariantCulture)} AND SignedAmount <= {maxAmount.ToString(CultureInfo.InvariantCulture)}");
                }
                else
                {
                    parts.Add($"SignedAmount = {f.Amount.Value.ToString(CultureInfo.InvariantCulture)}");
                }
            }
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
            if (!string.IsNullOrWhiteSpace(f.DwCommissionId)) parts.Add($"DWINGS_BGPMT LIKE '%{Esc(f.DwCommissionId)}%'");

            if (!string.IsNullOrWhiteSpace(f.Status))
            {
                var status = f.Status.Trim();
                if (string.Equals(status, "Matched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Not Null AND DWINGS_GuaranteeID <> '') OR (DWINGS_BGPMT Is Not Null AND DWINGS_BGPMT <> ''))");
                else if (string.Equals(status, "Unmatched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Null OR DWINGS_GuaranteeID = '') AND (DWINGS_BGPMT Is Null OR DWINGS_BGPMT = ''))");
                else if (string.Equals(status, "Live", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NULL");
                else if (string.Equals(status, "Archived", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NOT NULL");
            }

            // Action filter (by Action ID)
            if (f.ActionId.HasValue)
                parts.Add($"r.Action = {f.ActionId.Value}");

            // KPI filter (by KPI ID)
            if (f.KpiId.HasValue)
                parts.Add($"r.KPI = {f.KpiId.Value}");

            // Incident Type filter
            if (f.IncidentTypeId.HasValue)
                parts.Add($"r.IncidentType = {f.IncidentTypeId.Value}");

            if (f.ActionDone.HasValue)
                parts.Add(f.ActionDone.Value ? "r.ActionStatus = TRUE" : "(r.ActionStatus = FALSE OR r.ActionStatus IS NULL)");

            // ActionDate (specific date filter)
            if (f.ActionDate.HasValue)
            {
                var d = f.ActionDate.Value.Date;
                var next = d.AddDays(1);
                parts.Add($"r.ActionDate >= {DateLit(d)} AND r.ActionDate < {DateLit(next)}");
            }
            
            // ToRemind filter
            if (f.ToRemind.HasValue)
                parts.Add(f.ToRemind.Value ? "r.ToRemind = TRUE" : "(r.ToRemind = FALSE OR r.ToRemind IS NULL)");
            
            // RemindDate (specific date filter)
            if (f.RemindDate.HasValue)
            {
                var d = f.RemindDate.Value.Date;
                var next = d.AddDays(1);
                parts.Add($"r.ToRemindDate >= {DateLit(d)} AND r.ToRemindDate < {DateLit(next)}");
            }

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
