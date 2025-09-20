using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace RecoTool.Windows
{
    // Partial: KPI and Status bar helpers for ReconciliationView
    public partial class ReconciliationView
    {
        private void UpdateKpis(IEnumerable<RecoTool.Services.DTOs.ReconciliationViewData> data)
        {
            try
            {
                var list = data?.ToList() ?? new List<RecoTool.Services.DTOs.ReconciliationViewData>();
                int total = list.Count;
                int matched = list.Count(a => !string.IsNullOrWhiteSpace(a.DWINGS_GuaranteeID)
                                           || !string.IsNullOrWhiteSpace(a.DWINGS_InvoiceID)
                                           || !string.IsNullOrWhiteSpace(a.DWINGS_BGPMT));
                int unmatched = total - matched;

                decimal totalAmt = list.Sum(a => a.SignedAmount);
                decimal matchedAmt = list.Where(a => !string.IsNullOrWhiteSpace(a.DWINGS_GuaranteeID)
                                                   || !string.IsNullOrWhiteSpace(a.DWINGS_InvoiceID)
                                                   || !string.IsNullOrWhiteSpace(a.DWINGS_BGPMT))
                                         .Sum(a => a.SignedAmount);
                decimal unmatchedAmt = totalAmt - matchedAmt;

                if (KpiTotalCountText != null) KpiTotalCountText.Text = total.ToString();
                if (KpiMatchedCountText != null) KpiMatchedCountText.Text = matched.ToString();
                if (KpiUnmatchedCountText != null) KpiUnmatchedCountText.Text = unmatched.ToString();
                if (KpiTotalAmountText != null) KpiTotalAmountText.Text = totalAmt.ToString("N2");
                if (KpiMatchedAmountText != null) KpiMatchedAmountText.Text = matchedAmt.ToString("N2");
                if (KpiUnmatchedAmountText != null) KpiUnmatchedAmountText.Text = unmatchedAmt.ToString("N2");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"KPI error: {ex.Message}");
            }
        }

        private void UpdateStatusInfo(string status)
        {
            try
            {
                if (AccountInfoText != null)
                {
                    AccountInfoText.Text = $"Country: {_currentCountryId ?? "N/A"} | {status}";
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error updating status: {ex.Message}");
            }
        }
    }
}
