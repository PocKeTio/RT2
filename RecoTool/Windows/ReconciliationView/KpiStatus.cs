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
                
                // Calculate KPI counts
                int toReview = list.Count(a => !a.IsReviewed);
                int reviewed = list.Count(a => a.IsReviewed);
                int toRemind = list.Count(a => a.HasActiveReminder); // Active reminders (ToRemind = true and ToRemindDate <= today)
                int notLinkedCount = list.Count(a => a.StatusColor == "#F44336"); // Red - No DWINGS link
                int notGroupedCount = list.Count(a => !a.IsMatchedAcrossAccounts); // NOT grouped (no "G" in grid)
                int discrepancyCount = list.Count(a => a.StatusColor == "#FFC107" || a.StatusColor == "#FF6F00"); // Yellow or Dark Amber
                int matchedCount = list.Count(a => a.StatusColor == "#4CAF50"); // Green - Balanced and grouped

                decimal totalAmt = list.Sum(a => a.SignedAmount);

                // Update KPI text blocks
                if (KpiTotalCountText != null) KpiTotalCountText.Text = total.ToString();
                if (KpiTotalAmountText != null) KpiTotalAmountText.Text = totalAmt.ToString("N2");
                if (KpiToReviewCountText != null) KpiToReviewCountText.Text = toReview.ToString(CultureInfo.InvariantCulture);
                
                // Update new KPI indicators
                var reviewedText = this.FindName("KpiReviewedCountText") as System.Windows.Controls.TextBlock;
                var toRemindText = this.FindName("KpiToRemindCountText") as System.Windows.Controls.TextBlock;
                var notLinkedText = this.FindName("KpiNotLinkedCountText") as System.Windows.Controls.TextBlock;
                var notGroupedText = this.FindName("KpiNotGroupedCountText") as System.Windows.Controls.TextBlock;
                var discrepancyText = this.FindName("KpiDiscrepancyCountText") as System.Windows.Controls.TextBlock;
                var matchedText = this.FindName("KpiMatchedCountText") as System.Windows.Controls.TextBlock;
                
                if (reviewedText != null) reviewedText.Text = reviewed.ToString();
                if (toRemindText != null) toRemindText.Text = toRemind.ToString();
                if (notLinkedText != null) notLinkedText.Text = notLinkedCount.ToString();
                if (notGroupedText != null) notGroupedText.Text = notGroupedCount.ToString();
                if (discrepancyText != null) discrepancyText.Text = discrepancyCount.ToString();
                if (matchedText != null) matchedText.Text = matchedCount.ToString();
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
                    var accountType = VM.FilterAccountId switch
                    {
                        "PIVOT" => "Pivot",
                        "RECEIVABLE" => "Receivable",
                        _ => ""
                    };
                    
                    // Display only account type and status, no "Country: XX" prefix
                    if (!string.IsNullOrEmpty(accountType))
                    {
                        AccountInfoText.Text = $"{accountType} | {status}";
                    }
                    else
                    {
                        AccountInfoText.Text = status;
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error updating status: {ex.Message}");
            }
        }
    }
}
