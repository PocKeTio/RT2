using System;
using System.Windows;

namespace RecoTool.Windows
{
    // Partial: Saved filters and reset handlers for ReconciliationView
    public partial class ReconciliationView
    {
        // Reset all filter controls to default values, preserving some parent-provided constraints
        private void ResetFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Preserve Account and Status provided by the parent page (do NOT clear)
                // FilterAccountId = null;
                // FilterStatus = null;

                // Basic fields
                FilterCurrency = null;
                FilterCountry = null;
                FilterFromDate = null;
                FilterToDate = null;
                FilterDeletedDate = null;
                FilterMinAmount = null;
                FilterMaxAmount = null;
                FilterReconciliationNum = null;
                FilterRawLabel = null;
                FilterEventNum = null;
                FilterDwGuaranteeId = null;
                FilterDwCommissionId = null;

                // String-backed combos (kept)
                FilterGuaranteeType = null;
                FilterTransactionType = null;
                FilterTransactionTypeId = null;
                FilterGuaranteeStatus = null;

                // ID-backed referentials
                FilterActionId = null;
                FilterKpiId = null;
                FilterIncidentTypeId = null;

                // Assignee
                FilterAssigneeId = null;

                // Toggles
                FilterPotentialDuplicates = false;
                FilterUnmatched = false;
                FilterNewLines = false;
                FilterActionDone = null; // reset to All
                FilterActionDateFrom = null;
                FilterActionDateTo = null;

                // Apply and refresh title/status
                ApplyFilters();
                UpdateViewTitle();
            }
            catch { }
        }
    }
}
