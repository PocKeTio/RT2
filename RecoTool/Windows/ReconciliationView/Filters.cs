using System;
using System.Windows;

namespace RecoTool.Windows
{
    // Partial: Saved filters and reset handlers for ReconciliationView
    public partial class ReconciliationView
    {
        // Reapply current filters (useful to refresh after data changes)
        private void ReapplyFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                ApplyFilters();
                UpdateViewTitle();
            }
            catch { }
        }

        // Reset all filter controls to initial state (todolist/filter) or default values
        private void ResetFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // If we have an initial filter state (from todolist/saved filter), restore it
                if (!string.IsNullOrWhiteSpace(_initialFilterSql))
                {
                    ApplySavedFilterSql(_initialFilterSql);
                    ApplyFilters();
                    UpdateViewTitle();
                    UpdateStatusInfo("Filters reset to initial state");
                    return;
                }

                // Otherwise, clear all filters (default behavior)
                // Preserve Account and Status provided by the parent page (do NOT clear)
                // FilterAccountId = null;
                // FilterStatus = null;

                // Basic fields
                FilterCurrency = null;
                FilterCountry = null;
                FilterFromDate = null;
                FilterToDate = null;
                FilterDeletedDate = null;
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
                FilterLastReviewed = null; // reset to All

                // Apply and refresh title/status
                ApplyFilters();
                UpdateViewTitle();
            }
            catch { }
        }
    }
}
