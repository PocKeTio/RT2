using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;

namespace RecoTool.Windows
{
    // Partial: Filtering (apply + clear) for ReconciliationView
    public partial class ReconciliationView
    {
        // Applique les filtres aux données
        private void ApplyFilters()
        {
            if (_allViewData == null) return;
            var sw = Stopwatch.StartNew();

            // Refresh TransactionType options with all other filters applied
            var preTransactionType = VM.ApplyFilters(_allViewData, excludeTransactionType: true);
            VM.UpdateTransactionTypeOptionsForData(preTransactionType);

            // Apply full filter set
            var filteredList = VM.ApplyFilters(_allViewData);

            // Update display with pagination (first N lines) but totals on full filtered set
            _filteredData = filteredList;
            _loadedCount = Math.Min(InitialPageSize, _filteredData.Count);
            ViewData = new ObservableCollection<RecoTool.Services.DTOs.ReconciliationViewData>(_filteredData.Take(_loadedCount));
            UpdateKpis(_filteredData); // totals on entire set
            UpdateStatusInfo($"{ViewData.Count} / {_filteredData.Count} lines displayed");
            var acc = VM.CurrentFilter?.AccountId ?? "All";
            var stat = VM.CurrentFilter?.Status ?? "All";
            LogAction("ApplyFilters", $"{ViewData.Count} / {_filteredData.Count} displayed | Account={acc} | Status={stat}");
            sw.Stop();
            try { LogPerf("ApplyFilters", $"source={_allViewData.Count} | displayed={ViewData.Count} | ms={sw.ElapsedMilliseconds}"); } catch { }
        }

        // Réinitialise tous les filtres
        private void ClearFilters()
        {
            // Preserve Account filter (managed by parent) and Status
            // Reset all other filters through the property bridge to update VM and UI
            try
            {
                FilterCurrency = null;
                _filterCountry = null; // informational only
                FilterMinAmount = null;
                FilterMaxAmount = null;
                FilterFromDate = null;
                FilterGuaranteeType = null;
                FilterTransactionType = null;
                FilterTransactionTypeId = null;
                FilterGuaranteeStatus = null;
                FilterComments = null;
                FilterActionId = null;
                FilterKpiId = null;
                FilterIncidentTypeId = null;
                FilterAssigneeId = null;
                FilterPotentialDuplicates = false;
                FilterUnmatched = false;
                FilterNewLines = false;
                FilterActionDone = null;
                FilterActionDateFrom = null;
                FilterActionDateTo = null;
                FilterDwGuaranteeId = null;
                FilterDwCommissionId = null;
                FilterReconciliationNum = null;
                FilterRawLabel = null;
                FilterEventNum = null;
                // Keep Status as-is
            }
            catch { }

            // Optionally clear any UI controls with explicit names (legacy)
            ClearFilterControls();
            ApplyFilters();
        }

        // Efface les contrôles de filtre dans l'UI
        private void ClearFilterControls()
        {
            try
            {
                // Effacer les TextBox de filtres (noms basés sur le XAML)
                // Do NOT clear AccountId control to preserve Account filter from parent page
                ClearTextBox("CurrencyFilterTextBox");
                ClearTextBox("CountryFilterTextBox");
                ClearTextBox("MinAmountFilterTextBox");
                ClearTextBox("MaxAmountFilterTextBox");
                ClearDatePicker("FromDatePicker");
                ClearDatePicker("ToDatePicker");
                ClearComboBox("ActionComboBox");
                ClearComboBox("KPIComboBox");
                ClearComboBox("IncidentTypeComboBox");
                ClearComboBox("AssigneeComboBox");
                // New ComboBoxes in Ambre Filters
                ClearComboBox("TypeComboBox");
                ClearComboBox("TransactionTypeComboBox");
                ClearComboBox("GuaranteeStatusComboBox");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error clearing filters: {ex.Message}");
            }
        }
    }
}
