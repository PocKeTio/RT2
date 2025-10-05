using System;
using System.Windows;
using System.Windows.Controls;

namespace RecoTool.Windows
{
    // Partial: Title helpers for ReconciliationView
    public partial class ReconciliationView
    {
        // Met à jour le titre de la vue
        private void UpdateViewTitle()
        {
            try
            {
                if (TitleText != null)
                {
                    // Display only the view name (account info is shown on line 3 via AccountTypeText)
                    var hasNamedFilter = !string.IsNullOrWhiteSpace(_currentView) && !string.Equals(_currentView, "Default View", StringComparison.OrdinalIgnoreCase);
                    TitleText.Text = hasNamedFilter ? _currentView : "Default View";
                }
                
                // Update the account type display (line 3)
                UpdateCountryPivotReceivableInfo();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error updating title: {ex.Message}");
            }
        }
    }
}
