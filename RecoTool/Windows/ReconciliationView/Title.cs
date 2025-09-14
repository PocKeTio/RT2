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
                    // Construire un titre convivial selon l'état courant
                    // 1) Nom du filtre (si défini)
                    var hasNamedFilter = !string.IsNullOrWhiteSpace(_currentView) && !string.Equals(_currentView, "Default View", StringComparison.OrdinalIgnoreCase);

                    // 2) Account
                    var accId = VM?.FilterAccountId;
                    var accPart = string.IsNullOrWhiteSpace(accId) ? "All" : accId;

                    // 3) Status
                    var stat = VM?.FilterStatus;
                    var statPart = string.IsNullOrWhiteSpace(stat) ? "All" : stat;

                    // 4) Construire le titre final
                    // Format: "Account: {acc} | Status: {status}" et si filtre nommé: " - {filterName}"
                    var baseTitle = $"Account: {accPart} | Status: {statPart}";
                    var finalTitle = hasNamedFilter ? baseTitle + $" - {_currentView}" : baseTitle;

                    TitleText.Text = finalTitle;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error updating title: {ex.Message}");
            }
        }

        // Définit le titre de la vue (ex: nom du filtre sélectionné) et met à jour l'UI
        public void SetViewTitle(string title)
        {
            _currentView = string.IsNullOrWhiteSpace(title) ? _currentView : title;
            UpdateViewTitle();
        }
    }
}
