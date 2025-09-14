using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: UI event handlers for ReconciliationView
    public partial class ReconciliationView
    {
        private void CloseViewButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Si hébergé dans une fenêtre popup, fermer la fenêtre propriétaire
                var wnd = Window.GetWindow(this);
                if (wnd != null && wnd.Owner != null)
                {
                    // Avoid closing synchronously during a closing cycle; defer to dispatcher
                    if (wnd.IsLoaded && !wnd.Dispatcher.HasShutdownStarted)
                    {
                        wnd.Dispatcher.BeginInvoke(new Action(() =>
                        {
                            try { if (wnd.IsLoaded) wnd.Close(); } catch { }
                        }), DispatcherPriority.Background);
                    }
                    return;
                }

                // Sinon, demander à la page parente de supprimer cette vue
                CloseRequested?.Invoke(this, EventArgs.Empty);
            }
            catch { }
        }

        // Basculer l'affichage des filtres
        private void ToggleFilters_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                FiltersExpander.IsExpanded = !FiltersExpander.IsExpanded;
            }
            catch (Exception ex)
            {
                ShowError($"Error switching filters: {ex.Message}");
            }
        }

        // Sélection changée dans la grille
        private void ResultsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                // Avoid showing long reconciliation line IDs in the header to prevent layout shifts.
                // Instead, show a compact selection count.
                int count = 0;
                try { count = ResultsDataGrid?.SelectedItems?.Count ?? 0; } catch { count = 0; }
                if (count > 0)
                {
                    var msg = count == 1 ? "Selected 1 row" : $"Selected {count} rows";
                    UpdateStatusInfo(msg);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Selection error: {ex.Message}");
            }
        }

        // Double-clic sur une ligne de la grille
        private async void ResultsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var selectedItem = ResultsDataGrid.SelectedItem as ReconciliationViewData;
                if (selectedItem != null)
                {
                    var win = new RecoTool.UI.Views.Windows.ReconciliationDetailWindow(selectedItem, _allViewData, _reconciliationService, _offlineFirstService);
                    win.Owner = Window.GetWindow(this);
                    var result = win.ShowDialog();
                    if (result == true)
                    {
                        await RefreshAsync();
                        // After successful detail save, push pending changes best-effort (debounced)
                        try
                        {
                            ScheduleBulkPushDebounced();
                        }
                        catch { }
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error opening detail: {ex.Message}");
            }
        }

        // Efface les filtres (événement du bouton)
        private void ClearFilters_Click(object sender, RoutedEventArgs e)
        {
            ClearFilters();
        }

        private void ApplyAgeFilter_Click(object sender, RoutedEventArgs e)
        {
            try { ApplyFilters(); } catch { }
        }
    }
}
