using System;
using System.Text.Json;
using System.Windows;
using Microsoft.VisualBasic;
using RecoTool.Properties;
using RecoTool.Services;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    // Partial: Saved Filters and Views handlers for ReconciliationView
    public partial class ReconciliationView
    {
        // Save current grid layout and filters as a named "View" in T_Ref_User_Fields_Preference
        private async void SaveView_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    ShowError("Service not available");
                    return;
                }

                var name = Interaction.InputBox("View name to save:", "Save View", _currentView ?? "My View");
                if (string.IsNullOrWhiteSpace(name)) return;

                // Build SQL snapshot with JSON preset comment
                var preset = GetCurrentFilterPreset();
                var wherePart = _backendFilterSql; // already pure WHERE or null
                var sqlWithJson = BuildSqlWithJsonComment(preset, wherePart);

                // Capture grid layout
                var layout = CaptureGridLayout();
                var layoutJson = JsonSerializer.Serialize(layout);

                // Persist via dedicated service (decoupled from ReconciliationService)
                var refCs = Settings.Default.ReferentialDB;
                var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                var viewSvc = new UserViewPreferenceService(refCs, curUser);
                var id = await viewSvc.UpsertAsync(name, sqlWithJson, layoutJson);
                if (id > 0)
                {
                    CurrentView = name;
                    UpdateStatusInfo($"View '{name}' saved.");
                    
                    // Refresh the SavedViews combo on the host page
                    try
                    {
                        var hostPage = VisualTreeHelpers.FindAncestor<RecoTool.Windows.ReconciliationPage>(this);
                        if (hostPage != null)
                        {
                            _ = hostPage.ReloadSavedViewsOnly();
                        }
                    }
                    catch { }
                }
                else
                {
                    ShowError("Failed to save view.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error saving view: {ex.Message}");
            }
        }
        // Save current filters to DB (T_Ref_User_Filter)
        private void SaveFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var name = Interaction.InputBox("Filter name:", "Save filter", "My Filter");
                if (string.IsNullOrWhiteSpace(name)) return;

                var where = GenerateWhereClause();
                // Embed a JSON snapshot for full restoration of all fields
                var sqlToSave = BuildSqlWithJsonComment(GetCurrentFilterPreset(), where);
                var service = new UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                service.SaveUserFilter(name, sqlToSave);
                
                UpdateStatusInfo($"Filter '{name}' saved");
                
                // If hosted inside ReconciliationPage, refresh only the SavedFilters combo (no data reload)
                // Use fire-and-forget pattern (same as SaveView_Click)
                try
                {
                    var hostPage = VisualTreeHelpers.FindAncestor<RecoTool.Windows.ReconciliationPage>(this);
                    if (hostPage != null)
                    {
                        _ = hostPage.ReloadSavedFiltersOnly();
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Error saving filter: {ex.Message}");
            }
        }

        // Load filters from DB (T_Ref_User_Filter)
        private void LoadFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Open filter picker
                var picker = new FilterPickerWindow { Owner = Window.GetWindow(this) };
                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                var service = new UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                var sql = service.LoadUserFilterWhere(name);
                if (sql == null)
                {
                    ShowError($"Filter '{name}' not found");
                    return;
                }
                // Apply saved filter SQL (restores UI preset and sets _backendFilterSql)
                ApplySavedFilterSql(sql);
                // Reload data from backend using _backendFilterSql
                Refresh();
                SetViewTitle(name);
                UpdateStatusInfo($"Filter '{name}' loaded");
            }
            catch (Exception ex)
            {
                ShowError($"Error loading filters: {ex.Message}");
            }
        }

        // Load Views from DB (T_Ref_User_Fields_Preference)
        private async void LoadView_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    ShowError("Service not available");
                    return;
                }

                // Use the same FilterPickerWindow with providers backed by UserViewPreferenceService
                var refCs = Settings.Default.ReferentialDB;
                var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                var viewSvc = new UserViewPreferenceService(refCs, curUser);
                var picker = new FilterPickerWindow("Saved views",
                    s => viewSvc.ListNamesAsync(s).GetAwaiter().GetResult(),
                    name => viewSvc.DeleteByNameAsync(name).GetAwaiter().GetResult(),
                    s => viewSvc.ListDetailedAsync(s).GetAwaiter().GetResult())
                { Owner = Window.GetWindow(this) };

                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                if (string.IsNullOrWhiteSpace(name)) return;

                var pref = await viewSvc.GetByNameAsync(name);
                if (pref == null || string.IsNullOrWhiteSpace(pref.UPF_ColumnWidths))
                {
                    ShowError($"View '{name}' not found");
                    return;
                }

                var sql = pref.UPF_SQL;
                // Apply saved SQL (restores UI preset and sets _backendFilterSql)
                ApplySavedFilterSql(sql);

                // If a saved layout exists, apply it to the grid
                if (!string.IsNullOrWhiteSpace(pref.UPF_ColumnWidths))
                {
                    try
                    {
                        var layout = JsonSerializer.Deserialize<GridLayout>(pref.UPF_ColumnWidths);
                        ApplyGridLayout(layout);
                    }
                    catch { /* ignore layout parsing errors */ }
                }

                // Reload data from backend using _backendFilterSql
                Refresh();
                SetViewTitle(name);
                UpdateStatusInfo($"View '{name}' loaded");
            }
            catch (Exception ex)
            {
                ShowError($"Error loading view: {ex.Message}");
            }
        }

        // Delete a saved filter from DB and refresh the page Saved Filters combo
        private void DeleteFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var picker = new FilterPickerWindow { Owner = Window.GetWindow(this) };
                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                if (string.IsNullOrWhiteSpace(name)) return;

                var confirm = MessageBox.Show($"Supprimer le filtre '{name}' ?", "Confirmation", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (confirm != MessageBoxResult.Yes) return;

                var service = new UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                if (service.DeleteUserFilter(name))
                {
                    UpdateStatusInfo($"Filter '{name}' deleted");
                    
                    // Refresh only the filters ComboBox on the host page
                    // Use fire-and-forget pattern (same as SaveView_Click)
                    try
                    {
                        var hostPage = VisualTreeHelpers.FindAncestor<RecoTool.Windows.ReconciliationPage>(this);
                        if (hostPage != null)
                        {
                            _ = hostPage.ReloadSavedFiltersOnly();
                        }
                    }
                    catch { }
                }
                else
                {
                    ShowError($"Filter '{name}' not found or not deleted");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error deleting filter: {ex.Message}");
            }
        }
    }
}
