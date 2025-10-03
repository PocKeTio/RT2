using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using RecoTool.Services;
using RecoTool.Services.DTOs;
using RecoTool.Models;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    public partial class ReconciliationView
    {
        private async void RunRulesNow_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    ShowError("Service not available.");
                    return;
                }

                // Use selection if any, otherwise all rows in current filtered set
                var selected = GetCurrentSelection();
                var rows = (selected != null && selected.Count > 0)
                    ? selected
                    : (_filteredData?.ToList() ?? ViewData?.ToList() ?? new List<ReconciliationViewData>());

                // Always exclude archived rows
                rows = rows?.Where(r => r != null && !r.IsDeleted).ToList();

                if (rows == null || rows.Count == 0)
                {
                    UpdateStatusInfo("No active rows to apply rules.");
                    return;
                }

                var ids = rows.Select(r => r?.ID)
                              .Where(id => !string.IsNullOrWhiteSpace(id))
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .ToList();
                if (ids.Count == 0)
                {
                    UpdateStatusInfo("No valid IDs to apply rules.");
                    return;
                }

                UpdateStatusInfo($"Applying rules to {ids.Count} row(s)...");
                var count = await _reconciliationService.ApplyRulesNowAsync(ids).ConfigureAwait(false);
                await Dispatcher.InvokeAsync(() =>
                {
                    UpdateStatusInfo($"Rules applied to {count} row(s). Reloading...");
                    Refresh();
                });
            }
            catch (Exception ex)
            {
                ShowError($"Failed to run rules: {ex.Message}");
            }
        }

        private IEnumerable<UserField> GetUserFieldOptionsForRow(string category, ReconciliationViewData row)
        {
            try
            {
                var all = AllUserFields;
                var country = CurrentCountryObject;
                if (row == null || all == null || country == null) return Enumerable.Empty<UserField>();

                bool isPivot = string.Equals(row.Account_ID?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = string.Equals(row.Account_ID?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                bool incident = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                 || string.Equals(category, "INC", StringComparison.OrdinalIgnoreCase);
                IEnumerable<UserField> query = incident
                    ? all.Where(u => string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                     || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                    : all.Where(u => string.Equals(u.USR_Category, category, StringComparison.OrdinalIgnoreCase));

                if (isPivot)
                    query = query.Where(u => u.USR_Pivot);
                else if (isReceivable)
                    query = query.Where(u => u.USR_Receivable);
                else
                    return Enumerable.Empty<UserField>();

                return query.OrderBy(u => u.USR_FieldName).ToList();
            }
            catch { return Enumerable.Empty<UserField>(); }
        }

        // Open full conversation and allow appending a new comment line
        private async void CommentsCell_Click(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var fe = sender as FrameworkElement;
                var row = fe?.DataContext as ReconciliationViewData;
                if (row == null || _reconciliationService == null) return;

                var dlg = new CommentsDialog
                {
                    Owner = Window.GetWindow(this)
                };
                dlg.SetConversationText(row.Comments ?? string.Empty);
                var res = dlg.ShowDialog();
                if (res == true)
                {
                    var user = _reconciliationService.CurrentUser;
                    var newLine = dlg.GetNewCommentText()?.Trim();
                    if (!string.IsNullOrWhiteSpace(newLine))
                    {
                        string prefix = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {user}: ";
                        string existing = row.Comments?.TrimEnd();
                        string appended = string.IsNullOrWhiteSpace(existing)
                            ? prefix + newLine
                            : existing + Environment.NewLine + prefix + newLine;

                        // Update view model immediately
                        row.Comments = appended;

                        // Persist to reconciliation
                        var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);
                        reco.Comments = appended;
                        await _reconciliationService.SaveReconciliationAsync(reco);
                        
                        // Refresh KPIs to reflect changes immediately
                        UpdateKpis(_filteredData);

                        // Best-effort background sync
                        try { ScheduleBulkPushDebounced(); } catch { }
                    }
                }
            } catch (Exception ex) 
            {
                ShowError($"Failed to update comments: {ex.Message}");
            }
        }
        

        private async void QuickSetUserFieldMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var mi = sender as MenuItem;
                if (mi == null) return;

                // Resolve the row data: prefer the item's DataContext, fallback to ContextMenu.PlacementTarget
                var row = mi.DataContext as ReconciliationViewData;
                if (row == null)
                {
                    var cm = VisualTreeHelpers.FindParent<ContextMenu>(mi);
                    var fe = cm?.PlacementTarget as FrameworkElement;
                    row = fe?.DataContext as ReconciliationViewData;
                }
                if (row == null) return;

                var category = mi.Tag as string;
                int? newId = null;
                if (mi.CommandParameter != null)
                {
                    if (mi.CommandParameter is int id)
                        newId = id;
                    else if (int.TryParse(mi.CommandParameter.ToString(), out var parsed))
                        newId = parsed;
                }

                if (_reconciliationService == null) return;

                // Determine target rows: if multiple selected, apply to all
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                var targetRows = new List<ReconciliationViewData>();
                if (dg?.SelectedItems != null && dg.SelectedItems.Count > 1)
                {
                    targetRows.AddRange(dg.SelectedItems.OfType<ReconciliationViewData>());
                }
                else
                {
                    targetRows.Add(row);
                }

                // Confirm clear once if needed
                bool isAction = string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase);
                bool isKpi = string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase);
                bool isInc = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) || string.Equals(category, "IncidentType", StringComparison.OrdinalIgnoreCase);
                if (newId == null)
                {
                    bool anyHasValue = targetRows.Any(r => (isAction && r.Action.HasValue) || (isKpi && r.KPI.HasValue) || (isInc && r.IncidentType.HasValue));
                    if (anyHasValue)
                    {
                        var label = isAction ? "Action" : isKpi ? "KPI" : "Incident Type";
                        if (MessageBox.Show($"Clear {label} for {targetRows.Count} selected row(s)?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
                    }
                }

                // Build batch updates
                var updates = new List<Reconciliation>();
                foreach (var r in targetRows)
                {
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(r.ID);
                    if (isAction)
                    {
                        UserFieldUpdateService.ApplyAction(r, reco, newId, AllUserFields);
                    }
                    else if (isKpi)
                    {
                        UserFieldUpdateService.ApplyKpi(r, reco, newId);
                    }
                    else if (isInc)
                    {
                        UserFieldUpdateService.ApplyIncidentType(r, reco, newId);
                    }
                    updates.Add(reco);
                }

                await _reconciliationService.SaveReconciliationsAsync(updates);
                
                // Refresh KPIs to reflect changes immediately
                UpdateKpis(_filteredData);

                // background sync (debounced)
                try
                {
                    ScheduleBulkPushDebounced();
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Save error: {ex.Message}");
            }
        }

        // Set comment on selected rows (append as conversation line)
        private async void QuickSetCommentMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null || _reconciliationService == null) return;

                var selected = dg.SelectedItems?.OfType<ReconciliationViewData>().ToList() ?? new List<ReconciliationViewData>();
                if (selected.Count == 0) return;

                // Build an ad-hoc prompt window for comment input
                var prompt = new Window
                {
                    Title = $"Set Comment for {selected.Count} row(s)",
                    Width = 480,
                    Height = 220,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner,
                    Owner = Window.GetWindow(this),
                    ResizeMode = ResizeMode.NoResize,
                    Content = null
                };

                var grid = new Grid { Margin = new Thickness(12) };
                grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                var tb = new TextBox { AcceptsReturn = true, TextWrapping = TextWrapping.Wrap, VerticalScrollBarVisibility = ScrollBarVisibility.Auto };
                Grid.SetRow(tb, 0);
                var panel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right, Margin = new Thickness(0, 10, 0, 0) };
                var btnOk = new Button { Content = "OK", Width = 80, Margin = new Thickness(0, 0, 8, 0), IsDefault = true };
                var btnCancel = new Button { Content = "Cancel", Width = 80, IsCancel = true };
                btnOk.Click += (s, ea) => { prompt.DialogResult = true; prompt.Close(); };
                btnCancel.Click += (s, ea) => { prompt.DialogResult = false; prompt.Close(); };
                panel.Children.Add(btnOk);
                panel.Children.Add(btnCancel);
                Grid.SetRow(panel, 1);
                grid.Children.Add(tb);
                grid.Children.Add(panel);
                prompt.Content = grid;

                var result = prompt.ShowDialog();
                if (result != true) return;
                var text = tb.Text ?? string.Empty;

                var updates = new List<Reconciliation>();
                var user = _reconciliationService?.CurrentUser ?? Environment.UserName;
                string prefix = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {user}: ";
                foreach (var r in selected)
                {
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(r.ID);
                    string existing = r.Comments?.TrimEnd();
                    string appended = string.IsNullOrWhiteSpace(existing)
                        ? prefix + text
                        : existing + Environment.NewLine + prefix + text;
                    r.Comments = appended;
                    reco.Comments = appended;
                    updates.Add(reco);
                }

                await _reconciliationService.SaveReconciliationsAsync(updates);
                
                // Refresh KPIs to reflect changes immediately
                UpdateKpis(_filteredData);

                // Background sync best effort (debounced)
                try
                {
                    ScheduleBulkPushDebounced();
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to set bulk comment: {ex.Message}");
            }
        }

        // Quick mark action as done
        private async void QuickMarkActionDoneMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null || _reconciliationService == null) return;
                var rowCtx = (sender as FrameworkElement)?.DataContext as ReconciliationViewData;
                var targetRows = dg.SelectedItems.Count > 1 && rowCtx != null && dg.SelectedItems.OfType<ReconciliationViewData>().Contains(rowCtx)
                    ? dg.SelectedItems.OfType<ReconciliationViewData>().ToList()
                    : (rowCtx != null ? new List<ReconciliationViewData> { rowCtx } : new List<ReconciliationViewData>());
                if (targetRows.Count == 0) return;

                var updates = new List<Reconciliation>();
                foreach (var r in targetRows)
                {
                    if (!r.Action.HasValue) continue; // only mark done if an action exists
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(r.ID);
                    r.ActionStatus = true;
                    r.ActionDate = DateTime.Now;
                    reco.ActionStatus = true;
                    reco.ActionDate = r.ActionDate;
                    updates.Add(reco);
                }
                if (updates.Count == 0) return;
                await _reconciliationService.SaveReconciliationsAsync(updates);
                
                // Refresh KPIs to reflect changes immediately
                UpdateKpis(_filteredData);
                
                try { ScheduleBulkPushDebounced(); } catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to mark action as DONE: {ex.Message}");
            }
        }

        // Quick take ownership
        private async void QuickTakeMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;
                var rowCtx = (sender as FrameworkElement)?.DataContext as ReconciliationViewData;
                var targetRows = dg.Items.OfType<ReconciliationViewData>().ToList();
                if (targetRows.Count == 0) return;

                var updates = new List<Reconciliation>();
                foreach (var r in targetRows)
                {
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(r.ID);
                    var user = _reconciliationService.CurrentUser;
                    r.Assignee = user;
                    reco.Assignee = user;
                    updates.Add(reco);
                }
                await _reconciliationService.SaveReconciliationsAsync(updates);

                // Schedule debounced background sync
                try
                {
                    ScheduleBulkPushDebounced();
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to assign: {ex.Message}");
            }
        }

        // Quick set reminder
        private async void QuickSetReminderMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;
                var rowCtx = (sender as FrameworkElement)?.DataContext as ReconciliationViewData;
                var targetRows = dg.SelectedItems.Count > 1 && rowCtx != null && dg.SelectedItems.OfType<ReconciliationViewData>().Contains(rowCtx)
                    ? dg.SelectedItems.OfType<ReconciliationViewData>().ToList()
                    : (rowCtx != null ? new List<ReconciliationViewData> { rowCtx } : new List<ReconciliationViewData>());
                if (targetRows.Count == 0) return;

                // Prompt date selection
                var prompt = new Window
                {
                    Title = "Set Reminder Date",
                    Width = 320,
                    Height = 160,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner,
                    Owner = Window.GetWindow(this),
                    ResizeMode = ResizeMode.NoResize
                };
                var grid = new Grid { Margin = new Thickness(10) };
                grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                var datePicker = new DatePicker { SelectedDate = DateTime.Today };
                Grid.SetRow(datePicker, 0);
                grid.Children.Add(datePicker);
                var panel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right, Margin = new Thickness(0, 10, 0, 0) };
                var btnOk = new Button { Content = "OK", Width = 80, Margin = new Thickness(0, 0, 8, 0), IsDefault = true };
                var btnCancel = new Button { Content = "Cancel", Width = 80, IsCancel = true };
                btnOk.Click += (s, ea) => { prompt.DialogResult = true; prompt.Close(); };
                btnCancel.Click += (s, ea) => { prompt.DialogResult = false; prompt.Close(); };
                panel.Children.Add(btnOk);
                panel.Children.Add(btnCancel);
                Grid.SetRow(panel, 1);
                grid.Children.Add(panel);
                prompt.Content = grid;
                var res = prompt.ShowDialog();
                if (res != true) return;
                var selDate = datePicker.SelectedDate;
                if (!selDate.HasValue) return;

                var updates = new List<Reconciliation>();
                var currentUser = _reconciliationService.CurrentUser;
                foreach (var r in targetRows)
                {
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(r.ID);
                    r.ToRemindDate = selDate.Value;
                    r.ToRemind = true;
                    if (string.IsNullOrWhiteSpace(r.Assignee))
                    {
                        r.Assignee = currentUser;
                        reco.Assignee = currentUser;
                    }
                    reco.ToRemindDate = selDate.Value;
                    reco.ToRemind = true;
                    updates.Add(reco);
                }
                await _reconciliationService.SaveReconciliationsAsync(updates);
                
                // Refresh KPIs to reflect changes immediately
                UpdateKpis(_filteredData);

                // Background sync best effort (debounced)
                try
                {
                    ScheduleBulkPushDebounced();
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to set reminder: {ex.Message}");
            }
        }
    }
}
