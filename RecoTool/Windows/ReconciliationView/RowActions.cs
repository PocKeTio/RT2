using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using RecoTool.Models;
using RecoTool.Services.DTOs;
using RecoTool.Services;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    // Partial: Row context menu and quick-set actions for ReconciliationView
    public partial class ReconciliationView
    {
        private IEnumerable<UserField> GetUserFieldOptionsForRow(string category, ReconciliationViewData row)
        {
            try
            {
                var all = AllUserFields ?? Array.Empty<UserField>();
                var country = CurrentCountryObject;
                if (country == null) return Array.Empty<UserField>();

                bool isPivot = string.Equals(row?.Account_ID?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = string.Equals(row?.Account_ID?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                // Category mapping: handle synonyms (e.g., Incident Type vs INC)
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
                    return Array.Empty<UserField>();

                return query.OrderBy(u => u.USR_FieldName).ToList();
            }
            catch { return Array.Empty<UserField>(); }
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

                        // Best-effort background sync
                        try { ScheduleBulkPushDebounced(); } catch { }
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to update comments: {ex.Message}");
            }
        }
        // Populate the context menu items at open time to ensure correct DataContext
        private void RowContextMenu_Opened(object sender, RoutedEventArgs e)
        {
            try
            {
                var cm = sender as ContextMenu;
                if (cm == null) return;
                var fe = cm.PlacementTarget as FrameworkElement;
                var rowData = fe?.DataContext as ReconciliationViewData;
                if (rowData == null) return;

                // Resolve the root submenus
                MenuItem actionRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Action");
                MenuItem kpiRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "KPI");
                MenuItem incRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Incident Type");

                void Populate(MenuItem root, string category)
                {
                    if (root == null) return;
                    root.Items.Clear();

                    var options = GetUserFieldOptionsForRow(category, rowData).ToList();

                    // Clear option (always present)
                    var clearItem = new MenuItem { Header = $"Clear {category}", Tag = category, CommandParameter = null };
                    clearItem.Click += QuickSetUserFieldMenuItem_Click;
                    // Disable Clear if already empty
                    bool hasValue = (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase) && rowData.Action.HasValue)
                                     || (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase) && rowData.KPI.HasValue)
                                     || (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) && rowData.IncidentType.HasValue);
                    clearItem.IsEnabled = hasValue;
                    root.Items.Add(clearItem);

                    foreach (var opt in options)
                    {
                        var mi = new MenuItem
                        {
                            Header = opt.USR_FieldName,
                            Tag = category,
                            CommandParameter = opt.USR_ID,
                            IsCheckable = true,
                            IsChecked = (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase) && rowData.Action == opt.USR_ID)
                                        || (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase) && rowData.KPI == opt.USR_ID)
                                        || (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) && rowData.IncidentType == opt.USR_ID)
                        };
                        mi.Click += QuickSetUserFieldMenuItem_Click;
                        root.Items.Add(mi);
                    }

                    // Disable the root if there are no applicable options and value is empty
                    root.IsEnabled = options.Any() || hasValue; // keep enabled if Clear is relevant
                }

                Populate(actionRoot, "Action");
                Populate(kpiRoot, "KPI");
                Populate(incRoot, "Incident Type");
            }
            catch { }
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

                // Background sync best effort (debounced)
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
                var targetRows = dg.SelectedItems.Count > 1 && rowCtx != null && dg.SelectedItems.OfType<ReconciliationViewData>().Contains(rowCtx)
                    ? dg.SelectedItems.OfType<ReconciliationViewData>().ToList()
                    : (rowCtx != null ? new List<ReconciliationViewData> { rowCtx } : new List<ReconciliationViewData>());
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
