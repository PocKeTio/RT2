using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: Editing handlers and save plumbing
    public partial class ReconciliationView
    {
        // Persist selection changes for Action/KPI/Incident and ActionStatus
        private async void UserFieldComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                var cb = sender as ComboBox;
                if (cb == null) return;
                var row = cb.DataContext as ReconciliationViewData;
                if (row == null) return;

                // Determine which field changed via Tag
                var tag = cb.Tag as string;
                int? newId = null;
                // Accept explicit null to clear selection; otherwise attempt to convert to int?
                try
                {
                    if (cb.SelectedValue == null)
                    {
                        newId = null;
                    }
                    else if (cb.SelectedValue is int directInt)
                    {
                        newId = directInt;
                    }
                    else if (int.TryParse(cb.SelectedValue.ToString(), out var parsed))
                    {
                        newId = parsed;
                    }
                    else
                    {
                        // If we cannot parse, treat as clear
                        newId = null;
                    }
                }
                catch
                {
                    newId = null;
                }

                // Load current reconciliation from DB to avoid overwriting unrelated fields
                if (_reconciliationService == null) return;
                var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

                if (string.Equals(tag, "Action", StringComparison.OrdinalIgnoreCase))
                {
                    UserFieldUpdateService.ApplyAction(row, reco, newId, AllUserFields);
                }
                else if (string.Equals(tag, "ActionStatus", StringComparison.OrdinalIgnoreCase))
                {
                    // Toggle pending/done directly; auto manage ActionDate only if status actually changes
                    bool? newStatus = null;
                    if (cb.SelectedValue is bool sb) newStatus = sb;
                    else if (cb.SelectedValue != null && bool.TryParse(cb.SelectedValue.ToString(), out var parsedBool)) newStatus = parsedBool;

                    UserFieldUpdateService.ApplyActionStatus(row, reco, newStatus);
                }
                else if (string.Equals(tag, "KPI", StringComparison.OrdinalIgnoreCase))
                {
                    UserFieldUpdateService.ApplyKpi(row, reco, newId);
                }
                else if (string.Equals(tag, "Incident Type", StringComparison.OrdinalIgnoreCase) || string.Equals(tag, "IncidentType", StringComparison.OrdinalIgnoreCase))
                {
                    UserFieldUpdateService.ApplyIncidentType(row, reco, newId);
                }
                else
                {
                    return;
                }

                await _reconciliationService.SaveReconciliationAsync(reco);

                // Fire-and-forget background sync to network DB to reduce sync debt (debounced)
                try
                {
                    ScheduleBulkPushDebounced();
                }
                catch { /* ignore any scheduling errors */ }
            }
            catch (Exception ex)
            {
                // Enrichir le message avec des infos de diagnostic de connexion
                try
                {
                    string country = _offlineFirstService?.CurrentCountryId ?? "<null>";
                    bool isInit = _offlineFirstService?.IsInitialized ?? false;
                    string cs = null;
                    try { cs = _offlineFirstService?.GetCurrentLocalConnectionString(); } catch (Exception csex) { cs = $"<error: {csex.Message}>"; }
                    string dw = null;
                    try { dw = _offlineFirstService?.GetLocalDWDatabasePath(); } catch { }
                    ShowError($"Save error: {ex.Message}\nCountry: {country} | Init: {isInit}\nCS: {cs}\nDW: {dw}");
                }
                catch
                {
                    ShowError($"Save error: {ex.Message}");
                }
            }
        }

        // Persist text/checkbox/date edits as soon as a cell commit occurs
        private async void ResultsDataGrid_CellEditEnding(object sender, DataGridCellEditEndingEventArgs e)
        {
            try
            {
                if (e.EditAction != DataGridEditAction.Commit) return;
                var rowData = e.Row?.Item as ReconciliationViewData;
                if (rowData == null) return;

                // Skip here for ComboBox-based columns handled by UserFieldComboBox_SelectionChanged
                var headerText = Convert.ToString(e.Column?.Header) ?? string.Empty;
                if (string.Equals(headerText, "Action", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(headerText, "KPI", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(headerText, "Incident Type", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                // Ensure the editing element pushes its value to the binding source before we save
                if (e.EditingElement is TextBox tb)
                {
                    try { tb.GetBindingExpression(TextBox.TextProperty)?.UpdateSource(); } catch { }
                }
                else if (e.EditingElement is CheckBox cbx)
                {
                    try { cbx.GetBindingExpression(ToggleButton.IsCheckedProperty)?.UpdateSource(); } catch { }
                }
                else if (e.EditingElement is DatePicker dp)
                {
                    try { dp.GetBindingExpression(DatePicker.SelectedDateProperty)?.UpdateSource(); } catch { }
                }

                await SaveEditedRowAsync(rowData);
            }
            catch (Exception ex)
            {
                ShowError($"Save error (cell): {ex.Message}");
            }
        }

        // Loads existing reconciliation and maps editable fields from the view row, then saves
        private async Task SaveEditedRowAsync(ReconciliationViewData row)
        {
            if (_reconciliationService == null || row == null) return;
            var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

            // Map user-editable fields
            reco.Action = row.Action;
            reco.ActionStatus = row.ActionStatus;
            reco.ActionDate = row.ActionDate;
            reco.KPI = row.KPI;
            reco.IncidentType = row.IncidentType;
            reco.Comments = row.Comments;
            reco.InternalInvoiceReference = row.InternalInvoiceReference;
            reco.FirstClaimDate = row.FirstClaimDate;
            reco.LastClaimDate = row.LastClaimDate;
            // Persist assignee if edited via grid
            reco.Assignee = row.Assignee;
            reco.ToRemind = row.ToRemind;
            reco.ToRemindDate = row.ToRemindDate;
            reco.ACK = row.ACK;
            reco.SwiftCode = row.SwiftCode;
            reco.PaymentReference = row.PaymentReference;
            reco.RiskyItem = row.RiskyItem;
            reco.ReasonNonRisky = row.ReasonNonRisky;

            await _reconciliationService.SaveReconciliationAsync(reco);

            // Best-effort background sync (debounced)
            try
            {
                ScheduleBulkPushDebounced();
            }
            catch { }
        }
    }
}
