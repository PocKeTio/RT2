using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using RecoTool.Services;
using RecoTool.Services.DTOs;
using RecoTool.Infrastructure.Logging;
using RecoTool.Services.Rules;

namespace RecoTool.Windows
{
    // Partial: Editing handlers and save plumbing
    public partial class ReconciliationView
    {
        // Prevent double invocation of confirmation when multiple handlers fire
        private bool _ruleConfirmBusy;
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

                // Preview rules for edit and ask confirmation if self outputs are proposed
                await ConfirmAndApplyRuleOutputsAsync(row, reco);

                // Save without applying rules again (we already applied selected outputs above)
                await _reconciliationService.SaveReconciliationAsync(reco, applyRulesOnEdit: false);
                
                // Refresh KPIs to reflect changes immediately
                UpdateKpis(_filteredData);

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
                    || string.Equals(headerText, "Incident Type", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(headerText, "Reason Non Risky", StringComparison.OrdinalIgnoreCase))
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
            // DEPRECATED: ReviewDate removed - use ActionStatus instead
            // reco.ReviewDate = row.ReviewDate;
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

            // Preview rules and ask user confirmation for self outputs; apply to UI immediately
            await ConfirmAndApplyRuleOutputsAsync(row, reco);

            // Save without applying rules again (we already applied chosen outputs above)
            await _reconciliationService.SaveReconciliationAsync(reco, applyRulesOnEdit: false);
            
            // Refresh KPIs to reflect changes immediately
            UpdateKpis(_filteredData);

            // Best-effort background sync (debounced)
            try
            {
                ScheduleBulkPushDebounced();
            }
            catch { }
            finally { _ruleConfirmBusy = false; }
        }

        // Resolve a user-field display name by id and category
        private string GetUserFieldName(int? id, string category)
        {
            try
            {
                if (!id.HasValue) return null;
                var list = AllUserFields;
                if (list == null) return id.Value.ToString();
                var q = list.AsEnumerable();
                if (!string.IsNullOrWhiteSpace(category))
                    q = q.Where(u => string.Equals(u?.USR_Category ?? string.Empty, category, StringComparison.OrdinalIgnoreCase));
                var uf = q.FirstOrDefault(u => u?.USR_ID == id.Value) ?? list.FirstOrDefault(u => u?.USR_ID == id.Value);
                return uf?.USR_FieldName ?? id.Value.ToString();
            }
            catch { return id?.ToString(); }
        }

        // Display rule outputs and apply to current row upon confirmation; notify counterpart outputs via toast
        private async Task ConfirmAndApplyRuleOutputsAsync(ReconciliationViewData row, RecoTool.Models.Reconciliation reco)
        {
            try
            {
                if (_ruleConfirmBusy) return;
                _ruleConfirmBusy = true;
                var res = await _reconciliationService.PreviewRulesForEditAsync(row.ID);
                if (res == null || res.Rule == null) return;

                // Prepare SELF outputs summary (labels, not IDs)
                var selfChanges = new List<string>();
                if (res.NewActionIdSelf.HasValue) selfChanges.Add($"Action: {GetUserFieldName(res.NewActionIdSelf.Value, "Action")}");
                if (res.NewKpiIdSelf.HasValue) selfChanges.Add($"KPI: {GetUserFieldName(res.NewKpiIdSelf.Value, "KPI")}");
                if (res.NewIncidentTypeIdSelf.HasValue) selfChanges.Add($"Incident Type: {GetUserFieldName(res.NewIncidentTypeIdSelf.Value, "Incident Type")}");
                if (res.NewRiskyItemSelf.HasValue) selfChanges.Add($"Risky Item: {(res.NewRiskyItemSelf.Value ? "Yes" : "No")}");
                if (res.NewReasonNonRiskyIdSelf.HasValue) selfChanges.Add($"Reason Non Risky: {GetUserFieldName(res.NewReasonNonRiskyIdSelf.Value, "Reason Non Risky")}");
                if (res.NewToRemindSelf.HasValue) selfChanges.Add($"To Remind: {(res.NewToRemindSelf.Value ? "Yes" : "No")}");
                if (res.NewToRemindDaysSelf.HasValue) selfChanges.Add($"To Remind Days: {res.NewToRemindDaysSelf.Value}");
                if (res.NewFirstClaimTodaySelf == true) selfChanges.Add("First Claim Date: Today");

                // Show counterpart toast if rule applies to counterpart
                if (res.Rule.ApplyTo == ApplyTarget.Counterpart || res.Rule.ApplyTo == ApplyTarget.Both)
                {
                    var cp = new List<string>();
                    if (res.Rule.OutputActionId.HasValue) cp.Add($"Action: {GetUserFieldName(res.Rule.OutputActionId.Value, "Action")}");
                    if (res.Rule.OutputKpiId.HasValue) cp.Add($"KPI: {GetUserFieldName(res.Rule.OutputKpiId.Value, "KPI")}");
                    if (res.Rule.OutputIncidentTypeId.HasValue) cp.Add($"Incident Type: {GetUserFieldName(res.Rule.OutputIncidentTypeId.Value, "Incident Type")}");
                    if (res.Rule.OutputRiskyItem.HasValue) cp.Add($"Risky Item: {(res.Rule.OutputRiskyItem.Value ? "Yes" : "No")}");
                    if (res.Rule.OutputReasonNonRiskyId.HasValue) cp.Add($"Reason Non Risky: {GetUserFieldName(res.Rule.OutputReasonNonRiskyId.Value, "Reason Non Risky")}");
                    if (res.Rule.OutputToRemind.HasValue) cp.Add($"To Remind: {(res.Rule.OutputToRemind.Value ? "Yes" : "No")}");
                    if (res.Rule.OutputToRemindDays.HasValue) cp.Add($"To Remind Days: {res.Rule.OutputToRemindDays.Value}");
                    var txt = cp.Count > 0 ? string.Join(", ", cp) : "(no change)";
                    try { ShowToast($"Rule '{res.Rule.RuleId}' (counterpart): {txt}", onClick: () => { try { OpenMatchedPopup(row); } catch { } }); } catch { }
                }

                if (selfChanges.Count == 0)
                {
                    // Nothing to confirm/apply on current row
                    return;
                }

                var details = string.Join("\n - ", selfChanges);
                var msgText = $"Rule '{res.Rule.RuleId}' proposes to apply on this row:\n - {details}\n\nDo you want to apply these changes?";
                var answer = MessageBox.Show(msgText, "Confirm rule application", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (answer != MessageBoxResult.Yes) return;

                // Apply to in-memory row for instant UI feedback
                if (res.NewActionIdSelf.HasValue) { UserFieldUpdateService.ApplyAction(row, reco, res.NewActionIdSelf.Value, AllUserFields); }
                if (res.NewKpiIdSelf.HasValue) { row.KPI = res.NewKpiIdSelf.Value; reco.KPI = res.NewKpiIdSelf.Value; }
                if (res.NewIncidentTypeIdSelf.HasValue) { row.IncidentType = res.NewIncidentTypeIdSelf.Value; reco.IncidentType = res.NewIncidentTypeIdSelf.Value; }
                if (res.NewRiskyItemSelf.HasValue) { row.RiskyItem = res.NewRiskyItemSelf.Value; reco.RiskyItem = res.NewRiskyItemSelf.Value; }
                if (res.NewReasonNonRiskyIdSelf.HasValue) { row.ReasonNonRisky = res.NewReasonNonRiskyIdSelf.Value; reco.ReasonNonRisky = res.NewReasonNonRiskyIdSelf.Value; }
                if (res.NewToRemindSelf.HasValue) { row.ToRemind = res.NewToRemindSelf.Value; reco.ToRemind = res.NewToRemindSelf.Value; }
                if (res.NewToRemindDaysSelf.HasValue)
                {
                    try { var d = DateTime.Today.AddDays(res.NewToRemindDaysSelf.Value); row.ToRemindDate = d; reco.ToRemindDate = d; } catch { }
                }
                if (res.NewFirstClaimTodaySelf == true)
                {
                    try { row.FirstClaimDate = DateTime.Today; reco.FirstClaimDate = DateTime.Today; } catch { }
                }

                // Log file entry (origin=edit) to preserve trace
                try
                {
                    var outs = new List<string>();
                    if (res.NewActionIdSelf.HasValue) outs.Add($"Action={res.NewActionIdSelf.Value}");
                    if (res.NewKpiIdSelf.HasValue) outs.Add($"KPI={res.NewKpiIdSelf.Value}");
                    if (res.NewIncidentTypeIdSelf.HasValue) outs.Add($"IncidentType={res.NewIncidentTypeIdSelf.Value}");
                    if (res.NewRiskyItemSelf.HasValue) outs.Add($"RiskyItem={res.NewRiskyItemSelf.Value}");
                    if (res.NewReasonNonRiskyIdSelf.HasValue) outs.Add($"ReasonNonRisky={res.NewReasonNonRiskyIdSelf.Value}");
                    if (res.NewToRemindSelf.HasValue) outs.Add($"ToRemind={res.NewToRemindSelf.Value}");
                    if (res.NewToRemindDaysSelf.HasValue) outs.Add($"ToRemindDays={res.NewToRemindDaysSelf.Value}");
                    if (res.NewFirstClaimTodaySelf == true) outs.Add("FirstClaimDate=Today");
                    var outsStr = string.Join("; ", outs);
                    var countryId = _offlineFirstService?.CurrentCountryId;
                    LogHelper.WriteRuleApplied("edit", countryId, row.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                }
                catch { }
            }
            catch { }
        }
    }
}
