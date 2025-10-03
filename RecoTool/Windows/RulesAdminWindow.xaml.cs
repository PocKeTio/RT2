using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using RecoTool.Services;
using RecoTool.Services.Rules;
using RecoTool.Services.DTOs;
using RecoTool.UI.Models;

namespace RecoTool.Windows
{
    public partial class RulesAdminWindow : Window
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly TruthTableRepository _repository;
        private readonly ReconciliationService _reconciliationService;

        public ObservableCollection<TruthRule> Rules { get; set; } = new ObservableCollection<TruthRule>();

        public RuleScope[] Scopes { get; } = new[] { RuleScope.Both, RuleScope.Import, RuleScope.Edit };
        public string[] AccountSides { get; } = new[] { "*", "P", "R" };
        public string[] Signs { get; } = new[] { "*", "C", "D" };
        public ApplyTarget[] ApplyTargets { get; } = new[] { ApplyTarget.Self, ApplyTarget.Counterpart, ApplyTarget.Both };
        public string[] GuaranteeTypes { get; private set; }
        public string[] TransactionTypes { get; private set; }
        public ObservableCollection<OptionItem> ActionOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> KpiOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> IncidentTypeOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ReasonOptions { get; } = new ObservableCollection<OptionItem>();

        private List<TruthRule> _allRules = new List<TruthRule>();

        public RulesAdminWindow()
        {
            InitializeComponent();
            // Register converter instances at runtime to avoid XAML designer resolution issues
            try
            {
                Resources["ScopeToBadgeBrushConverter"] = new ScopeToBadgeBrushConverter();
                Resources["PriorityToBadgeBrushConverter"] = new PriorityToBadgeBrushConverter();
            }
            catch { }
            DataContext = this;
            _offlineFirstService = (App.ServiceProvider?.GetService(typeof(OfflineFirstService))) as OfflineFirstService;
            if (_offlineFirstService == null)
            {
                MessageBox.Show("OfflineFirstService not available.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                Close();
                return;
            }
            _repository = new TruthTableRepository(_offlineFirstService);
            _reconciliationService = (App.ServiceProvider?.GetService(typeof(ReconciliationService))) as ReconciliationService;
            BuildStaticLists();
            BuildReferentialOptions();
            Loaded += async (s, e) => await ReloadRulesAsync();
        }

        private async void RunRulesNow_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    StatusText.Text = "ReconciliationService not available.";
                    return;
                }
                var countryId = _offlineFirstService?.CurrentCountry?.CNT_Id;
                if (string.IsNullOrWhiteSpace(countryId))
                {
                    StatusText.Text = "No current country selected.";
                    return;
                }
                StatusText.Text = "Collecting rows…";
                var view = await _reconciliationService.GetReconciliationViewAsync(countryId, null, false).ConfigureAwait(true);
                var active = (view ?? new List<ReconciliationViewData>())
                    .Where(v => v != null && !v.IsDeleted)
                    .ToList();
                if (active.Count == 0)
                {
                    StatusText.Text = "No active rows found to apply rules.";
                    return;
                }
                var ids = active.Select(v => v.ID)
                                .Where(id => !string.IsNullOrWhiteSpace(id))
                                .Distinct(StringComparer.OrdinalIgnoreCase)
                                .ToList();
                if (MessageBox.Show($"Run rules now on {ids.Count} row(s)?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                {
                    StatusText.Text = "Run cancelled.";
                    return;
                }
                StatusText.Text = $"Applying rules to {ids.Count} row(s)…";
                var n = await _reconciliationService.ApplyRulesNowAsync(ids).ConfigureAwait(true);
                StatusText.Text = $"Rules applied to {n} row(s).";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Run error: {ex.Message}";
            }
        }

        private async Task SaveAndRunAsync(TruthRule rule)
        {
            try
            {
                StatusText.Text = "Saving rule…";
                var ok = await _repository.UpsertRuleAsync(rule).ConfigureAwait(true);
                if (!ok)
                {
                    StatusText.Text = "Save failed; not running rules.";
                    return;
                }
                // Reflect saved rule in local list
                var existing = _allRules.FirstOrDefault(x => string.Equals(x.RuleId, rule.RuleId, StringComparison.OrdinalIgnoreCase));
                if (existing == null)
                {
                    _allRules.Add(rule);
                    ApplySearchAndRefresh(selectRuleId: rule.RuleId);
                }
                StatusText.Text = "Rule saved. Collecting rows…";
                if (_reconciliationService == null)
                {
                    StatusText.Text = "ReconciliationService not available.";
                    return;
                }
                var countryId = _offlineFirstService?.CurrentCountry?.CNT_Id;
                var view = await _reconciliationService.GetReconciliationViewAsync(countryId, null, false).ConfigureAwait(true);
                var active = (view ?? new List<ReconciliationViewData>())
                    .Where(v => v != null && !v.IsDeleted)
                    .ToList();
                if (active.Count == 0)
                {
                    StatusText.Text = "No active rows found to apply rules.";
                    return;
                }
                var ids = active.Select(v => v.ID)
                                .Where(id => !string.IsNullOrWhiteSpace(id))
                                .Distinct(StringComparer.OrdinalIgnoreCase)
                                .ToList();
                StatusText.Text = $"Applying rules to {ids.Count} row(s)…";
                var n = await _reconciliationService.ApplyRulesNowAsync(ids).ConfigureAwait(true);
                StatusText.Text = $"Saved and applied rules to {n} row(s).";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Run error: {ex.Message}";
            }
        }

        private async Task ReloadRulesAsync()
        {
            try
            {
                StatusText.Text = "Loading rules…";
                var list = await _repository.LoadRulesAsync();
                _allRules = list?.ToList() ?? new List<TruthRule>();
                ApplySearchAndRefresh();
                StatusText.Text = $"Loaded {Rules.Count} rule(s).";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Load error: {ex.Message}";
            }
        }

        private void BuildStaticLists()
        {
            GuaranteeTypes = new[] { "*", "ISSUANCE", "REISSUANCE", "ADVISING" };
            var tx = new List<string> { "*" };
            try { tx.AddRange(System.Enum.GetNames(typeof(TransactionType))); } catch { }
            TransactionTypes = tx.ToArray();
        }

        private void BuildReferentialOptions()
        {
            try
            {
                ActionOptions.Clear(); KpiOptions.Clear(); IncidentTypeOptions.Clear(); ReasonOptions.Clear();
                var ufs = _offlineFirstService?.UserFields;
                if (ufs == null) return;
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "Action", System.StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    ActionOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "KPI", System.StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    KpiOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "Incident Type", System.StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    IncidentTypeOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "RISKY", System.StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    ReasonOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
            }
            catch { }
        }

        private async void EnsureTable_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Ensuring table…";
                var ok = await _repository.EnsureRulesTableAsync();
                StatusText.Text = ok ? "Rules table is ready." : "Failed to ensure rules table.";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Ensure error: {ex.Message}";
            }
        }

        private async void Reload_Click(object sender, RoutedEventArgs e)
        {
            await ReloadRulesAsync();
        }

        private void Add_Click(object sender, RoutedEventArgs e)
        {
            var draft = new TruthRule
            {
                RuleId = SuggestRuleId(),
                Enabled = true,
                Priority = 100,
                Scope = RuleScope.Both,
                AccountSide = "*",
                Sign = "*",
                ApplyTo = ApplyTarget.Self,
                AutoApply = true
            };
            var result = ShowEditor(draft);
            if (result != null)
            {
                // If user requested to run now, persist immediately then run
                if (_runNowFromEditor)
                {
                    _ = SaveAndRunAsync(result);
                }
                else
                {
                    _allRules.Add(result);
                    ApplySearchAndRefresh(selectRuleId: result.RuleId);
                    StatusText.Text = "Rule added (not yet saved).";
                }
            }
        }

        private string SuggestRuleId()
        {
            string baseId = "RULE_" + DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string id = baseId;
            int i = 1;
            while (Rules.Any(r => string.Equals(r.RuleId, id, StringComparison.OrdinalIgnoreCase)))
            {
                id = baseId + "_" + (++i);
            }
            return id;
        }

        private async void SaveSelected_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var rule = RulesGrid.SelectedItem as TruthRule;
                if (rule == null)
                {
                    StatusText.Text = "Select a rule first.";
                    return;
                }
                if (string.IsNullOrWhiteSpace(rule.RuleId))
                {
                    StatusText.Text = "RuleId is required.";
                    return;
                }
                var ok = await _repository.UpsertRuleAsync(rule);
                StatusText.Text = ok ? $"Saved '{rule.RuleId}'." : $"Nothing saved for '{rule.RuleId}'.";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Save error: {ex.Message}";
            }
        }

        private async void DeleteSelected_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var rule = RulesGrid.SelectedItem as TruthRule;
                if (rule == null)
                {
                    StatusText.Text = "Select a rule first.";
                    return;
                }
                if (string.IsNullOrWhiteSpace(rule.RuleId))
                {
                    StatusText.Text = "Selected rule has no RuleId.";
                    return;
                }
                if (MessageBox.Show($"Delete rule '{rule.RuleId}'?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes)
                    return;
                var n = await _repository.DeleteRuleAsync(rule.RuleId);
                if (n > 0)
                {
                    _allRules.RemoveAll(r => string.Equals(r.RuleId, rule.RuleId, StringComparison.OrdinalIgnoreCase));
                    ApplySearchAndRefresh();
                    StatusText.Text = $"Deleted '{rule.RuleId}'.";
                }
                else
                {
                    StatusText.Text = $"No rule deleted for '{rule.RuleId}'.";
                }
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Delete error: {ex.Message}";
            }
        }

        private async void SeedDefaults_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Seeding default rules…";
                var n = await _repository.SeedDefaultRulesAsync();
                await ReloadRulesAsync();
                StatusText.Text = n > 0 ? $"Seeded/updated {n} rule(s)." : "No default rules were seeded.";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Seed error: {ex.Message}";
            }
        }

        private void EditSelected_Click(object sender, RoutedEventArgs e)
        {
            var rule = RulesGrid.SelectedItem as TruthRule;
            if (rule == null) { StatusText.Text = "Select a rule first."; return; }
            var updated = ShowEditor(CloneRule(rule));
            if (updated != null)
            {
                ApplyRuleUpdates(rule, updated);
                ApplySearchAndRefresh(selectRuleId: rule.RuleId);
                if (_runNowFromEditor)
                {
                    _ = SaveAndRunAsync(rule);
                }
                else
                {
                    StatusText.Text = "Rule updated (not saved).";
                }
            }
        }

        private void EditRowButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.DataContext is TruthRule r)
            {
                var updated = ShowEditor(CloneRule(r));
                if (updated != null)
                {
                    ApplyRuleUpdates(r, updated);
                    ApplySearchAndRefresh(selectRuleId: r.RuleId);
                    if (_runNowFromEditor)
                    {
                        _ = SaveAndRunAsync(r);
                    }
                    else
                    {
                        StatusText.Text = "Rule updated (not saved).";
                    }
                }
            }
        }

        private void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplySearchAndRefresh();
        }

        private void ApplySearchAndRefresh(string selectRuleId = null)
        {
            var query = (SearchBox?.Text ?? string.Empty).Trim();
            IEnumerable<TruthRule> src = _allRules;
            if (!string.IsNullOrWhiteSpace(query))
            {
                src = src.Where(r => (r.RuleId ?? string.Empty).IndexOf(query, System.StringComparison.OrdinalIgnoreCase) >= 0
                                   || (r.Message ?? string.Empty).IndexOf(query, System.StringComparison.OrdinalIgnoreCase) >= 0
                                   || (r.TransactionType ?? string.Empty).IndexOf(query, System.StringComparison.OrdinalIgnoreCase) >= 0
                                   || (r.GuaranteeType ?? string.Empty).IndexOf(query, System.StringComparison.OrdinalIgnoreCase) >= 0);
            }
            Rules.Clear();
            foreach (var r in src)
                Rules.Add(r);
            if (!string.IsNullOrWhiteSpace(selectRuleId))
            {
                var sel = Rules.FirstOrDefault(x => string.Equals(x.RuleId, selectRuleId, System.StringComparison.OrdinalIgnoreCase));
                if (sel != null)
                {
                    RulesGrid.SelectedItem = sel;
                    RulesGrid.ScrollIntoView(sel);
                }
            }
        }

        private bool _runNowFromEditor;
        private TruthRule ShowEditor(TruthRule draft)
        {
            try
            {
                var win = new RuleEditorWindow(draft, _offlineFirstService);
                win.Owner = this;
                var ok = win.ShowDialog();
                if (ok == true)
                {
                    _runNowFromEditor = win.RunNow;
                    return win.ResultRule;
                }
            }
            catch { }
            _runNowFromEditor = false;
            return null;
        }

        private static TruthRule CloneRule(TruthRule r)
        {
            if (r == null) return null;
            return new TruthRule
            {
                RuleId = r.RuleId,
                Enabled = r.Enabled,
                Priority = r.Priority,
                Scope = r.Scope,
                AccountSide = r.AccountSide,
                GuaranteeType = r.GuaranteeType,
                TransactionType = r.TransactionType,
                Booking = r.Booking,
                HasDwingsLink = r.HasDwingsLink,
                IsGrouped = r.IsGrouped,
                IsAmountMatch = r.IsAmountMatch,
                Sign = r.Sign,
                // New DWINGS-related inputs
                MTStatusAcked = r.MTStatusAcked,
                CommIdEmail = r.CommIdEmail,
                BgiStatusInitiated = r.BgiStatusInitiated,
                // Time/state conditions
                TriggerDateIsNull = r.TriggerDateIsNull,
                DaysSinceTriggerMin = r.DaysSinceTriggerMin,
                DaysSinceTriggerMax = r.DaysSinceTriggerMax,
                IsTransitory = r.IsTransitory,
                OperationDaysAgoMin = r.OperationDaysAgoMin,
                OperationDaysAgoMax = r.OperationDaysAgoMax,
                IsMatched = r.IsMatched,
                HasManualMatch = r.HasManualMatch,
                IsFirstRequest = r.IsFirstRequest,
                DaysSinceReminderMin = r.DaysSinceReminderMin,
                DaysSinceReminderMax = r.DaysSinceReminderMax,
                CurrentActionId = r.CurrentActionId,
                OutputActionId = r.OutputActionId,
                OutputKpiId = r.OutputKpiId,
                OutputIncidentTypeId = r.OutputIncidentTypeId,
                OutputRiskyItem = r.OutputRiskyItem,
                OutputReasonNonRiskyId = r.OutputReasonNonRiskyId,
                OutputToRemind = r.OutputToRemind,
                OutputToRemindDays = r.OutputToRemindDays,
                // New output
                OutputFirstClaimToday = r.OutputFirstClaimToday,
                ApplyTo = r.ApplyTo,
                AutoApply = r.AutoApply,
                Message = r.Message
            };
        }

        private static void ApplyRuleUpdates(TruthRule target, TruthRule source)
        {
            if (target == null || source == null) return;
            target.RuleId = source.RuleId;
            target.Enabled = source.Enabled;
            target.Priority = source.Priority;
            target.Scope = source.Scope;
            target.AccountSide = source.AccountSide;
            target.GuaranteeType = source.GuaranteeType;
            target.TransactionType = source.TransactionType;
            target.HasDwingsLink = source.HasDwingsLink;
            target.IsGrouped = source.IsGrouped;
            target.IsAmountMatch = source.IsAmountMatch;
            target.Sign = source.Sign;
            // New DWINGS-related inputs
            target.MTStatusAcked = source.MTStatusAcked;
            target.CommIdEmail = source.CommIdEmail;
            target.BgiStatusInitiated = source.BgiStatusInitiated;
            // Time/state conditions
            target.TriggerDateIsNull = source.TriggerDateIsNull;
            target.DaysSinceTriggerMin = source.DaysSinceTriggerMin;
            target.DaysSinceTriggerMax = source.DaysSinceTriggerMax;
            target.IsTransitory = source.IsTransitory;
            target.OperationDaysAgoMin = source.OperationDaysAgoMin;
            target.OperationDaysAgoMax = source.OperationDaysAgoMax;
            target.IsMatched = source.IsMatched;
            target.HasManualMatch = source.HasManualMatch;
            target.IsFirstRequest = source.IsFirstRequest;
            target.DaysSinceReminderMin = source.DaysSinceReminderMin;
            target.DaysSinceReminderMax = source.DaysSinceReminderMax;
            target.CurrentActionId = source.CurrentActionId;
            target.OutputActionId = source.OutputActionId;
            target.OutputKpiId = source.OutputKpiId;
            target.OutputIncidentTypeId = source.OutputIncidentTypeId;
            target.OutputRiskyItem = source.OutputRiskyItem;
            target.OutputReasonNonRiskyId = source.OutputReasonNonRiskyId;
            target.OutputToRemind = source.OutputToRemind;
            target.OutputToRemindDays = source.OutputToRemindDays;
            // New output
            target.OutputFirstClaimToday = source.OutputFirstClaimToday;
            target.ApplyTo = source.ApplyTo;
            target.AutoApply = source.AutoApply;
            target.Message = source.Message;
        }
    }
}
