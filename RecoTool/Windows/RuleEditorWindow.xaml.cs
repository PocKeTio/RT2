using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using RecoTool.Services;
using RecoTool.Services.Rules;
using RecoTool.UI.Models;

namespace RecoTool.Windows
{
    public partial class RuleEditorWindow : Window
    {
        // When true, caller will run rules immediately after saving
        public bool RunNow { get; set; }

        public class BoolChoice
        {
            public string Label { get; set; }
            public bool? Value { get; set; }
        }

        private readonly OfflineFirstService _offlineFirstService;

        public TruthRule EditedRule { get; set; }
        public TruthRule ResultRule { get; private set; }

        public RuleScope[] Scopes { get; } = new[] { RuleScope.Both, RuleScope.Import, RuleScope.Edit };
        public ApplyTarget[] ApplyTargets { get; } = new[] { ApplyTarget.Self, ApplyTarget.Counterpart, ApplyTarget.Both };
        public string[] AccountSides { get; } = new[] { "*", "P", "R" };
        public string[] Signs { get; } = new[] { "*", "C", "D" };
        public string[] GuaranteeTypes { get; private set; }
        public string[] TransactionTypes { get; private set; }

        public ObservableCollection<OptionItem> ActionOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> KpiOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> IncidentTypeOptions { get; } = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ReasonOptions { get; } = new ObservableCollection<OptionItem>();

        public ObservableCollection<BoolChoice> BoolChoices { get; } = new ObservableCollection<BoolChoice>
        {
            new BoolChoice { Label = "— (None) —", Value = null },
            new BoolChoice { Label = "Yes", Value = true },
            new BoolChoice { Label = "No", Value = false },
        };

        public RuleEditorWindow(TruthRule seed, OfflineFirstService offlineFirstService)
        {
            InitializeComponent();
            _offlineFirstService = offlineFirstService;
            BuildStaticLists();
            BuildReferentialOptions();

            EditedRule = CloneRule(seed ?? new TruthRule());
            RunNow = false;
            DataContext = this;
        }

        private void BuildStaticLists()
        {
            GuaranteeTypes = new[] { "*", "ISSUANCE", "REISSUANCE", "ADVISING" };
            var tx = new List<string> { "*" };
            try { tx.AddRange(Enum.GetNames(typeof(TransactionType))); } catch { }
            TransactionTypes = tx.ToArray();
        }

        private void BuildReferentialOptions()
        {
            try
            {
                ActionOptions.Clear(); KpiOptions.Clear(); IncidentTypeOptions.Clear(); ReasonOptions.Clear();
                var ufs = _offlineFirstService?.UserFields;
                if (ufs == null) return;
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "Action", StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    ActionOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "KPI", StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    KpiOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    IncidentTypeOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
                foreach (var uf in ufs.Where(f => string.Equals(f.USR_Category, "RISKY", StringComparison.OrdinalIgnoreCase)))
                {
                    var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                    ReasonOptions.Add(new OptionItem { Id = uf.USR_ID, Name = label });
                }
            }
            catch { }
        }

        private static TruthRule CloneRule(TruthRule r)
        {
            if (r == null) return new TruthRule();
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

        private void Save_Click(object sender, RoutedEventArgs e)
        {
            if (string.IsNullOrWhiteSpace(EditedRule?.RuleId))
            {
                MessageBox.Show("RuleId is required.", "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            ResultRule = CloneRule(EditedRule);
            DialogResult = true;
            Close();
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        private static readonly Regex DigitsOnly = new Regex("^[0-9]+$", RegexOptions.Compiled);
        private void NumberOnly_PreviewTextInput(object sender, TextCompositionEventArgs e)
        {
            if (!DigitsOnly.IsMatch(e.Text)) e.Handled = true;
        }
        private void NumberOnly_Pasting(object sender, DataObjectPastingEventArgs e)
        {
            if (e.DataObject.GetDataPresent(typeof(string)))
            {
                string text = (string)e.DataObject.GetData(typeof(string));
                if (!DigitsOnly.IsMatch(text ?? string.Empty)) e.CancelCommand();
            }
            else
            {
                e.CancelCommand();
            }
        }
    }
}
