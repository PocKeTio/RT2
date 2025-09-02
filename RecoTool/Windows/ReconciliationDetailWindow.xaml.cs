using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using RecoTool.Models;
using RecoTool.Services;

namespace RecoTool.UI.Views.Windows
{
    /// <summary>
    /// Interaction logic for ReconciliationDetailWindow.xaml
    /// </summary>
    public partial class ReconciliationDetailWindow : Window, INotifyPropertyChanged
    {
        private readonly ReconciliationViewData _item;
        private readonly List<ReconciliationViewData> _all;
        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;
        private Reconciliation _reconciliation;

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        public class OptionItem
        {
            public int Id { get; set; }
            public string Content { get; set; }
            public override string ToString() => Content;
        }

        private ObservableCollection<OptionItem> _kpiOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> KPIOptions
        {
            get => _kpiOptions;
            set { _kpiOptions = value; OnPropertyChanged(nameof(KPIOptions)); }
        }

        private ObservableCollection<OptionItem> _actionOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ActionOptions
        {
            get => _actionOptions;
            set { _actionOptions = value; OnPropertyChanged(nameof(ActionOptions)); }
        }

        private ObservableCollection<OptionItem> _incidentTypeOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> IncidentTypeOptions
        {
            get => _incidentTypeOptions;
            set { _incidentTypeOptions = value; OnPropertyChanged(nameof(IncidentTypeOptions)); }
        }

        private ObservableCollection<OptionItem> _reasonOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ReasonOptions
        {
            get => _reasonOptions;
            set { _reasonOptions = value; OnPropertyChanged(nameof(ReasonOptions)); }
        }

        private int? _selectedKPIId;
        public int? SelectedKPIId
        {
            get => _selectedKPIId;
            set { _selectedKPIId = value; OnPropertyChanged(nameof(SelectedKPIId)); }
        }

        private int? _selectedActionId;
        public int? SelectedActionId
        {
            get => _selectedActionId;
            set { _selectedActionId = value; OnPropertyChanged(nameof(SelectedActionId)); }
        }

        private int? _selectedIncidentTypeId;
        public int? SelectedIncidentTypeId
        {
            get => _selectedIncidentTypeId;
            set { _selectedIncidentTypeId = value; OnPropertyChanged(nameof(SelectedIncidentTypeId)); }
        }

        private int? _selectedReasonId;
        public int? SelectedReasonId
        {
            get => _selectedReasonId;
            set { _selectedReasonId = value; OnPropertyChanged(nameof(SelectedReasonId)); }
        }

        public class MatchingItem
        {
            public string Id { get; set; }
            public DateTime? OperationDate { get; set; }
            public string Description { get; set; }
            public decimal Amount { get; set; }
            public string Currency { get; set; }
            public string Status { get; set; }
            public string DwingsRef { get; set; }
            public string DebtorName { get; set; }
        }

        public ReconciliationDetailWindow(ReconciliationViewData item, IEnumerable<ReconciliationViewData> all)
        {
            InitializeComponent();
            DataContext = this;
            _item = item;
            _all = all?.ToList() ?? new List<ReconciliationViewData>();
            // Resolve service from DI
            try
            {
                _reconciliationService = (App.ServiceProvider?.GetService(typeof(ReconciliationService))) as ReconciliationService;
                _offlineFirstService = (App.ServiceProvider?.GetService(typeof(OfflineFirstService))) as OfflineFirstService;
            }
            catch { /* ignore DI resolve issues */ }

            PopulateHeader();
            PopulateTopDetails();
            PopulateReferentialOptions();
            _ = LoadReconciliationAsync();
            BuildMatchingAssistance();
        }

        private void PopulateHeader()
        {
            try
            {
                if (HeaderText != null)
                {
                    // Avoid showing technical IDs; prefer a friendly label
                    HeaderText.Text = $"Reconciliation Detail - {(_item?.RawLabel ?? "-")}";
                }

                if (AccountTypeText != null)
                {
                    // Avoid displaying internal account IDs; show country if available
                    AccountTypeText.Text = $"Country: {_item?.Country ?? "N/A"}";
                }
            }
            catch { /* ignore UI update issues */ }
        }

        private void PopulateTopDetails()
        {
            if (_item == null) return;

            OperationDateValue.Text = _item.Operation_Date?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "";
            DescriptionValue.Text = _item.RawLabel ?? "";
            AmountValue.Text = _item.SignedAmount.ToString("N2", CultureInfo.InvariantCulture);
            CurrencyValue.Text = _item.CCY ?? "";
            StatusValue.Text = IsMatched(_item) ? "Matched" : "Unmatched";

            // Reference preference order
            // Avoid DWINGS or technical IDs in the displayed reference; prefer friendly fields
            var reference = _item.Receivable_InvoiceFromAmbre
                            ?? _item.Reconciliation_Num
                            ?? _item.Event_Num
                            ?? _item.RawLabel
                            ?? string.Empty;
            ReferenceValue.Text = reference;

            AmountInEURValue.Text = _item.CCY?.Equals("EUR", StringComparison.OrdinalIgnoreCase) == true
                ? _item.SignedAmount.ToString("N2", CultureInfo.InvariantCulture)
                : string.Empty;

            AmountInKEURValue.Text = _item.CCY?.Equals("EUR", StringComparison.OrdinalIgnoreCase) == true
                ? (_item.SignedAmount / 1000m).ToString("N2", CultureInfo.InvariantCulture)
                : string.Empty;

            FXRateValue.Text = string.Empty; // Unknown without FX data
            ValueDateValue.Text = _item.Value_Date?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "";
            TransactionTypeValue.Text = !string.IsNullOrWhiteSpace(_item.Pivot_TransactionCodesFromLabel)
                ? _item.Pivot_TransactionCodesFromLabel
                : _item.Pivot_TRNFromLabel;

            DebtorNameValue.Text = !string.IsNullOrWhiteSpace(_item.Pivot_TRNFromLabel)
                ? _item.Pivot_TRNFromLabel
                : _item.Folder;

            InvoiceValue.Text = _item.Receivable_InvoiceFromAmbre ?? "";
            GroupNameValue.Text = _item.SYNDICATE ?? "";
            SwiftCodeValue.Text = _item.SwiftCode ?? "";

            // Additional display fields added to the XAML
            if (CountryValue != null) CountryValue.Text = _item.Country ?? string.Empty;
            if (AccountIdValue != null) AccountIdValue.Text = _item.Account_ID ?? string.Empty;
            if (EventNumValue != null) EventNumValue.Text = _item.Event_Num ?? string.Empty;

            if (DwingsIdsValue != null)
            {
                var ids = new[]
                {
                    _item.DWINGS_GuaranteeID,
                    _item.DWINGS_InvoiceID,
                    _item.DWINGS_CommissionID,
                    _item.GUARANTEE_ID,
                    _item.INVOICE_ID,
                    _item.COMMISSION_ID
                }
                .Where(s => !string.IsNullOrWhiteSpace(s));
                DwingsIdsValue.Text = string.Join(" | ", ids);
            }

            // DWINGS Guarantee extended fields (if present)
            if (GNatureValue != null) GNatureValue.Text = _item.G_NATURE ?? string.Empty;
            if (GStatusValue != null) GStatusValue.Text = _item.G_EVENT_STATUS ?? string.Empty;
            if (GIssueDateValue != null) GIssueDateValue.Text = _item.G_ISSUEDATE ?? string.Empty;
            if (GPartyIdValue != null) GPartyIdValue.Text = _item.G_PARTY_ID ?? string.Empty;
            if (GName1Value != null) GName1Value.Text = _item.G_NAME1 ?? string.Empty;

            // DWINGS Invoice extended fields (if present)
            if (IRequestedAmountValue != null) IRequestedAmountValue.Text = _item.I_REQUESTED_INVOICE_AMOUNT ?? string.Empty;
            if (IBillingCurrencyValue != null) IBillingCurrencyValue.Text = _item.I_BILLING_CURRENCY ?? string.Empty;
            if (IStartDateValue != null) IStartDateValue.Text = _item.I_START_DATE ?? string.Empty;
            if (IEndDateValue != null) IEndDateValue.Text = _item.I_END_DATE ?? string.Empty;
            if (IStatusValue != null) IStatusValue.Text = _item.I_T_INVOICE_STATUS ?? string.Empty;
            if (IBusinessCaseRefValue != null) IBusinessCaseRefValue.Text = _item.I_BUSINESS_CASE_REFERENCE ?? string.Empty;
        }

        private void PopulateReferentialOptions()
        {
            try
            {
                var kpiItems = new List<OptionItem>();
                var actionItems = new List<OptionItem>();
                var incidentItems = new List<OptionItem>();
                var reasonItems = new List<OptionItem>();
                var userFields = _offlineFirstService?.UserFields;
                if (userFields != null)
                {
                    foreach (var uf in userFields.Where(f => string.Equals(f.USR_Category, "Action", StringComparison.OrdinalIgnoreCase)))
                    {
                        var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                        actionItems.Add(new OptionItem { Id = uf.USR_ID, Content = label });
                    }
                    foreach (var uf in userFields.Where(f => string.Equals(f.USR_Category, "KPI", StringComparison.OrdinalIgnoreCase)))
                    {
                        var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                        kpiItems.Add(new OptionItem { Id = uf.USR_ID, Content = label });
                    }
                    foreach (var uf in userFields.Where(f => string.Equals(f.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)))
                    {
                        var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                        incidentItems.Add(new OptionItem { Id = uf.USR_ID, Content = label });
                    }
                    foreach (var uf in userFields.Where(f => string.Equals(f.USR_Category, "RISKY", StringComparison.OrdinalIgnoreCase)))
                    {
                        var label = !string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : uf.USR_FieldName;
                        reasonItems.Add(new OptionItem { Id = uf.USR_ID, Content = label });
                    }
                }
                ActionOptions = new ObservableCollection<OptionItem>(actionItems.OrderBy(i => i.Content));
                KPIOptions = new ObservableCollection<OptionItem>(kpiItems.OrderBy(i => i.Content));
                IncidentTypeOptions = new ObservableCollection<OptionItem>(incidentItems.OrderBy(i => i.Content));
                ReasonOptions = new ObservableCollection<OptionItem>(reasonItems.OrderBy(i => i.Content));

                // Preselect from _item if present (before async load)
                if (_item?.KPI != null)
                    SelectedKPIId = _item.KPI;
                if (_item?.Action != null)
                    SelectedActionId = _item.Action;
                if (_item?.IncidentType != null)
                    SelectedIncidentTypeId = _item.IncidentType;
                if (_item?.ReasonNonRisky != null)
                    SelectedReasonId = _item.ReasonNonRisky;
            }
            catch { /* ignore */ }
        }

        private async System.Threading.Tasks.Task LoadReconciliationAsync()
        {
            try
            {
                if (_reconciliationService == null || _item == null || string.IsNullOrWhiteSpace(_item.ID))
                    return;

                _reconciliation = await _reconciliationService.GetOrCreateReconciliationAsync(_item.ID);

                // Populate editable fields
                if (CommentTextBox != null)
                    CommentTextBox.Text = _reconciliation?.Comments ?? _item?.Comments ?? string.Empty;

                if (ReminderDatePicker != null)
                    ReminderDatePicker.SelectedDate = _reconciliation?.ToRemindDate ?? _item?.ToRemindDate;

                if (ToRemindCheckBox != null)
                    ToRemindCheckBox.IsChecked = _reconciliation?.ToRemind ?? _item?.ToRemind ?? false;

                if (AckCheckBox != null)
                    AckCheckBox.IsChecked = _reconciliation?.ACK ?? _item?.ACK ?? false;

                if (SwiftCodeTextBox != null)
                    SwiftCodeTextBox.Text = _reconciliation?.SwiftCode ?? _item?.SwiftCode ?? string.Empty;

                if (PaymentRefTextBox != null)
                    PaymentRefTextBox.Text = _reconciliation?.PaymentReference ?? _item?.PaymentReference ?? string.Empty;

                if (InternalRefTextBox != null)
                    InternalRefTextBox.Text = _reconciliation?.InternalInvoiceReference ?? _item?.InternalInvoiceReference ?? string.Empty;

                if (RiskyItemTextBox != null)
                    RiskyItemTextBox.Text = (_reconciliation?.RiskyItem ?? _item?.RiskyItem)?.ToString() ?? string.Empty;

                // Load new long text fields
                if (MbawDataTextBox != null)
                    MbawDataTextBox.Text = _reconciliation?.MbawData ?? string.Empty;
                if (SpiritDataTextBox != null)
                    SpiritDataTextBox.Text = _reconciliation?.SpiritData ?? string.Empty;

                // Preselect reason
                SelectedReasonId = _reconciliation?.ReasonNonRisky ?? SelectedReasonId;

                // Trigger Date
                if (TriggerDatePicker != null)
                    TriggerDatePicker.SelectedDate = _reconciliation?.TriggerDate;

                // Initialize KPI selection from persisted reconciliation
                if (_reconciliation != null)
                {
                    SelectedKPIId = _reconciliation.KPI ?? SelectedKPIId;
                    SelectedActionId = _reconciliation.Action ?? SelectedActionId;
                    SelectedIncidentTypeId = _reconciliation.IncidentType ?? SelectedIncidentTypeId;
                    SelectedReasonId = _reconciliation.ReasonNonRisky ?? SelectedReasonId;
                }
            }
            catch
            {
                // ignore load errors in UI
            }
        }

        private static bool IsMatched(ReconciliationViewData x)
        {
            return !string.IsNullOrWhiteSpace(x?.DWINGS_GuaranteeID)
                   || !string.IsNullOrWhiteSpace(x?.DWINGS_InvoiceID)
                   || !string.IsNullOrWhiteSpace(x?.DWINGS_CommissionID);
        }

        private void BuildMatchingAssistance()
        {
            try
            {
                var matches = FindPotentialMatches(_item);
                MatchingDataGrid.ItemsSource = new ObservableCollection<MatchingItem>(matches);
                StatusText.Text = $"{matches.Count} matching line(s)";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Matching error: {ex.Message}";
            }
        }

        private List<MatchingItem> FindPotentialMatches(ReconciliationViewData source)
        {
            var result = new List<MatchingItem>();
            if (source == null || _all == null) return result;

            // Basic heuristics:
            // 1) Same Reconciliation number (if present) but different account
            // 2) Same Event_Num but different account
            // 3) Opposite amount with similar absolute value and close operation date (±7 days)
            // 4) Same DWINGS ref if any exists on either side

            var tol = 0.01m; // amount tolerance
            var maxDays = 7;

            var q = _all.Where(x => x.ID != source.ID);

            // Same reconciliation ref
            if (!string.IsNullOrWhiteSpace(source.Reconciliation_Num))
            {
                q = q.Where(x => x.Reconciliation_Num == source.Reconciliation_Num || x.ReconciliationOrigin_Num == source.Reconciliation_Num);
            }

            // Prefer other account
            q = q.Where(x => x.Account_ID != source.Account_ID);

            // Score candidates
            var candidates = q.Select(x => new
            {
                Item = x,
                Score = (source.Event_Num != null && x.Event_Num == source.Event_Num ? 3 : 0)
                        + (HasCommonDwingsRef(source, x) ? 3 : 0)
                        + (IsOppositeAmountClose(source.SignedAmount, x.SignedAmount, tol) ? 2 : 0)
                        + (IsCloseDate(source.Operation_Date, x.Operation_Date, maxDays) ? 1 : 0)
            })
            .Where(s => s.Score > 0)
            .OrderByDescending(s => s.Score)
            .Take(100)
            .ToList();

            foreach (var c in candidates)
            {
                result.Add(new MatchingItem
                {
                    Id = c.Item.ID,
                    OperationDate = c.Item.Operation_Date,
                    Description = c.Item.RawLabel,
                    Amount = c.Item.SignedAmount,
                    Currency = c.Item.CCY,
                    Status = IsMatched(c.Item) ? "Matched" : "Unmatched",
                    DwingsRef = c.Item.DWINGS_GuaranteeID ?? c.Item.DWINGS_InvoiceID ?? c.Item.DWINGS_CommissionID ?? c.Item.Receivable_DWRefFromAmbre,
                    DebtorName = !string.IsNullOrWhiteSpace(c.Item.Pivot_TRNFromLabel) ? c.Item.Pivot_TRNFromLabel : c.Item.Folder,
                });
            }

            return result;
        }

        private static bool HasCommonDwingsRef(ReconciliationViewData a, ReconciliationViewData b)
        {
            var refsA = new[] { a.DWINGS_GuaranteeID, a.DWINGS_InvoiceID, a.DWINGS_CommissionID, a.Receivable_DWRefFromAmbre }
                .Where(r => !string.IsNullOrWhiteSpace(r)).ToHashSet();
            var refsB = new[] { b.DWINGS_GuaranteeID, b.DWINGS_InvoiceID, b.DWINGS_CommissionID, b.Receivable_DWRefFromAmbre }
                .Where(r => !string.IsNullOrWhiteSpace(r)).ToHashSet();
            return refsA.Overlaps(refsB);
        }

        private static bool IsOppositeAmountClose(decimal a, decimal b, decimal tolerance)
        {
            return Math.Abs(a + b) <= tolerance * Math.Max(1m, Math.Max(Math.Abs(a), Math.Abs(b)));
        }

        private static bool IsCloseDate(DateTime? a, DateTime? b, int maxDays)
        {
            if (!a.HasValue || !b.HasValue) return false;
            return Math.Abs((a.Value.Date - b.Value.Date).TotalDays) <= maxDays;
        }

        private void RefreshMatching_Click(object sender, RoutedEventArgs e)
        {
            BuildMatchingAssistance();
        }

        private void ViewDetail_Click(object sender, RoutedEventArgs e)
        {
            if (MatchingDataGrid?.SelectedItem is MatchingItem mi)
            {
                // Do not show internal IDs in dialogs
                MessageBox.Show($"Item: {mi.Description}", "Detail", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        private void LinkItems_Click(object sender, RoutedEventArgs e)
        {
            // Placeholder for link action
            MessageBox.Show("Link action not implemented yet.", "Info", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private async void SaveButton_Click(object sender, RoutedEventArgs e)
        {
            if (_item == null || _reconciliationService == null || string.IsNullOrWhiteSpace(_item.ID))
            {
                MessageBox.Show("Cannot save: missing context.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                return;
            }

            try
            {
                var reco = _reconciliation ?? await _reconciliationService.GetOrCreateReconciliationAsync(_item.ID);

                // Map editable fields
                reco.ID = _item.ID;
                if (CommentTextBox != null)
                    reco.Comments = CommentTextBox.Text?.Trim();

                // Persist KPI selection
                reco.KPI = SelectedKPIId;

                // Persist Action and Incident Type
                reco.Action = SelectedActionId;
                reco.IncidentType = SelectedIncidentTypeId;

                if (ReminderDatePicker != null)
                {
                    var sel = ReminderDatePicker.SelectedDate;
                    reco.ToRemindDate = sel;
                    // If checkbox exists, prefer its value; otherwise infer from date
                    if (ToRemindCheckBox != null && ToRemindCheckBox.IsChecked.HasValue)
                        reco.ToRemind = ToRemindCheckBox.IsChecked.Value;
                    else
                        reco.ToRemind = sel.HasValue;
                }

                if (AckCheckBox != null && AckCheckBox.IsChecked.HasValue)
                    reco.ACK = AckCheckBox.IsChecked.Value;

                if (SwiftCodeTextBox != null)
                    reco.SwiftCode = SwiftCodeTextBox.Text?.Trim();

                if (PaymentRefTextBox != null)
                    reco.PaymentReference = PaymentRefTextBox.Text?.Trim();

                if (InternalRefTextBox != null)
                    reco.InternalInvoiceReference = InternalRefTextBox.Text?.Trim();

                if (RiskyItemTextBox != null)
                {
                    var txt = RiskyItemTextBox.Text?.Trim();
                    if (string.IsNullOrWhiteSpace(txt))
                    {
                        reco.RiskyItem = null;
                    }
                    else
                    {
                        // Accept true/false, yes/no, 1/0
                        if (bool.TryParse(txt, out var b))
                            reco.RiskyItem = b;
                        else if (int.TryParse(txt, out var n))
                            reco.RiskyItem = n != 0;
                        else if (string.Equals(txt, "yes", StringComparison.OrdinalIgnoreCase) || string.Equals(txt, "y", StringComparison.OrdinalIgnoreCase))
                            reco.RiskyItem = true;
                        else if (string.Equals(txt, "no", StringComparison.OrdinalIgnoreCase) || string.Equals(txt, "n", StringComparison.OrdinalIgnoreCase))
                            reco.RiskyItem = false;
                        else
                            reco.RiskyItem = null;
                    }
                }

                // Persist selected reason (nullable)
                reco.ReasonNonRisky = SelectedReasonId;

                // Persist new long text fields
                if (MbawDataTextBox != null)
                {
                    var v = MbawDataTextBox.Text?.Trim();
                    reco.MbawData = string.IsNullOrWhiteSpace(v) ? null : v;
                }
                if (SpiritDataTextBox != null)
                {
                    var v = SpiritDataTextBox.Text?.Trim();
                    reco.SpiritData = string.IsNullOrWhiteSpace(v) ? null : v;
                }

                // Persist Trigger Date
                if (TriggerDatePicker != null)
                    reco.TriggerDate = TriggerDatePicker.SelectedDate;

                // Persist
                await _reconciliationService.SaveReconciliationAsync(reco);
                _reconciliation = reco;

                // Signal success to the caller and close the dialog
                this.DialogResult = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Save failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }
}
