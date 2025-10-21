using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Helpers;
using System.Text;
using RecoTool.Services.DTOs;

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
        private bool _autoSearched;

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        public class OptionItem
        {
            public int Id { get; set; }
            public string Content { get; set; }
            public override string ToString() => Content;
        }

        private async void DwingsSuggestButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null || _item == null)
                {
                    StatusText.Text = "Suggestion unavailable: service or item not ready.";
                    return;
                }

                var invoices = await _reconciliationService.GetDwingsInvoicesAsync();
                var suggestions = DwingsLinkingHelper.SuggestInvoicesForAmbre(
                    invoices,
                    _item.RawLabel,
                    _item.Reconciliation_Num,
                    _item.ReconciliationOrigin_Num,
                    _item.Receivable_InvoiceFromAmbre,
                    _item.GUARANTEE_ID,
                    _item.Value_Date,
                    _item.SignedAmount,
                    take: 50);

                var rows = suggestions.Select(i => new DwingsResult
                {
                    Type = "Invoice",
                    Id = i.INVOICE_ID,
                    Status = i.T_INVOICE_STATUS,
                    Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                    Currency = i.BILLING_CURRENCY,
                    BGPMT = i.BGPMT,
                    BusinessCase = i.BUSINESS_CASE_REFERENCE,
                    Description = $"Invoice {i.INVOICE_ID} (suggested)"
                }).ToList();

                DwingsResultsGrid.ItemsSource = new ObservableCollection<DwingsResult>(rows);
                StatusText.Text = rows.Count == 0 ? "No suggestion found." : $"DWINGS: {rows.Count} suggestion(s).";
                if (rows.Count > 0) DwingsResultsGrid.SelectedIndex = 0;
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Suggestion error: {ex.Message}";
            }
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


        public class DwingsResult
        {
            public string Type { get; set; } // "Invoice" or "Guarantee"
            public string Id { get; set; }   // INVOICE_ID or GUARANTEE_ID
            public string Description { get; set; }
            public string Status { get; set; }
            public string Amount { get; set; }
            public string Currency { get; set; }
            public string BGPMT { get; set; }
            public string BusinessCase { get; set; }
        }

        public ReconciliationDetailWindow(ReconciliationViewData item, IEnumerable<ReconciliationViewData> all)
            : this(
                  item,
                  all,
                  (App.ServiceProvider?.GetService(typeof(ReconciliationService))) as ReconciliationService,
                  (App.ServiceProvider?.GetService(typeof(OfflineFirstService))) as OfflineFirstService)
        {
        }

        public ReconciliationDetailWindow(
            ReconciliationViewData item,
            IEnumerable<ReconciliationViewData> all,
            ReconciliationService reconciliationService,
            OfflineFirstService offlineFirstService)
        {
            InitializeComponent();
            DataContext = this;
            _item = item;
            _all = all?.ToList() ?? new List<ReconciliationViewData>();
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService;

            PopulateHeader();
            PopulateTopDetails();
            PopulateReferentialOptions();
            _ = LoadReconciliationAsync();
            // Initial badge state (may be refined once reconciliation is loaded)
            UpdateLinkStatusBadge();
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
                    _item.DWINGS_BGPMT,
                    _item.GUARANTEE_ID,
                    _item.INVOICE_ID,
                    _item.COMMISSION_ID
                }
                .Where(s => !string.IsNullOrWhiteSpace(s));
                DwingsIdsValue.Text = string.Join(" | ", ids);
            }

            // DWINGS Guarantee extended fields (if present)
            if (GNatureValue != null) GNatureValue.Text = _item.G_NATUREOFDEAL ?? string.Empty;
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

                // Action status/date
                try
                {
                    if (ActionDoneCheckBox != null)
                        ActionDoneCheckBox.IsChecked = _reconciliation?.ActionStatus ?? _item?.ActionStatus;
                }
                catch { }
                try
                {
                    if (ActionDatePicker != null)
                        ActionDatePicker.SelectedDate = _reconciliation?.ActionDate ?? _item?.ActionDate;
                }
                catch { }

                // Initialize KPI selection from persisted reconciliation
                if (_reconciliation != null)
                {
                    SelectedKPIId = _reconciliation.KPI ?? SelectedKPIId;
                    SelectedActionId = _reconciliation.Action ?? SelectedActionId;
                    SelectedIncidentTypeId = _reconciliation.IncidentType ?? SelectedIncidentTypeId;
                    SelectedReasonId = _reconciliation.ReasonNonRisky ?? SelectedReasonId;
                }
                // Refresh badge once reconciliation is known
                UpdateLinkStatusBadge();

                // Show linked item if already linked; otherwise auto-search
                var linkedInvoiceId = _reconciliation?.DWINGS_InvoiceID ?? _item?.DWINGS_InvoiceID;
                var linkedGuaranteeId = _reconciliation?.DWINGS_GuaranteeID ?? _item?.DWINGS_GuaranteeID;
                // Hydrate extended UI fields from DWINGS when linked IDs exist but _item fields are empty
                await HydrateDwingsExtendedFieldsAsync(linkedInvoiceId, linkedGuaranteeId);
                if (!string.IsNullOrWhiteSpace(linkedInvoiceId) || !string.IsNullOrWhiteSpace(linkedGuaranteeId))
                {
                    _ = ShowLinkedInDwingsGridAsync(linkedInvoiceId, linkedGuaranteeId);
                }
                else if (!_autoSearched)
                {
                    _ = AutoSearchAsync();
                }
            }
            catch
            {
                // ignore load errors in UI
            }
        }

        private async System.Threading.Tasks.Task ShowLinkedInDwingsGridAsync(string invoiceId, string guaranteeId)
        {
            try
            {
                var rows = new List<DwingsResult>();
                if (!string.IsNullOrWhiteSpace(invoiceId))
                {
                    var invoices = await _reconciliationService.GetDwingsInvoicesAsync();
                    var i = invoices.FirstOrDefault(x => string.Equals(x.INVOICE_ID, invoiceId, StringComparison.OrdinalIgnoreCase));
                    if (i != null)
                    {
                        rows.Add(new DwingsResult
                        {
                            Type = "Invoice",
                            Id = i.INVOICE_ID,
                            Status = i.T_INVOICE_STATUS,
                            Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                            Currency = i.BILLING_CURRENCY,
                            BGPMT = i.BGPMT,
                            BusinessCase = i.BUSINESS_CASE_REFERENCE,
                            Description = $"Invoice {i.INVOICE_ID}"
                        });
                    }
                }
                if (!string.IsNullOrWhiteSpace(guaranteeId))
                {
                    var guarantees = await _reconciliationService.GetDwingsGuaranteesAsync();
                    var g = guarantees.FirstOrDefault(x => string.Equals(x.GUARANTEE_ID, guaranteeId, StringComparison.OrdinalIgnoreCase));
                    if (g != null)
                    {
                        rows.Add(new DwingsResult
                        {
                            Type = "Guarantee",
                            Id = g.GUARANTEE_ID,
                            Status = g.GUARANTEE_STATUS,
                            Amount = null,
                            Currency = null,
                            BGPMT = null,
                            BusinessCase = null,
                            Description = $"Guarantee {g.GUARANTEE_ID}"
                        });
                    }
                }

                DwingsResultsGrid.ItemsSource = new ObservableCollection<DwingsResult>(rows);
                if (rows.Count > 0)
                {
                    StatusText.Text = rows.Count == 1 ? "DWINGS: 1 linked item." : $"DWINGS: {rows.Count} linked items.";
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(invoiceId) || !string.IsNullOrWhiteSpace(guaranteeId))
                        StatusText.Text = "DWINGS: linked ID present but not found locally (check local DWINGS DB sync).";
                    else
                        StatusText.Text = "DWINGS: linked item not found.";
                }
                if (rows.Count > 0)
                {
                    DwingsResultsGrid.SelectedIndex = 0;
                }
            }
            catch (Exception ex)
            {
                StatusText.Text = $"DWINGS display error: {ex.Message}";
            }
        }

        private async System.Threading.Tasks.Task HydrateDwingsExtendedFieldsAsync(string invoiceId, string guaranteeId)
        {
            try
            {
                // If linked to an invoice and invoice extended fields are empty, hydrate from DWINGS
                bool needsInvoice = !string.IsNullOrWhiteSpace(invoiceId) && (
                    string.IsNullOrWhiteSpace(IRequestedAmountValue?.Text) &&
                    string.IsNullOrWhiteSpace(IBillingCurrencyValue?.Text) &&
                    string.IsNullOrWhiteSpace(IStatusValue?.Text)
                );
                if (needsInvoice)
                {
                    var invoices = await _reconciliationService.GetDwingsInvoicesAsync();
                    var i = invoices.FirstOrDefault(x => string.Equals(x.INVOICE_ID, invoiceId, StringComparison.OrdinalIgnoreCase));
                    if (i != null)
                    {
                        if (IRequestedAmountValue != null) IRequestedAmountValue.Text = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                        if (IBillingCurrencyValue != null) IBillingCurrencyValue.Text = i.BILLING_CURRENCY ?? string.Empty;
                        if (IStartDateValue != null) IStartDateValue.Text = i.START_DATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty;
                        if (IEndDateValue != null) IEndDateValue.Text = i.END_DATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty;
                        if (IStatusValue != null) IStatusValue.Text = i.T_INVOICE_STATUS ?? string.Empty;
                        if (IBusinessCaseRefValue != null) IBusinessCaseRefValue.Text = i.BUSINESS_CASE_REFERENCE ?? string.Empty;
                    }
                }

                // If linked to a guarantee and guarantee extended fields are empty, hydrate from DWINGS
                bool needsGuarantee = !string.IsNullOrWhiteSpace(guaranteeId) && (
                    string.IsNullOrWhiteSpace(GName1Value?.Text) &&
                    string.IsNullOrWhiteSpace(GStatusValue?.Text)
                );
                if (needsGuarantee)
                {
                    var guarantees = await _reconciliationService.GetDwingsGuaranteesAsync();
                    var g = guarantees.FirstOrDefault(x => string.Equals(x.GUARANTEE_ID, guaranteeId, StringComparison.OrdinalIgnoreCase));
                    if (g != null)
                    {
                        if (GStatusValue != null) GStatusValue.Text = g.GUARANTEE_STATUS ?? string.Empty;
                        if (GName1Value != null) GName1Value.Text = g.NAME1 ?? string.Empty;
                    }
                }
            }
            catch { /* ignore hydration issues */ }
        }

        private void UpdateLinkStatusBadge()
        {
            try
            {
                if (LinkStatusBadge == null || LinkStatusBadgeContainer == null) return;

                var linkedInvoiceId = _reconciliation?.DWINGS_InvoiceID ?? _item?.DWINGS_InvoiceID;
                var linkedGuaranteeId = _reconciliation?.DWINGS_GuaranteeID ?? _item?.DWINGS_GuaranteeID;
                bool linked = !string.IsNullOrWhiteSpace(linkedInvoiceId) || !string.IsNullOrWhiteSpace(linkedGuaranteeId);

                if (linked)
                {
                    LinkStatusBadge.Text = "LINKED";
                    LinkStatusBadge.Foreground = (SolidColorBrush)(new BrushConverter().ConvertFromString("#0F5132"));
                    LinkStatusBadgeContainer.Background = (SolidColorBrush)(new BrushConverter().ConvertFromString("#E8F8EE"));
                    if (!string.IsNullOrWhiteSpace(linkedInvoiceId) && !string.IsNullOrWhiteSpace(linkedGuaranteeId))
                        LinkStatusBadgeContainer.ToolTip = $"Linked to Invoice {linkedInvoiceId} and Guarantee {linkedGuaranteeId}";
                    else if (!string.IsNullOrWhiteSpace(linkedInvoiceId))
                        LinkStatusBadgeContainer.ToolTip = $"Linked to Invoice {linkedInvoiceId}";
                    else
                        LinkStatusBadgeContainer.ToolTip = $"Linked to Guarantee {linkedGuaranteeId}";
                }
                else
                {
                    LinkStatusBadge.Text = "NOT LINKED";
                    LinkStatusBadge.Foreground = (SolidColorBrush)(new BrushConverter().ConvertFromString("#B00020"));
                    LinkStatusBadgeContainer.Background = (SolidColorBrush)(new BrushConverter().ConvertFromString("#FCE8E6"));
                    LinkStatusBadgeContainer.ToolTip = "No DWINGS invoice linked";
                }
            }
            catch
            {
                // ignore badge errors
            }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            UpdateLinkStatusBadge();
        }

        private static bool IsMatched(ReconciliationViewData x)
        {
            return !string.IsNullOrWhiteSpace(x?.DWINGS_GuaranteeID)
                   || !string.IsNullOrWhiteSpace(x?.DWINGS_InvoiceID)
                   || !string.IsNullOrWhiteSpace(x?.DWINGS_BGPMT);
        }


        private async void DwingsSearchButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_offlineFirstService == null)
                {
                    StatusText.Text = "DWINGS search unavailable: service not ready.";
                    return;
                }

                var dwPath = _offlineFirstService.GetLocalDWDatabasePath();
                if (string.IsNullOrWhiteSpace(dwPath) || !System.IO.File.Exists(dwPath))
                {
                    StatusText.Text = "DWINGS DB not found locally.";
                    return;
                }

                string key = DwingsSearchTextBox?.Text?.Trim();

                // Auto-extract if empty (try all patterns)
                if (string.IsNullOrWhiteSpace(key) && _item != null)
                {
                    key = DwingsLinkingHelper.ExtractBgpmtToken(_item.RawLabel) 
                       ?? DwingsLinkingHelper.ExtractBgpmtToken(_item.Reconciliation_Num)
                       ?? DwingsLinkingHelper.ExtractBgiToken(_item.RawLabel)
                       ?? DwingsLinkingHelper.ExtractBgiToken(_item.Reconciliation_Num)
                       ?? DwingsLinkingHelper.ExtractGuaranteeId(_item.RawLabel)
                       ?? DwingsLinkingHelper.ExtractGuaranteeId(_item.Reconciliation_Num)
                       ?? _item.Reconciliation_Num; // Fallback to raw value for OfficialRef
                }

                if (string.IsNullOrWhiteSpace(key))
                {
                    StatusText.Text = "Enter a search key or place it in the label/reference.";
                    return;
                }

                // Search in ALL types automatically
                var results = await SearchDwingsAllTypesAsync(key);
                DwingsResultsGrid.ItemsSource = new ObservableCollection<DwingsResult>(results);
                StatusText.Text = $"DWINGS: {results.Count} result(s).";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"DWINGS search error: {ex.Message}";
            }
        }

        private async System.Threading.Tasks.Task<List<DwingsResult>> SearchDwingsAllTypesAsync(string key)
        {
            var results = new List<DwingsResult>();
            if (string.IsNullOrWhiteSpace(key)) return results;

            var invoices = await _reconciliationService.GetDwingsInvoicesAsync();
            var guarantees = await _reconciliationService.GetDwingsGuaranteesAsync();
            var keyAlnum = System.Text.RegularExpressions.Regex.Replace(key, @"[^A-Za-z0-9]", "");

            // 1. Search by BGI (INVOICE_ID) - exact match
            var byBgi = invoices.Where(i => string.Equals(i.INVOICE_ID, key, StringComparison.OrdinalIgnoreCase)).ToList();
            foreach (var i in byBgi.Take(50))
            {
                results.Add(new DwingsResult
                {
                    Type = "Invoice (BGI)",
                    Id = i.INVOICE_ID,
                    Status = i.T_INVOICE_STATUS,
                    Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                    Currency = i.BILLING_CURRENCY,
                    BGPMT = i.BGPMT,
                    BusinessCase = i.BUSINESS_CASE_ID ?? i.BUSINESS_CASE_REFERENCE,
                    Description = $"Matched by BGI: {i.INVOICE_ID}"
                });
            }

            // 2. Search by BGPMT - exact match
            var byBgpmt = invoices.Where(i => string.Equals(i.BGPMT, key, StringComparison.OrdinalIgnoreCase)).ToList();
            foreach (var i in byBgpmt.Take(50))
            {
                if (results.Any(r => r.Id == i.INVOICE_ID)) continue; // Skip duplicates
                results.Add(new DwingsResult
                {
                    Type = "Invoice (BGPMT)",
                    Id = i.INVOICE_ID,
                    Status = i.T_INVOICE_STATUS,
                    Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                    Currency = i.BILLING_CURRENCY,
                    BGPMT = i.BGPMT,
                    BusinessCase = i.BUSINESS_CASE_ID ?? i.BUSINESS_CASE_REFERENCE,
                    Description = $"Matched by BGPMT: {i.BGPMT}"
                });
            }

            // 3. Search by Guarantee ID
            var byGuaranteeId = guarantees.Where(g => string.Equals(g.GUARANTEE_ID, key, StringComparison.OrdinalIgnoreCase)).ToList();
            foreach (var g in byGuaranteeId.Take(10))
            {
                // Add the guarantee itself as a result (to allow linking by guarantee)
                results.Add(new DwingsResult
                {
                    Type = "Guarantee",
                    Id = g.GUARANTEE_ID,
                    Status = g.GUARANTEE_STATUS,
                    Amount = g.OUTSTANDING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                    Currency = g.CURRENCYNAME,
                    BGPMT = null,
                    BusinessCase = g.GUARANTEE_ID,
                    Description = $"Guarantee: {g.GUARANTEE_ID}"
                });

                // Find invoices linked to this guarantee
                var related = invoices.Where(i => 
                    string.Equals(i.BUSINESS_CASE_ID, g.GUARANTEE_ID, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(i.BUSINESS_CASE_REFERENCE, g.GUARANTEE_ID, StringComparison.OrdinalIgnoreCase)
                ).ToList();

                foreach (var i in related.Take(20))
                {
                    if (results.Any(r => r.Id == i.INVOICE_ID)) continue;
                    results.Add(new DwingsResult
                    {
                        Type = "Invoice (Guarantee)",
                        Id = i.INVOICE_ID,
                        Status = i.T_INVOICE_STATUS,
                        Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                        Currency = i.BILLING_CURRENCY,
                        BGPMT = i.BGPMT,
                        BusinessCase = g.GUARANTEE_ID,
                        Description = $"Linked to Guarantee: {g.GUARANTEE_ID}"
                    });
                }
            }

            // 4. Search by OFFICIALREF (alphanumeric comparison)
            if (!string.IsNullOrWhiteSpace(keyAlnum) && keyAlnum.Length >= 3)
            {
                var byOfficialRef = guarantees.Where(g =>
                {
                    var officialRefAlnum = string.IsNullOrWhiteSpace(g.OFFICIALREF) ? null : System.Text.RegularExpressions.Regex.Replace(g.OFFICIALREF, @"[^A-Za-z0-9]", "");
                    return !string.IsNullOrWhiteSpace(officialRefAlnum) && officialRefAlnum.Equals(keyAlnum, StringComparison.OrdinalIgnoreCase);
                }).ToList();

                foreach (var g in byOfficialRef.Take(10))
                {
                    // Add the guarantee itself as a result (to allow linking by guarantee)
                    if (!results.Any(r => r.Id == g.GUARANTEE_ID))
                    {
                        results.Add(new DwingsResult
                        {
                            Type = "Guarantee (OfficialRef)",
                            Id = g.GUARANTEE_ID,
                            Status = g.GUARANTEE_STATUS,
                            Amount = g.OUTSTANDING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                            Currency = g.CURRENCYNAME,
                            BGPMT = null,
                            BusinessCase = g.GUARANTEE_ID,
                            Description = $"Guarantee via OfficialRef: {g.OFFICIALREF}"
                        });
                    }

                    var related = invoices.Where(i =>
                        string.Equals(i.BUSINESS_CASE_ID, g.GUARANTEE_ID, StringComparison.OrdinalIgnoreCase) ||
                        string.Equals(i.BUSINESS_CASE_REFERENCE, g.GUARANTEE_ID, StringComparison.OrdinalIgnoreCase)
                    ).ToList();

                    foreach (var i in related.Take(20))
                    {
                        if (results.Any(r => r.Id == i.INVOICE_ID)) continue;
                        results.Add(new DwingsResult
                        {
                            Type = "Invoice (OfficialRef)",
                            Id = i.INVOICE_ID,
                            Status = i.T_INVOICE_STATUS,
                            Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                            Currency = i.BILLING_CURRENCY,
                            BGPMT = i.BGPMT,
                            BusinessCase = g.GUARANTEE_ID,
                            Description = $"Matched by OfficialRef: {g.OFFICIALREF} → Guarantee: {g.GUARANTEE_ID}"
                        });
                    }
                }
            }

            // 5. Search by SENDER_REFERENCE (alphanumeric comparison)
            if (!string.IsNullOrWhiteSpace(keyAlnum) && keyAlnum.Length >= 3)
            {
                var bySenderRef = invoices.Where(i =>
                {
                    var senderRefAlnum = string.IsNullOrWhiteSpace(i.SENDER_REFERENCE) ? null : System.Text.RegularExpressions.Regex.Replace(i.SENDER_REFERENCE, @"[^A-Za-z0-9]", "");
                    return !string.IsNullOrWhiteSpace(senderRefAlnum) && senderRefAlnum.Equals(keyAlnum, StringComparison.OrdinalIgnoreCase);
                }).ToList();

                foreach (var i in bySenderRef.Take(50))
                {
                    if (results.Any(r => r.Id == i.INVOICE_ID)) continue;
                    results.Add(new DwingsResult
                    {
                        Type = "Invoice (SenderRef)",
                        Id = i.INVOICE_ID,
                        Status = i.T_INVOICE_STATUS,
                        Amount = i.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture),
                        Currency = i.BILLING_CURRENCY,
                        BGPMT = i.BGPMT,
                        BusinessCase = i.BUSINESS_CASE_ID ?? i.BUSINESS_CASE_REFERENCE,
                        Description = $"Matched by SenderRef: {i.SENDER_REFERENCE}"
                    });
                }
            }

            return results.Take(200).ToList();
        }


        private void SetDiagnostics(string text, bool expand = false)
        {
            try
            {
                if (DiagnosticsText != null)
                    DiagnosticsText.Text = text ?? string.Empty;
                if (expand && DiagnosticsExpander != null)
                    DiagnosticsExpander.IsExpanded = true;
            }
            catch { /* ignore diagnostics UI errors */ }
        }

        private async System.Threading.Tasks.Task AutoSearchAsync(bool force = false)
        {
            if (_autoSearched && !force) return;
            _autoSearched = true;

            try
            {
                // Accumulate candidates from multiple sources
                var candidates = new List<(string Type, string Key)>();

                // From label / reconciliation / payment refs
                var fromLabel = _item?.RawLabel;
                var fromRecoNum = _item?.Reconciliation_Num;
                var fromPayRef = _item?.PaymentReference ?? _reconciliation?.PaymentReference;
                var fromAmbreRef = _item?.Receivable_DWRefFromAmbre;

                // BGPMT
                string tryBgpmt(string s) => DwingsLinkingHelper.ExtractBgpmtToken(s);
                var bgpmt = tryBgpmt(fromLabel) ?? tryBgpmt(fromRecoNum) ?? tryBgpmt(fromPayRef) ?? tryBgpmt(fromAmbreRef);
                if (!string.IsNullOrWhiteSpace(bgpmt)) candidates.Add(("BGPMT", bgpmt));

                // BGI (invoice id)
                string tryBgi(string s) => DwingsLinkingHelper.ExtractBgiToken(s);
                var bgi = tryBgi(fromLabel) ?? tryBgi(fromRecoNum) ?? tryBgi(fromAmbreRef) ?? _item?.INVOICE_ID;
                if (!string.IsNullOrWhiteSpace(bgi)) candidates.Add(("BGI", bgi));

                // Guarantee ID
                string tryGid(string s) => DwingsLinkingHelper.ExtractGuaranteeId(s);
                var gid = tryGid(fromLabel) ?? tryGid(fromRecoNum) ?? tryGid(fromAmbreRef) ?? _item?.GUARANTEE_ID;
                if (!string.IsNullOrWhiteSpace(gid)) candidates.Add(("Guarantee ID", gid));
                // Also try raw Ambre ref directly as a Guarantee ID
                if (!string.IsNullOrWhiteSpace(fromAmbreRef)) candidates.Add(("Guarantee ID", fromAmbreRef));

                // Business Case Ref
                var bcRef = _item?.I_BUSINESS_CASE_REFERENCE;
                if (!string.IsNullOrWhiteSpace(bcRef)) candidates.Add(("Business Case Ref", bcRef));
                if (!string.IsNullOrWhiteSpace(fromAmbreRef)) candidates.Add(("Business Case Ref", fromAmbreRef));
                // Business Case ID (try with Ambre ref as well, to capture either field)
                if (!string.IsNullOrWhiteSpace(fromAmbreRef)) candidates.Add(("Business Case ID", fromAmbreRef));

                // Prepare diagnostics
                var sb = new StringBuilder();
                sb.AppendLine("[Auto Search Diagnostics]");
                try
                {
                    if (_reconciliationService != null)
                    {
                        var invCount = (await _reconciliationService.GetDwingsInvoicesAsync())?.Count ?? 0;
                        var gCount = (await _reconciliationService.GetDwingsGuaranteesAsync())?.Count ?? 0;
                        sb.AppendLine($"Datasets: invoices={invCount}, guarantees={gCount}");
                    }
                }
                catch { /* ignore dataset count issues */ }

                if (candidates.Count == 0)
                {
                    sb.AppendLine("No candidate keys extracted.");
                }
                else
                {
                    sb.AppendLine("Candidate keys:");
                    foreach (var c in candidates)
                    {
                        sb.AppendLine($"- {c.Type}: {c.Key}");
                    }
                }

                // Aggregate results from all candidates using unified search
                var map = new Dictionary<string, DwingsResult>(StringComparer.OrdinalIgnoreCase);

                foreach (var (Type, Key) in candidates)
                {
                    var results = await SearchDwingsAllTypesAsync(Key);
                    sb.AppendLine($"Results for {Type} '{Key}': {results.Count}");
                    foreach (var r in results)
                    {
                        var k = r.Type + "|" + r.Id;
                        if (!map.ContainsKey(k)) map[k] = r;
                    }
                }

                // Enrich with related guarantees from invoice results
                if (map.Count > 0)
                {
                    var invoices = map.Values.Where(v => string.Equals(v.Type, "Invoice", StringComparison.OrdinalIgnoreCase)).ToList();
                    if (invoices.Count > 0)
                    {
                        var guarantees = await _reconciliationService.GetDwingsGuaranteesAsync();
                        var byId = guarantees.ToDictionary(g => g.GUARANTEE_ID, StringComparer.OrdinalIgnoreCase);
                        foreach (var inv in invoices)
                        {
                            if (!string.IsNullOrWhiteSpace(inv.BusinessCase) && byId.TryGetValue(inv.BusinessCase, out var g))
                            {
                                var key = "Guarantee|" + g.GUARANTEE_ID;
                                if (!map.ContainsKey(key))
                                {
                                    map[key] = new DwingsResult
                                    {
                                        Type = "Guarantee",
                                        Id = g.GUARANTEE_ID,
                                        Status = g.GUARANTEE_STATUS,
                                        Description = $"Guarantee {g.GUARANTEE_ID}",
                                        BusinessCase = null,
                                    };
                                }
                            }
                        }
                    }
                }

                var aggregate = map.Values.ToList();
                DwingsResultsGrid.ItemsSource = new ObservableCollection<DwingsResult>(aggregate);
                StatusText.Text = $"DWINGS: {aggregate.Count} result(s).";
                sb.AppendLine($"Final aggregate: {aggregate.Count} unique result(s).");
                SetDiagnostics(sb.ToString(), expand: aggregate.Count == 0);
            }
            catch (Exception ex)
            {
                StatusText.Text = $"DWINGS auto-search error: {ex.Message}";
                SetDiagnostics($"Auto-search exception: {ex}", expand: true);
            }
        }

        private async void ReRunAutoSearch_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Force a new run regardless of the _autoSearched flag
                await AutoSearchAsync(force: true);
            }
            catch (Exception ex)
            {
                SetDiagnostics($"Re-run auto-search error: {ex.Message}", expand: true);
            }
        }

        private async void LinkDwingsItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_item == null || _reconciliationService == null) return;
                var dr = DwingsResultsGrid?.SelectedItem as DwingsResult;
                if (dr == null) return;

                var reco = _reconciliation ?? await _reconciliationService.GetOrCreateReconciliationAsync(_item.ID);
                reco.ID = _item.ID;

                // Link invoice: take ALL available fields (BGI, BGPMT, GuaranteeID)
                if (dr.Type?.Contains("Invoice") == true)
                {
                    // Always set BGI (INVOICE_ID)
                    if (!string.IsNullOrWhiteSpace(dr.Id))
                    {
                        reco.DWINGS_InvoiceID = dr.Id;
                        _item.DWINGS_InvoiceID = dr.Id;
                    }

                    // Set BGPMT if available
                    if (!string.IsNullOrWhiteSpace(dr.BGPMT))
                    {
                        reco.DWINGS_BGPMT = dr.BGPMT;
                        reco.PaymentReference = dr.BGPMT;
                        _item.DWINGS_BGPMT = dr.BGPMT;
                        _item.PaymentReference = dr.BGPMT;
                    }

                    // Set GuaranteeID (from BusinessCase) if available
                    if (!string.IsNullOrWhiteSpace(dr.BusinessCase))
                    {
                        reco.DWINGS_GuaranteeID = dr.BusinessCase;
                        _item.DWINGS_GuaranteeID = dr.BusinessCase;
                    }
                }
                else if (string.Equals(dr.Type, "Guarantee", StringComparison.OrdinalIgnoreCase))
                {
                    reco.DWINGS_GuaranteeID = dr.Id;
                    _item.DWINGS_GuaranteeID = reco.DWINGS_GuaranteeID;
                }

                await _reconciliationService.SaveReconciliationAsync(reco);
                _reconciliation = reco;
                StatusText.Text = "Linked and saved.";
                UpdateLinkStatusBadge();
                
                // Signal that data was modified (will trigger refresh in ReconciliationView)
                this.DialogResult = true;
                
                // Update cross-account "G" flags in the in-memory list immediately for this invoice
                try { UpdateGFlagsAfterLink(reco.DWINGS_InvoiceID); } catch { }
                await ShowLinkedInDwingsGridAsync(reco.DWINGS_InvoiceID, reco.DWINGS_GuaranteeID);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Link failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        // Recompute IsMatchedAcrossAccounts in the current view model after a link operation
        private void UpdateGFlagsAfterLink(string invoiceId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(invoiceId) || _all == null) return;

                var key = invoiceId.Trim();
                var cc = _offlineFirstService?.CurrentCountry;
                var pivotId = cc?.CNT_AmbrePivot?.Trim();
                var recvId = cc?.CNT_AmbreReceivable?.Trim();

                var group = _all.Where(r => string.Equals(r?.DWINGS_InvoiceID?.Trim(), key, StringComparison.OrdinalIgnoreCase)).ToList();
                if (group.Count == 0) return;

                // Ensure AccountSide is set consistently
                foreach (var row in group)
                {
                    try
                    {
                        var acc = row?.Account_ID?.Trim();
                        if (!string.IsNullOrWhiteSpace(pivotId) && string.Equals(acc, pivotId, StringComparison.OrdinalIgnoreCase))
                            row.AccountSide = "P";
                        else if (!string.IsNullOrWhiteSpace(recvId) && string.Equals(acc, recvId, StringComparison.OrdinalIgnoreCase))
                            row.AccountSide = "R";
                    }
                    catch { }
                }

                bool hasP = group.Any(x => string.Equals(x?.AccountSide, "P", StringComparison.OrdinalIgnoreCase));
                bool hasR = group.Any(x => string.Equals(x?.AccountSide, "R", StringComparison.OrdinalIgnoreCase));
                foreach (var row in group)
                {
                    try { row.IsMatchedAcrossAccounts = hasP && hasR; } catch { }
                }
            }
            catch { }
        }

        // Local regex extractors removed in favor of centralized RecoTool.Helpers.DwingsLinkingHelper

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

                // Defaults: when an Action is set but no status/date provided, set PENDING with today's date
                if (SelectedActionId.HasValue)
                {
                    if (ActionDoneCheckBox != null && !ActionDoneCheckBox.IsChecked.HasValue)
                        ActionDoneCheckBox.IsChecked = false;
                    if (ActionDatePicker != null && !ActionDatePicker.SelectedDate.HasValue)
                        ActionDatePicker.SelectedDate = DateTime.Today;
                }

                // Persist
                if (ActionDoneCheckBox != null)
                    reco.ActionStatus = ActionDoneCheckBox.IsChecked;
                if (ActionDatePicker != null)
                    reco.ActionDate = ActionDatePicker.SelectedDate;
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

        private async void UnlinkButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_item == null || _reconciliationService == null) return;

                var result = MessageBox.Show(
                    "Remove all DWINGS links (BGI, BGPMT, GuaranteeID)?\n\nThis will clear:\n• DWINGS_InvoiceID (BGI)\n• DWINGS_BGPMT\n• DWINGS_GuaranteeID\n\nPaymentReference will be kept.\n\nContinue?",
                    "Confirm Unlink",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Warning);

                if (result != MessageBoxResult.Yes) return;

                var reco = _reconciliation ?? await _reconciliationService.GetOrCreateReconciliationAsync(_item.ID);
                reco.ID = _item.ID;

                // Clear all DWINGS links (but keep PaymentReference)
                reco.DWINGS_InvoiceID = null;
                reco.DWINGS_BGPMT = null;
                reco.DWINGS_GuaranteeID = null;

                // Update view model
                _item.DWINGS_InvoiceID = null;
                _item.DWINGS_BGPMT = null;
                _item.DWINGS_GuaranteeID = null;

                await _reconciliationService.SaveReconciliationAsync(reco);
                _reconciliation = reco;
                
                // Signal that data was modified (will trigger refresh in ReconciliationView)
                this.DialogResult = true;
                
                // Close the window to force refresh
                this.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Unlink failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }
}
