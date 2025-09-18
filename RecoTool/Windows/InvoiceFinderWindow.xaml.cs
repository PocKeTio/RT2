using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using RecoTool.Services;
using RecoTool.Services.DTOs;
using RecoTool.Helpers;

namespace RecoTool.Windows
{
    public partial class InvoiceFinderWindow : Window
    {
        private ReconciliationService _reconciliationService;
        private OfflineFirstService _offlineFirstService;

        private List<DwingsInvoiceDto> _invoices = new List<DwingsInvoiceDto>();
        private List<DwingsGuaranteeDto> _guarantees = new List<DwingsGuaranteeDto>();
        private ObservableCollection<dynamic> _viewRows = new ObservableCollection<dynamic>();
        private DispatcherTimer _debounceTimer;

        public InvoiceFinderWindow()
        {
            InitializeComponent();
            try { _reconciliationService = (App.ServiceProvider?.GetService(typeof(ReconciliationService))) as ReconciliationService; } catch { }
            try { _offlineFirstService = (App.ServiceProvider?.GetService(typeof(OfflineFirstService))) as OfflineFirstService; } catch { }
            // Initialize debounce timer (1.5 seconds)
            _debounceTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(1500) };
            _debounceTimer.Tick += (s, e) =>
            {
                try { _debounceTimer.Stop(); ApplyFilters(); } catch { }
            };

            Loaded += async (s, e) =>
            {
                try
                {
                    if (_reconciliationService == null)
                    {
                        MessageBox.Show("Service not available.", "Invoice Finder", MessageBoxButton.OK, MessageBoxImage.Warning);
                        return;
                    }
                    _invoices = (await _reconciliationService.GetDwingsInvoicesAsync().ConfigureAwait(true))?.ToList() ?? new List<DwingsInvoiceDto>();
                    _guarantees = (await _reconciliationService.GetDwingsGuaranteesAsync().ConfigureAwait(true))?.ToList() ?? new List<DwingsGuaranteeDto>();
                    ResultsGrid.ItemsSource = _viewRows;
                    PopulateFilterCombos();
                    DebouncedApplyFiltersStart();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Error loading DWINGS: {ex.Message}", "Invoice Finder", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            };
        }

        private void DebouncedApplyFiltersStart()
        {
            try
            {
                _debounceTimer.Stop();
                if (FilterSpinner != null) FilterSpinner.Visibility = Visibility.Visible;
                _debounceTimer.Start();
            }
            catch { }
        }

        private void PopulateFilterCombos()
        {
            try
            {
                // PAYMENT_METHOD
                var methods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var m in _invoices.Select(i => i.PAYMENT_METHOD).Where(s => !string.IsNullOrWhiteSpace(s))) methods.Add(m.Trim());
                // Fallbacks
                string[] fallbackMethods = new[] { "DIRECT_DEBIT", "EXTERNAL_DEBIT_PAYMENT", "INCOMING_PAYMENT", "MANUAL_OUTGOING", "OUTGOING_PAYMENT" };
                foreach (var m in fallbackMethods) methods.Add(m);
                var methodList = methods.OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToList();
                methodList.Insert(0, string.Empty);
                if (PaymentMethodCombo != null)
                {
                    PaymentMethodCombo.ItemsSource = methodList;
                    PaymentMethodCombo.SelectedIndex = 0;
                }

                // T_PAYMENT_REQUEST_STATUS
                var statuses = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var s in _invoices.Select(i => i.T_PAYMENT_REQUEST_STATUS).Where(x => !string.IsNullOrWhiteSpace(x))) statuses.Add(s.Trim());
                string[] fallbackStatuses = new[] { "CANCELLED", "FULLY_EXECUTED", "INITIATED", "REJECTED", "REQUEST_FAILED", "REQUESTED" };
                foreach (var s in fallbackStatuses) statuses.Add(s);
                var statusList = statuses.OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToList();
                statusList.Insert(0, string.Empty);
                if (PaymentRequestStatusCombo != null)
                {
                    PaymentRequestStatusCombo.ItemsSource = statusList;
                    PaymentRequestStatusCombo.SelectedIndex = 0;
                }
            }
            catch { }
        }

        private static bool ContainsCI(string hay, string needle)
        {
            if (string.IsNullOrWhiteSpace(needle)) return true;
            return !string.IsNullOrWhiteSpace(hay) && hay.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool EqualsCI(string a, string b)
        {
            if (string.IsNullOrWhiteSpace(b)) return true;
            return string.Equals(a ?? string.Empty, b, StringComparison.OrdinalIgnoreCase);
        }

        private static decimal? ParseDecimal(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d;
            if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out d)) return d;
            return null;
        }

        private void ApplyFilters()
        {
            try
            {
                var inv = _invoices.AsEnumerable();

                // Invoice-side filters
                var invoiceId = InvoiceIdText?.Text?.Trim();
                var senderName = SenderNameText?.Text?.Trim();
                var receiverName = ReceiverNameText?.Text?.Trim();
                var senderRef = SenderRefText?.Text?.Trim();
                var receiverRef = ReceiverRefText?.Text?.Trim();
                var status = (InvoiceStatusCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
                var businessCase = BusinessCaseText?.Text?.Trim();
                var payMethod = PaymentMethodCombo?.SelectedItem as string;
                var payReqStatus = PaymentRequestStatusCombo?.SelectedItem as string;
                var bgpmt = BgpmtText?.Text?.Trim();
                var reqExecDate = RequestedExecDatePicker?.SelectedDate;
                var reqAmtText = RequestedAmountMinText?.Text?.Trim();
                var reqAmtVal = ParseDecimal(reqAmtText);
                var reqAmtHasDecimal = !string.IsNullOrWhiteSpace(reqAmtText) && (reqAmtText.Contains(".") || reqAmtText.Contains(","));
                var sndAccNum = SenderAccountNumberText?.Text?.Trim();
                var sndAccBic = SenderAccountBicText?.Text?.Trim();
                var startFrom = StartDateFromPicker?.SelectedDate;
                var endTo = EndDateToPicker?.SelectedDate;

                inv = inv.Where(i =>
                    ContainsCI(i.INVOICE_ID, invoiceId)
                    && ContainsCI(i.SENDER_NAME, senderName)
                    && ContainsCI(i.RECEIVER_NAME, receiverName)
                    && ContainsCI(i.SENDER_REFERENCE, senderRef)
                    && ContainsCI(i.RECEIVER_REFERENCE, receiverRef)
                    && (string.IsNullOrWhiteSpace(status) || string.Equals(i.T_INVOICE_STATUS ?? string.Empty, status, StringComparison.OrdinalIgnoreCase))
                    && (string.IsNullOrWhiteSpace(businessCase) || ContainsCI(i.BUSINESS_CASE_REFERENCE, businessCase) || ContainsCI(i.BUSINESS_CASE_ID, businessCase))
                    && (string.IsNullOrWhiteSpace(payMethod) || string.Equals(i.PAYMENT_METHOD ?? string.Empty, payMethod, StringComparison.OrdinalIgnoreCase))
                    && (string.IsNullOrWhiteSpace(payReqStatus) || string.Equals(i.T_PAYMENT_REQUEST_STATUS ?? string.Empty, payReqStatus, StringComparison.OrdinalIgnoreCase))
                    && ContainsCI(i.BGPMT, bgpmt)
                    && (!reqExecDate.HasValue || (i.REQUESTED_EXECUTION_DATE.HasValue && i.REQUESTED_EXECUTION_DATE.Value.Date == reqExecDate.Value.Date))
                    && (string.IsNullOrWhiteSpace(reqAmtText)
                        || (i.REQUESTED_AMOUNT.HasValue && reqAmtVal.HasValue
                            && (
                                (reqAmtHasDecimal && i.REQUESTED_AMOUNT.Value == reqAmtVal.Value)
                                || (!reqAmtHasDecimal && i.REQUESTED_AMOUNT.Value >= reqAmtVal.Value && i.REQUESTED_AMOUNT.Value < reqAmtVal.Value + 1)
                               )
                           )
                       )
                    && ContainsCI(i.SENDER_ACCOUNT_NUMBER, sndAccNum)
                    && ContainsCI(i.SENDER_ACCOUNT_BIC, sndAccBic)
                    && (!startFrom.HasValue || (i.START_DATE.HasValue && i.START_DATE.Value.Date >= startFrom.Value.Date))
                    && (!endTo.HasValue || (i.END_DATE.HasValue && i.END_DATE.Value.Date <= endTo.Value.Date))
                );

                // Guarantee-side filters
                var gIdOrLegacy = GuaranteeIdOrLegacyText?.Text?.Trim();
                var gType = GuaranteeTypeText?.Text?.Trim();
                var gStatus = GuaranteeStatusText?.Text?.Trim();
                var gName1 = GuaranteeName1Text?.Text?.Trim();
                var gNature = NatureOfDealText?.Text?.Trim();

                var gById = _guarantees.Where(g => !string.IsNullOrWhiteSpace(g.GUARANTEE_ID))
                                       .ToDictionary(g => g.GUARANTEE_ID, g => g, StringComparer.OrdinalIgnoreCase);

                var filtered = new List<dynamic>();
                foreach (var i in inv)
                {
                    DwingsGuaranteeDto g = null;
                    if (!string.IsNullOrWhiteSpace(i.BUSINESS_CASE_ID) && gById.TryGetValue(i.BUSINESS_CASE_ID, out var byId))
                        g = byId;
                    else if (!string.IsNullOrWhiteSpace(i.BUSINESS_CASE_REFERENCE) && gById.TryGetValue(i.BUSINESS_CASE_REFERENCE, out var byRef))
                        g = byRef;

                    // Apply guarantee-side filters if any provided
                    bool anyGuaranteeFilter = !string.IsNullOrWhiteSpace(gIdOrLegacy) || !string.IsNullOrWhiteSpace(gType) || !string.IsNullOrWhiteSpace(gStatus) || !string.IsNullOrWhiteSpace(gName1) || !string.IsNullOrWhiteSpace(gNature);
                    if (anyGuaranteeFilter)
                    {
                        if (g == null) continue; // require a match
                        if (!(ContainsCI(g.GUARANTEE_ID, gIdOrLegacy) || ContainsCI(g.LEGACYREF, gIdOrLegacy))) continue;
                        if (!ContainsCI(g.GUARANTEE_TYPE, gType)) continue;
                        if (!ContainsCI(g.GUARANTEE_STATUS, gStatus)) continue;
                        if (!ContainsCI(g.NAME1, gName1)) continue;
                        if (!ContainsCI(g.NATUREOFDEAL, gNature)) continue;
                    }

                    filtered.Add(new
                    {
                        // Invoice
                        i.INVOICE_ID,
                        i.BUSINESS_CASE_ID,
                        i.BUSINESS_CASE_REFERENCE,
                        i.T_INVOICE_STATUS,
                        i.PAYMENT_METHOD,
                        i.T_PAYMENT_REQUEST_STATUS,
                        i.BGPMT,
                        i.REQUESTED_EXECUTION_DATE,
                        i.REQUESTED_AMOUNT,
                        i.BILLING_AMOUNT,
                        i.BILLING_CURRENCY,
                        i.START_DATE,
                        i.END_DATE,
                        i.SENDER_NAME,
                        i.RECEIVER_NAME,
                        i.SENDER_REFERENCE,
                        i.RECEIVER_REFERENCE,
                        i.SENDER_ACCOUNT_NUMBER,
                        i.SENDER_ACCOUNT_BIC,
                        // Guarantee (may be null)
                        GUARANTEE_ID = g?.GUARANTEE_ID,
                        LEGACYREF = g?.LEGACYREF,
                        GUARANTEE_TYPE = g?.GUARANTEE_TYPE,
                        GUARANTEE_STATUS = g?.GUARANTEE_STATUS,
                        NAME1 = g?.NAME1,
                        NATUREOFDEAL = g?.NATUREOFDEAL
                    });
                }

                _viewRows.Clear();
                foreach (var r in filtered.Take(1000)) _viewRows.Add(r);
            }
            catch { }
            finally
            {
                try { if (FilterSpinner != null) FilterSpinner.Visibility = Visibility.Collapsed; } catch { }
            }
        }

        private void OnFilterChanged(object sender, RoutedEventArgs e)
        {
            DebouncedApplyFiltersStart();
        }

        private void ResultsGrid_ContextMenuOpening(object sender, ContextMenuEventArgs e)
        {
            try
            {
                var row = ResultsGrid.SelectedItem;
                var cm = (sender as DataGrid)?.ContextMenu ?? ResultsGrid.ContextMenu;
                if (cm == null) return;
                var mi = cm.Items.OfType<MenuItem>().FirstOrDefault();
                if (mi != null)
                {
                    // Enable/disable depending on a selected result row
                    mi.IsEnabled = row != null;

                    // Personalize the header with the target Reconciliation Number from the last-focused view
                    string header = "Link to ...";
                    try
                    {
                        var targetView = ReconciliationViewFocusTracker.GetLastFocused();
                        var dgReco = targetView?.FindName("ResultsDataGrid") as DataGrid;
                        var rowData = dgReco?.SelectedItem as RecoTool.Services.DTOs.ReconciliationViewData;
                        var recoNum = rowData?.Reconciliation_Num;
                        if (!string.IsNullOrWhiteSpace(recoNum))
                        {
                            header = $"Link to {recoNum}";
                        }
                        else if (rowData != null && !string.IsNullOrWhiteSpace(rowData.ID))
                        {
                            // Fallback: show internal row ID if Reconciliation_Num is missing
                            header = $"Link to {rowData.ID}";
                        }
                    }
                    catch { }
                    mi.Header = header;
                }
            }
            catch { }
        }

        private async void LinkMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                dynamic sel = ResultsGrid.SelectedItem;
                if (sel == null) return;
                var targetView = ReconciliationViewFocusTracker.GetLastFocused();
                if (targetView == null)
                {
                    MessageBox.Show("No Reconciliation view focused.", "Invoice Finder", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                // Highlight proposal
                try { targetView.FlashLinkProposalHighlight(); } catch { }

                // Find selected row in target view
                var dg = targetView.FindName("ResultsDataGrid") as DataGrid;
                var rowData = dg?.SelectedItem as Services.DTOs.ReconciliationViewData;
                if (rowData == null)
                {
                    MessageBox.Show("Select a row in the Reconciliation view to link.", "Invoice Finder", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                // Persist link
                var reco = await _reconciliationService.GetOrCreateReconciliationAsync(rowData.ID);
                reco.DWINGS_InvoiceID = sel.INVOICE_ID as string;
                if (sel.GUARANTEE_ID is string gid && !string.IsNullOrWhiteSpace(gid)) reco.DWINGS_GuaranteeID = gid;
                await _reconciliationService.SaveReconciliationAsync(reco);

                // Reflect in UI row and refresh view
                try
                {
                    rowData.DWINGS_InvoiceID = sel.INVOICE_ID as string;
                    if (sel.GUARANTEE_ID is string gid2 && !string.IsNullOrWhiteSpace(gid2)) rowData.DWINGS_GuaranteeID = gid2;
                }
                catch { }

                try { targetView.Refresh(); } catch { }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Link error: {ex.Message}", "Invoice Finder", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }
}
