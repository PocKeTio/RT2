using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    public partial class DwingsButtonsWindow : Window
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly ReconciliationService _reconciliationService;
        private readonly string _countryId;
        private List<DwingsTriggerItem> _items = new List<DwingsTriggerItem>();

        public DwingsButtonsWindow(OfflineFirstService offlineFirstService, ReconciliationService reconciliationService, string countryId)
        {
            InitializeComponent();
            _offlineFirstService = offlineFirstService;
            _reconciliationService = reconciliationService;
            _countryId = countryId;

            this.Loaded += async (_, __) => await LoadDataAsync();
        }

        private async Task LoadDataAsync()
        {
            try
            {
                GridReconciliations.ItemsSource = null;
                
                // Get all reconciliation view data with TRIGGER action
                var viewData = await _reconciliationService.GetReconciliationViewAsync(_countryId, null, false);
                if (viewData == null) viewData = new List<ReconciliationViewData>();

                var country = _offlineFirstService?.CurrentCountry;
                var receivableId = country?.CNT_AmbreReceivable;

                // Filter: Receivable side + Action = Trigger (all, grouped or not)
                var filtered = viewData
                    .Where(r => r.Account_ID == receivableId 
                             && r.Action == (int)ActionType.Trigger
                             && !r.IsDeleted)
                    .ToList();

                // Group by BGPMT: sum amounts, take first of other fields
                var grouped = filtered
                    .GroupBy(r => r.DWINGS_BGPMT ?? "")
                    .Select(g => new
                    {
                        BGPMT = g.Key,
                        Items = g.ToList(),
                        First = g.First(),
                        TotalAmount = g.Sum(x => x.SignedAmount),
                        IsGrouped = g.Any(x => x.IsMatchedAcrossAccounts),
                        AllIDs = string.Join(",", g.Select(x => x.ID))
                    })
                    .ToList();

                // Map to display DTO (one row per BGPMT)
                _items = grouped.Select(g => new DwingsTriggerItem
                {
                    ID = g.AllIDs,  // Store all IDs for batch update
                    DWINGS_GuaranteeID = g.First.DWINGS_GuaranteeID,
                    DWINGS_InvoiceID = g.First.DWINGS_InvoiceID,
                    DWINGS_BGPMT = g.BGPMT,
                    Amount = g.TotalAmount,
                    Currency = g.First.CCY,
                    Comments = g.First.Comments,
                    PaymentReference = g.First.PaymentReference,
                    IsGrouped = g.IsGrouped,
                    ValueDate = g.First.Value_Date,
                    LineCount = g.Items.Count
                }).ToList();

                GridReconciliations.ItemsSource = _items;
                Progress.Minimum = 0;
                Progress.Maximum = _items.Count == 0 ? 1 : _items.Count;
                Progress.Value = 0;
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, $"Error during loading: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async void BulkButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var list = _items?.ToList() ?? new List<DwingsTriggerItem>();
                if (list.Count == 0)
                {
                    MessageBox.Show(this, "No rows to process.", "Information", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                // Validate: all rows must have PaymentReference (either from grouping or manual)
                var missingRef = list.Where(r => string.IsNullOrWhiteSpace(r.PaymentReference)).ToList();
                if (missingRef.Any())
                {
                    MessageBox.Show(this, $"{missingRef.Count} row(s) are missing Payment Reference. Please fill them before processing.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                // Warning: non-grouped lines with manual trigger
                var nonGroupedManual = list.Where(r => !r.IsGrouped && !string.IsNullOrWhiteSpace(r.PaymentReference)).ToList();
                if (nonGroupedManual.Any())
                {
                    var result = MessageBox.Show(this, 
                        $"WARNING: {nonGroupedManual.Count} line(s) are NOT grouped but have a manual Payment Reference.\n\n" +
                        "This means the trigger was set manually in ReconciliationView without proper grouping.\n" +
                        "Do you want to continue anyway?",
                        "Non-Grouped Lines Warning", 
                        MessageBoxButton.YesNo, 
                        MessageBoxImage.Warning);
                    if (result != MessageBoxResult.Yes) return;
                }

                (sender as FrameworkElement).IsEnabled = false;
                Progress.Value = 0;

                var updated = new List<Reconciliation>();
                foreach (var item in list)
                {
                    var ok = await SimulateProcessAsync(item.DWINGS_GuaranteeID, item.DWINGS_InvoiceID);
                    await Task.Delay(100); // small UI breath
                    if (ok)
                    {
                        // Process all IDs in this grouped item (comma-separated)
                        var ids = item.ID.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (var id in ids)
                        {
                            var reco = await _reconciliationService.GetReconciliationByIdAsync(_countryId, id.Trim());
                            if (reco != null)
                            {
                                reco.Action = (int)ActionType.Triggered;
                                reco.TriggerDate = DateTime.UtcNow;
                                // Save PaymentReference if it was manually entered
                                if (!item.IsGrouped && !string.IsNullOrWhiteSpace(item.PaymentReference))
                                {
                                    reco.PaymentReference = item.PaymentReference;
                                }
                                updated.Add(reco);
                            }
                        }
                    }
                    Progress.Value += 1;
                }

                if (updated.Count > 0)
                {
                    await _reconciliationService.SaveReconciliationsAsync(updated);
                    // Push pending local changes to network to persist across restarts
                    try { await _offlineFirstService.PushReconciliationIfPendingAsync(_countryId); } catch { }
                    // Refresh to reflect any DB side effects
                    await LoadDataAsync();
                }

                MessageBox.Show(this, $"Processing completed. Success: {updated.Count}/{list.Count}", "Completed", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, $"Error during processing: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                (sender as FrameworkElement).IsEnabled = true;
            }
        }

        // Simulated call - 1s delay and returns true
        private async Task<bool> SimulateProcessAsync(string guaranteeId, string invoiceId)
        {
            await Task.Delay(1000);
            return true;
        }
    }

    // DTO for DWINGS Trigger display
    public class DwingsTriggerItem : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;
        protected void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        public string ID { get; set; }  // Can contain multiple IDs (comma-separated) for grouped items
        public string DWINGS_GuaranteeID { get; set; }
        public string DWINGS_InvoiceID { get; set; }
        public string DWINGS_BGPMT { get; set; }
        public decimal Amount { get; set; }
        public string Currency { get; set; }
        public string Comments { get; set; }
        public bool IsGrouped { get; set; }
        public DateTime? ValueDate { get; set; }  // Value date from Pivot line
        public int LineCount { get; set; }  // Number of lines in this BGPMT group

        private string _paymentReference;
        public string PaymentReference
        {
            get => _paymentReference;
            set { _paymentReference = value; OnPropertyChanged(nameof(PaymentReference)); }
        }
    }
}
