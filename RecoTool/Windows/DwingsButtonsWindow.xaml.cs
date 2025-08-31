using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using RecoTool.Models;
using RecoTool.Services;

namespace RecoTool.Windows
{
    public partial class DwingsButtonsWindow : Window
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly ReconciliationService _reconciliationService;
        private readonly string _countryId;
        private List<Reconciliation> _items = new List<Reconciliation>();

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
                _items = await _reconciliationService.GetTriggerReconciliationsAsync(_countryId) ?? new List<Reconciliation>();
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
                var list = _items?.ToList() ?? new List<Reconciliation>();
                if (list.Count == 0)
                {
                    MessageBox.Show(this, "No rows to process.", "Information", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                (sender as FrameworkElement).IsEnabled = false;
                Progress.Value = 0;

                var updated = new List<Reconciliation>();
                foreach (var r in list)
                {
                    var ok = await SimulateProcessAsync(r.DWINGS_GuaranteeID, r.DWINGS_InvoiceID);
                    await Task.Delay(100); // small UI breath
                    if (ok)
                    {
                        r.Action = (int)ActionType.Triggered;
                        updated.Add(r);
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
}
