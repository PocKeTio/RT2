using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: Matched rows popup logic extracted from ReconciliationView.xaml.cs
    public partial class ReconciliationView
    {
        private void OpenMatchedPopup(ReconciliationViewData rowData)
        {
            try
            {
                if (rowData == null) return;

                // Build backend WHERE clause
                string where = null;
                if (!string.IsNullOrWhiteSpace(rowData.DWINGS_InvoiceID))
                {
                    var key = rowData.DWINGS_InvoiceID.Replace("'", "''");
                    where = $"r.DWINGS_InvoiceID = '{key}'";
                }
                else if (!string.IsNullOrWhiteSpace(rowData.InternalInvoiceReference))
                {
                    var key = rowData.InternalInvoiceReference.Replace("'", "''");
                    where = $"r.InternalInvoiceReference = '{key}'";
                }

                if (string.IsNullOrWhiteSpace(where)) return;

                // Create and open a popup ReconciliationView with the applied filter
                var view = new ReconciliationView(_reconciliationService, _offlineFirstService)
                {
                    Margin = new Thickness(0)
                };
                try { view.SyncCountryFromService(); } catch { }
                try { view.ApplySavedFilterSql(where); } catch { }
                try { view.SetViewTitle($"Matched: {where}"); } catch { }

                var wnd = new Window
                {
                    Title = "Matched Reconciliations",
                    Content = view,
                    Owner = Window.GetWindow(this),
                    Width = 1100,
                    Height = 750,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner
                };
                view.CloseRequested += (s, ev) => { try { wnd.Close(); } catch { } };
                wnd.Show();
            }
            catch { }
        }
    }
}
