using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;

namespace RecoTool.Windows
{
    // Partial: Export helpers for ReconciliationView
    public partial class ReconciliationView
    {
        private void Export_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dlg = new Microsoft.Win32.SaveFileDialog
                {
                    Title = "Export",
                    Filter = "Excel Workbook (*.xlsx)|*.xlsx",
                    DefaultExt = ".xlsx",
                    AddExtension = true,
                    OverwritePrompt = true,
                    FileName = $"reconciliation_export_{DateTime.Now:yyyyMMdd_HHmmss}"
                };
                if (dlg.ShowDialog() != true) return;

                // Export all filtered rows (not only currently loaded page)
                // Coalesce list sources first to avoid mixed-type '??' between List<> and ObservableCollection<>
                var items = (_filteredData ?? _allViewData)?.ToList()
                           ?? ViewData?.ToList()
                           ?? new List<RecoTool.Services.DTOs.ReconciliationViewData>();
                if (items.Count == 0)
                {
                    ShowError("No rows to export.");
                    return;
                }

                // Build headers and value accessors from DataGrid columns (visible order)
                var columns = ResultsDataGrid?.Columns?.Where(c => c.Visibility == Visibility.Visible).ToList() ?? new List<DataGridColumn>();
                if (columns.Count == 0)
                {
                    ShowError("No visible columns to export.");
                    return;
                }

                var headers = columns.Select(c => (c.Header ?? string.Empty).ToString()).ToList();

                string path = dlg.FileName;
                var ext = System.IO.Path.GetExtension(path)?.ToLowerInvariant();
                if (string.IsNullOrEmpty(ext) || ext != ".xlsx")
                {
                    path = System.IO.Path.ChangeExtension(path, ".xlsx");
                }

                // Always export to XLSX
                ExportToExcel(path, headers, columns, items);

                UpdateStatusInfo($"Exported {items.Count} rows to {path}");
                LogAction("Export", $"{items.Count} rows to {path}");
            }
            catch (Exception ex)
            {
                ShowError($"Export error: {ex.Message}");
            }
        }

        private void ExportToCsv(string filePath, List<string> headers, List<DataGridColumn> columns, List<RecoTool.Services.DTOs.ReconciliationViewData> items)
        {
            using (var writer = new System.IO.StreamWriter(filePath, false, Encoding.UTF8))
            {
                string Escape(object v)
                {
                    if (v == null) return string.Empty;
                    var s = v.ToString();
                    return "\"" + s.Replace("\"", "\"\"") + "\"";
                }

                // headers
                writer.WriteLine(string.Join(",", headers.Select(h => Escape(h))));

                foreach (var item in items)
                {
                    var values = columns.Select(col => Escape(GetColumnValue(col, item)));
                    writer.WriteLine(string.Join(",", values));
                }
            }
        }

        private void ExportToExcel(string filePath, List<string> headers, List<DataGridColumn> columns, List<RecoTool.Services.DTOs.ReconciliationViewData> items)
        {
            Microsoft.Office.Interop.Excel.Application app = null;
            Microsoft.Office.Interop.Excel.Workbook wb = null;
            Microsoft.Office.Interop.Excel.Worksheet ws = null;
            try
            {
                app = new Microsoft.Office.Interop.Excel.Application { Visible = false, DisplayAlerts = false };
                wb = app.Workbooks.Add();
                ws = wb.ActiveSheet as Microsoft.Office.Interop.Excel.Worksheet;

                // headers
                for (int c = 0; c < headers.Count; c++)
                {
                    ws.Cells[1, c + 1] = headers[c];
                }

                // rows
                for (int r = 0; r < items.Count; r++)
                {
                    var item = items[r];
                    for (int c = 0; c < columns.Count; c++)
                    {
                        var val = GetColumnValue(columns[c], item);
                        ws.Cells[r + 2, c + 1] = val;
                    }
                }

                // Autofit
                ws.Columns.AutoFit();
                wb.SaveAs(filePath, Microsoft.Office.Interop.Excel.XlFileFormat.xlOpenXMLWorkbook);
            }
            finally
            {
                if (wb != null)
                {
                    wb.Close(false);
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(wb);
                    wb = null;
                }
                if (ws != null)
                {
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(ws);
                    ws = null;
                }
                if (app != null)
                {
                    app.Quit();
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(app);
                    app = null;
                }
                System.GC.Collect();
                System.GC.WaitForPendingFinalizers();
            }
        }

        private string GetColumnValue(DataGridColumn column, RecoTool.Services.DTOs.ReconciliationViewData item)
        {
            try
            {
                // Support DataGridTextColumn and DataGridCheckBoxColumn bound to a property
                string bindingPath = null;
                if (column is DataGridBoundColumn)
                {
                    var b = ((DataGridBoundColumn)column).Binding as System.Windows.Data.Binding;
                    bindingPath = b?.Path?.Path;
                }
                else if (column is DataGridCheckBoxColumn)
                {
                    var b = ((DataGridCheckBoxColumn)column).Binding as System.Windows.Data.Binding;
                    bindingPath = b?.Path?.Path;
                }

                if (string.IsNullOrWhiteSpace(bindingPath))
                {
                    return string.Empty; // unbound or template column not supported
                }

                var prop = item.GetType().GetProperty(bindingPath);
                if (prop == null) return string.Empty;
                var raw = prop.GetValue(item, null);
                if (raw == null) return string.Empty;

                // Basic formatting similar to grid
                if (raw is DateTime dt)
                    return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                if (raw is bool bval)
                    return bval ? "True" : "False";
                if (raw is decimal dec)
                    return dec.ToString("N2");
                if (raw is double dbl)
                    return dbl.ToString("N2");

                return raw.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }
    }
}
