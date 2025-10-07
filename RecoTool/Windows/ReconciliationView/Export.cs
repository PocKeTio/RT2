using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace RecoTool.Windows
{
    // Partial: Export helpers for ReconciliationView
    public partial class ReconciliationView
    {
        private void Export_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Hourglass during export
                try { Mouse.OverrideCursor = Cursors.Wait; } catch { }
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

                // Verify file exists before showing success
                if (System.IO.File.Exists(path))
                {
                    UpdateStatusInfo($"Exported {items.Count} rows to {path}");
                    LogAction("Export", $"{items.Count} rows to {path}");
                    MessageBox.Show("Export completed successfully.", "Export", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                else
                {
                    throw new Exception("Export failed: file was not created. Please check Excel is installed and you have write permissions.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Export error: {ex.Message}");
                LogAction("ExportError", ex.ToString());
                System.Diagnostics.Debug.WriteLine($"Export exception details: {ex}");
            }
            finally
            {
                // Restore cursor
                try { Mouse.OverrideCursor = null; } catch { }
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
            var CalculationState = Microsoft.Office.Interop.Excel.XlCalculation.xlCalculationAutomatic;
            bool prevScreenUpdating = true, prevEnableEvents = true;
            bool saveSucceeded = false;
            try
            {
                app = new Microsoft.Office.Interop.Excel.Application { Visible = false, DisplayAlerts = false };
                // Speed up: turn off screen updating and calculation during bulk write
                try
                {
                    prevScreenUpdating = app.ScreenUpdating;
                    prevEnableEvents = app.EnableEvents;
                    app.ScreenUpdating = false;
                    app.DisplayAlerts = false;
                    app.EnableEvents = false;
                    app.Calculation = Microsoft.Office.Interop.Excel.XlCalculation.xlCalculationManual;
                }
                catch { }

                wb = app.Workbooks.Add();
                ws = wb.ActiveSheet as Microsoft.Office.Interop.Excel.Worksheet;

                int rowCount = items.Count;
                int colCount = columns.Count;

                // 1) Write headers in one block
                var headerArr = new object[1, colCount];
                for (int c = 0; c < colCount; c++) headerArr[0, c] = headers[c];
                var headerRange = ws.Range[ws.Cells[1, 1], ws.Cells[1, colCount]];
                headerRange.Value2 = headerArr;

                // 2) Prepare data array (1-based for Excel interop)
                var data = new object[rowCount, colCount];
                // Cache binding paths and PropertyInfo to avoid repeated reflection per cell
                var bindingPaths = new string[colCount];
                var props = new System.Reflection.PropertyInfo[colCount];
                var itemType = typeof(RecoTool.Services.DTOs.ReconciliationViewData);
                for (int c = 0; c < colCount; c++)
                {
                    string path = null;
                    if (columns[c] is DataGridBoundColumn dbc)
                        path = (dbc.Binding as System.Windows.Data.Binding)?.Path?.Path;
                    else if (columns[c] is DataGridCheckBoxColumn chbc)
                        path = (chbc.Binding as System.Windows.Data.Binding)?.Path?.Path;
                    // Template/unbound columns not supported: leave null -> empty string
                    bindingPaths[c] = path;
                    if (!string.IsNullOrWhiteSpace(path))
                        props[c] = itemType.GetProperty(path);
                }

                for (int r = 0; r < rowCount; r++)
                {
                    var it = items[r];
                    for (int c = 0; c < colCount; c++)
                    {
                        var pi = props[c];
                        if (pi == null) { data[r, c] = string.Empty; continue; }
                        object raw = null;
                        try { raw = pi.GetValue(it, null); } catch { raw = null; }
                        if (raw == null) { data[r, c] = string.Empty; continue; }

                        // Minimal formatting to keep parity with previous export
                        if (raw is DateTime dt)
                            data[r, c] = dt.ToString("dd-MM-yyyy", CultureInfo.InvariantCulture);
                        else if (raw is bool bval)
                            data[r, c] = bval ? "True" : "False";
                        else if (raw is decimal dec)
                            data[r, c] = dec.ToString("N2");
                        else if (raw is double dbl)
                            data[r, c] = dbl.ToString("N2");
                        else
                            data[r, c] = raw.ToString();
                    }
                }

                if (rowCount > 0 && colCount > 0)
                {
                    var dataRange = ws.Range[ws.Cells[2, 1], ws.Cells[rowCount + 2 - 1, colCount]]; // rows start at 2
                    dataRange.Value2 = data;
                }

                // Autofit
                ws.Columns.AutoFit();
                wb.SaveAs(filePath, Microsoft.Office.Interop.Excel.XlFileFormat.xlOpenXMLWorkbook);
                saveSucceeded = true;
            }
            finally
            {
                try
                {
                    if (ws != null)
                    {
                        System.Runtime.InteropServices.Marshal.ReleaseComObject(ws);
                        ws = null;
                    }
                }
                catch { }

                try
                {
                    if (wb != null)
                    {
                        wb.Close(false);
                        System.Runtime.InteropServices.Marshal.ReleaseComObject(wb);
                        wb = null;
                    }
                }
                catch { }

                try
                {
                    if (app != null)
                    {
                        // Restore calculation/events
                        try
                        {
                            app.Calculation = CalculationState;
                            app.ScreenUpdating = prevScreenUpdating;
                            app.EnableEvents = prevEnableEvents;
                            app.DisplayAlerts = true;
                        }
                        catch { }
                        app.Quit();
                        System.Runtime.InteropServices.Marshal.ReleaseComObject(app);
                        app = null;
                    }
                }
                catch { }

                System.GC.Collect();
                System.GC.WaitForPendingFinalizers();
            }

            // Verify file was actually created
            if (!saveSucceeded)
            {
                throw new Exception("Failed to save Excel file. Please verify Excel is installed and the path is writable.");
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
