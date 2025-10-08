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

                // Format date columns as text BEFORE writing data to prevent Excel auto-conversion
                for (int c = 0; c < colCount; c++)
                {
                    var header = headers[c];
                    if (header.Contains("Date") || header == "ACK")
                    {
                        try
                        {
                            var colRange = ws.Range[ws.Cells[2, c + 1], ws.Cells[rowCount + 1, c + 1]];
                            colRange.NumberFormat = "@"; // Text format
                        }
                        catch { }
                    }
                }

                // 2) Prepare data array using GetColumnValue for proper conversion
                var data = new object[rowCount, colCount];
                
                for (int r = 0; r < rowCount; r++)
                {
                    var it = items[r];
                    for (int c = 0; c < colCount; c++)
                    {
                        data[r, c] = GetColumnValue(columns[c], it);
                    }
                }

                if (rowCount > 0 && colCount > 0)
                {
                    var dataRange = ws.Range[ws.Cells[2, 1], ws.Cells[rowCount + 2 - 1, colCount]]; // rows start at 2
                    dataRange.Value2 = data;
                }

                // Autofit
                ws.Columns.AutoFit();
                app.Visible = true;
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
                        // Restore calculation/events (keep DisplayAlerts = false to prevent FileConfidentiality prompts)
                        try
                        {
                            app.ScreenUpdating = prevScreenUpdating;
                            app.EnableEvents = prevEnableEvents;
                            // Keep DisplayAlerts = false to bypass FileConfidentiality prompts
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
                var header = (column.Header ?? string.Empty).ToString();
                
                // Get binding path from column
                string bindingPath = null;
                if (column is DataGridBoundColumn boundCol)
                {
                    var b = boundCol.Binding as System.Windows.Data.Binding;
                    bindingPath = b?.Path?.Path;
                }
                else if (column is DataGridTemplateColumn templateCol)
                {
                    // For template columns, use SortMemberPath as binding path
                    bindingPath = templateCol.SortMemberPath;
                }

                if (string.IsNullOrWhiteSpace(bindingPath))
                {
                    return string.Empty;
                }

                var prop = item.GetType().GetProperty(bindingPath);
                if (prop == null)
                {
                    return string.Empty;
                }
                var raw = prop.GetValue(item, null);
                
                // Handle special columns that need ID-to-text conversion
                // Action, KPI, Incident Type, Reason Non Risky: convert ID to display name
                if (header == "Action" || header == "KPI" || header == "Incident Type" || header == "Reason Non Risky")
                {
                    if (raw == null) return string.Empty;
                    var id = raw as int?;
                    if (id == null || id == 0) return string.Empty;
                    
                    var userField = AllUserFields?.FirstOrDefault(uf => uf.USR_ID == id.Value);
                    return userField?.USR_FieldName ?? string.Empty;
                }
                
                // Assignee: convert ID to user name
                if (header == "Assignee")
                {
                    if (raw == null) return string.Empty;
                    var id = raw as int?;
                    if (id == null || id == 0) return string.Empty;
                    
                    var user = AssigneeOptions?.FirstOrDefault(u => u.Id == id.Value.ToString());
                    return user?.Name ?? string.Empty;
                }
                
                // Action Status: convert bool to Pending/Done
                if (header == "Action Status")
                {
                    if (raw == null) return string.Empty;
                    var bval = raw as bool?;
                    return bval == true ? "DONE" : "PENDING";
                }
                
                // Comments: use LastComment for display
                if (header == "Comments")
                {
                    return item.LastComment ?? string.Empty;
                }
                
                // ToRemind: convert bool to Yes/No
                if (header == "To Remind")
                {
                    if (raw == null) return string.Empty;
                    var bval = raw as bool?;
                    return bval == true ? "Yes" : "No";
                }
                
                // HasEmail: convert bool to Yes/No
                if (header == "HasEmail")
                {
                    if (raw == null) return string.Empty;
                    var bval = raw as bool?;
                    return bval == true ? "Yes" : "No";
                }
                
                // Default formatting
                if (raw == null) return string.Empty;
                
                if (raw is DateTime dt)
                    return dt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                if (raw is bool boolVal)
                    return boolVal ? "Yes" : "No";
                if (raw is decimal dec)
                    return dec.ToString("N2", CultureInfo.InvariantCulture);
                if (raw is double dbl)
                    return dbl.ToString("N2", CultureInfo.InvariantCulture);

                return raw.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }
    }
}
