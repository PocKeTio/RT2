using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Microsoft.Win32;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Services;
using RecoTool.Helpers;
using RecoTool.Models;
using OfflineFirstAccess.Data;
using OfflineFirstAccess.Models;

namespace RecoTool.Windows
{
    public partial class ReconciliationImportWindow : Window, INotifyPropertyChanged
    {
        private readonly OfflineFirstService _offlineFirstService;

        private string _mappingFilePath;
        private string _dataFilePath;
        private bool _isImporting;
        private CancellationTokenSource _cts;

        public event PropertyChangedEventHandler PropertyChanged;

        public string MappingFilePath
        {
            get => _mappingFilePath;
            set
            {
                _mappingFilePath = value;
                OnPropertyChanged(nameof(MappingFilePath));
                OnFilePathChanged();
            }
        }

        public string DataFilePath
        {
            get => _dataFilePath;
            set
            {
                _dataFilePath = value;
                OnPropertyChanged(nameof(DataFilePath));
                OnFilePathChanged();
            }
        }

        public bool IsImporting
        {
            get => _isImporting;
            set
            {
                _isImporting = value;
                OnPropertyChanged(nameof(IsImporting));
                UpdateButtonStates();
            }
        }

        public ReconciliationImportWindow()
        {
            _offlineFirstService = App.ServiceProvider?.GetRequiredService<OfflineFirstService>();
            InitializeComponent();
            DataContext = this;
            UpdateButtonStates();
        }

        public ReconciliationImportWindow(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? App.ServiceProvider?.GetRequiredService<OfflineFirstService>();
            InitializeComponent();
            DataContext = this;
            UpdateButtonStates();
        }

        private void BrowseMappingButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var ofd = new OpenFileDialog
                {
                    Title = "Select mapping Excel",
                    Filter = "Excel Files (*.xlsx;*.xls)|*.xlsx;*.xls|All Files (*.*)|*.*",
                    Multiselect = false
                };
                if (ofd.ShowDialog() == true)
                {
                    MappingFilePath = ofd.FileName;
                    MappingFileTextBox.Text = MappingFilePath;
                    UpdateFileInfo(MappingFilePath, FileInfoMappingText);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error selecting mapping file: {ex.Message}");
            }
        }

        private void BrowseDataButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var ofd = new OpenFileDialog
                {
                    Title = "Select reconciliation data Excel",
                    Filter = "Excel Files (*.xlsx;*.xls)|*.xlsx;*.xls|All Files (*.*)|*.*",
                    Multiselect = false
                };
                if (ofd.ShowDialog() == true)
                {
                    DataFilePath = ofd.FileName;
                    DataFileTextBox.Text = DataFilePath;
                    UpdateFileInfo(DataFilePath, FileInfoDataText);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error selecting data file: {ex.Message}");
            }
        }

        private async void ImportButton_Click(object sender, RoutedEventArgs e)
        {
            if (IsImporting) return;
            ImportButton.IsEnabled = false;
            await StartImportAsync();
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            if (IsImporting)
            {
                try { _cts?.Cancel(); LogMessage("Cancellation requested..."); } catch { }
            }
            else
            {
                Close();
            }
        }

        private void UpdateButtonStates()
        {
            var ready = !string.IsNullOrEmpty(MappingFilePath) && !string.IsNullOrEmpty(DataFilePath) && !IsImporting;
            ImportButton.IsEnabled = ready;
            CancelButton.Content = IsImporting ? "Cancel" : "Close";
        }

        private void OnFilePathChanged()
        {
            var parts = new List<string>();
            if (!string.IsNullOrEmpty(MappingFilePath)) parts.Add($"Mapping: {System.IO.Path.GetFileName(MappingFilePath)}");
            if (!string.IsNullOrEmpty(DataFilePath)) parts.Add($"Data: {System.IO.Path.GetFileName(DataFilePath)}");
            StatusSummaryText.Text = parts.Count > 0 ? string.Join(" | ", parts) : "Select mapping and data files";
            UpdateButtonStates();
        }

        private void UpdateFileInfo(string path, System.Windows.Controls.TextBlock target)
        {
            try
            {
                var fi = new FileInfo(path);
                target.Text = $"File: {fi.Name} ({fi.Length / 1024:N0} KB) - Modified: {fi.LastWriteTime:dd/MM/yyyy HH:mm}";
            }
            catch
            {
                target.Text = "";
            }
        }

        private async Task StartImportAsync()
        {
            try
            {
                if (string.IsNullOrEmpty(MappingFilePath) || string.IsNullOrEmpty(DataFilePath))
                {
                    ShowWarning("Please select both mapping and data files.");
                    return;
                }

                IsImporting = true;
                _cts = new CancellationTokenSource();
                UpdateProgress(0, "Starting import...");
                LogMessage("Starting reconciliation import...");

                // Validate Excel files
                if (!ExcelHelper.ValidateExcelFormat(MappingFilePath) || !ExcelHelper.ValidateExcelFormat(DataFilePath))
                {
                    ShowError("Invalid Excel file format. Please select .xlsx or .xls files.");
                    return;
                }

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(10, "Opening mapping file...");
                LogMessage("Opening mapping Excel and parsing mapping template (multi-row supported)...");

                // Parse mapping (row1 headers, row2 values)
                string bookingCode = null;
                int pivotStartRow = 0;
                int receivableStartRow = 0;
                var pivotLetterToDest = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var pivotConstants = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                var recvLetterToDest = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var recvConstants = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                using (var mappingExcel = new ExcelHelper())
                {
                    mappingExcel.OpenFile(MappingFilePath);
                    var mapRows = mappingExcel.ReadSheetData(importFields: null, startRow: 2);
                    if (mapRows == null || mapRows.Count == 0)
                        throw new InvalidOperationException("Le fichier de mapping doit contenir au moins 2 lignes (entêtes + au moins une ligne mapping).");

                    // Current booking (country) selected in the app
                    var currentBooking = _offlineFirstService?.CurrentCountryId;
                    if (string.IsNullOrWhiteSpace(currentBooking))
                        throw new InvalidOperationException("Aucun pays/booking sélectionné. Veuillez sélectionner un pays dans l'application.");

                    // Helper: from a given row, get the value for a header starting with prefix (case-insensitive)
                    string GetRowValue(Dictionary<string, object> row, string prefix)
                    {
                        foreach (var kv in row)
                        {
                            var k = kv.Key?.Trim();
                            if (string.IsNullOrEmpty(k)) continue;
                            if (k.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                                return kv.Value?.ToString()?.Trim();
                        }
                        return null;
                    }

                    // Choose the row matching the current booking (case-insensitive)
                    var map = mapRows.FirstOrDefault(r =>
                        string.Equals(GetRowValue(r, "Booking"), currentBooking, StringComparison.OrdinalIgnoreCase));

                    if (map == null)
                    {
                        var available = string.Join(", ", mapRows
                            .Select(r => GetRowValue(r, "Booking"))
                            .Where(v => !string.IsNullOrWhiteSpace(v))
                            .Distinct(StringComparer.OrdinalIgnoreCase));
                        throw new InvalidOperationException($"Aucune ligne de mapping trouvée pour le booking '{currentBooking}'. Bookings disponibles dans le mapping: {available}");
                    }

                    LogMessage($"Selected mapping row for booking '{currentBooking}'.");

                    // Local helpers using the selected row
                    string GetStr(string prefix) => GetRowValue(map, prefix);

                    int GetIntOrDefault(string prefix, int fallback)
                    {
                        var s = GetStr(prefix);
                        if (int.TryParse(s, out var n)) return n;
                        return fallback;
                    }

                    bool IsColumnLetter(string s)
                    {
                        if (string.IsNullOrWhiteSpace(s)) return false;
                        s = s.Trim();
                        return s.All(ch => (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z'));
                    }

                    void Assign(string headerPrefix, string dest, bool receivable)
                    {
                        var val = GetStr(headerPrefix);
                        if (string.IsNullOrWhiteSpace(val)) return;
                        if (IsColumnLetter(val))
                        {
                            if (receivable) recvLetterToDest[val.Trim()] = dest;
                            else pivotLetterToDest[val.Trim()] = dest;
                        }
                        else
                        {
                            if (receivable) recvConstants[dest] = val;
                            else pivotConstants[dest] = val;
                        }
                    }

                    bookingCode = GetStr("Booking");
                    pivotStartRow = GetIntOrDefault("Pivot Starting", 2);
                    receivableStartRow = GetIntOrDefault("Receivable Starting", 2);

                    // Pivot fields
                    Assign("Pivot Comment", "Comments", false);
                    Assign("Pivot Action", "Action", false);
                    Assign("Pivot KPI", "KPI", false);
                    Assign("Pivot RISKY ITEM", "RiskyItem", false);
                    Assign("Pivot REASON NON RISKY", "ReasonNonRisky", false);

                    // Receivable fields
                    Assign("Receivable Comment", "Comments", true);
                    Assign("Receivable Action", "Action", true);
                    Assign("Receivable KPI", "KPI", true);
                    Assign("Receivable 1ST CLAIM", "FirstClaimDate", true);
                    Assign("Receivable LAST CLAIM", "LastClaimDate", true);
                    Assign("Receivable RISKY ITEM", "RiskyItem", true);
                    Assign("Receivable REASON NON RISKY", "ReasonNonRisky", true);
                }

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(30, "Reading PIVOT/RECEIVABLE sheets...");
                LogMessage($"Pivot start row: {pivotStartRow}, Receivable start row: {receivableStartRow}");

                var allById = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase);
                int totalRows = 0;
                using (var dataExcel = new ExcelHelper())
                {
                    dataExcel.OpenFile(DataFilePath);

                    // Read PIVOT
                    var pivotRows = dataExcel.ReadSheetByColumns("PIVOT", pivotLetterToDest, pivotStartRow, idColumnLetter: "A");
                    foreach (var row in pivotRows)
                    {
                        var id = row.TryGetValue("ID", out var idv) ? idv?.ToString() : null;
                        if (string.IsNullOrWhiteSpace(id)) continue;

                        if (!allById.TryGetValue(id, out var agg))
                        {
                            agg = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { ["ID"] = id };
                            allById[id] = agg;
                        }

                        // apply columns
                        foreach (var kv in row)
                        {
                            if (kv.Key.Equals("ID", StringComparison.OrdinalIgnoreCase)) continue;
                            agg[kv.Key] = kv.Value;
                        }
                        // apply constants (do not override if already present?) -> fill if null/absent
                        foreach (var c in pivotConstants)
                        {
                            if (!agg.ContainsKey(c.Key) || agg[c.Key] == null)
                                agg[c.Key] = c.Value;
                        }
                    }
                    totalRows += pivotRows.Count;

                    // Read RECEIVABLE (overrides pivot on conflicts)
                    var recvRows = dataExcel.ReadSheetByColumns("RECEIVABLE", recvLetterToDest, receivableStartRow, idColumnLetter: "A");
                    foreach (var row in recvRows)
                    {
                        var id = row.TryGetValue("ID", out var idv) ? idv?.ToString() : null;
                        if (string.IsNullOrWhiteSpace(id)) continue;

                        if (!allById.TryGetValue(id, out var agg))
                        {
                            agg = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { ["ID"] = id };
                            allById[id] = agg;
                        }

                        foreach (var kv in row)
                        {
                            if (kv.Key.Equals("ID", StringComparison.OrdinalIgnoreCase)) continue;
                            agg[kv.Key] = kv.Value; // override
                        }
                        foreach (var c in recvConstants)
                        {
                            // override constants too (receivable has priority)
                            agg[c.Key] = c.Value;
                        }
                    }
                    totalRows += recvRows.Count;
                }

                LogMessage($"Data rows read (Pivot+Receivable): {totalRows:N0}. Unique IDs: {allById.Count:N0}");
                var rawRows = allById.Values.ToList();
                if (rawRows.Count == 0)
                {
                    UpdateProgress(100, "Nothing to import");
                    LogMessage("No data rows found. Nothing to import.");
                    MessageBox.Show("No data rows found.", "Information", MessageBoxButton.OK, MessageBoxImage.Information);
                    DialogResult = true;
                    return;
                }

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(50, "Transforming data...");

                // Normalize and validate rows
                var toApply = new List<Dictionary<string, object>>(rawRows.Count);
                int processed = 0;
                foreach (var row in rawRows)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    // Ensure ID
                    if (!row.TryGetValue("ID", out var idVal) || idVal == null || string.IsNullOrWhiteSpace(idVal.ToString()))
                    {
                        // skip invalid rows but log once per few
                        if ((processed % 100) == 0)
                            LogMessage($"Skipping row without ID (row #{processed + 1}).", isError: true);
                        processed++;
                        continue;
                    }

                    var rec = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    foreach (var kv in row)
                    {
                        var key = kv.Key?.Trim();
                        if (string.IsNullOrWhiteSpace(key)) continue;

                        object norm = NormalizeValueForReconciliation(key, kv.Value);
                        rec[key] = norm;
                    }

                    // Minimal metadata (do not rely on OfflineFirst flow here)
                    rec["ID"] = rec["ID"]?.ToString();

                    toApply.Add(rec);

                    processed++;
                    if ((processed % 250) == 0)
                    {
                        int pct = 50 + (int)Math.Round(processed * 40.0 / rawRows.Count); // 50->90 during transform
                        if (pct > 90) pct = 90;
                        UpdateProgress(pct, $"Transforming... {processed:N0}/{rawRows.Count:N0}");
                    }
                }

                _cts.Token.ThrowIfCancellationRequested();
                if (toApply.Count == 0)
                    throw new InvalidOperationException("No valid rows to import (all missing IDs).");

                UpdateProgress(92, "Connecting to database...");
                LogMessage("Preparing database upsert into T_Reconciliation...");

                // Build data provider using local connection string
                if (_offlineFirstService == null)
                    throw new InvalidOperationException("OfflineFirstService is not available.");

                var connStr = _offlineFirstService.GetCurrentLocalConnectionString();
                var syncCfg = new SyncConfiguration
                {
                    LocalDatabasePath = connStr, // informational
                    PrimaryKeyColumn = "ID",
                    LastModifiedColumn = "LastModified",
                    IsDeletedColumn = "IsDeleted"
                };

                var provider = await AccessDataProvider.CreateAsync(connStr, syncCfg);

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(95, "Applying changes (upsert)...");

                await provider.ApplyChangesAsync("T_Reconciliation", toApply);

                UpdateProgress(100, "Completed");
                LogMessage($"Import completed. Upserted rows: {toApply.Count:N0}");

                MessageBox.Show($"Import completed successfully. Rows processed: {toApply.Count:N0}", "Information", MessageBoxButton.OK, MessageBoxImage.Information);
                DialogResult = true;
            }
            catch (OperationCanceledException)
            {
                UpdateProgress(0, "Import canceled");
                LogMessage("Import canceled by user");
            }
            catch (Exception ex)
            {
                LogMessage($"Error during import: {ex.Message}", true);
                ShowError($"Error during import: {ex.Message}");
            }
            finally
            {
                IsImporting = false;
                _cts?.Dispose();
                _cts = null;
            }
        }

        private static object NormalizeValueForReconciliation(string key, object value)
        {
            if (value == null) return DBNull.Value;

            // Unwrap COM interop double for Excel dates if necessary
            bool IsDateKey(string k) =>
                k.Equals("FirstClaimDate", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("LastClaimDate", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("ToRemindDate", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("CreationDate", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("DeleteDate", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("LastModified", StringComparison.OrdinalIgnoreCase);

            bool IsBoolKey(string k) =>
                k.Equals("ToRemind", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("ACK", StringComparison.OrdinalIgnoreCase);

            bool IsIntKey(string k) =>
                k.Equals("Action", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("KPI", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("IncidentType", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("RiskyItem", StringComparison.OrdinalIgnoreCase);

            try
            {
                if (IsDateKey(key))
                {
                    if (value is DateTime dt) return dt;
                    if (value is double d) return DateTime.FromOADate(d);
                    if (DateTime.TryParse(value.ToString(), out var parsed)) return parsed;
                    return DBNull.Value;
                }
                if (IsBoolKey(key))
                {
                    if (value is bool b) return b;
                    var s = value.ToString().Trim();
                    if (string.Equals(s, "1") || s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("yes", StringComparison.OrdinalIgnoreCase)) return true;
                    if (string.Equals(s, "0") || s.Equals("false", StringComparison.OrdinalIgnoreCase) || s.Equals("no", StringComparison.OrdinalIgnoreCase)) return false;
                    return DBNull.Value;
                }
                if (IsIntKey(key))
                {
                    if (value is int i) return i;
                    if (value is double d) return (int)Math.Round(d);
                    if (int.TryParse(value.ToString(), out var p)) return p;
                    return DBNull.Value;
                }

                // Default: string or pass-through
                if (value is string) return value;
                return value;
            }
            catch
            {
                return DBNull.Value;
            }
        }

        private void UpdateProgress(int percentage, string status)
        {
            Dispatcher.Invoke(() =>
            {
                ImportProgressBar.Value = percentage;
                ProgressStatusText.Text = status;
            });
        }

        private void LogMessage(string message, bool isError = false)
        {
            Dispatcher.Invoke(() =>
            {
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                var prefix = isError ? "[ERROR]" : "[INFO]";
                ImportLogText.Text += $"{timestamp} {prefix} {message}\n";
                LogScrollViewer.ScrollToEnd();
            });
        }

        private void ShowWarning(string message)
        {
            MessageBox.Show(message, "Warning", MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        private void ShowError(string message)
        {
            MessageBox.Show(message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        private void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
