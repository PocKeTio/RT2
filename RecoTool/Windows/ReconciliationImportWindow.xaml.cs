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
using System.Data.OleDb;
using System.Globalization;
using System.Drawing;
using System.IO;
using System.Text;

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

        private static int? InferActionFromComments(string comments)
        {
            if (string.IsNullOrWhiteSpace(comments)) return null;
            var s = comments.Trim();
            var u = s.ToUpperInvariant();

            // Keyword-based mapping (extendable)
            // Direct matches for explicit actions
            if (u.Contains("REFUND")) return (int)ActionType.Refund;
            if (u.Contains("REMIND")) return (int)ActionType.Remind;
            if (u.Contains("TRIGGERED")) return (int)ActionType.Triggered;
            if (u.Contains("TRIGGER")) return (int)ActionType.Trigger;
            if (u.Contains("EXECUTE")) return (int)ActionType.Execute;
            if (u.Contains("MATCH")) return (int)ActionType.Match;
            if (u.Contains("ADJUST")) return (int)ActionType.Adjust;
            if (u.Contains("REQUEST")) return (int)ActionType.Request;
            if (u.Contains("INVESTIGATE") || u.Contains("CHECK")) return (int)ActionType.Investigate;
            if (u.Contains("PRICING")) return (int)ActionType.DoPricing;
            if (u.Contains("CLAIM")) return (int)ActionType.ToClaim;
            if (u.Contains("SDD")) return (int)ActionType.ToDoSDD;

            // Status-like keywords
            if (u.Contains("DONE")) return (int)ActionType.Match; // treat as resolved/matched
            if (u.Contains("TO DO") || u.Contains("TODO")) return (int)ActionType.Investigate; // pending work
            if (u.Contains("TO BE CLEANED") || u.Contains("CLEAN")) return (int)ActionType.Adjust;

            return null;
        }

        private static int? InferActionFromColor(int? oleColor)
        {
            if (oleColor == null || oleColor.Value == 0) return null;
            try
            {
                var color = ColorTranslator.FromOle(oleColor.Value);
                // Rough thresholds to classify main colors
                bool isRed = color.R >= 200 && color.G <= 100 && color.B <= 100;
                bool isGreen = color.G >= 200 && color.R <= 120 && color.B <= 120;
                bool isBlue = color.B >= 200 && color.R <= 120 && color.G <= 150;
                bool isYellow = color.R >= 200 && color.G >= 200 && color.B <= 120;

                // Map to existing enum members
                if (isRed) return (int)ActionType.Investigate;     // TO DO
                if (isYellow) return (int)ActionType.Investigate;  // TO CHECK
                if (isGreen) return (int)ActionType.Match;         // DONE
                if (isBlue) return (int)ActionType.Adjust;         // TO BE CLEANED
            }
            catch { }
            return null;
        }

        private static int? TryGetOleColor(Dictionary<string, object> row, string key)
        {
            if (row == null || key == null) return null;
            if (!row.TryGetValue(key, out var v) || v == null) return null;
            try
            {
                if (v is int i) return i;
                if (v is double d) return (int)Math.Round(d);
                if (int.TryParse(v.ToString(), out var p)) return p;
            }
            catch { }
            return null;
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
                target.Text = $"File: {fi.Name} ({(fi.Length / 1024).ToString("N0", CultureInfo.InvariantCulture)} KB) - Modified: {fi.LastWriteTime.ToString("dd/MM/yyyy HH:mm", CultureInfo.InvariantCulture)}";
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

                    // Robustly find the mapping sheet that contains the row for the current booking
                    var sheetNames = mappingExcel.GetSheetNames();
                    Dictionary<string, object> map = null;
                    string selectedSheet = null;
                    var perSheetBookings = new Dictionary<string, List<string>>();

                    foreach (var sn in sheetNames)
                    {
                        try
                        {
                            var rows = mappingExcel.ReadSheetData(sheetName: sn, importFields: null, startRow: 2);
                            if (rows != null && rows.Count > 0)
                            {
                                var bookings = rows
                                    .Select(r => GetRowValue(r, "Booking"))
                                    .Where(v => !string.IsNullOrWhiteSpace(v))
                                    .Distinct(StringComparer.OrdinalIgnoreCase)
                                    .ToList();
                                perSheetBookings[sn] = bookings;

                                var candidate = rows.FirstOrDefault(r => string.Equals(GetRowValue(r, "Booking"), currentBooking, StringComparison.OrdinalIgnoreCase));
                                if (candidate != null)
                                {
                                    map = candidate;
                                    selectedSheet = sn;
                                    break;
                                }
                            }
                        }
                        catch { /* ignore and continue scanning */ }
                    }

                    if (map == null)
                    {
                        var details = string.Join(" | ", perSheetBookings.Select(kv => $"{kv.Key}: [{string.Join(", ", kv.Value)}]"));
                        throw new InvalidOperationException($"Aucune ligne de mapping trouvée pour le booking '{currentBooking}'. Bookings disponibles par feuille: {details}");
                    }

                    LogMessage($"Selected mapping row for booking '{currentBooking}' in sheet '{selectedSheet}'.");

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
                    // Pivot ID components
                    Assign("Pivot RawLabel", "RawLabel", false);
                    Assign("Pivot Event_Num", "Event_Num", false);
                    Assign("Pivot Operation_Date", "Operation_Date", false);
                    Assign("Pivot Reconciliation_Num", "Reconciliation_Num", false);
                    Assign("Pivot ReconciliationOrigin_Num", "ReconciliationOrigin_Num", false);

                    // Receivable fields
                    Assign("Receivable Comment", "Comments", true);
                    Assign("Receivable Action", "Action", true);
                    Assign("Receivable KPI", "KPI", true);
                    Assign("Receivable 1ST CLAIM", "FirstClaimDate", true);
                    Assign("Receivable LAST CLAIM", "LastClaimDate", true);
                    Assign("Receivable RISKY ITEM", "RiskyItem", true);
                    Assign("Receivable REASON NON RISKY", "ReasonNonRisky", true);
                    // Receivable ID component
                    Assign("Receivable Event_Num", "Event_Num", true);
                }

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(30, "Reading PIVOT/RECEIVABLE sheets...");
                LogMessage($"Pivot start row: {pivotStartRow}, Receivable start row: {receivableStartRow}");
                // Mapping diagnostics
                try
                {
                    string DumpMap(string title, Dictionary<string, string> map)
                    {
                        if (map == null || map.Count == 0) return $"{title}: (empty)";
                        var items = map.Select(kv => $"{kv.Key}->{kv.Value}");
                        return $"{title}: " + string.Join(", ", items);
                    }
                    string DumpConsts(string title, Dictionary<string, object> map)
                    {
                        if (map == null || map.Count == 0) return $"{title}: (empty)";
                        var items = map.Select(kv => $"{kv.Key}='{kv.Value}'");
                        return $"{title}: " + string.Join(", ", items);
                    }
                    LogMessage(DumpMap("Pivot letter->dest", pivotLetterToDest));
                    LogMessage(DumpConsts("Pivot constants", pivotConstants));
                    LogMessage(DumpMap("Receivable letter->dest", recvLetterToDest));
                    LogMessage(DumpConsts("Receivable constants", recvConstants));
                }
                catch { }

                var allById = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase);

                // Helpers to normalize values for ID composition
                string S(object v)
                {
                    var s = v?.ToString()?.Trim();
                    return string.IsNullOrWhiteSpace(s) ? null : s;
                }
                string FormatDdMmYyyy(object v)
                {
                    try
                    {
                        if (v == null) return null;
                        if (v is DateTime dt) return dt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                        if (v is double d) return DateTime.FromOADate(d).ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                        if (DateTime.TryParse(v.ToString(), out var parsed)) return parsed.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                    }
                    catch { }
                    return null;
                }
                int totalRows = 0;
                using (var dataExcel = new ExcelHelper())
                {
                    dataExcel.OpenFile(DataFilePath);

                    // Read PIVOT
                    // Only capture cell color for Comments (not Action)
                    var colorFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Comments" };
                    var pivotRows = dataExcel.ReadSheetByColumns("PIVOT", pivotLetterToDest, pivotStartRow, idColumnLetter: "A", colorForDestinations: colorFields);
                    // Sample diagnostics for PIVOT
                    if (pivotRows.Count > 0)
                    {
                        var s = pivotRows[0];
                        string Sv(string k) => s.TryGetValue(k, out var v) ? v?.ToString() : null;
                        LogMessage($"PIVOT sample: ID={Sv("ID")}, Comments='{Sv("Comments")}', Action='{Sv("Action")}', KPI='{Sv("KPI")}', RiskyItem='{Sv("RiskyItem")}', ReasonNonRisky='{Sv("ReasonNonRisky")}', Comments__Color='{Sv("Comments__Color")}'");
                    }

                    foreach (var row in pivotRows)
                    {
                        // ID is ALWAYS column A. Do not recompute. Deduplicate identical A values (keep first).
                        var id = row.TryGetValue("ID", out var idv) ? idv?.ToString()?.Trim() : null;
                        if (string.IsNullOrWhiteSpace(id)) continue;
                        if (allById.ContainsKey(id)) continue; // duplicate natural key from Excel -> skip
                        row["ID"] = id;

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
                    var recvRows = dataExcel.ReadSheetByColumns("RECEIVABLE", recvLetterToDest, receivableStartRow, idColumnLetter: "A", colorForDestinations: colorFields);
                    // Sample diagnostics for RECEIVABLE
                    if (recvRows.Count > 0)
                    {
                        var s = recvRows[0];
                        string Sv(string k) => s.TryGetValue(k, out var v) ? v?.ToString() : null;
                        LogMessage($"RECEIVABLE sample: ID={Sv("ID")}, Comments='{Sv("Comments")}', Action='{Sv("Action")}', KPI='{Sv("KPI")}', RiskyItem='{Sv("RiskyItem")}', ReasonNonRisky='{Sv("ReasonNonRisky")}', FirstClaimDate='{Sv("FirstClaimDate")}', LastClaimDate='{Sv("LastClaimDate")}', Comments__Color='{Sv("Comments__Color")}'");
                    }
                    foreach (var row in recvRows)
                    {
                        // ID is ALWAYS column A. For receivable, this contains Event_Num.
                        var id = row.TryGetValue("ID", out var idv) ? idv?.ToString()?.Trim() : null;
                        if (string.IsNullOrWhiteSpace(id)) continue;
                        row["ID"] = id;

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

                // Build DB lookup indices to resolve real IDs from Excel column A values (per sheet rules)
                UpdateProgress(48, "Building database lookup indices...");
                LogMessage("Building in-memory indices from AMBRE (T_Data_Ambre) for ID resolution...");
                var receivableIndex = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // Event_Num (Excel A in Receivable) -> DB ID
                var pivotIndex = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // Excel A for Pivot (concat) -> DB ID

                string Nk(string s)
                {
                    if (string.IsNullOrWhiteSpace(s)) return null;
                    var up = s.Trim().ToUpperInvariant();
                    // light normalization: remove spaces and pipes to accept minor formatting diffs; keep date slashes
                    up = up.Replace(" ", string.Empty)
                           .Replace("|", string.Empty)
                           .Replace("\t", string.Empty);
                    return up;
                }

                if (_offlineFirstService == null)
                    throw new InvalidOperationException("OfflineFirstService is not available.");
                var ambrePath = _offlineFirstService.GetLocalAmbreDatabasePath();
                if (string.IsNullOrWhiteSpace(ambrePath) || !File.Exists(ambrePath))
                    throw new InvalidOperationException("Local AMBRE database not found. Please refresh AMBRE for the current country.");
                var ambreConnStr = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ambrePath};";
                using (var conn = new OleDbConnection(ambreConnStr))
                {
                    await conn.OpenAsync();
                    var sql = "SELECT ID, Event_Num, RawLabel, Reconciliation_Num, ReconciliationOrigin_Num, Operation_Date FROM [T_Data_Ambre]";
                    using (var cmd = new OleDbCommand(sql, conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var id = reader["ID"]?.ToString();
                            if (string.IsNullOrWhiteSpace(id)) continue;

                            var ev = reader["Event_Num"];
                            var evs = ev == null || ev is DBNull ? null : ev.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(evs))
                            {
                                if (!receivableIndex.ContainsKey(evs)) receivableIndex[evs] = id;            // exact
                                var ek = Nk(evs);
                                if (ek != null && !receivableIndex.ContainsKey(ek)) receivableIndex[ek] = id;  // normalized
                            }

                            var recNumDb = reader["Reconciliation_Num"];
                            var recOriDb = reader["ReconciliationOrigin_Num"];
                            var rawLabelDb = reader["RawLabel"];
                            var opDb = reader["Operation_Date"];

                            string left = null;
                            var recNumStr = recNumDb == null || recNumDb is DBNull ? null : recNumDb.ToString()?.Trim();
                            var recOriStr = recOriDb == null || recOriDb is DBNull ? null : recOriDb.ToString()?.Trim();
                            left = !string.IsNullOrWhiteSpace(recNumStr) ? recNumStr : recOriStr;
                            var rawLabelStr = rawLabelDb == null || rawLabelDb is DBNull ? null : rawLabelDb.ToString()?.Trim();
                            var evStr = evs;
                            var opStr = FormatDdMmYyyy(opDb);

                            if (!string.IsNullOrWhiteSpace(left) && !string.IsNullOrWhiteSpace(rawLabelStr) && !string.IsNullOrWhiteSpace(evStr) && !string.IsNullOrWhiteSpace(opStr))
                            {
                                // Store multiple acceptable representations of the Excel column A value
                                var keyPipe = $"{left}|{rawLabelStr}|{evStr}|{opStr}";          // with pipes
                                var keyConcat = $"{left}{rawLabelStr}{evStr}{opStr}";            // pure concatenation
                                var k1 = keyPipe;
                                var k2 = keyConcat;
                                var k1n = Nk(keyPipe);
                                var k2n = Nk(keyConcat);
                                if (!pivotIndex.ContainsKey(k1)) pivotIndex[k1] = id;
                                if (!pivotIndex.ContainsKey(k2)) pivotIndex[k2] = id;
                                if (k1n != null && !pivotIndex.ContainsKey(k1n)) pivotIndex[k1n] = id;
                                if (k2n != null && !pivotIndex.ContainsKey(k2n)) pivotIndex[k2n] = id;
                            }
                        }
                    }
                }
                LogMessage($"Built DB indices: Receivable keys={receivableIndex.Count:N0}, Pivot keys={pivotIndex.Count:N0}");

                UpdateProgress(50, "Transforming data...");

                // Normalize and validate rows
                var toApply = new List<Dictionary<string, object>>(rawRows.Count);
                int processed = 0;
                var mappingKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "Action", "KPI", "ReasonNonRisky", "IncidentType", "RiskyItem"
                };
                int resolvedPivot = 0, resolvedReceivable = 0, unresolved = 0;
                // Diagnostics counters for data presence
                int commentsWithText = 0, firstClaimCount = 0, lastClaimCount = 0;
                // Collect unresolved Excel lines for CSV export
                var unresolvedDetails = new List<(string ExcelId, string Comments, string FirstClaim, string LastClaim)>();
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

                        // Warn for unmapped textual values for enum/boolean mapping keys
                        if (mappingKeys.Contains(key))
                        {
                            var rawStr = kv.Value?.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(rawStr))
                            {
                                bool looksNumeric = int.TryParse(rawStr, out _);
                                bool looksBool = rawStr.Equals("1") || rawStr.Equals("0") ||
                                                 rawStr.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                                                 rawStr.Equals("false", StringComparison.OrdinalIgnoreCase) ||
                                                 rawStr.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                                                 rawStr.Equals("no", StringComparison.OrdinalIgnoreCase);
                                if (Equals(norm, DBNull.Value) && !(looksNumeric || looksBool))
                                {
                                    LogMessage($"Unmapped label for {key}: '{rawStr}' -> NULL", isError: false);
                                }
                            }
                        }
                    }

                    // Track presence diagnostics for key fields
                    try
                    {
                        if (rec.TryGetValue("Comments", out var cmtVal) && !string.IsNullOrWhiteSpace(cmtVal?.ToString()))
                            commentsWithText++;
                        if (rec.TryGetValue("FirstClaimDate", out var fcVal) && fcVal != null && fcVal != DBNull.Value)
                            firstClaimCount++;
                        if (rec.TryGetValue("LastClaimDate", out var lcVal) && lcVal != null && lcVal != DBNull.Value)
                            lastClaimCount++;
                    }
                    catch { }

                    // Additional logic: infer Action from Comments text or (Comments) cell color if Action not clearly mapped
                    try
                    {
                        int? currentAction = null;
                        if (rec.TryGetValue("Action", out var actVal) && actVal != null && actVal != DBNull.Value)
                        {
                            try { currentAction = Convert.ToInt32(actVal); } catch { currentAction = null; }
                        }

                        // Prefer text parsing from Comments when available
                        var commentsText = rec.TryGetValue("Comments", out var comVal) ? comVal?.ToString() : null;
                        if (currentAction == null)
                        {
                            var textInferred = InferActionFromComments(commentsText);
                            if (textInferred != null)
                            {
                                rec["Action"] = textInferred.Value;
                                currentAction = textInferred;
                                LogMessage($"Action inferred from comments: {commentsText} -> {(ActionType)textInferred.Value}");
                            }
                        }

                        // If still not set, fallback to cell color from Comments column ONLY
                        if (currentAction == null)
                        {
                            int? commentsColor = TryGetOleColor(row, "Comments__Color");
                            var colorInferred = InferActionFromColor(commentsColor);
                            if (colorInferred != null)
                            {
                                rec["Action"] = colorInferred.Value;
                                currentAction = colorInferred;
                                LogMessage($"Action inferred from Comments cell color -> {(ActionType)colorInferred.Value}");
                            }
                        }
                    }
                    catch { /* best effort inference */ }

                    // Remove any temporary color keys before persisting
                    var colorKeys = rec.Keys.Where(k => k.EndsWith("__Color", StringComparison.OrdinalIgnoreCase)).ToList();
                    foreach (var ck in colorKeys) rec.Remove(ck);

                    // Resolve real DB ID using Excel column A value against indices (Pivot vs Receivable)
                    string TryResolveId(Dictionary<string, object> r)
                    {
                        var excelId = r.TryGetValue("ID", out var iv) ? iv?.ToString()?.Trim() : null; // column A as read
                        if (string.IsNullOrWhiteSpace(excelId)) { unresolved++; return null; }

                        // Try Receivable (Event_Num)
                        if (receivableIndex.TryGetValue(excelId, out var idr)) { resolvedReceivable++; return idr; }
                        var excelIdN = Nk(excelId);
                        if (excelIdN != null && receivableIndex.TryGetValue(excelIdN, out idr)) { resolvedReceivable++; return idr; }

                        // Try Pivot (concat variants)
                        if (pivotIndex.TryGetValue(excelId, out var idp)) { resolvedPivot++; return idp; }
                        if (excelIdN != null && pivotIndex.TryGetValue(excelIdN, out idp)) { resolvedPivot++; return idp; }

                        unresolved++;
                        // Collect details for this unresolved line for later CSV export
                        try
                        {
                            string commentsText = row.TryGetValue("Comments", out var cv) ? (cv?.ToString() ?? string.Empty) : string.Empty;
                            string firstClaim = row.TryGetValue("FirstClaimDate", out var fcv) ? (FormatDdMmYyyy(fcv) ?? string.Empty) : string.Empty;
                            string lastClaim = row.TryGetValue("LastClaimDate", out var lcv) ? (FormatDdMmYyyy(lcv) ?? string.Empty) : string.Empty;
                            unresolvedDetails.Add((excelId, commentsText, firstClaim, lastClaim));
                        }
                        catch { }
                        return null;
                    }

                    var resolvedId = TryResolveId(rec);
                    if (!string.IsNullOrWhiteSpace(resolvedId))
                    {
                        rec["ID"] = resolvedId;
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

                LogMessage($"ID resolution summary: Pivot={resolvedPivot:N0}, Receivable={resolvedReceivable:N0}, Unresolved={unresolved:N0}");
                LogMessage($"Captured fields: Comments with text={commentsWithText:N0}, 1ST CLAIM={firstClaimCount:N0}, LAST CLAIM={lastClaimCount:N0}");

                // If unresolved lines exist, dump a CSV for debugging
                if (unresolvedDetails.Count > 0)
                {
                    try
                    {
                        string CsvEscape(string s)
                        {
                            if (s == null) return string.Empty;
                            if (s.Contains(";") || s.Contains("\"") || s.Contains("\n") || s.Contains("\r"))
                            {
                                return "\"" + s.Replace("\"", "\"\"") + "\"";
                            }
                            return s;
                        }

                        var dir = System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "RecoTool");
                        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                        var csvPath = System.IO.Path.Combine(dir, $"import_unresolved_{DateTime.Now:yyyyMMdd_HHmmss}.csv");
                        var sb = new StringBuilder();
                        sb.AppendLine("ExcelID;Comments;FirstClaimDate;LastClaimDate");
                        foreach (var it in unresolvedDetails)
                        {
                            sb.Append(CsvEscape(it.ExcelId)); sb.Append(';');
                            sb.Append(CsvEscape(it.Comments)); sb.Append(';');
                            sb.Append(CsvEscape(it.FirstClaim)); sb.Append(';');
                            sb.Append(CsvEscape(it.LastClaim)); sb.AppendLine();
                        }
                        File.WriteAllText(csvPath, sb.ToString(), Encoding.UTF8);
                        LogMessage($"Unresolved details exported to {csvPath}");
                    }
                    catch (Exception ex)
                    {
                        LogMessage($"Failed to export unresolved details CSV: {ex.Message}", isError: true);
                    }
                }

                _cts.Token.ThrowIfCancellationRequested();
                if (toApply.Count == 0)
                    throw new InvalidOperationException("No valid rows to import (all missing IDs).");

                UpdateProgress(92, "Connecting to database...");
                LogMessage("Preparing database update into T_Reconciliation (no inserts)...");

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

                // Enforce update-only: filter to existing IDs and set LastModified to ensure update path
                var idList = toApply
                    .Select(r => r.TryGetValue("ID", out var v) ? v?.ToString() : null)
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var existingRecords = await provider.GetRecordsByIds("T_Reconciliation", idList);
                var existingIds = new HashSet<string>(
                    existingRecords
                        .Select(rec => rec.TryGetValue("ID", out var v) ? v?.ToString() : null)
                        .Where(s => !string.IsNullOrWhiteSpace(s)),
                    StringComparer.OrdinalIgnoreCase);

                var beforeCount = toApply.Count;
                toApply = toApply
                    .Where(r => existingIds.Contains(r["ID"]?.ToString()))
                    .ToList();
                var dropped = beforeCount - toApply.Count;

                // Ensure at least one updatable field by setting LastModified; this also timestamps the update
                var now = DateTime.Now;
                foreach (var rec in toApply)
                {
                    rec["LastModified"] = now;
                }

                LogMessage($"Update-only: {toApply.Count:N0} existing IDs; skipped {dropped:N0} non-existing IDs (no inserts).");

                _cts.Token.ThrowIfCancellationRequested();
                UpdateProgress(95, "Applying changes (update-only)...");

                await provider.ApplyChangesAsync("T_Reconciliation", toApply);

                UpdateProgress(100, "Completed");
                LogMessage($"Import completed. Updated rows: {toApply.Count:N0}");

                MessageBox.Show($"Import completed successfully. Rows updated: {toApply.Count:N0}", "Information", MessageBoxButton.OK, MessageBoxImage.Information);
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

        // Build once: mappings from labels (enum names or descriptions) to integer IDs
        private static Dictionary<string, int> _actionLabelToId;
        private static Dictionary<string, int> _kpiLabelToId;
        private static Dictionary<string, int> _riskReasonLabelToId;
        private static Dictionary<string, int> _incidentLabelToId;

        private static void EnsureLabelMapsBuilt()
        {
            if (_actionLabelToId != null) return;

            string N(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return null;
                var up = s.Trim().ToUpperInvariant();
                up = up.Replace(" ", string.Empty)
                       .Replace("-", string.Empty)
                       .Replace("_", string.Empty)
                       .Replace("/", string.Empty);
                return up;
            }

            _actionLabelToId = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in Enum.GetNames(typeof(ActionType)))
            {
                var value = (int)Enum.Parse(typeof(ActionType), name);
                var key = N(name);
                if (key != null && !_actionLabelToId.ContainsKey(key)) _actionLabelToId[key] = value;

                // Description attribute
                try
                {
                    var fi = typeof(ActionType).GetField(name);
                    var attrs = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                    var desc = attrs != null && attrs.Length > 0 ? attrs[0].Description : null;
                    var dk = N(desc);
                    if (dk != null && !_actionLabelToId.ContainsKey(dk)) _actionLabelToId[dk] = value;
                }
                catch { }
            }

            _kpiLabelToId = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in Enum.GetNames(typeof(KPIType)))
            {
                var value = (int)Enum.Parse(typeof(KPIType), name);
                var key = N(name);
                if (key != null && !_kpiLabelToId.ContainsKey(key)) _kpiLabelToId[key] = value;

                // Friendly label via EnumHelper
                try
                {
                    var friendly = EnumHelper.GetKPIName(value);
                    var fk = N(friendly);
                    if (fk != null && !_kpiLabelToId.ContainsKey(fk)) _kpiLabelToId[fk] = value;
                }
                catch { }
            }

            _riskReasonLabelToId = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in Enum.GetNames(typeof(Risky)))
            {
                var value = (int)Enum.Parse(typeof(Risky), name);
                var key = N(name);
                if (key != null && !_riskReasonLabelToId.ContainsKey(key)) _riskReasonLabelToId[key] = value;

                try
                {
                    var fi = typeof(Risky).GetField(name);
                    var attrs = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                    var desc = attrs != null && attrs.Length > 0 ? attrs[0].Description : null;
                    var dk = N(desc);
                    if (dk != null && !_riskReasonLabelToId.ContainsKey(dk)) _riskReasonLabelToId[dk] = value;
                }
                catch { }
            }

            // Optional: Incident type mapping (for completeness)
            _incidentLabelToId = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in Enum.GetNames(typeof(INC)))
            {
                var value = (int)Enum.Parse(typeof(INC), name);
                var key = N(name);
                if (key != null && !_incidentLabelToId.ContainsKey(key)) _incidentLabelToId[key] = value;
                try
                {
                    var fi = typeof(INC).GetField(name);
                    var attrs = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                    var desc = attrs != null && attrs.Length > 0 ? attrs[0].Description : null;
                    var dk = N(desc);
                    if (dk != null && !_incidentLabelToId.ContainsKey(dk)) _incidentLabelToId[dk] = value;
                }
                catch { }
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
                k.Equals("ACK", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("RiskyItem", StringComparison.OrdinalIgnoreCase);

            bool IsIntKey(string k) =>
                k.Equals("Action", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("KPI", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("IncidentType", StringComparison.OrdinalIgnoreCase) ||
                k.Equals("ReasonNonRisky", StringComparison.OrdinalIgnoreCase);

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
                    var s = value.ToString().Trim();
                    if (int.TryParse(s, out var p)) return p;

                    // Attempt text -> ID resolution from enums
                    EnsureLabelMapsBuilt();
                    string N(string t)
                    {
                        if (string.IsNullOrWhiteSpace(t)) return null;
                        var up = t.Trim().ToUpperInvariant();
                        up = up.Replace(" ", string.Empty).Replace("-", string.Empty).Replace("_", string.Empty).Replace("/", string.Empty);
                        return up;
                    }
                    var nk = N(s);
                    if (nk != null)
                    {
                        if (key.Equals("Action", StringComparison.OrdinalIgnoreCase) && _actionLabelToId.TryGetValue(nk, out var aid)) return aid;
                        if (key.Equals("KPI", StringComparison.OrdinalIgnoreCase) && _kpiLabelToId.TryGetValue(nk, out var kid)) return kid;
                        if (key.Equals("ReasonNonRisky", StringComparison.OrdinalIgnoreCase) && _riskReasonLabelToId.TryGetValue(nk, out var rid)) return rid;
                        if (key.Equals("IncidentType", StringComparison.OrdinalIgnoreCase) && _incidentLabelToId.TryGetValue(nk, out var iid)) return iid;
                    }

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
                var timestamp = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
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
