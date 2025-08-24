using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Microsoft.Win32;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Models;
using RecoTool.Services;
using System.Threading;
using Excel = Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour ReportsWindow.xaml
    /// Fenêtre de génération de rapports et exports
    /// </summary>
    public partial class ReportsWindow : Window, INotifyPropertyChanged
    {
        #region Fields and Properties

        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;
        private ExportService _exportService;
        private DateTime _startDate;
        private DateTime _endDate;
        private bool _isGenerating;
        private string _outputPath;
        private CancellationTokenSource _exportCts;

        public Country CurrentCountry => _offlineFirstService?.CurrentCountry;

        public DateTime StartDate
        {
            get => _startDate;
            set
            {
                _startDate = value;
                OnPropertyChanged(nameof(StartDate));
            }
        }

        public DateTime EndDate
        {
            get => _endDate;
            set
            {
                _endDate = value;
                OnPropertyChanged(nameof(EndDate));
            }
        }

        public bool IsGenerating
        {
            get => _isGenerating;
            set
            {
                _isGenerating = value;
                OnPropertyChanged(nameof(IsGenerating));
            }
        }

        public string OutputPath
        {
            get => _outputPath;
            set
            {
                _outputPath = value;
                OnPropertyChanged(nameof(OutputPath));
            }
        }

        #endregion

        protected override void OnActivated(EventArgs e)
        {
            base.OnActivated(e);
            // Rafraîchir l'affichage du pays courant si la sélection a changé dans la fenêtre principale
            OnPropertyChanged(nameof(CurrentCountry));
        }

        #region Constructor

        public ReportsWindow()
        {
            InitializeComponent();
            DataContext = this;
            InitializeData();
        }

        public ReportsWindow(ReconciliationService reconciliationService, OfflineFirstService offlineFirstService) : this()
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService ?? App.ServiceProvider?.GetRequiredService<OfflineFirstService>();
            if (_reconciliationService != null)
            {
                _exportService = new ExportService(_reconciliationService);
            }

            // Mettre à jour les bindings dépendants du pays courant
            OnPropertyChanged(nameof(CurrentCountry));
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            
            // Dates par défaut : mois courant
            StartDate = new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1);
            EndDate = DateTime.Now;
            
            // Répertoire de sortie par défaut
            OutputPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "RecoTool_Reports");
        }

        #endregion

        #region UI Busy / Wait Cursor

        private sealed class DisposableAction : IDisposable
        {
            private readonly Action _onDispose;
            public DisposableAction(Action onDispose) { _onDispose = onDispose; }
            public void Dispose() { try { _onDispose?.Invoke(); } catch { } }
        }

        private IDisposable BeginWaitCursor()
        {
            try
            {
                if (!Dispatcher.CheckAccess())
                    Dispatcher.Invoke(() => Mouse.OverrideCursor = Cursors.Wait);
                else
                    Mouse.OverrideCursor = Cursors.Wait;
            }
            catch { }
            return new DisposableAction(() =>
            {
                try
                {
                    if (!Dispatcher.CheckAccess())
                        Dispatcher.Invoke(() => Mouse.OverrideCursor = null);
                    else
                        Mouse.OverrideCursor = null;
                }
                catch { }
            });
        }

        #endregion

        #region Event Handlers

        /// <summary>
        /// Sélection du répertoire de sortie
        /// </summary>
        private void BrowseOutputPathButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var folderDialog = new System.Windows.Forms.FolderBrowserDialog
                {
                    Description = "Select the reports output directory",
                    SelectedPath = OutputPath
                };

                if (folderDialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
                {
                    OutputPath = folderDialog.SelectedPath;
                    UpdateTextBox("OutputPathTextBox", OutputPath);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error selecting directory: {ex.Message}");
            }
        }

        /// <summary>
        /// Génération du rapport de réconciliation complet
        /// </summary>
        private async void GenerateReconciliationReportButton_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("Reconciliation", "Reconciliation Report");
        }

        /// <summary>
        /// Génération du rapport KPI
        /// </summary>
        private async void GenerateKPIReportButton_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("KPI", "KPI Report");
        }

        /// <summary>
        /// Génération du rapport d'anomalies
        /// </summary>
        private async void GenerateAnomaliesReportButton_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("Anomalies", "Anomalies Report");
        }

        /// <summary>
        /// Génération du rapport détaillé par devise
        /// </summary>
        private async void GenerateCurrencyReportButton_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("Currency", "Currency Report");
        }

        /// <summary>
        /// Export des données brutes
        /// </summary>
        private async void ExportRawDataButton_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("RawData", "Raw Data Export");
        }

        /// <summary>
        /// Export des données réconciliées seulement
        /// </summary>
        private async void ExportReconciledDataButton_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("Reconciled", "Reconciled Data Export");
        }

        /// <summary>
        /// Export des données en attente
        /// </summary>
        private async void ExportPendingDataButton_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("Pending", "Pending Data Export");
        }

        /// <summary>
        /// Export personnalisé avec sélection de fichier
        /// </summary>
        private async void ExportCustomButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var saveFileDialog = new SaveFileDialog
                {
                    Title = "Custom Export",
                    Filter = "Excel Files (*.xlsx)|*.xlsx|CSV Files (*.csv)|*.csv",
                    DefaultExt = "xlsx",
                    FileName = $"RecoTool_Custom_{DateTime.Now:yyyyMMdd_HHmmss}"
                };

                if (saveFileDialog.ShowDialog() == true)
                {
                    await ExportDataToFile(saveFileDialog.FileName, "Custom");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error during custom export: {ex.Message}");
            }
        }

        /// <summary>
        /// Validation des paramètres
        /// </summary>
        private void ValidateParametersButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var validationResults = ValidateParameters();
                if (validationResults.Count == 0)
                {
                    ShowInfo("All parameters are valid.");
                }
                else
                {
                    var message = "Issues detected:\n" + string.Join("\n", validationResults);
                    ShowWarning(message);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error during validation: {ex.Message}");
            }
        }

        /// <summary>
        /// Fermeture de la fenêtre
        /// </summary>
        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        #endregion

        #region Report Generation

        /// <summary>
        /// Génère un rapport selon le type spécifié
        /// </summary>
        private async Task GenerateReport(string reportType, string reportName)
        {
            if (!ValidateParametersForGeneration())
                return;

            try
            {
                using var _ = BeginWaitCursor();
                IsGenerating = true;
                UpdateStatusText($"Generating {reportName}...");

                // Création du répertoire de sortie si nécessaire
                Directory.CreateDirectory(OutputPath);

                // Nom du fichier
                var fileName = $"{reportType}_Report_{CurrentCountry?.CNT_Id}_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
                var fullPath = Path.Combine(OutputPath, fileName);

                // Récupération des données
                var data = await GetReportData(reportType);
                
                // Génération du rapport
                await GenerateReportFile(fullPath, reportType, data);

                UpdateStatusText($"{reportName} generated successfully: {fullPath}");
                ShowInfo($"{reportName} generated successfully:\n{fullPath}");
            }
            catch (Exception ex)
            {
                UpdateStatusText($"Error generating {reportName}");
                ShowError($"Error generating {reportName}: {ex.Message}");
            }
            finally
            {
                IsGenerating = false;
            }
        }

        /// <summary>
        /// Exporte des données selon le type spécifié
        /// </summary>
        private async Task ExportData(string exportType, string exportName)
        {
            if (!ValidateParametersForGeneration())
                return;

            try
            {
                using var _ = BeginWaitCursor();
                IsGenerating = true;
                UpdateStatusText($"Exporting: {exportName}...");

                // Création du répertoire de sortie si nécessaire
                Directory.CreateDirectory(OutputPath);

                // Nom du fichier
                var fileName = $"{exportType}_Export_{CurrentCountry?.CNT_Id}_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
                var fullPath = Path.Combine(OutputPath, fileName);

                await ExportDataToFile(fullPath, exportType);

                UpdateStatusText($"{exportName} completed successfully: {fullPath}");
                ShowInfo($"{exportName} completed successfully:\n{fullPath}");
            }
            catch (Exception ex)
            {
                UpdateStatusText($"Error during export: {exportName}");
                ShowError($"Error during export {exportName}: {ex.Message}");
            }
            finally
            {
                IsGenerating = false;
            }
        }

        /// <summary>
        /// Récupère les données pour un rapport
        /// </summary>
        private async Task<List<ReconciliationViewData>> GetReportData(string reportType)
        {
            if (_reconciliationService == null || CurrentCountry == null)
                return new List<ReconciliationViewData>();

            // Construction du filtre selon le type de rapport
            string filter = reportType switch
            {
                "Anomalies" => "KPI = 0 OR Action = 2", // IT Issues or Investigate
                "Currency" => null, // All data, grouped by currency
                "KPI" => null, // All data for KPI calculations
                _ => null // Réconciliation complète
            };

            return await _reconciliationService.GetReconciliationViewAsync(CurrentCountry.CNT_Id, filter);
        }

        /// <summary>
        /// Génère un fichier de rapport
        /// </summary>
        private async Task GenerateReportFile(string filePath, string reportType, List<ReconciliationViewData> data)
        {
            using var _ = BeginWaitCursor();
            // TODO: Implémenter la génération Excel selon le type de rapport
            // Pour l'instant, simulation avec délai
            await Task.Delay(2000);

            // Écriture basique d'un fichier de test
            var content = $"Report {reportType}\nCountry: {CurrentCountry?.CNT_Name}\nPeriod: {StartDate:dd/MM/yyyy} - {EndDate:dd/MM/yyyy}\nNumber of rows: {data.Count}\nGenerated on: {DateTime.Now}";
            File.WriteAllText(filePath.Replace(".xlsx", ".txt"), content);
        }

        /// <summary>
        /// Exporte des données vers un fichier
        /// </summary>
        private async Task ExportDataToFile(string filePath, string exportType)
        {
            using var _ = BeginWaitCursor();
            if (_reconciliationService == null || CurrentCountry == null)
                return;

            // Construction du filtre selon le type d'export
            string filter = exportType switch
            {
                "Reconciled" => "KPI = 1", // Paid But Not Reconciled
                "Pending" => "Action IS NULL OR Action = 2", // Pending or to investigate
                "Custom" => GetCustomFilter(),
                _ => null // Toutes les données
            };

            var data = await _reconciliationService.GetReconciliationViewAsync(CurrentCountry.CNT_Id, filter);

            // TODO: Implémenter l'export Excel/CSV
            // Pour l'instant, simulation avec délai
            await Task.Delay(1500);

            // Écriture basique d'un fichier de test
            var content = $"Export {exportType}\nCountry: {CurrentCountry?.CNT_Name}\nPeriod: {StartDate:dd/MM/yyyy} - {EndDate:dd/MM/yyyy}\nNumber of rows: {data.Count}\nExported on: {DateTime.Now}";
            File.WriteAllText(filePath.Replace(".xlsx", ".txt").Replace(".csv", ".txt"), content);
        }

        #endregion

        #region Validation

        /// <summary>
        /// Valide les paramètres généraux
        /// </summary>
        private List<string> ValidateParameters()
        {
            var issues = new List<string>();

            if (CurrentCountry == null)
                issues.Add("No country selected");

            if (StartDate > EndDate)
                issues.Add("Start date must be before end date");

            if (string.IsNullOrWhiteSpace(OutputPath))
                issues.Add("Output directory not specified");

            return issues;
        }

        /// <summary>
        /// Valide les paramètres avant génération
        /// </summary>
        private bool ValidateParametersForGeneration()
        {
            var issues = ValidateParameters();
            if (issues.Count > 0)
            {
                var message = "Cannot generate report:\n" + string.Join("\n", issues);
                ShowWarning(message);
                return false;
            }

            if (_reconciliationService == null)
            {
                ShowWarning("Reconciliation service not available");
                return false;
            }

            return true;
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Obtient le filtre personnalisé depuis l'interface
        /// </summary>
        private string GetCustomFilter()
        {
            // TODO: Récupérer depuis les contrôles de filtre personnalisé
            return null;
        }

        /// <summary>
        /// Met à jour le texte de statut
        /// </summary>
        private void UpdateStatusText(string status)
        {
            try
            {
                var statusText = FindName("StatusText") as TextBlock;
                if (statusText != null)
                {
                    statusText.Text = status;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error updating status: {ex.Message}");
            }
        }

        /// <summary>
        /// Met à jour un TextBox par son nom
        /// </summary>
        private void UpdateTextBox(string name, string value)
        {
            try
            {
                var textBox = FindName(name) as TextBox;
                if (textBox != null)
                {
                    textBox.Text = value;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"TextBox '{name}' not found: {ex.Message}");
            }
        }

        /// <summary>
        /// Affiche un message d'information
        /// </summary>
        private void ShowInfo(string message)
        {
            MessageBox.Show(message, "Information", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        /// <summary>
        /// Affiche un message d'avertissement
        /// </summary>
        private void ShowWarning(string message)
        {
            MessageBox.Show(message, "Warning", MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        #endregion

        #region INotifyPropertyChanged

        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        #endregion

        #region Event Handlers

        /// <summary>
        /// Génère un rapport de synthèse par pays
        /// </summary>
        private async void GenerateCountrySummary_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("CountrySummary", "Country summary report");
        }

        /// <summary>
        /// Génère un rapport de performance
        /// </summary>
        private async void GeneratePerformanceReport_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("Performance", "Performance report");
        }

        /// <summary>
        /// Génère un rapport d'activité quotidienne
        /// </summary>
        private async void GenerateDailyActivity_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("DailyActivity", "Daily activity report");
        }

        /// <summary>
        /// Génère un rapport d'écarts
        /// </summary>
        private async void GenerateDiscrepancyReport_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("Discrepancy", "Discrepancy report");
        }

        /// <summary>
        /// Génère une répartition par devise
        /// </summary>
        private async void GenerateCurrencyBreakdown_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("CurrencyBreakdown", "Currency breakdown");
        }

        /// <summary>
        /// Génère un rapport de série temporelle
        /// </summary>
        private async void GenerateTimeSeriesReport_Click(object sender, RoutedEventArgs e)
        {
            await GenerateReport("TimeSeries", "Time series report");
        }

        /// <summary>
        /// Exporte vers Excel
        /// </summary>
        private async void ExportToExcel_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("xlsx", "Export Excel");
        }

        /// <summary>
        /// Exporte vers CSV
        /// </summary>
        private async void ExportToCSV_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("csv", "Export CSV");
        }

        /// <summary>
        /// Exporte vers PDF
        /// </summary>
        private async void ExportToPDF_Click(object sender, RoutedEventArgs e)
        {
            await ExportData("pdf", "Export PDF");
        }

        // Param exports via T_param keys
        private async void ExportKPIParam_Click(object sender, RoutedEventArgs e)
        {
            await RunParamExportAsync("Export_KPI");
        }

        private async void ExportPastDueParam_Click(object sender, RoutedEventArgs e)
        {
            await RunParamExportAsync("Export_PastDUE");
        }

        private async void ExportITParam_Click(object sender, RoutedEventArgs e)
        {
            await RunParamExportAsync("Export_IT");
        }

        /// <summary>
        /// Exporte l'historique des KPI (snapshots quotidiens) sur la période sélectionnée au format CSV
        /// </summary>
        private async void ExportKpiHistory_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateParametersForGeneration())
                return;

            if (_reconciliationService == null || CurrentCountry == null)
            {
                ShowWarning("Reconciliation service or country not available");
                return;
            }

            try
            {
                using var _ = BeginWaitCursor();
                IsGenerating = true;
                UpdateStatusText("Exporting KPI history...");

                Directory.CreateDirectory(OutputPath);
                var fileName = $"KpiHistory_{CurrentCountry.CNT_Id}_{StartDate:yyyyMMdd}_{EndDate:yyyyMMdd}.xlsx";
                var fullPath = Path.Combine(OutputPath, fileName);

                // Fetch snapshots in range
                var table = await _reconciliationService.GetKpiSnapshotsAsync(StartDate.Date, EndDate.Date, CurrentCountry.CNT_Id);

                // Write Excel (.xlsx)
                await WriteDataTableToExcelAsync(table, fullPath);

                UpdateStatusText($"KPI history exported: {fullPath}");
                ShowInfo($"KPI history exported successfully:\n{fullPath}");
            }
            catch (Exception ex)
            {
                UpdateStatusText("Error exporting KPI history");
                ShowError($"Error exporting KPI history: {ex.Message}");
            }
            finally
            {
                IsGenerating = false;
            }
        }

        private static Task WriteDataTableToExcelAsync(DataTable table, string path)
        {
            return Task.Run(() =>
            {
                Excel.Application app = null;
                Excel.Workbook wb = null;
                Excel.Worksheet ws = null;
                try
                {
                    app = new Excel.Application { Visible = false, DisplayAlerts = false };
                    wb = app.Workbooks.Add();
                    ws = wb.Sheets[1];
                    ws.Name = "KPI History";

                    // Headers
                    for (int c = 0; c < table.Columns.Count; c++)
                    {
                        ws.Cells[1, c + 1] = table.Columns[c].ColumnName;
                    }

                    // Data
                    int rowIndex = 2;
                    foreach (DataRow row in table.Rows)
                    {
                        for (int c = 0; c < table.Columns.Count; c++)
                        {
                            ws.Cells[rowIndex, c + 1] = row[c];
                        }
                        rowIndex++;
                    }

                    // Format
                    var used = ws.UsedRange;
                    used.Columns.AutoFit();

                    // Save as .xlsx
                    wb.SaveAs(path, Excel.XlFileFormat.xlOpenXMLWorkbook);
                }
                finally
                {
                    if (wb != null)
                    {
                        wb.Close(false);
                        Marshal.ReleaseComObject(wb);
                        wb = null;
                    }
                    if (ws != null)
                    {
                        Marshal.ReleaseComObject(ws);
                        ws = null;
                    }
                    if (app != null)
                    {
                        app.Quit();
                        Marshal.ReleaseComObject(app);
                        app = null;
                    }
                }
            });
        }

        /// <summary>
        /// Actualise les données
        /// </summary>
        private async void RefreshData_Click(object sender, RoutedEventArgs e)
        {
            //await LoadCountries();
            UpdateStatusText("Data refreshed");
        }

        /// <summary>
        /// Ouvre le dossier d'exports
        /// </summary>
        private void OpenExportsFolder_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (Directory.Exists(OutputPath))
                {
                    System.Diagnostics.Process.Start("explorer.exe", OutputPath);
                }
                else
                {
                    MessageBox.Show("The exports folder does not exist yet. Generate a report first.", "Folder not found", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Unable to open folder: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Annule l'export en cours
        /// </summary>
        private void CancelExport_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                _exportCts?.Cancel();
                UpdateStatusText("Cancellation requested...");
            }
            catch { }
        }

        /// <summary>
        /// Ferme la fenêtre
        /// </summary>
        private void Close_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        #endregion

        #region Param Export Implementation

        private async Task RunParamExportAsync(string paramKey)
        {
            if (!ValidateParametersForGeneration())
                return;

            if (_exportService == null)
            {
                ShowWarning("Export service not available");
                return;
            }

            try
            {
                using var _ = BeginWaitCursor();
                IsGenerating = true;
                UpdateStatusText($"Exporting {paramKey}...");

                var ctx = BuildExportContext();
                _exportCts?.Dispose();
                _exportCts = new CancellationTokenSource();
                var token = _exportCts.Token;
                var path = await _exportService.ExportFromParamAsync(paramKey, ctx, token).ConfigureAwait(false);
                UpdateStatusText($"Export finished: {path}");
                ShowInfo($"Export completed:\n{path}");
            }
            catch (OperationCanceledException)
            {
                UpdateStatusText($"Export canceled: {paramKey}");
                ShowWarning($"Export canceled: {paramKey}");
            }
            catch (Exception ex)
            {
                UpdateStatusText($"Error during export {paramKey}");
                ShowError($"Error during export {paramKey}: {ex.Message}");
            }
            finally
            {
                IsGenerating = false;
                _exportCts?.Dispose();
                _exportCts = null;
            }
        }

        private ExportContext BuildExportContext()
        {
            var countryId = CurrentCountry?.CNT_Id;
            var userId = TryGetUserIdFromTextBox();
            return new ExportContext
            {
                CountryId = countryId,
                AccountId = null, // pas de sélection de compte dans cette fenêtre
                FromDate = StartDate,
                ToDate = EndDate,
                UserId = string.IsNullOrWhiteSpace(userId) ? _reconciliationService?.CurrentUser : userId,
                OutputDirectory = OutputPath
            };
        }

        private string TryGetUserIdFromTextBox()
        {
            try
            {
                var tb = FindName("ExportUserIdTextBox") as TextBox;
                return tb?.Text;
            }
            catch
            {
                return null;
            }
        }

        #endregion
    }
}
