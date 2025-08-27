using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Helpers;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour ImportAmbreWindow.xaml
    /// Fenêtre d'import des fichiers Excel AMBRE avec prévisualisation et progression
    /// </summary>
    public partial class ImportAmbreWindow : Window, INotifyPropertyChanged
    {
        #region Fields and Properties

        private readonly OfflineFirstService _offlineFirstService;
        private readonly AmbreImportService _ambreImportService;
        private string _selectedFilePath;
        private string _selectedFilePath1;
        private string _selectedFilePath2;
        private string[] _selectedFilePaths = Array.Empty<string>();
        private bool _isImporting;
        private bool _isValidating;
        private CancellationTokenSource _cancellationTokenSource;
        private ObservableCollection<ColumnMappingInfo> _columnMappings;
        private ImportValidationResult _validationResult;

        public Country CurrentCountry => _offlineFirstService?.CurrentCountry;

        public string SelectedFilePath
        {
            get => _selectedFilePath;
            set
            {
                _selectedFilePath = value;
                OnPropertyChanged(nameof(SelectedFilePath));
                OnFilePathChanged();
            }
        }

        public string[] SelectedFilePaths
        {
            get => _selectedFilePaths;
            set
            {
                _selectedFilePaths = value ?? Array.Empty<string>();
                OnPropertyChanged(nameof(SelectedFilePaths));
                // Keep compatibility: expose first file as SelectedFilePath
                SelectedFilePath = _selectedFilePaths.FirstOrDefault();
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

        public bool IsValidating
        {
            get => _isValidating;
            set
            {
                _isValidating = value;
                OnPropertyChanged(nameof(IsValidating));
                UpdateButtonStates();
            }
        }

        public ObservableCollection<ColumnMappingInfo> ColumnMappings
        {
            get => _columnMappings;
            set
            {
                _columnMappings = value;
                OnPropertyChanged(nameof(ColumnMappings));
            }
        }

        #endregion

        #region Constructor

        public ImportAmbreWindow()
        {
        }

        public ImportAmbreWindow(OfflineFirstService offlineFirstService, AmbreImportService ambreImportService)
        {
            _offlineFirstService = offlineFirstService ?? App.ServiceProvider?.GetRequiredService<OfflineFirstService>();
            _ambreImportService = ambreImportService ?? App.ServiceProvider?.GetRequiredService<AmbreImportService>();
            InitializeComponent();
            DataContext = this;
            InitializeData();
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            ColumnMappings = new ObservableCollection<ColumnMappingInfo>();
            
            // Configuration initiale des contrôles
            // Note: Options d'import et preview mapping supprimés de l'UI
            
            UpdateButtonStates();
        }
        #endregion

        #region Event Handlers

        /// <summary>
        /// Parcourir et sélectionner le premier fichier Excel
        /// </summary>
        private void BrowseFile1Button_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var openFileDialog = new OpenFileDialog
                {
                    Title = "Select AMBRE file (Pivot or Receivable)",
                    Filter = "Excel Files (*.xlsx;*.xls)|*.xlsx;*.xls|All Files (*.*)|*.*",
                    Multiselect = false
                };

                if (openFileDialog.ShowDialog() == true)
                {
                    _selectedFilePath1 = openFileDialog.FileName;
                    SelectedFilePath = _selectedFilePath1; // keep compatibility for validation/preview
                    FilePathTextBox1.Text = _selectedFilePath1;

                    try
                    {
                        var fi = new FileInfo(_selectedFilePath1);
                        FileInfoText1.Text = $"File: {fi.Name} ({fi.Length / 1024:N0} KB) - Modified: {fi.LastWriteTime:dd/MM/yyyy HH:mm}";
                    }
                    catch { FileInfoText1.Text = string.Empty; }

                    RebuildSelectedFilePaths();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error selecting file: {ex.Message}");
            }
        }

        /// <summary>
        /// Parcourir et sélectionner le second fichier Excel (optionnel)
        /// </summary>
        private void BrowseFile2Button_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var openFileDialog = new OpenFileDialog
                {
                    Title = "Select second AMBRE file (optional)",
                    Filter = "Excel Files (*.xlsx;*.xls)|*.xlsx;*.xls|All Files (*.*)|*.*",
                    Multiselect = false
                };

                if (openFileDialog.ShowDialog() == true)
                {
                    _selectedFilePath2 = openFileDialog.FileName;
                    FilePathTextBox2.Text = _selectedFilePath2;

                    try
                    {
                        var fi = new FileInfo(_selectedFilePath2);
                        FileInfoText2.Text = $"File: {fi.Name} ({fi.Length / 1024:N0} KB) - Modified: {fi.LastWriteTime:dd/MM/yyyy HH:mm}";
                    }
                    catch { FileInfoText2.Text = string.Empty; }

                    RebuildSelectedFilePaths();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error selecting file: {ex.Message}");
            }
        }

        /// <summary>
        /// Prévisualiser le mapping des colonnes
        /// </summary>
        private async void PreviewMappingButton_Click(object sender, RoutedEventArgs e)
        {
            await PreviewColumnMapping();
        }

        /// <summary>
        /// Valider le fichier sélectionné
        /// </summary>
        private async void ValidateButton_Click(object sender, RoutedEventArgs e)
        {
            await ValidateFile();
        }

        /// <summary>
        /// Démarrer l'import
        /// </summary>
        private async void ImportButton_Click(object sender, RoutedEventArgs e)
        {
            // Empêcher le double clic immédiat
            if (IsImporting || IsValidating)
                return;

            // Désactiver tout de suite avant l'async pour éviter 2ème déclenchement
            ImportButton.IsEnabled = false;
            await StartImport();
        }

        /// <summary>
        /// Annuler l'opération
        /// </summary>
        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            if (IsImporting || IsValidating)
            {
                CancelCurrentOperation();
            }
            else
            {
                Close();
            }
        }

        /// <summary>
        /// Ouvre la fenêtre d'import de réconciliation
        /// </summary>
        private void OpenReconciliationImportButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var window = new ReconciliationImportWindow(_offlineFirstService);
                window.Owner = this;
                window.ShowDialog();
            }
            catch (Exception ex)
            {
                ShowError($"Unable to open Reconciliation Import: {ex.Message}");
            }
        }

        #endregion

        #region File Processing

        /// <summary>
        /// Gestion du changement de fichier
        /// </summary>
        private void OnFilePathChanged()
        {
            // Keep status/UI in sync when SelectedFilePath changes
            UpdateStatusSummary();
            UpdateButtonStates();
        }

        /// <summary>
        /// Reconstruit la liste des fichiers sélectionnés à partir des deux champs
        /// </summary>
        private void RebuildSelectedFilePaths()
        {
            var list = new List<string>();
            if (!string.IsNullOrWhiteSpace(_selectedFilePath1)) list.Add(_selectedFilePath1);
            if (!string.IsNullOrWhiteSpace(_selectedFilePath2)) list.Add(_selectedFilePath2);
            SelectedFilePaths = list.ToArray();

            LogMessage(list.Count switch
            {
                0 => "No file selected",
                1 => $"File selected: {list[0]}",
                _ => $"Files selected: {string.Join(", ", list)}"
            });

            UpdateStatusSummary();
            UpdateButtonStates();
        }

        /// <summary>
        /// Prévisualise le mapping des colonnes
        /// </summary>
        private async Task PreviewColumnMapping()
        {
            if ((SelectedFilePaths == null || SelectedFilePaths.Length == 0) || CurrentCountry == null)
            {
                ShowWarning("Please select a country and an Excel file.");
                return;
            }
            if (SelectedFilePaths.Length > 1)
            {
                ShowInfo("Preview/validation is available for a single file. To validate both, please validate each file separately, or proceed directly to import.");
            }

            try
            {
                LogMessage("Analyzing file mapping...");
                ColumnMappings.Clear();

                // TODO: Utiliser le service pour analyser le fichier
                var mappings = await AnalyzeFileMapping(SelectedFilePath);
                foreach (var mapping in mappings)
                {
                    ColumnMappings.Add(mapping);
                }

                LogMessage($"Mapping analyzed: {ColumnMappings.Count} columns detected");
            }
            catch (Exception ex)
            {
                LogMessage($"Mapping error: {ex.Message}", true);
                ShowError($"Error analyzing mapping: {ex.Message}");
            }
        }

        /// <summary>
        /// Analyse le mapping des colonnes du fichier Excel réel
        /// </summary>
        private async Task<List<ColumnMappingInfo>> AnalyzeFileMapping(string filePath)
        {
            var mappings = new List<ColumnMappingInfo>();
            
            try
            {
                // Récupérer la configuration des champs d'import
                var importFields = _offlineFirstService.GetAmbreImportFields();
                if (!importFields.Any())
                {
                    return mappings; // Retourner liste vide si pas de configuration
                }
                
                // Ouvrir le fichier Excel pour analyser les en-têtes
                using (var excelHelper = new ExcelHelper())
                {
                    excelHelper.OpenFile(filePath);
                    var headers = excelHelper.GetHeaders(); // Méthode à implémenter dans ExcelHelper
                    var sampleData = excelHelper.ReadSampleData(3); // Lire 3 premières lignes comme échantillon
                    
                    // Créer le mapping basé sur la configuration
                    for (int i = 0; i < headers.Count && i < importFields.Count(); i++)
                    {
                        var field = importFields.ElementAt(i);
                        var columnLetter = GetExcelColumnName(i + 1); // A, B, C, etc.
                        var sampleValue = sampleData.Count > 0 && sampleData[0].Count > i 
                            ? sampleData[0][i]?.ToString() ?? ""
                            : "";
                            
                        mappings.Add(new ColumnMappingInfo
                        {
                            ExcelColumn = columnLetter,
                            ColumnName = headers[i],
                            DestinationField = field.AMB_Destination,
                            Transformation = GetTransformationType(field.AMB_Destination),
                            IsMandatory = IsMandatoryField(field.AMB_Destination),
                            SampleValue = sampleValue
                        });
                    }
                }
                
                return mappings;
            }
            catch (Exception ex)
            {
                LogMessage($"Error analyzing mapping: {ex.Message}", true);
                return mappings; // Retourner liste vide en cas d'erreur
            }
        }
        
        /// <summary>
        /// Convertit un numéro de colonne en lettre Excel (1=A, 2=B, etc.)
        /// </summary>
        private string GetExcelColumnName(int columnNumber)
        {
            string columnName = "";
            while (columnNumber > 0)
            {
                int modulo = (columnNumber - 1) % 26;
                columnName = Convert.ToChar('A' + modulo) + columnName;
                columnNumber = (columnNumber - modulo) / 26;
            }
            return columnName;
        }
        
        /// <summary>
        /// Détermine le type de transformation basé sur le champ de destination
        /// </summary>
        private string GetTransformationType(string destinationField)
        {
            switch (destinationField?.ToLower())
            {
                case "valuedate":
                case "operationdate":
                    return "Date";
                case "signedamount":
                case "localsignedamount":
                    return "Decimal";
                default:
                    return "Text";
            }
        }
        
        /// <summary>
        /// Détermine si un champ est obligatoire
        /// </summary>
        private bool IsMandatoryField(string destinationField)
        {
            var mandatoryFields = new[] { "valuedate", "signedamount", "ccy" };
            return mandatoryFields.Contains(destinationField?.ToLower());
        }

        #endregion

        #region Validation

        /// <summary>
        /// Valide le fichier sélectionné via AmbreImportService
        /// </summary>
        private async Task ValidateFile()
        {
            if (string.IsNullOrEmpty(SelectedFilePath) || CurrentCountry == null)
            {
                ShowWarning("Please select a country and an Excel file.");
                return;
            }

            try
            {
                IsValidating = true;
                _cancellationTokenSource = new CancellationTokenSource();
                
                LogMessage("Starting file validation...");
                UpdateProgress(0, "Validation in progress...");

                // Validation réelle via AmbreImportService (sans import)
                _validationResult = await ValidateFileViaService(_cancellationTokenSource.Token);

                if (_validationResult.IsValid)
                {
                    LogMessage($"Validation successful: {_validationResult.RecordCount} rows detected");
                    UpdateProgress(100, "Validation completed successfully");
                    ShowInfo($"Validation successful!\n\nRows detected: {_validationResult.RecordCount}\nNo errors found.");
                }
                else
                {
                    LogMessage("Validation failed:", true);
                    foreach (var error in _validationResult.Errors)
                    {
                        LogMessage($"  - {error}", true);
                    }
                    ShowError($"Validation failed:\n{string.Join("\n", _validationResult.Errors)}");
                }
            }
            catch (OperationCanceledException)
            {
                LogMessage("Validation canceled by user");
                UpdateProgress(0, "Validation canceled");
            }
            catch (Exception ex)
            {
                LogMessage($"Error during validation: {ex.Message}", true);
                ShowError($"Error during validation: {ex.Message}");
            }
            finally
            {
                IsValidating = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        /// <summary>
        /// Valide le fichier via AmbreImportService (validation réelle sans import complet)
        /// </summary>
        private async Task<ImportValidationResult> ValidateFileViaService(CancellationToken cancellationToken)
        {
            var result = new ImportValidationResult();
            
            try
            {
                UpdateProgress(10, "Loading import fields...");
                cancellationToken.ThrowIfCancellationRequested();
                
                // Récupérer les champs d'import depuis OfflineFirstService
                var importFields = _offlineFirstService.GetAmbreImportFields();
                if (!importFields.Any())
                {
                    result.Errors.Add("No import fields configured for this country");
                    return result;
                }

                UpdateProgress(20, "Opening Excel file...");
                cancellationToken.ThrowIfCancellationRequested();
                
                // Ouvrir et lire le fichier Excel pour validation
                using (var excelHelper = new ExcelHelper())
                {
                    excelHelper.OpenFile(SelectedFilePath);
                    var rawData = excelHelper.ReadSheetData(null, importFields);
                    
                    UpdateProgress(50, "Validating data...");
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    if (!rawData.Any())
                    {
                        result.Errors.Add("No data found in file");
                        return result;
                    }
                    
                    result.RecordCount = rawData.Count();
                    
                    UpdateProgress(70, "Validating transformations...");
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    // Valider quelques échantillons pour détecter les erreurs
                    var sampleData = rawData.Take(Math.Min(10, rawData.Count())).ToList();
                    var errors = new List<string>();
                    
                    foreach (var row in sampleData)
                    {
                        // Validation basique des champs obligatoires
                        if (!row.ContainsKey("ValueDate") || row["ValueDate"] == null)
                            errors.Add("Field 'ValueDate' missing or empty");
                            
                        if (!row.ContainsKey("SignedAmount") || row["SignedAmount"] == null)
                            errors.Add("Field 'SignedAmount' missing or empty");
                            
                        if (!row.ContainsKey("CCY") || string.IsNullOrEmpty(row["CCY"]?.ToString()))
                            errors.Add("Field 'CCY' missing or empty");
                    }
                    
                    UpdateProgress(90, "Finalizing validation...");
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    result.Errors = errors.Distinct().ToList();
                    result.IsValid = !result.Errors.Any();
                }
                
                UpdateProgress(100, "Validation completed");
                return result;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Error during validation: {ex.Message}");
                result.IsValid = false;
                return result;
            }
        }

        #endregion

        #region Import Process

        /// <summary>
        /// Démarre le processus d'import
        /// </summary>
        private async Task StartImport()
        {
            //if (_validationResult == null || !_validationResult.IsValid)
            //{
            //    ShowWarning("Veuillez d'abord valider le fichier avant de démarrer l'import.");
            //    return;
            //}

            //var result = MessageBox.Show(
            //    $"Êtes-vous sûr de vouloir importer {_validationResult.RecordCount} lignes pour {CurrentCountry.CNT_Name}?\n\nCette opération peut prendre plusieurs minutes.",
            //    "Confirmation d'import",
            //    MessageBoxButton.YesNo,
            //    MessageBoxImage.Question);

            //if (result != MessageBoxResult.Yes)
            //    return;

            try
            {
                IsImporting = true;
                _cancellationTokenSource = new CancellationTokenSource();
                
                LogMessage("Starting import...");
                
                var importOptions = new ImportOptions
                {
                    CountryId = CurrentCountry.CNT_Id,
                    FilePath = SelectedFilePath,
                    StartRow = 2, // Valeur par défaut
                    DetectChanges = true,
                    ApplyTransformations = true,
                    ApplyRules = true,
                    CreateBackup = true
                };

                // (policy change) No pre-import snapshot. We'll freeze the previous snapshot after success.

                var importResult = await PerformImport(importOptions, _cancellationTokenSource.Token);

                if (importResult.Success)
                {
                    LogMessage($"Import completed successfully!");
                    LogMessage($"  - Imported rows: {importResult.ImportedCount}");
                    LogMessage($"  - Updated rows: {importResult.UpdatedCount}");
                    LogMessage($"  - Deleted rows: {importResult.DeletedCount}");
                    
                    UpdateProgress(100, "Import completed successfully");
                    
                    ShowInfo($"Import completed successfully!\n\nImported rows: {importResult.ImportedCount}\nUpdated rows: {importResult.UpdatedCount}\nDeleted rows: {importResult.DeletedCount}");

                    // Snapshot freeze/insert is handled in AmbreImportService under the global lock.
                    
                    // Fermer la fenêtre après succès
                    DialogResult = true;
                }
                else
                {
                    LogMessage("Import failed:", true);
                    foreach (var error in importResult.Errors)
                    {
                        LogMessage($"  - {error}", true);
                    }
                    ShowError($"Import failed:\n{string.Join("\n", importResult.Errors)}");
                }
            }
            catch (OperationCanceledException)
            {
                LogMessage("Import canceled by user");
                UpdateProgress(0, "Import canceled");
            }
            catch (Exception ex)
            {
                LogMessage($"Error during import: {ex.Message}", true);
                ShowError($"Error during import: {ex.Message}");
            }
            finally
            {
                IsImporting = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        /// <summary>
        /// Effectue l'import via AmbreImportService
        /// </summary>
        private async Task<ImportResult> PerformImport(ImportOptions options, CancellationToken cancellationToken)
        {
            try
            {
                // Appel réel à AmbreImportService avec callback de progression
                var svcResult = (SelectedFilePaths != null && SelectedFilePaths.Length > 1)
                    ? await _ambreImportService.ImportAmbreFiles(
                        SelectedFilePaths,
                        CurrentCountry?.CNT_Id,
                        (message, progress) =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            UpdateProgress(progress, message);
                            LogMessage(message);
                        })
                    : await _ambreImportService.ImportAmbreFile(
                        SelectedFilePath,
                        CurrentCountry?.CNT_Id,
                        (message, progress) =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            UpdateProgress(progress, message);
                            LogMessage(message);
                        });

                // Convertir ImportResult d'AmbreImportService vers celui de l'UI
                return new ImportResult
                {
                    Success = svcResult.IsSuccess,
                    ImportedCount = svcResult.NewRecords,
                    UpdatedCount = svcResult.UpdatedRecords,
                    DeletedCount = svcResult.DeletedRecords,
                    Errors = svcResult.Errors?.ToList() ?? new List<string>()
                };
            }
            catch (OperationCanceledException)
            {
                throw; // Laisser passer l'annulation
            }
            catch (Exception ex)
            {
                return new ImportResult
                {
                    Success = false,
                    ImportedCount = 0,
                    UpdatedCount = 0,
                    DeletedCount = 0,
                    Errors = new List<string> { ex.Message }
                };
            }
        }

        #endregion

        #region UI Updates

        /// <summary>
        /// Met à jour l'état des boutons
        /// </summary>
        private void UpdateButtonStates()
        {
            var hasFile = SelectedFilePaths != null && SelectedFilePaths.Length > 0;
            var hasCountry = CurrentCountry != null;
            var canValidate = hasFile && hasCountry && !IsImporting && !IsValidating;
            // Autoriser l'import si fichier + pays sélectionnés et aucune opération en cours
            var canImport = hasFile && hasCountry && !IsImporting && !IsValidating;

            // Empêcher les doubles clics en désactivant le bouton pendant import/validation
            ImportButton.IsEnabled = canImport;

            // Changer le texte du bouton Annuler
            CancelButton.Content = (IsImporting || IsValidating) ? "Cancel operation" : "Close";
        }

        /// <summary>
        /// Met à jour la barre de progression
        /// </summary>
        private void UpdateProgress(int percentage, string status)
        {
            Dispatcher.Invoke(() =>
            {
                ImportProgressBar.Value = percentage;
                ProgressStatusText.Text = status;
            });
        }

        /// <summary>
        /// Met à jour le résumé de statut
        /// </summary>
        private void UpdateStatusSummary()
        {
            var parts = new List<string>();
            
            if (CurrentCountry != null)
                parts.Add($"Pays: {CurrentCountry.CNT_Name}");
            
            if (SelectedFilePaths != null && SelectedFilePaths.Length > 0)
            {
                if (SelectedFilePaths.Length == 1)
                    parts.Add($"Fichier: {Path.GetFileName(SelectedFilePaths[0])}");
                else
                    parts.Add($"Fichiers: {string.Join(", ", SelectedFilePaths.Select(Path.GetFileName))}");
            }
            
            if (_validationResult?.IsValid == true)
                parts.Add($"Validé ({_validationResult.RecordCount} lignes)");
            
            StatusSummaryText.Text = parts.Count > 0 ? string.Join(" | ", parts) : "Aucun fichier sélectionné";
        }

        /// <summary>
        /// Ajoute un message au log
        /// </summary>
        private void LogMessage(string message, bool isError = false)
        {
            Dispatcher.Invoke(() =>
            {
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                var prefix = isError ? "[ERREUR]" : "[INFO]";
                var logEntry = $"{timestamp} {prefix} {message}\n";
                
                ImportLogText.Text += logEntry;
                
                // Auto-scroll vers le bas
                LogScrollViewer.ScrollToEnd();
            });
        }

        #endregion

        #region Operations

        /// <summary>
        /// Annule l'opération en cours
        /// </summary>
        private void CancelCurrentOperation()
        {
            try
            {
                _cancellationTokenSource?.Cancel();
                LogMessage("Annulation demandée...");
            }
            catch (Exception ex)
            {
                LogMessage($"Erreur lors de l'annulation: {ex.Message}", true);
            }
        }

        #endregion

        #region Helper Methods

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
    }

    #region Helper Classes

    /// <summary>
    /// Informations de mapping d'une colonne
    /// </summary>
    public class ColumnMappingInfo
    {
        public string ExcelColumn { get; set; }
        public string ColumnName { get; set; }
        public string DestinationField { get; set; }
        public string Transformation { get; set; }
        public bool IsMandatory { get; set; }
        public string SampleValue { get; set; }
    }

    /// <summary>
    /// Résultat de validation d'un fichier
    /// </summary>
    public class ImportValidationResult
    {
        public bool IsValid { get; set; }
        public int RecordCount { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
    }

    /// <summary>
    /// Options d'import
    /// </summary>
    public class ImportOptions
    {
        public string CountryId { get; set; }
        public string FilePath { get; set; }
        public int StartRow { get; set; }
        public bool DetectChanges { get; set; }
        public bool ApplyTransformations { get; set; }
        public bool ApplyRules { get; set; }
        public bool CreateBackup { get; set; }
    }

    /// <summary>
    /// Résultat d'import
    /// </summary>
    public class ImportResult
    {
        public bool Success { get; set; }
        public int ImportedCount { get; set; }
        public int UpdatedCount { get; set; }
        public int DeletedCount { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
    }

    #endregion
}
