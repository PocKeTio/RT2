using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Microsoft.Office.Interop.Excel;
using RecoTool.Models;

namespace RecoTool.Helpers
{
    /// <summary>
    /// Helper pour la lecture et manipulation des fichiers Excel
    /// </summary>
    public class ExcelHelper : IDisposable
    {
        private Application _excelApp;
        private Workbook _workbook;
        private bool _disposed = false;

        public ExcelHelper()
        {
            _excelApp = new Application();
            _excelApp.Visible = false;
            _excelApp.DisplayAlerts = false;
        }

        /// <summary>
        /// Ouvre un fichier Excel
        /// </summary>
        /// <param name="filePath">Chemin vers le fichier Excel</param>
        /// <returns>True si succès</returns>
        public bool OpenFile(string filePath)
        {
            try
            {
                if (!File.Exists(filePath))
                    throw new FileNotFoundException($"Le fichier {filePath} n'existe pas.");

                _workbook = _excelApp.Workbooks.Open(filePath);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors de l'ouverture du fichier Excel: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Lit les données d'une feuille Excel en appliquant le mapping des champs
        /// </summary>
        /// <param name="sheetName">Nom de la feuille (optionnel, prend la première si null)</param>
        /// <param name="importFields">Mapping des champs d'import</param>
        /// <param name="startRow">Ligne de début (défaut: 2 pour ignorer l'en-tête)</param>
        /// <returns>Liste des données lues sous forme de dictionnaire</returns>
        public List<Dictionary<string, object>> ReadSheetData(string sheetName = null, 
            IEnumerable<AmbreImportField> importFields = null, int startRow = 2)
        {
            if (_workbook == null)
                throw new InvalidOperationException("Aucun fichier Excel ouvert.");

            try
            {
                // Sélectionner la feuille
                Worksheet worksheet = string.IsNullOrEmpty(sheetName) 
                    ? _workbook.Sheets[1] 
                    : _workbook.Sheets[sheetName];

                // Lire l'en-tête (ligne 1)
                var headers = ReadRowHeaders(worksheet);
                
                // Créer le mapping des colonnes
                var columnMapping = CreateColumnMapping(headers, importFields);

                // Vérifier que tous les champs d'import sont bien mappés
                if (importFields != null)
                {
                    var expectedDestinations = importFields
                        .Where(f => !string.IsNullOrWhiteSpace(f.AMB_Destination))
                        .Select(f => f.AMB_Destination)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    var mappedDestinations = new HashSet<string>(columnMapping.Values, StringComparer.OrdinalIgnoreCase);
                    var missing = expectedDestinations
                        .Where(d => !mappedDestinations.Contains(d))
                        .ToList();

                    if (missing.Any())
                    {
                        var msg = $"Champs manquants dans le fichier Excel: {string.Join(", ", missing)}";
                        throw new Exception(msg);
                    }
                }

                // Lire les données
                var data = new List<Dictionary<string, object>>();
                var lastRow = worksheet.UsedRange.Rows.Count;

                for (int row = startRow; row <= lastRow; row++)
                {
                    var rowData = ReadRowData(worksheet, row, columnMapping);
                    if (rowData.Any(kvp => kvp.Value != null && !string.IsNullOrEmpty(kvp.Value.ToString())))
                    {
                        data.Add(rowData);
                    }
                }

                return data;
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors de la lecture des données Excel: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Lit les en-têtes de colonne (ligne 1)
        /// </summary>
        private List<string> ReadRowHeaders(Worksheet worksheet)
        {
            var headers = new List<string>();
            var lastColumn = worksheet.UsedRange.Columns.Count;

            for (int col = 1; col <= lastColumn; col++)
            {
                var cellValue = worksheet.Cells[1, col].Value2;
                headers.Add(cellValue?.ToString() ?? $"Column{col}");
            }

            return headers;
        }

        /// <summary>
        /// Crée le mapping entre les colonnes Excel et les champs de destination
        /// </summary>
        private Dictionary<int, string> CreateColumnMapping(List<string> headers, 
            IEnumerable<AmbreImportField> importFields)
        {
            var mapping = new Dictionary<int, string>();

            if (importFields == null)
            {
                // Si pas de mapping défini, utilise les en-têtes directement
                for (int i = 0; i < headers.Count; i++)
                {
                    mapping[i + 1] = headers[i];
                }
            }
            else
            {
                // Applique le mapping configuré
                foreach (var field in importFields)
                {
                    var columnIndex = headers.FindIndex(h => 
                        string.Equals(h, field.AMB_Source, StringComparison.OrdinalIgnoreCase));
                    
                    if (columnIndex >= 0)
                    {
                        mapping[columnIndex + 1] = field.AMB_Destination;
                    }
                }
            }

            return mapping;
        }

        /// <summary>
        /// Lit les données d'une ligne spécifique
        /// </summary>
        private Dictionary<string, object> ReadRowData(Worksheet worksheet, int rowNumber, 
            Dictionary<int, string> columnMapping)
        {
            var rowData = new Dictionary<string, object>();

            foreach (var kvp in columnMapping)
            {
                var columnIndex = kvp.Key;
                var fieldName = kvp.Value;
                
                var cellValue = worksheet.Cells[rowNumber, columnIndex].Value2;
                rowData[fieldName] = cellValue;
            }

            return rowData;
        }

        /// <summary>
        /// Valide le format du fichier Excel
        /// </summary>
        /// <param name="filePath">Chemin vers le fichier</param>
        /// <returns>True si le format est valide</returns>
        public static bool ValidateExcelFormat(string filePath)
        {
            if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
                return false;

            var extension = Path.GetExtension(filePath).ToLower();
            return extension == ".xlsx" || extension == ".xls";
        }

        /// <summary>
        /// Obtient la liste des feuilles du classeur
        /// </summary>
        /// <returns>Liste des noms de feuilles</returns>
        public List<string> GetSheetNames()
        {
            if (_workbook == null)
                throw new InvalidOperationException("Aucun fichier Excel ouvert.");

            var sheetNames = new List<string>();
            
            foreach (Worksheet sheet in _workbook.Sheets)
            {
                sheetNames.Add(sheet.Name);
            }

            return sheetNames;
        }

        /// <summary>
        /// Obtient les en-têtes de colonnes de la première feuille
        /// </summary>
        /// <returns>Liste des en-têtes de colonnes</returns>
        public List<string> GetHeaders()
        {
            if (_workbook == null)
                throw new InvalidOperationException("Aucun fichier Excel ouvert.");

            var worksheet = _workbook.Sheets[1] as Worksheet;
            return ReadRowHeaders(worksheet);
        }

        /// <summary>
        /// Lit un échantillon de données (premières lignes) pour aperçu
        /// </summary>
        /// <param name="maxRows">Nombre maximum de lignes à lire</param>
        /// <returns>Liste de listes représentant les lignes de données</returns>
        public List<List<object>> ReadSampleData(int maxRows = 5)
        {
            if (_workbook == null)
                throw new InvalidOperationException("Aucun fichier Excel ouvert.");

            var sampleData = new List<List<object>>();
            var worksheet = _workbook.Sheets[1] as Worksheet;
            var lastColumn = worksheet.UsedRange.Columns.Count;
            var lastRow = Math.Min(worksheet.UsedRange.Rows.Count, maxRows + 1); // +1 pour ignorer l'en-tête

            // Commencer à la ligne 2 pour ignorer l'en-tête
            for (int row = 2; row <= lastRow; row++)
            {
                var rowData = new List<object>();
                for (int col = 1; col <= lastColumn; col++)
                {
                    var cellValue = worksheet.Cells[row, col].Value2;
                    rowData.Add(cellValue);
                }
                sampleData.Add(rowData);
            }

            return sampleData;
        }

        /// <summary>
        /// Ferme le fichier Excel
        /// </summary>
        public void CloseFile()
        {
            if (_workbook != null)
            {
                _workbook.Close(false);
                _workbook = null;
            }
        }

        /// <summary>
        /// Libère les ressources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    CloseFile();
                    
                    if (_excelApp != null)
                    {
                        _excelApp.Quit();
                        System.Runtime.InteropServices.Marshal.ReleaseComObject(_excelApp);
                        _excelApp = null;
                    }
                }
                _disposed = true;
            }
        }

        ~ExcelHelper()
        {
            Dispose(false);
        }
    }
}
