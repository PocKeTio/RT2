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
            IEnumerable<AmbreImportField> importFields = null, int startRow = 2, Action<int> progress = null)
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

                // Lire les données via un accès massif (Range.Value2) pour réduire les appels COM
                var data = new List<Dictionary<string, object>>();

                var usedRange = worksheet.UsedRange;
                int lastRow = usedRange.Rows.Count;
                int lastColumn = usedRange.Columns.Count;

                if (lastRow < startRow)
                {
                    return data; // aucune donnée
                }

                Range dataRange = worksheet.Range[worksheet.Cells[startRow, 1], worksheet.Cells[lastRow, lastColumn]];
                object raw = dataRange.Value2;

                // Excel retourne un object[,] si plusieurs cellules, sinon une valeur simple
                object[,] values = raw as object[,];
                if (values == null)
                {
                    // Cas d'une seule ligne/colonne: normaliser en tableau 2D 1-based
                    values = new object[2, 2];
                    values[1, 1] = raw;
                }

                int totalRows = lastRow - startRow + 1;
                for (int r = 1; r <= values.GetLength(0); r++)
                {
                    var rowDict = new Dictionary<string, object>();

                    foreach (var kvp in columnMapping)
                    {
                        int colIndex = kvp.Key;
                        string fieldName = kvp.Value;

                        if (colIndex >= 1 && colIndex <= values.GetLength(1))
                        {
                            rowDict[fieldName] = values[r, colIndex];
                        }
                        else
                        {
                            rowDict[fieldName] = null;
                        }
                    }

                    if (rowDict.Any(kvp => kvp.Value != null && !string.IsNullOrEmpty(kvp.Value.ToString())))
                    {
                        data.Add(rowDict);
                    }

                    // Progression (en pourcentage)
                    //if (progress != null)
                    //{
                    //    int currentRow = r;
                    //    int percent = (int)Math.Round(currentRow * 100.0 / totalRows);
                    //    if (percent > 100) percent = 100;
                    //    progress(percent);
                    //}
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
            Range usedRange = null;
            Range headerRange = null;
            try
            {
                usedRange = worksheet.UsedRange;
                var lastColumn = usedRange.Columns.Count;

                headerRange = worksheet.Range[worksheet.Cells[1, 1], worksheet.Cells[1, lastColumn]];
                object raw = headerRange.Value2;

                if (raw is object[,] values)
                {
                    for (int col = 1; col <= lastColumn; col++)
                    {
                        var cellValue = values[1, col];
                        headers.Add(cellValue?.ToString() ?? $"Column{col}");
                    }
                }
                else
                {
                    // Fallback: une seule cellule
                    headers.Add(raw?.ToString() ?? "Column1");
                }
            }
            finally
            {
                try { if (headerRange != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(headerRange); } catch { }
                try { if (usedRange != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(usedRange); } catch { }
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
        /// Lit une feuille par lettres de colonnes (ex: A, B, AA) à partir d'une ligne de départ.
        /// Arrête à la première ligne où l'ID (colonne idColumnLetter) est vide.
        /// Retourne des dictionnaires avec au minimum la clé "ID" et les clés de destination fournies.
        /// </summary>
        /// <param name="sheetName">Nom de la feuille (ex: PIVOT, RECEIVABLE)</param>
        /// <param name="letterToDestination">Mapping lettre de colonne -> nom de champ destination</param>
        /// <param name="startRow">Ligne de départ des données (1-based)</param>
        /// <param name="idColumnLetter">Lettre de la colonne contenant l'ID (par défaut A)</param>
        /// <returns>Liste de lignes (dictionnaires)</returns>
        public List<Dictionary<string, object>> ReadSheetByColumns(string sheetName,
            Dictionary<string, string> letterToDestination,
            int startRow,
            string idColumnLetter = "A")
        {
            if (_workbook == null)
                throw new InvalidOperationException("Aucun fichier Excel ouvert.");

            if (string.IsNullOrWhiteSpace(sheetName))
                throw new ArgumentException("sheetName est requis");

            Worksheet worksheet = null;
            try
            {
                // Resolve worksheet robustly (case/trim-insensitive)
                try
                {
                    worksheet = FindWorksheetByName(sheetName);
                    if (worksheet == null)
                        throw new Exception($"Feuille '{sheetName}' introuvable dans le classeur.");
                }
                catch (System.Runtime.InteropServices.COMException)
                {
                    // Transient COM glitch: small wait and retry once
                    System.Threading.Thread.Sleep(100);
                    worksheet = FindWorksheetByName(sheetName);
                    if (worksheet == null)
                        throw;
                }

                if (startRow < 1) startRow = 1;

                // 1) Resolve columns and offsets
                var (idColIndex, destToOffset, minCol, maxCol) =
                    ResolveColumnMappings(letterToDestination, idColumnLetter);

                // 2) Read ID column to determine actual row count
                int actualRows = ReadIdColumn(worksheet, startRow, idColIndex);
                if (actualRows == 0)
                {
                    return new List<Dictionary<string, object>>();
                }

                // 3) Read all required values in one block
                object[,] blockVals = ReadDataBlock(worksheet, startRow, actualRows, minCol, maxCol);

                // 4) Materialize dictionaries from the block
                return MaterializeRows(blockVals, actualRows, idColIndex, minCol, destToOffset);
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors de la lecture de la feuille '{sheetName}': {ex.Message}", ex);
            }
            finally
            {
                try { if (worksheet != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(worksheet); } catch { }
            }
        }

        /// <summary>
        /// Recherche une feuille par nom de manière robuste (trim + case-insensitive).
        /// Retourne un RCW Worksheet que l'appelant DOIT libérer (FinalReleaseComObject) quand terminé.
        /// </summary>
        private Worksheet FindWorksheetByName(string sheetName)
        {
            var target = (sheetName ?? string.Empty).Trim();
            // Essai direct (souvent plus rapide)
            try
            {
                var direct = _workbook.Sheets[target] as Worksheet;
                if (direct != null) return direct;
            }
            catch { /* continue with scan */ }

            // Recherche en scannant et en comparant de façon insensible à la casse et aux espaces
            foreach (object obj in _workbook.Sheets)
            {
                Worksheet ws = null;
                bool keep = false;
                try
                {
                    ws = obj as Worksheet;
                    if (ws != null)
                    {
                        var name = ws.Name?.Trim();
                        if (!string.IsNullOrEmpty(name) && string.Equals(name, target, StringComparison.OrdinalIgnoreCase))
                        {
                            // Conserver cette feuille et la retourner sans la libérer
                            keep = true;
                            return ws;
                        }
                    }
                }
                finally
                {
                    if (!keep)
                    {
                        // Libérer les éléments non retenus
                        if (ws != null)
                        {
                            try { System.Runtime.InteropServices.Marshal.FinalReleaseComObject(ws); } catch { }
                        }
                        else
                        {
                            try { System.Runtime.InteropServices.Marshal.FinalReleaseComObject(obj); } catch { }
                        }
                    }
                }
            }
            return null;
        }

        private (int idColIndex, Dictionary<string, int> destToOffset, int minCol, int maxCol) ResolveColumnMappings(
            Dictionary<string, string> letterToDestination, string idColumnLetter)
        {
            int idColIndex = ColumnLetterToIndex(idColumnLetter);
            var destToColIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in letterToDestination)
            {
                var letter = (kv.Key ?? string.Empty).Trim();
                if (string.IsNullOrEmpty(letter)) continue;
                destToColIndex[kv.Value] = ColumnLetterToIndex(letter);
            }

            int minCol = idColIndex;
            int maxCol = idColIndex;
            foreach (var c in destToColIndex.Values)
            {
                if (c < minCol) minCol = c;
                if (c > maxCol) maxCol = c;
            }

            var destToOffset = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in destToColIndex)
            {
                destToOffset[kv.Key] = kv.Value - minCol + 1; // 1-based in block
            }

            return (idColIndex, destToOffset, minCol, maxCol);
        }

        private int ReadIdColumn(Worksheet worksheet, int startRow, int idColIndex)
        {
            // Prefer End(xlUp) on the ID column to find last used row instead of UsedRange
            int sheetMaxRow = worksheet.Rows.Count;
            int lastRow = worksheet.Cells[sheetMaxRow, idColIndex].End(XlDirection.xlUp).Row;

            int actualRows = 0;
            if (lastRow >= startRow)
            {
                Range idRange = null;
                try
                {
                    idRange = worksheet.Range[
                        worksheet.Cells[startRow, idColIndex],
                        worksheet.Cells[lastRow, idColIndex]
                    ];
                    object idRaw = idRange.Value2;
                    if (idRaw is object[,] idVals)
                    {
                        int rowsCount = idVals.GetLength(0);
                        for (int i = 1; i <= rowsCount; i++)
                        {
                            var v = idVals[i, 1];
                            var s = v?.ToString();
                            if (string.IsNullOrWhiteSpace(s)) { break; }
                            actualRows++;
                        }
                    }
                    else
                    {
                        var s = idRaw?.ToString();
                        actualRows = string.IsNullOrWhiteSpace(s) ? 0 : 1;
                    }
                }
                finally
                {
                    try { if (idRange != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(idRange); } catch { }
                }
            }

            return actualRows;
        }

        private object[,] ReadDataBlock(Worksheet worksheet, int startRow, int actualRows, int minCol, int maxCol)
        {
            Range block = null;
            try
            {
                block = worksheet.Range[
                    worksheet.Cells[startRow, minCol],
                    worksheet.Cells[startRow + actualRows - 1, maxCol]
                ];
                object blockRaw = block.Value2;
                if (blockRaw is object[,] blockVals)
                {
                    return blockVals;
                }

                // Single cell block (rare): normalize to 1x1
                var single = new object[2, 2];
                single[1, 1] = blockRaw;
                return single;
            }
            finally
            {
                try { if (block != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(block); } catch { }
            }
        }

        private List<Dictionary<string, object>> MaterializeRows(
            object[,] blockVals,
            int actualRows,
            int idColIndex,
            int minCol,
            Dictionary<string, int> destToOffset)
        {
            var rows = new List<Dictionary<string, object>>(actualRows > 0 ? actualRows : 0);
            int idOffset = idColIndex - minCol + 1; // 1-based inside block
            for (int rRel = 1; rRel <= actualRows; rRel++)
            {
                var idObj = blockVals[rRel, idOffset];
                var idStr = idObj?.ToString();
                if (string.IsNullOrWhiteSpace(idStr)) break; // safety

                var dict = new Dictionary<string, object>(destToOffset.Count + 1, StringComparer.OrdinalIgnoreCase)
                {
                    ["ID"] = idStr
                };

                foreach (var kv in destToOffset)
                {
                    string dest = kv.Key;
                    int offset = kv.Value;
                    object v = blockVals[rRel, offset];
                    dict[dest] = v;
                }

                rows.Add(dict);
            }

            return rows;
        }

        private static int ColumnLetterToIndex(string letter)
        {
            if (string.IsNullOrWhiteSpace(letter)) throw new ArgumentException("Lettre de colonne invalide");
            letter = letter.Trim().ToUpperInvariant();
            int sum = 0;
            foreach (char c in letter)
            {
                if (c < 'A' || c > 'Z') throw new ArgumentException($"Lettre de colonne invalide: {letter}");
                sum = sum * 26 + (c - 'A' + 1);
            }
            return sum;
        }

        /// <summary>
        /// Ferme le fichier Excel
        /// </summary>
        public void CloseFile()
        {
            if (_workbook != null)
            {
                try
                {
                    _workbook.Close(false);
                }
                catch
                {
                    // ignore, best-effort close
                }
                finally
                {
                    try
                    {
                        System.Runtime.InteropServices.Marshal.ReleaseComObject(_workbook);
                    }
                    catch
                    {
                        // ignore, best-effort release
                    }
                    _workbook = null;
                }
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
