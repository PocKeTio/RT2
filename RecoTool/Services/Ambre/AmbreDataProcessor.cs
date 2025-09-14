using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;

namespace RecoTool.Services.AmbreImport
{
    /// <summary>
    /// Processeur de données pour l'import Ambre
    /// </summary>
    public class AmbreDataProcessor
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly string _currentUser;
        private AmbreConfigurationLoader _configurationLoader;

        public AmbreDataProcessor(OfflineFirstService offlineFirstService, string currentUser)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = currentUser;
        }

        public void SetConfigurationLoader(AmbreConfigurationLoader loader)
        {
            _configurationLoader = loader;
        }

        /// <summary>
        /// Lit les fichiers Excel et retourne les données brutes
        /// </summary>
        public async Task<List<Dictionary<string, object>>> ReadExcelFilesAsync(
            string[] files, 
            IEnumerable<AmbreImportField> importFields, 
            bool isMultiFile, 
            Action<string, int> progressCallback)
        {
            try { await _offlineFirstService.SetSyncStatusAsync("Importing"); } catch { }
            
            var allRaw = new List<Dictionary<string, object>>();
            
            for (int i = 0; i < files.Length; i++)
            {
                var filePath = files[i];
                var baseStart = isMultiFile ? 20 + (i * 10) : 20;
                
                var raw = await ReadExcelFileAsync(filePath, importFields, progress =>
                {
                    int mapped = baseStart + (progress * (isMultiFile ? 10 : 20) / 100);
                    if (mapped > 40) mapped = 40;
                    
                    var label = isMultiFile 
                        ? $"Reading: {Path.GetFileName(filePath)} ({progress}%)" 
                        : $"Reading Excel file... ({progress}%)";
                    progressCallback?.Invoke(label, mapped);
                });
                
                allRaw.AddRange(raw);
            }
            
            progressCallback?.Invoke(isMultiFile ? "Excel files read complete" : "Excel file read complete", 40);
            return allRaw;
        }

        /// <summary>
        /// Transforme les données brutes en objets DataAmbre
        /// </summary>
        public async Task<List<DataAmbre>> TransformDataAsync(
            List<Dictionary<string, object>> rawData,
            IEnumerable<AmbreTransform> transforms, 
            Country country)
        {
            return await Task.Run(() =>
            {
                var transformedData = new List<DataAmbre>();
                var transformationService = _configurationLoader?.TransformationService;

                foreach (var row in rawData)
                {
                    var dataAmbre = new DataAmbre();

                    // Application des transformations configurées
                    foreach (var transform in transforms)
                    {
                        var transformedValue = transformationService?.ApplyTransformation(row, transform);
                        SetPropertyValue(dataAmbre, transform.AMB_Destination, transformedValue);
                    }

                    // Copie des champs directs
                    CopyDirectFields(row, dataAmbre);

                    // Génération de l'ID unique
                    dataAmbre.ID = dataAmbre.GetUniqueKey();

                    // Application de la catégorisation
                    ApplyCategorization(dataAmbre, country);

                    transformedData.Add(dataAmbre);
                }

                return transformedData;
            });
        }

        /// <summary>
        /// Filtre les lignes par comptes du pays
        /// </summary>
        public List<Dictionary<string, object>> FilterRowsByCountryAccounts(
            List<Dictionary<string, object>> rawData, 
            Country country)
        {
            if (country == null)
                throw new InvalidOperationException("Configuration pays manquante.");

            var pivot = country.CNT_AmbrePivot?.Trim();
            var recv = country.CNT_AmbreReceivable?.Trim();

            if (string.IsNullOrWhiteSpace(pivot) && string.IsNullOrWhiteSpace(recv))
            {
                throw new InvalidOperationException(
                    $"Aucun compte Pivot/Receivable défini pour le pays {country.CNT_Id}.");
            }

            return rawData.Where(row =>
            {
                if (!row.TryGetValue("Account_ID", out var val) || val == null)
                    return false;
                    
                var accountId = val.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(accountId)) 
                    return false;
                    
                return IsMatchingAccount(accountId, pivot, recv);
            }).ToList();
        }

        private async Task<List<Dictionary<string, object>>> ReadExcelFileAsync(
            string filePath,
            IEnumerable<AmbreImportField> importFields, 
            Action<int> progress = null)
        {
            return await Task.Run(() =>
            {
                using (var excelHelper = new ExcelHelper())
                {
                    excelHelper.OpenFile(filePath);
                    return excelHelper.ReadSheetData(null, importFields, 2, progress);
                }
            });
        }

        private void CopyDirectFields(Dictionary<string, object> rawData, DataAmbre dataAmbre)
        {
            var fieldMappings = GetFieldMappings();

            foreach (var mapping in fieldMappings)
            {
                if (rawData.ContainsKey(mapping.Key))
                {
                    mapping.Value(dataAmbre, rawData[mapping.Key]);
                }
            }
        }

        private Dictionary<string, Action<DataAmbre, object>> GetFieldMappings()
        {
            return new Dictionary<string, Action<DataAmbre, object>>
            {
                { "Account_ID", (d, v) => d.Account_ID = v?.ToString() },
                { "CCY", (d, v) => d.CCY = v?.ToString() },
                { "Country", (d, v) => d.Country = v?.ToString() },
                { "Event_Num", (d, v) => d.Event_Num = v?.ToString() },
                { "Folder", (d, v) => d.Folder = v?.ToString() },
                { "RawLabel", (d, v) => d.RawLabel = v?.ToString() },
                { "LocalSignedAmount", (d, v) => d.LocalSignedAmount = ValidationHelper.SafeParseDecimal(v) },
                { "Operation_Date", (d, v) => d.Operation_Date = ValidationHelper.SafeParseDateTime(v) },
                { "Reconciliation_Num", (d, v) => d.Reconciliation_Num = v?.ToString() },
                { "ReconciliationOrigin_Num", (d, v) => d.ReconciliationOrigin_Num = v?.ToString() },
                { "SignedAmount", (d, v) => d.SignedAmount = ValidationHelper.SafeParseDecimal(v) },
                { "Value_Date", (d, v) => d.Value_Date = ValidationHelper.SafeParseDateTime(v) }
            };
        }

        private void SetPropertyValue(object obj, string propertyName, string value)
        {
            if (string.IsNullOrEmpty(propertyName))
                return;

            var property = obj.GetType().GetProperty(propertyName);
            if (property != null && property.CanWrite)
            {
                var convertedValue = ConvertPropertyValue(value, property.PropertyType);
                property.SetValue(obj, convertedValue);
            }
        }

        private object ConvertPropertyValue(string value, Type targetType)
        {
            if (targetType == typeof(string))
                return value;
                
            if (targetType == typeof(decimal) || targetType == typeof(decimal?))
                return ValidationHelper.SafeParseDecimal(value);
                
            if (targetType == typeof(DateTime) || targetType == typeof(DateTime?))
                return ValidationHelper.SafeParseDateTime(value);
                
            return null;
        }

        private void ApplyCategorization(DataAmbre dataAmbre, Country country)
        {
            SetCategoryFromTransactionCodes(dataAmbre);
            
            var isPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot);
            var transformationService = _configurationLoader?.TransformationService;
            
            if (transformationService != null)
            {
                var transactionType = transformationService.DetermineTransactionType(
                    dataAmbre.RawLabel, isPivot, dataAmbre.Category);
                    
                var creditDebit = transformationService.DetermineCreditDebit(dataAmbre.SignedAmount);
                
                string guaranteeType = !isPivot ? ExtractGuaranteeType(dataAmbre.RawLabel) : null;
                
                var (action, kpi) = transformationService.ApplyAutomaticCategorization(
                    transactionType, dataAmbre.SignedAmount, isPivot, guaranteeType);
            }
        }

        private void SetCategoryFromTransactionCodes(DataAmbre dataAmbre)
        {
            if (dataAmbre == null) return;
            
            var codeToCategory = _configurationLoader?.CodeToCategory;
            if (codeToCategory == null || codeToCategory.Count == 0) return;
            
            if (string.IsNullOrWhiteSpace(dataAmbre.Pivot_TransactionCodesFromLabel)) return;

            var label = dataAmbre.Pivot_TransactionCodesFromLabel.Trim();

            if (codeToCategory.TryGetValue(label, out var tx))
            {
                dataAmbre.Category = (int)tx;
                return;
            }

            // Fallback for multiple codes separated by '|'
            if (label.Contains("|"))
            {
                ProcessMultipleCodes(label, codeToCategory, dataAmbre);
            }
        }

        private void ProcessMultipleCodes(string label, Dictionary<string, TransactionType> codeToCategory, DataAmbre dataAmbre)
        {
            var parts = label.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries)
                              .Select(p => p.Trim())
                              .Where(p => !string.IsNullOrWhiteSpace(p));
                              
            TransactionType? found = null;
            foreach (var p in parts)
            {
                if (codeToCategory.TryGetValue(p, out var c))
                {
                    if (found == null) 
                    {
                        found = c;
                    }
                    else if (found.Value != c)
                    {
                        LogManager.Warning($"Ambiguous ATC transaction types for codes '{label}'. Using first match '{found}'.");
                        break;
                    }
                }
            }
            
            if (found != null)
            {
                dataAmbre.Category = (int)found.Value;
            }
        }

        private string ExtractGuaranteeType(string label)
        {
            if (string.IsNullOrEmpty(label))
                return null;

            var upperLabel = label.ToUpper();

            if (upperLabel.Contains("REISSUANCE"))
                return "REISSUANCE";
            if (upperLabel.Contains("ISSUANCE"))
                return "ISSUANCE";
            if (upperLabel.Contains("ADVISING"))
                return "ADVISING";

            return null;
        }

        private bool IsMatchingAccount(string accountId, string pivot, string recv)
        {
            return (!string.IsNullOrWhiteSpace(pivot) && 
                    string.Equals(accountId, pivot, StringComparison.OrdinalIgnoreCase))
                   || 
                   (!string.IsNullOrWhiteSpace(recv) && 
                    string.Equals(accountId, recv, StringComparison.OrdinalIgnoreCase));
        }
    }
}