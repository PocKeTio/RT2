using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;
using System.Globalization;

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
                var culture = ResolvePreferredCulture(country);

                foreach (var row in rawData)
                {
                    var dataAmbre = new DataAmbre();

                    // Application des transformations configurées
                    foreach (var transform in transforms)
                    {
                        var transformedValue = transformationService?.ApplyTransformation(row, transform);
                        SetPropertyValue(dataAmbre, transform.AMB_Destination, transformedValue, culture);
                    }

                    // Copie des champs directs
                    CopyDirectFields(row, dataAmbre, culture);

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

        private void CopyDirectFields(Dictionary<string, object> rawData, DataAmbre dataAmbre, CultureInfo culture)
        {
            var fieldMappings = GetFieldMappings(culture);

            foreach (var mapping in fieldMappings)
            {
                if (rawData.ContainsKey(mapping.Key))
                {
                    mapping.Value(dataAmbre, rawData[mapping.Key]);
                }
            }
        }

        private Dictionary<string, Action<DataAmbre, object>> GetFieldMappings(CultureInfo culture)
        {
            return new Dictionary<string, Action<DataAmbre, object>>
            {
                // Strings: set only if currently empty (do not override values set by transformations)
                { "Account_ID", (d, v) => { if (string.IsNullOrWhiteSpace(d.Account_ID)) d.Account_ID = v?.ToString(); } },
                { "CCY", (d, v) => { if (string.IsNullOrWhiteSpace(d.CCY)) d.CCY = v?.ToString(); } },
                { "Country", (d, v) => { if (string.IsNullOrWhiteSpace(d.Country)) d.Country = v?.ToString(); } },
                { "Event_Num", (d, v) => { if (string.IsNullOrWhiteSpace(d.Event_Num)) d.Event_Num = v?.ToString(); } },
                { "Folder", (d, v) => { if (string.IsNullOrWhiteSpace(d.Folder)) d.Folder = v?.ToString(); } },
                { "RawLabel", (d, v) => { if (string.IsNullOrWhiteSpace(d.RawLabel)) d.RawLabel = v?.ToString(); } },
                { "Reconciliation_Num", (d, v) => { if (string.IsNullOrWhiteSpace(d.Reconciliation_Num)) d.Reconciliation_Num = v?.ToString(); } },
                { "ReconciliationOrigin_Num", (d, v) => { if (string.IsNullOrWhiteSpace(d.ReconciliationOrigin_Num)) d.ReconciliationOrigin_Num = v?.ToString(); } },

                // Nullable dates: set only if currently null
                { "Operation_Date", (d, v) => { if (!d.Operation_Date.HasValue) d.Operation_Date = ValidationHelper.SafeParseDateTime(v, culture) ?? d.Operation_Date; } },
                { "Value_Date", (d, v) => { if (!d.Value_Date.HasValue) d.Value_Date = ValidationHelper.SafeParseDateTime(v, culture) ?? d.Value_Date; } },

                // Amounts (non-nullable decimals): set only if currently default (0)
                { "LocalSignedAmount", (d, v) => { if (d.LocalSignedAmount == default(decimal)) d.LocalSignedAmount = ValidationHelper.SafeParseDecimal(v, culture); } },
                { "SignedAmount", (d, v) => { if (d.SignedAmount == default(decimal)) d.SignedAmount = ValidationHelper.SafeParseDecimal(v, culture); } },
            };
        }

        private void SetPropertyValue(object obj, string propertyName, string value, CultureInfo culture)
        {
            if (string.IsNullOrEmpty(propertyName))
                return;

            var property = obj.GetType().GetProperty(propertyName);
            if (property != null && property.CanWrite)
            {
                var convertedValue = ConvertPropertyValue(value, property.PropertyType, culture);
                property.SetValue(obj, convertedValue);
            }
        }

        private object ConvertPropertyValue(string value, Type targetType, CultureInfo culture)
        {
            if (targetType == typeof(string))
                return value;
                
            if (targetType == typeof(decimal) || targetType == typeof(decimal?))
                return ValidationHelper.SafeParseDecimal(value, culture);
                
            if (targetType == typeof(DateTime) || targetType == typeof(DateTime?))
                return ValidationHelper.SafeParseDateTime(value, culture);
                
            return null;
        }

        private static string NormalizeSpaces(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var t = s.Trim();
            t = Regex.Replace(t, "\\s+", " ");
            return t;
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
            }
        }

        private void SetCategoryFromTransactionCodes(DataAmbre dataAmbre)
        {
            if (dataAmbre == null) return;
            
            var codeToCategory = _configurationLoader?.CodeToCategory;
            if (codeToCategory == null || codeToCategory.Count == 0) return;
            
            var labelNorm = NormalizeSpaces(dataAmbre.Pivot_TransactionCodesFromLabel);
            if (string.IsNullOrEmpty(labelNorm)) return;

            // Build a normalized dictionary (collapse multiple spaces in keys)
            var dict = new Dictionary<string, TransactionType>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in codeToCategory)
            {
                var k = NormalizeSpaces(kv.Key);
                if (string.IsNullOrEmpty(k)) continue;
                dict[k] = kv.Value; // last wins, as before
            }

            // 1) Exact match on normalized label
            if (dict.TryGetValue(labelNorm, out var tx))
            {
                dataAmbre.Category = (int)tx;
                return;
            }

            // 2) Split by '|' and try each part (exact)
            if (labelNorm.Contains("|"))
            {
                ProcessMultipleCodes(labelNorm, dict, dataAmbre);
                if (dataAmbre.Category.HasValue) return;
            }

            // 3) Substring match: label may be longer than the code
            TransactionType? found = null;
            foreach (var kv in dict)
            {
                if (!string.IsNullOrEmpty(kv.Key) && labelNorm.IndexOf(kv.Key, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    if (found == null)
                    {
                        found = kv.Value;
                    }
                    else if (found.Value != kv.Value)
                    {
                        LogManager.Warning($"Ambiguous ATC transaction types for label '{labelNorm}'. Using first match '{found}'.");
                        break;
                    }
                }
            }

            if (found != null)
            {
                dataAmbre.Category = (int)found.Value;
            }
        }

        private void ProcessMultipleCodes(string label, Dictionary<string, TransactionType> codeToCategory, DataAmbre dataAmbre)
        {
            var parts = label.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries)
                              .Select(p => NormalizeSpaces(p))
                              .Where(p => !string.IsNullOrWhiteSpace(p));

            TransactionType? found = null;
            foreach (var p in parts)
            {
                // Exact on normalized part
                if (codeToCategory.TryGetValue(p, out var c))
                {
                    if (found == null) { found = c; }
                    else if (found.Value != c)
                    {
                        LogManager.Warning($"Ambiguous ATC transaction types for codes '{label}'. Using first match '{found}'.");
                        break;
                    }
                    continue;
                }

                // Substring fallback: part may be longer than the code, or vice versa
                foreach (var kv in codeToCategory)
                {
                    var key = kv.Key;
                    if (string.IsNullOrEmpty(key)) continue;
                    if (p.IndexOf(key, StringComparison.OrdinalIgnoreCase) >= 0 ||
                        key.IndexOf(p, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        if (found == null) { found = kv.Value; }
                        else if (found.Value != kv.Value)
                        {
                            LogManager.Warning($"Ambiguous ATC transaction types for codes '{label}'. Using first match '{found}'.");
                            break;
                        }
                    }
                }
            }

            if (found != null)
            {
                dataAmbre.Category = (int)found.Value;
            }
        }

        private bool IsMatchingAccount(string accountId, string pivot, string recv)
        {
            return (!string.IsNullOrWhiteSpace(pivot) && 
                    string.Equals(accountId, pivot, StringComparison.OrdinalIgnoreCase))
                   || 
                   (!string.IsNullOrWhiteSpace(recv) && 
                    string.Equals(accountId, recv, StringComparison.OrdinalIgnoreCase));
        }

        private CultureInfo ResolvePreferredCulture(Country country)
        {
            try
            {
                // Prefer the OS (Windows) current culture so that parsing matches the user's
                // Excel/regional settings. This avoids issues where the country mapping differs
                // from the machine's number/date formats.
                return CultureInfo.CurrentCulture;
            }
            catch
            {
                // Fallback to invariant if the OS culture cannot be resolved for any reason.
                return CultureInfo.InvariantCulture;
            }
        }
    }
}