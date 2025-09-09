using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;
using RecoTool.Services;

namespace RecoTool.Services
{
    /// <summary>
    /// Service d'import des données Ambre avec gestion offline-first
    /// </summary>
    public class AmbreImportService
    {
        #region Fields

        private readonly OfflineFirstService _offlineFirstService;
        private ReconciliationService _reconciliationService; // for DWINGS in-memory cache access
        private TransformationService _transformationService;
        private bool _initialized;
        private readonly string _currentUser;
        private Dictionary<string, TransactionType> _codeToCategory;

        // Heuristic constants (centralized)
        private const string KeywordTopaze = "TOPAZE";

        /// <summary>
        /// Constructeur avec OfflineFirstService
        /// </summary>
        public AmbreImportService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = Environment.UserName;
        }

        private async Task EnsureInitializedAsync()
        {
            if (_initialized) return;
            var countries = await _offlineFirstService.GetCountries().ConfigureAwait(false);
            _transformationService = new TransformationService(countries);
            // Charger le référentiel des codes de transaction Ambre -> catégorie
            try
            {
                var codes = _offlineFirstService.GetAmbreTransactionCodes() ?? new List<AmbreTransactionCode>();
                _codeToCategory = new Dictionary<string, TransactionType>(StringComparer.OrdinalIgnoreCase);
                foreach (var c in codes)
                {
                    if (string.IsNullOrWhiteSpace(c?.ATC_CODE) || string.IsNullOrWhiteSpace(c?.ATC_TAG)) continue;
                    var normalized = c.ATC_TAG.Replace(" ", "_").Replace("-", "_");
                    if (Enum.TryParse<TransactionType>(normalized, ignoreCase: true, out var tx))
                    {
                        _codeToCategory[c.ATC_CODE.Trim()] = tx;
                    }
                    else
                    {
                        LogManager.Warning($"Unknown ATC_TAG '{c.ATC_TAG}' for code '{c.ATC_CODE}' in T_Ref_Ambre_TransactionCodes");
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to load T_Ref_Ambre_TransactionCodes", ex);
                _codeToCategory = new Dictionary<string, TransactionType>(StringComparer.OrdinalIgnoreCase);
            }
            _initialized = true;
        }

        private async Task EnsureRecoServiceAsync()
        {
            if (_reconciliationService != null) return;
            try
            {
                var cs = _offlineFirstService.GetCurrentLocalConnectionString();
                var countries = await _offlineFirstService.GetCountries().ConfigureAwait(false);
                _reconciliationService = new ReconciliationService(cs, Environment.UserName, countries, _offlineFirstService);
            }
            catch (Exception ex)
            {
                LogManager.Warning($"Unable to initialize ReconciliationService for DWINGS cache: {ex.Message}");
            }
        }

        /// <summary>
        /// Importe plusieurs fichiers Excel Ambre (fusionnés) pour un pays donné en une seule opération.
        /// Utiliser lorsque les fichiers sont séparés par compte (Pivot/Receivable). Limité à 2 fichiers.
        /// </summary>
        public async Task<ImportResult> ImportAmbreFiles(string[] filePaths, string countryId,
            Action<string, int> progressCallback = null)
        {
            // Wrapper: multi-fichier, limite à 2 fichiers, stratégie pré-vol: pousser réconciliation + refresh RECON
            if (filePaths == null || filePaths.Length == 0)
                return new ImportResult { CountryId = countryId, Errors = { "No files provided" } };

            var take = filePaths.Take(2).ToArray();
            return await ImportAmbreCoreAsync(take, countryId, isMultiFile: true, progressCallback);
        }
        /// <summary>
        /// Importe un fichier Excel Ambre pour un pays donné
        /// </summary>
        /// <param name="filePath">Chemin vers le fichier Excel</param>
        /// <param name="countryId">ID du pays</param>
        /// <param name="progressCallback">Callback pour le suivi de progression</param>
        /// <returns>Résultat de l'import</returns>
        public async Task<ImportResult> ImportAmbreFile(string filePath, string countryId,
            Action<string, int> progressCallback = null)
        {
            // Wrapper: mono-fichier, stratégie pré-vol: zéro changes locaux + refresh AMBRE
            if (string.IsNullOrWhiteSpace(filePath))
                return new ImportResult { CountryId = countryId, Errors = { "File path is required" } };

            return await ImportAmbreCoreAsync(new[] { filePath }, countryId, isMultiFile: false, progressCallback);
        }

        /// <summary>
        /// Logique commune d'import AMBRE pour 1 ou 2 fichiers.
        /// </summary>
        private async Task<ImportResult> ImportAmbreCoreAsync(string[] filePaths, string countryId, bool isMultiFile, Action<string, int> progressCallback)
        {
            var result = new ImportResult { CountryId = countryId, StartTime = DateTime.UtcNow };
            try
            {
                await EnsureInitializedAsync().ConfigureAwait(false);
                progressCallback?.Invoke(isMultiFile ? "Validating files..." : "Validating file...", 0);
                var files = ValidateFiles(filePaths, isMultiFile, result);
                if (result.Errors.Any()) return result;

                progressCallback?.Invoke("Loading configurations...", 10);
                var (country, importFields, transforms) = await LoadConfigurationsAsync(countryId, result).ConfigureAwait(false);
                if (result.Errors.Any()) return result;

                progressCallback?.Invoke("Preparing environment (single global lock: pre-sync, import, publish)...", 15);
                var switched = await _offlineFirstService.SetCurrentCountryAsync(countryId, suppressPush: true).ConfigureAwait(false);
                if (!switched)
                {
                    result.Errors.Add($"Unable to initialize local database for {countryId}");
                    return result;
                }

                try
                {
                    // Enter AMBRE import local-only scope: suppress background sync/push until final publish
                    using (_offlineFirstService.BeginAmbreImportScope())
                    using (var globalLock = await AcquireGlobalLockAsync(countryId, result))
                    {
                        if (globalLock == null) return result;

                        if (!await PrepareEnvironmentAsync(countryId, result).ConfigureAwait(false)) return result;

                        var allRaw = await ReadExcelFilesAsync(files, importFields, isMultiFile, progressCallback).ConfigureAwait(false);
                        if (!allRaw.Any())
                        {
                            result.Errors.Add(isMultiFile ? "No data found in the Excel files." : "No data found in the Excel file.");
                            return result;
                        }

                        var filtered = FilterRowsByCountryAccounts(allRaw, country);
                        if (!filtered.Any())
                        {
                            var pivot = country?.CNT_AmbrePivot; var recv = country?.CNT_AmbreReceivable;
                            result.Errors.Add($"No rows match the country's AMBRE accounts ({country?.CNT_Id}): Pivot={pivot}, Receivable={recv}.");
                            return result;
                        }
                        var accounts = filtered
                            .Select(r => r.ContainsKey("Account_ID") ? r["Account_ID"]?.ToString() : null)
                            .Where(s => !string.IsNullOrWhiteSpace(s))
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .ToList();
                        bool hasPivot = !string.IsNullOrWhiteSpace(country?.CNT_AmbrePivot) && accounts.Any(a => string.Equals(a, country.CNT_AmbrePivot, StringComparison.OrdinalIgnoreCase));
                        bool hasReceivable = !string.IsNullOrWhiteSpace(country?.CNT_AmbreReceivable) && accounts.Any(a => string.Equals(a, country.CNT_AmbreReceivable, StringComparison.OrdinalIgnoreCase));
                        if (!(hasPivot && hasReceivable))
                        {
                            var missing = new List<string>();
                            if (!hasPivot) missing.Add($"Pivot={country?.CNT_AmbrePivot}");
                            if (!hasReceivable) missing.Add($"Receivable={country?.CNT_AmbreReceivable}");
                            result.Errors.Add($"Import aborted: both AMBRE accounts are required. Missing: {string.Join(", ", missing)}.");
                            return result;
                        }

                        var valid = await TransformAndValidateAsync(filtered, transforms, country, result, progressCallback).ConfigureAwait(false);
                        if (!valid.Any())
                        {
                            result.Errors.Add("No valid data after transformation and validation.");
                            return result;
                        }

                        await SynchronizeAsync(valid, countryId, result, progressCallback).ConfigureAwait(false);
                    }
                }
                catch (Exception preCheckEx)
                {
                    LogManager.Error("Error during full-lock import flow (pre-sync/import/publish)", preCheckEx);
                    result.Errors.Add($"Error during full-lock import: {preCheckEx.Message}");
                    return result;
                }

                // 8) Refresh config + anchors (hors verrou pour réduire la contention)
                progressCallback?.Invoke("Refreshing local configuration...", 90);
                try
                {
                    await _offlineFirstService.RefreshConfigurationAsync();
                    // IMPORTANT: do not trigger push+pull right after import; we just published the snapshot.
                    await _offlineFirstService.SetCurrentCountryAsync(countryId, suppressPush: true);
                    await _offlineFirstService.SetLastSyncAnchorAsync(countryId, DateTime.UtcNow);
                }
                catch (Exception refreshEx)
                {
                    LogManager.Error("Error while refreshing configuration after import", refreshEx);
                    result.Errors.Add($"Error while refreshing local configuration: {refreshEx.Message}");
                    result.EndTime = DateTime.UtcNow;
                    return result;
                }

                // 9) Final
                progressCallback?.Invoke("Finalizing...", 100);
                result.IsSuccess = true;
                result.EndTime = DateTime.UtcNow;
                // Invalidate DWINGS caches so next usage reloads after this import
                try
                {
                    await EnsureRecoServiceAsync().ConfigureAwait(false);
                    _reconciliationService?.InvalidateDwingsCaches();
                }
                catch (Exception invEx)
                {
                    LogManager.Warning($"Could not invalidate DWINGS cache post-import: {invEx.Message}");
                }
                return result;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Error during import: {ex.Message}");
                try { await _offlineFirstService.SetSyncStatusAsync("Error"); } catch { }
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        private string[] ValidateFiles(string[] filePaths, bool isMultiFile, ImportResult result)
        {
            var files = (filePaths ?? Array.Empty<string>()).Where(p => !string.IsNullOrWhiteSpace(p)).Take(2).ToArray();
            if (files.Length == 0)
            {
                result.Errors.Add("No files provided");
                return Array.Empty<string>();
            }

            foreach (var fp in files)
            {
                var errs = ValidationHelper.ValidateImportFile(fp);
                if (errs.Any())
                {
                    if (isMultiFile)
                        result.Errors.AddRange(errs.Select(e => $"{Path.GetFileName(fp)}: {e}"));
                    else
                        result.Errors.AddRange(errs);
                }
            }

            return files;
        }

        private async Task<(Country country, List<AmbreImportField> importFields, List<AmbreTransform> transforms)> LoadConfigurationsAsync(string countryId, ImportResult result)
        {
            var country = await LoadCountryConfiguration(countryId);
            var importFields = await LoadImportFieldsConfiguration();
            var transforms = await LoadTransformConfigurations();
            if (country == null)
            {
                result.Errors.Add($"Configuration not found for country: {countryId}");
            }
            return (country, importFields, transforms);
        }

        private async Task<IDisposable> AcquireGlobalLockAsync(string countryId, ImportResult result)
        {
            var lockWaitStart = DateTime.UtcNow;
            var lockTimeout = TimeSpan.FromMinutes(2);
            LogManager.Info($"Attempting to acquire global lock for {countryId} (full import) with {lockTimeout.TotalSeconds} sec wait...");

            var acquireTask = _offlineFirstService.AcquireGlobalLockAsync(countryId, "AmbreImport", TimeSpan.FromMinutes(30));
            var completed = await Task.WhenAny(acquireTask, Task.Delay(lockTimeout)) == acquireTask;
            if (!completed)
            {
                var msg = $"Unable to obtain global lock for {countryId} within the allotted time ({lockTimeout.TotalSeconds} sec). Please try again.";
                LogManager.Error($"Timeout ({(DateTime.UtcNow - lockWaitStart).TotalSeconds:F0}s) while waiting for global lock for {countryId}. Operation canceled.", null);
                result.Errors.Add(msg);
                return null;
            }

            var globalLock = await acquireTask;
            if (globalLock == null)
            {
                result.Errors.Add($"Unable to acquire global lock for {countryId}");
                return null;
            }

            LogManager.Info($"Global lock acquired for {countryId} after {(DateTime.UtcNow - lockWaitStart).TotalSeconds:F0}s (full import)");
            return globalLock;
        }

        private async Task<bool> PrepareEnvironmentAsync(string countryId, ImportResult result)
        {
            var unsyncedCount = await _offlineFirstService.GetUnsyncedChangeCountAsync(countryId);
            if (unsyncedCount > 0)
            {
                LogManager.Info($"{unsyncedCount} unsynced change(s) found. Pushing to network under global lock (reconciliation only)...");
                try { await _offlineFirstService.SetSyncStatusAsync("PreSync"); } catch { }
                try
                {
                    var pushed = await _offlineFirstService.PushPendingChangesToNetworkAsync(countryId, assumeLockHeld: true);
                    LogManager.Info($"Pushed {pushed} local change(s) to network.");
                }
                catch (Exception pushEx)
                {
                    LogManager.Error("Error while pushing local reconciliation changes before import", pushEx);
                    result.Errors.Add("Unable to push local reconciliation changes before import.");
                    return false;
                }
            }

            try { await _offlineFirstService.SetSyncStatusAsync("RefreshingLocal"); } catch { }
            await _offlineFirstService.CopyNetworkToLocalAsync(countryId);
            return true;
        }

        private async Task<List<Dictionary<string, object>>> ReadExcelFilesAsync(string[] files, IEnumerable<AmbreImportField> importFields, bool isMultiFile, Action<string, int> progressCallback)
        {
            try { await _offlineFirstService.SetSyncStatusAsync("Importing"); } catch { }
            var allRaw = new List<Dictionary<string, object>>();
            for (int i = 0; i < files.Length; i++)
            {
                var fp = files[i];
                var baseStart = isMultiFile ? 20 + (i * 10) : 20;
                var raw = await ReadExcelFile(fp, importFields, p =>
                {
                    int mapped = baseStart + (p * (isMultiFile ? 10 : 20) / 100);
                    if (mapped > 40) mapped = 40;
                    var label = isMultiFile ? $"Reading: {Path.GetFileName(fp)} ({p}%)" : $"Reading Excel file... ({p}%)";
                    progressCallback?.Invoke(label, mapped);
                });
                allRaw.AddRange(raw);
            }
            progressCallback?.Invoke(isMultiFile ? "Excel files read complete" : "Excel file read complete", 40);
            return allRaw;
        }

        private async Task<List<DataAmbre>> TransformAndValidateAsync(List<Dictionary<string, object>> filtered, IEnumerable<AmbreTransform> transforms, Country country, ImportResult result, Action<string, int> progressCallback)
        {
            progressCallback?.Invoke("Transforming data...", 40);
            var transformed = await TransformData(filtered, transforms, country);
            progressCallback?.Invoke("Validating data...", 60);
            var val = ValidateTransformedData(transformed, country);
            result.ValidationErrors.AddRange(val.errors);
            return val.validData;
        }

        private async Task SynchronizeAsync(List<DataAmbre> valid, string countryId, ImportResult result, Action<string, int> progressCallback)
        {
            progressCallback?.Invoke("Synchronizing with database...", 80);
            var syncResult = await SynchronizeWithDatabase(valid, countryId, performNetworkSync: true, assumeGlobalLockHeld: true);
            result.NewRecords = syncResult.newCount;
            result.UpdatedRecords = syncResult.updatedCount;
            result.DeletedRecords = syncResult.deletedCount;
            result.ProcessedRecords = valid.Count;
        }

        /// <summary>
        /// Lit le fichier Excel en appliquant le mapping des champs
        /// </summary>
        private async Task<List<Dictionary<string, object>>> ReadExcelFile(string filePath,
            IEnumerable<AmbreImportField> importFields, Action<int> progress = null)
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

        private async Task<List<DataAmbre>> TransformData(List<Dictionary<string, object>> rawData,
            IEnumerable<AmbreTransform> transforms, Country country)
        {
            return await Task.Run(() =>
            {
                var transformedData = new List<DataAmbre>();

                foreach (var row in rawData)
                {
                    var dataAmbre = new DataAmbre();

                    // Application des transformations configurées
                    foreach (var transform in transforms)
                    {
                        var transformedValue = _transformationService.ApplyTransformation(row, transform);
                        SetPropertyValue(dataAmbre, transform.AMB_Destination, transformedValue);
                    }

                    // Copie des champs directs (non transformés)
                    CopyDirectFields(row, dataAmbre);

                    // Génération de l'ID unique  si nécessaire
                    //FIX => TOUJOURS ICI POUR LINSTANT OSEF DE LA TABLE
                    //if (string.IsNullOrEmpty(dataAmbre.ID))
                    //{
                        dataAmbre.ID = dataAmbre.GetUniqueKey();
                    //}

                    // Application de la catégorisation automatique
                    // 1) Déduire la catégorie Ambre depuis les codes extraits du label (si Pivot)
                    SetCategoryFromTransactionCodes(dataAmbre);
                    // 2) Appliquer la catégorisation
                    ApplyAutomaticCategorization(dataAmbre, country);

                    transformedData.Add(dataAmbre);
                }

                return transformedData;
            });
        }

        /// <summary>
        /// Copie les champs directs depuis les données brutes
        /// </summary>
        private void CopyDirectFields(Dictionary<string, object> rawData, DataAmbre dataAmbre)
        {
            // Mapping direct des champs standards
            var fieldMappings = new Dictionary<string, Action<object>>
            {
                { "Account_ID", v => dataAmbre.Account_ID = v?.ToString() },
                { "CCY", v => dataAmbre.CCY = v?.ToString() },
                { "Country", v => dataAmbre.Country = v?.ToString() },
                { "Event_Num", v => dataAmbre.Event_Num = v?.ToString() },
                { "Folder", v => dataAmbre.Folder = v?.ToString() },
                { "RawLabel", v => dataAmbre.RawLabel = v?.ToString() },
                { "LocalSignedAmount", v => dataAmbre.LocalSignedAmount = ValidationHelper.SafeParseDecimal(v) },
                { "Operation_Date", v => dataAmbre.Operation_Date = ValidationHelper.SafeParseDateTime(v) },
                { "Reconciliation_Num", v => dataAmbre.Reconciliation_Num = v?.ToString() },
                { "ReconciliationOrigin_Num", v => dataAmbre.ReconciliationOrigin_Num = v?.ToString() },
                { "SignedAmount", v => dataAmbre.SignedAmount = ValidationHelper.SafeParseDecimal(v) },
                { "Value_Date", v => dataAmbre.Value_Date = ValidationHelper.SafeParseDateTime(v) }
            };

            foreach (var mapping in fieldMappings)
            {
                if (rawData.ContainsKey(mapping.Key))
                {
                    mapping.Value(rawData[mapping.Key]);
                }
            }
        }

        /// <summary>
        /// Définit la valeur d'une propriété par réflexion
        /// </summary>
        private void SetPropertyValue(object obj, string propertyName, string value)
        {
            if (string.IsNullOrEmpty(propertyName))
                return;

            var property = obj.GetType().GetProperty(propertyName);
            if (property != null && property.CanWrite)
            {
                if (property.PropertyType == typeof(string))
                {
                    property.SetValue(obj, value);
                }
                else if (property.PropertyType == typeof(decimal) || property.PropertyType == typeof(decimal?))
                {
                    property.SetValue(obj, ValidationHelper.SafeParseDecimal(value));
                }
                else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                {
                    property.SetValue(obj, ValidationHelper.SafeParseDateTime(value));
                }
            }
        }

        /// <summary>
        /// Applique la catégorisation automatique selon les règles métier
        /// </summary>
        private void ApplyAutomaticCategorization(DataAmbre dataAmbre, Country country)
        {
            var isPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot);
            TransactionType? transactionType = _transformationService.DetermineTransactionType(dataAmbre.RawLabel, isPivot, dataAmbre.Category);
            var creditDebit = _transformationService.DetermineCreditDebit(dataAmbre.SignedAmount);

            // Extraction du type de garantie pour les comptes Receivable
            string guaranteeType = null;
            if (!isPivot)
            {
                guaranteeType = ExtractGuaranteeType(dataAmbre.RawLabel);
            }

            var (action, kpi) = _transformationService.ApplyAutomaticCategorization(
                transactionType, dataAmbre.SignedAmount, isPivot, guaranteeType);

            // Note: Les actions et KPI seront appliqués dans la table T_Reconciliation
            // lors de la synchronisation
        }

        /// <summary>
        /// Extrait le type de garantie depuis le libellé
        /// </summary>
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

        private sealed class DwingsRef
        {
            public string Type { get; set; } // "BGPMT" or "BGI"
            public string Code { get; set; } // e.g., "BGPMT123456" or "BGI123456"
        }

        /// <summary>
        /// Extract DWINGS reference from Ambre row, preferring BGPMT over BGI.
        /// Looks into both RawLabel and Reconciliation_Num.
        /// Formats:
        ///  - BGPMTXXXXXXXXXXX (alnum, typical 11+ chars)
        ///  - BGIYYYYMMXXXXXXX (13 digits) -> see InvoiceIdExtractor
        /// </summary>
        private DwingsRef ExtractDwingsReference(DataAmbre ambre)
        {
            if (ambre == null) return null;
            // 1) Try BGPMT in RawLabel then Reconciliation_Num
            string bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(ambre.RawLabel) ?? DwingsLinkingHelper.ExtractBgpmtToken(ambre.Reconciliation_Num);
            if (!string.IsNullOrWhiteSpace(bgpmt))
                return new DwingsRef { Type = "BGPMT", Code = bgpmt };

            // 2) Try BGI (strict pattern BGI + 13 digits) using existing helper plus Reconciliation_Num
            string bgi = DwingsLinkingHelper.ExtractBgiToken(ambre.RawLabel) ?? DwingsLinkingHelper.ExtractBgiToken(ambre.Reconciliation_Num);
            if (!string.IsNullOrWhiteSpace(bgi))
                return new DwingsRef { Type = "BGI", Code = bgi };

            return null;
        }

        // Local BGPMT extractor removed in favor of centralized RecoTool.Helpers.DwingsLinkingHelper

        /// <summary>
        /// Look up the payment method for a DWINGS reference (T_DW_Data as DWINGSInvoice).
        /// BGPMT is unique; BGI can have multiple rows — we return the first match.
        /// </summary>
        private async Task<string> TryGetPaymentMethodFromDwingsAsync(string countryId, string refType, string refCode)
        {
            if (string.IsNullOrWhiteSpace(refType) || string.IsNullOrWhiteSpace(refCode))
                return null;

            // Use in-memory DWINGS cache via ReconciliationService
            await EnsureRecoServiceAsync().ConfigureAwait(false);
            var invoices = await _reconciliationService?.GetDwingsInvoicesAsync();
            if (invoices == null || invoices.Count == 0) return null;

            var code = refCode?.Trim();
            if (string.IsNullOrEmpty(code)) return null;

            ReconciliationService.DwingsInvoiceDto hit = null;
            if (string.Equals(refType, "BGPMT", StringComparison.OrdinalIgnoreCase))
            {
                hit = invoices.FirstOrDefault(i => !string.IsNullOrWhiteSpace(i?.BGPMT)
                                                && string.Equals(i.BGPMT, code, StringComparison.OrdinalIgnoreCase));
            }
            else
            {
                // BGI can map to multiple reference columns; pick the first match
                hit = invoices.FirstOrDefault(i =>
                    (i.INVOICE_ID != null && string.Equals(i.INVOICE_ID, code, StringComparison.OrdinalIgnoreCase)) ||
                    (i.SENDER_REFERENCE != null && string.Equals(i.SENDER_REFERENCE, code, StringComparison.OrdinalIgnoreCase)) ||
                    (i.RECEIVER_REFERENCE != null && string.Equals(i.RECEIVER_REFERENCE, code, StringComparison.OrdinalIgnoreCase)) ||
                    (i.BUSINESS_CASE_REFERENCE != null && string.Equals(i.BUSINESS_CASE_REFERENCE, code, StringComparison.OrdinalIgnoreCase))
                );
            }

            var method = hit?.PAYMENT_METHOD;
            return string.IsNullOrWhiteSpace(method) ? null : method;
        }

        /// <summary>
        /// Valide les données transformées
        /// </summary>
        private (List<string> errors, List<DataAmbre> validData) ValidateTransformedData(
            List<DataAmbre> data, Country country)
        {
            var errors = new List<string>();
            var validData = new List<DataAmbre>();

            foreach (var item in data)
            {
                var itemErrors = ValidationHelper.ValidateDataCoherence(item, country);
                if (itemErrors.Any())
                {
                    errors.AddRange(itemErrors.Select(e => $"Ligne {item.Event_Num}: {e}"));
                }
                else
                {
                    validData.Add(item);
                }
            }

            return (errors, validData);
        }

        /// <summary>
        /// Synchronise les données transformées avec la base (offline-first avec verrous globaux)
        /// </summary>
        private async Task<(int newCount, int updatedCount, int deletedCount)> SynchronizeWithDatabase(List<DataAmbre> newData, string countryId, bool performNetworkSync, bool assumeGlobalLockHeld = false)
        {
            ImportChanges changes;
            try
            {
                // 1) Charger les données existantes et calculer les changements SANS verrou global
              var existingData = await LoadExistingDataAsync(countryId);
                changes = CalculateChanges(existingData, newData);
                LogManager.Info($"Calculated changes for {countryId} - New: {changes.ToAdd.Count}, Updated: {changes.ToUpdate.Count}, Deleted: {changes.ToArchive.Count}");

                // 2) Protéger uniquement la phase d'écriture (apply + reconciliation)
                // Définir la séquence d'application/publication/finalisation
                async Task ExecuteApplyPublishFinalizeAsync()
                {
                    try
                    {
                        // Sauvegarde locale RECON avant modifications (non bloquant si erreur)
                        try { await _offlineFirstService.CreateLocalReconciliationBackupAsync(countryId, "PreImport"); } catch { }

                        // Statut: application des changements
                        try { await _offlineFirstService.SetSyncStatusAsync("ApplyingChanges"); } catch { }

                        // Appliquer les changements via les API natives d'OfflineFirstService
                        await ApplyChangesAsync(changes);

                        // Status: reconciling
                        try { await _offlineFirstService.SetSyncStatusAsync("Reconciling"); } catch { }

                        // Update T_Reconciliation
                        await UpdateReconciliationTable(changes.ToAdd, changes.ToUpdate, changes.ToArchive, countryId);

                        // Before network publish: freeze the latest snapshot (if exists) and insert the snapshot for last AMBRE operation date
                        try
                        {
                            if (!string.IsNullOrWhiteSpace(countryId))
                            {
                                var cs = _offlineFirstService.GetCurrentLocalConnectionString();
                                var countries = await _offlineFirstService.GetCountries();
                                var recoSvc = new ReconciliationService(cs, Environment.UserName, countries, _offlineFirstService);
                                // Freeze previous non-frozen snapshot
                                await recoSvc.FreezeLatestSnapshotAsync(countryId);
                                // Determine snapshot date from AMBRE data (fallback to today)
                                var lastOpDate = await recoSvc.GetLastAmbreOperationDateAsync(countryId) ?? DateTime.Today;
                                await recoSvc.SaveDailyKpiSnapshotAsync(lastOpDate, countryId, sourceVersion: "PostAmbreImport");
                            }
                        }
                        catch (Exception snapEx)
                        {
                            LogManager.Warning($"Non-blocking KPI snapshot: {snapEx.Message}");
                        }

                        LogManager.Info($"Local import completed successfully for {countryId}");

                        // Publier les bases locales vers le réseau tant que le verrou global est détenu
                        try { await _offlineFirstService.SetSyncStatusAsync("Publishing"); } catch { }
                        await _offlineFirstService.CopyLocalToNetworkAsync(countryId);

                        // Finalize: import changes are already published (file copy). Mark them as synced in ChangeLog.
                        try { await _offlineFirstService.SetSyncStatusAsync("Finalizing"); } catch { }
                        await _offlineFirstService.MarkAllLocalChangesAsSyncedAsync(countryId);
                        
                        // Cleanup: purge synchronized ChangeLog entries and compact the control/lock DB under the global lock
                        try
                        {
                            await _offlineFirstService.CleanupChangeLogAndCompactAsync(countryId);
                        }
                        catch (Exception cleanupEx)
                        {
                            LogManager.Warning($"Cleanup of ChangeLog/compaction failed (non-blocking): {cleanupEx.Message}");
                        }

                        // Note: do NOT mark Completed here. We'll do it at the very end of ImportAmbreCoreAsync
                        // after post-refresh steps to guarantee that when we say "completed", everything is finished.
                    }
                    catch (Exception ex)
                    {
                        LogManager.Error($"Error during local import for {countryId}", ex);
                        try { await _offlineFirstService.SetSyncStatusAsync("Error"); } catch { }
                        throw new InvalidOperationException($"Error while synchronizing with the database: {ex.Message}", ex);
                    }
                }

                if (assumeGlobalLockHeld)
                {
                    // Le verrou global est déjà détenu par l'appelant (ImportAmbreCoreAsync)
                    await ExecuteApplyPublishFinalizeAsync();
                }
                else
                {
                    // Acquérir le verrou ici (comportement précédent)
                    var lockWaitStart = DateTime.UtcNow;
                    var lockTimeout = TimeSpan.FromMinutes(2); // Timeout explicite pour éviter l'attente infinie
                    LogManager.Info($"Attempting to acquire global lock for {countryId} (timeout {lockTimeout.TotalSeconds} sec)...");

                    var acquireTask = _offlineFirstService.AcquireGlobalLockAsync(countryId, "AmbreImport", TimeSpan.FromMinutes(30));
                    var completed = await Task.WhenAny(acquireTask, Task.Delay(lockTimeout));
                    if (completed != acquireTask)
                    {
                        var waited = DateTime.UtcNow - lockWaitStart;
                        var msg = $"Unable to obtain global lock for {countryId} within the allotted time ({lockTimeout.TotalSeconds} sec). Please try again.";
                        var timeoutEx = new TimeoutException(msg);
                        LogManager.Error($"Timeout ({waited.TotalSeconds:F0}s) while waiting for global lock for {countryId}. Operation canceled.", timeoutEx);
                        throw timeoutEx;
                    }

                    var globalLockHandle = await acquireTask;
                    using (var globalLock = globalLockHandle)
                    {
                        if (globalLock == null)
                        {
                            throw new InvalidOperationException($"Unable to acquire global lock for {countryId}. Import canceled.");
                        }

                        var waited = DateTime.UtcNow - lockWaitStart;
                        LogManager.Info($"Global lock acquired for {countryId} after {waited.TotalSeconds:F0}s wait");

                        await ExecuteApplyPublishFinalizeAsync();
                        // Le verrou sera relâché en sortie de using
                    }
                }

                if (performNetworkSync && !assumeGlobalLockHeld)
                {
                    try
                    {
                        LogManager.Info($"Starting network synchronization for {countryId}");

                        // Synchroniser avec la base réseau via OfflineFirstService
                        bool syncSuccess = await _offlineFirstService.SynchronizeData();

                        if (syncSuccess)
                        {
                            LogManager.Info($"Network synchronization succeeded for {countryId}");
                        }
                        else
                        {
                            LogManager.Warning($"Network synchronization failed for {countryId} - Data remains local");
                        }
                    }
                    catch (Exception syncEx)
                    {
                        LogManager.Error($"Error during network synchronization for {countryId}", syncEx);
                        // Do not fail the import if only the network sync fails
                    }
                }

                return (changes.ToAdd.Count, changes.ToUpdate.Count, changes.ToArchive.Count);
            }
            finally { }
        }

        /// <summary>
        /// Charge les données Ambre existantes depuis la base de données
        /// </summary>
        /// <param name="countryId">ID du pays pour lequel charger les données</param>
        private async Task<List<DataAmbre>> LoadExistingDataAsync(string countryId)
        {
            try
            {
                LogManager.Info($"Chargement des données Ambre existantes pour {countryId}");
                
                // Utiliser l'API native d'OfflineFirstService pour récupérer les entités
                // Cela garantit la gestion des verrous, versionnage, synchronisation, etc.
                var entities = await _offlineFirstService.GetEntitiesAsync(countryId, "T_Data_Ambre");
                
                // Convertir les entités en objets DataAmbre
                var existingData = new List<DataAmbre>();
                if (entities != null)
                {
                    foreach (var entity in entities)
                    {
                        //if (entity.Properties.ContainsKey("DeleteDate") && entity.Properties["DeleteDate"] != null)
                        //    continue; // Ignorer les données archivées
                            
                        var dataAmbre = new DataAmbre
                        {
                            ID = GetPropertyValue(entity.Properties, "ID")?.ToString(),
                            Country = GetPropertyValue(entity.Properties, "Country")?.ToString(),
                            Account_ID = GetPropertyValue(entity.Properties, "Account_ID")?.ToString(),
                            CCY = GetPropertyValue(entity.Properties, "CCY")?.ToString(),
                            Event_Num = GetPropertyValue(entity.Properties, "Event_Num")?.ToString(),
                            Folder = GetPropertyValue(entity.Properties, "Folder")?.ToString(),
                            RawLabel = GetPropertyValue(entity.Properties, "RawLabel")?.ToString(),
                            SignedAmount = ConvertToDecimal(GetPropertyValue(entity.Properties, "SignedAmount")),
                            LocalSignedAmount = ConvertToDecimal(GetPropertyValue(entity.Properties, "LocalSignedAmount")),
                            Operation_Date = ConvertToDateTime(GetPropertyValue(entity.Properties, "Operation_Date")),
                            Value_Date = ConvertToDateTime(GetPropertyValue(entity.Properties, "Value_Date")),
                            Category = int.TryParse(GetPropertyValue(entity.Properties, "Category")?.ToString(), out var cat) ? cat : (int?)null,
                            // Champs complémentaires nécessaires pour la clé métier et la détection de changements
                            ReconciliationOrigin_Num = GetPropertyValue(entity.Properties, "ReconciliationOrigin_Num")?.ToString(),
                            Reconciliation_Num = GetPropertyValue(entity.Properties, "Reconciliation_Num")?.ToString(),
                            Receivable_InvoiceFromAmbre = GetPropertyValue(entity.Properties, "Receivable_InvoiceFromAmbre")?.ToString(),
                            Receivable_DWRefFromAmbre = GetPropertyValue(entity.Properties, "Receivable_DWRefFromAmbre")?.ToString(),
                            LastModified = ConvertToDateTime(GetPropertyValue(entity.Properties, "LastModified")),
                            DeleteDate = ConvertToDateTime(GetPropertyValue(entity.Properties, "DeleteDate")),
                            CreationDate = ConvertToDateTime(GetPropertyValue(entity.Properties, "CreationDate")),
                            ModifiedBy = GetPropertyValue(entity.Properties, "ModifiedBy")?.ToString(),
                            Version = int.TryParse(GetPropertyValue(entity.Properties, "Version")?.ToString(), out var v) ? v : 1
                        };
                        existingData.Add(dataAmbre);
                    }
                }
                
                LogManager.Info($"Données Ambre existantes chargées : {existingData.Count} enregistrements");
                return existingData;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors du chargement des données existantes : {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Calcule les changements entre les données existantes et les nouvelles données
        /// </summary>
        private ImportChanges CalculateChanges(List<DataAmbre> existingData, List<DataAmbre> newData)
        {
            try
            {
                LogManager.Info($"Calcul des changements - Existantes: {existingData.Count}, Nouvelles: {newData.Count}");
                
                var changes = new ImportChanges();
                
                // Créer des dictionnaires pour accélérer les recherches (clé métier commune)
                // IMPORTANT: utiliser la même clé des deux côtés pour éviter les faux "nouveaux" ou "supprimés".
                var existingByKey = existingData.ToDictionary(d => d.GetUniqueKey(), d => d);
                var newByKey = newData.ToDictionary(d => d.GetUniqueKey(), d => d);
                
                // Identifier les ajouts et modifications
                foreach (var newItem in newData)
                {
                    var key = newItem.GetUniqueKey();
                    
                    if (existingByKey.TryGetValue(key, out var existingItem))
                    {
                        // Enregistrement existant
                        // Cas 1: l'existant est archivé -> "revival" : forcer une mise à jour qui remet DeleteDate à null
                        if (existingItem.DeleteDate.HasValue)
                        {
                            newItem.ID = existingItem.ID;
                            newItem.Version = existingItem.Version + 1; // Incrémenter la version
                            newItem.CreationDate = existingItem.CreationDate; // Conserver la date de création
                            newItem.DeleteDate = null; // UNDELETE côté AMBRE
                            newItem.LastModified = DateTime.UtcNow;
                            newItem.ModifiedBy = _currentUser;
                            changes.ToUpdate.Add(newItem);
                            continue;
                        }

                        // Cas 2: l'existant n'est pas archivé - vérifier s'il a changé
                        if (HasDataChanged(existingItem, newItem))
                        {
                            // Mise à jour nécessaire - conserver l'ID et la version de l'existant
                            newItem.ID = existingItem.ID;
                            newItem.Version = existingItem.Version + 1; // Incrémenter la version
                            newItem.CreationDate = existingItem.CreationDate; // Conserver la date de création
                            newItem.LastModified = DateTime.UtcNow;
                            newItem.ModifiedBy = _currentUser;
                            
                            changes.ToUpdate.Add(newItem);
                        }
                        // Sinon, aucun changement nécessaire
                    }
                    else
                    {
                        // Nouvel enregistrement
                        newItem.ID = newItem.GetUniqueKey();
                        newItem.Version = 1;
                        newItem.CreationDate = DateTime.UtcNow;
                        newItem.LastModified = DateTime.UtcNow;
                        newItem.ModifiedBy = _currentUser;
                        
                        changes.ToAdd.Add(newItem);
                    }
                }
                
                // Identifier les suppressions (enregistrements présents dans existing mais pas dans new)
                foreach (var existingItem in existingData)
                {
                    var key = existingItem.GetUniqueKey();
                    
                    if (!newByKey.ContainsKey(key))
                    {
                        // Enregistrement à supprimer (archivage logique)
                        existingItem.DeleteDate = DateTime.UtcNow;
                        existingItem.LastModified = DateTime.UtcNow;
                        existingItem.ModifiedBy = _currentUser;
                        existingItem.Version += 1;
                        
                        changes.ToArchive.Add(existingItem);
                    }
                }
                
                LogManager.Debug($"Changements calculés - Ajouts: {changes.ToAdd.Count}, Modifications: {changes.ToUpdate.Count}, Suppressions: {changes.ToArchive.Count}");
                return changes;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors du calcul des changements: {ex.Message}", ex);
                throw;
            }
        }
        
        /// <summary>
        /// Vérifie si les données d'un enregistrement ont changé
        /// </summary>
        private bool HasDataChanged(DataAmbre existing, DataAmbre newData)
        {
            // Comparer les champs métier principaux (ignorer les champs de versionnage)
            return existing.Account_ID != newData.Account_ID ||
                   existing.CCY != newData.CCY ||
                   existing.Event_Num != newData.Event_Num ||
                   existing.Folder != newData.Folder ||
                   existing.RawLabel != newData.RawLabel ||
                   existing.SignedAmount != newData.SignedAmount ||
                   existing.LocalSignedAmount != newData.LocalSignedAmount ||
                   existing.Operation_Date != newData.Operation_Date ||
                   existing.Value_Date != newData.Value_Date ||
                   existing.Category != newData.Category ||
                   existing.Reconciliation_Num != newData.Reconciliation_Num ||
                   existing.Receivable_InvoiceFromAmbre != newData.Receivable_InvoiceFromAmbre ||
                   existing.Receivable_DWRefFromAmbre != newData.Receivable_DWRefFromAmbre;
        }

        /// <summary>
        /// Applique les changements calculés à la base de données via les API natives d'OfflineFirstService
        /// </summary>
        private async Task ApplyChangesAsync(ImportChanges changes)
        {
            try
            {
                LogManager.Info($"Application des changements (batch) - Ajouts: {changes.ToAdd.Count}, Modifications: {changes.ToUpdate.Count}, Suppressions: {changes.ToArchive.Count}");

                var countryId = _offlineFirstService.CurrentCountryId;

                // Construire les entit\u00e9s pour le batch
                var toAdd = new List<OfflineFirstAccess.Models.Entity>(changes.ToAdd.Count);
                var toUpdate = new List<OfflineFirstAccess.Models.Entity>(changes.ToUpdate.Count);
                var toArchive = new List<OfflineFirstAccess.Models.Entity>(changes.ToArchive.Count);

                foreach (var item in changes.ToAdd)
                    toAdd.Add(ConvertDataAmbreToEntity(item));
                foreach (var item in changes.ToUpdate)
                    toUpdate.Add(ConvertDataAmbreToEntity(item));
                foreach (var item in changes.ToArchive)
                    toArchive.Add(ConvertDataAmbreToEntity(item));

                var sw = System.Diagnostics.Stopwatch.StartNew();
                  var ok = await _offlineFirstService.ApplyEntitiesBatchAsync(countryId, toAdd, toUpdate, toArchive, suppressChangeLog: true);
                sw.Stop();

                if (!ok)
                {
                    LogManager.Warning("L'application en lot a retourn\u00e9 false. V\u00e9rifiez les journaux d'erreur.");
                }

                LogManager.Info($"Changements appliqu\u00e9s (batch) en {sw.Elapsed.TotalSeconds:F1}s - {changes.ToAdd.Count} ajouts, {changes.ToUpdate.Count} mises \u00e0 jour, {changes.ToArchive.Count} archivages");
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de l'application des changements: {ex.Message}", ex);
                throw;
            }
        }
        
        /// <summary>
        /// Convertit un objet DataAmbre en entité OfflineFirstAccess
        /// </summary>
        private OfflineFirstAccess.Models.Entity ConvertDataAmbreToEntity(DataAmbre dataAmbre)
        {
            var entity = new OfflineFirstAccess.Models.Entity
            {
                TableName = "T_Data_Ambre",
                PrimaryKeyColumn = "ID",
                Properties = new Dictionary<string, object>
                {
                    ["ID"] = dataAmbre.ID,
                    ["Country"] = dataAmbre.Country,
                    ["Account_ID"] = dataAmbre.Account_ID,
                    ["CCY"] = dataAmbre.CCY,
                    ["Event_Num"] = dataAmbre.Event_Num,
                    ["Folder"] = dataAmbre.Folder,
                    ["RawLabel"] = dataAmbre.RawLabel,
                    ["SignedAmount"] = dataAmbre.SignedAmount,
                    ["LocalSignedAmount"] = dataAmbre.LocalSignedAmount,
                    ["Operation_Date"] = dataAmbre.Operation_Date,
                    ["Value_Date"] = dataAmbre.Value_Date,
                    ["Category"] = (object?)dataAmbre.Category ?? DBNull.Value,
                    ["Receivable_InvoiceFromAmbre"] = dataAmbre.Receivable_InvoiceFromAmbre,
                    ["Receivable_DWRefFromAmbre"] = dataAmbre.Receivable_DWRefFromAmbre,
                    ["Reconciliation_Num"] = dataAmbre.Reconciliation_Num,
                    ["ReconciliationOrigin_Num"] = dataAmbre.ReconciliationOrigin_Num,
                    // Champs de versionnage
                    ["Version"] = dataAmbre.Version,
                    ["CreationDate"] = dataAmbre.CreationDate,
                    ["LastModified"] = dataAmbre.LastModified,
                    ["ModifiedBy"] = dataAmbre.ModifiedBy,
                    ["DeleteDate"] = dataAmbre.DeleteDate
                }
            };
            
            return entity;
        }

        /// <summary>
        /// Règle DataAmbre.Category en utilisant Pivot_TransactionCodesFromLabel et le lookup référentiel.
        /// </summary>
        private void SetCategoryFromTransactionCodes(DataAmbre dataAmbre)
        {
            if (dataAmbre == null) return;
            if (_codeToCategory == null || _codeToCategory.Count == 0) return;
            if (string.IsNullOrWhiteSpace(dataAmbre.Pivot_TransactionCodesFromLabel)) return;

            var label = dataAmbre.Pivot_TransactionCodesFromLabel.Trim();

            // Ne pas splitter: les ATC_CODE peuvent contenir '/'. On tente un match exact case-insensitive.
            if (_codeToCategory.TryGetValue(label, out var tx))
            {
                dataAmbre.Category = (int)tx;
                return;
            }

            // Fallback optionnel: si le label contient un séparateur '|' listant plusieurs codes complets,
            // on peut tenter un split minimal sur '|', sans toucher aux '/'.
            if (label.Contains("|"))
            {
                var parts = label.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries)
                                  .Select(p => p.Trim())
                                  .Where(p => !string.IsNullOrWhiteSpace(p));
                TransactionType? found = null;
                foreach (var p in parts)
                {
                    if (_codeToCategory.TryGetValue(p, out var c))
                    {
                        if (found == null) found = c;
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
        }

        /// <summary>
        /// Charge la configuration d'un pays
        /// </summary>
        private async Task<Country> LoadCountryConfiguration(string countryId)
        {
            try
            {
                return await _offlineFirstService.GetCountryByIdAsync(countryId);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Erreur lors du chargement de la configuration du pays {countryId}: {ex.Message}", ex);
            }
        }

        private async Task<List<AmbreImportField>> LoadImportFieldsConfiguration()
        {
            try
            {
                // Utiliser directement les données référentielles chargées en mémoire
                return await Task.FromResult(_offlineFirstService.GetAmbreImportFields());
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Erreur lors du chargement des champs d'import: {ex.Message}", ex);
            }
        }

        private async Task<List<AmbreTransform>> LoadTransformConfigurations()
        {
            try
            {
                // Utiliser directement les données référentielles chargées en mémoire
                return await Task.FromResult(_offlineFirstService.GetAmbreTransforms());
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Erreur lors du chargement des transformations: {ex.Message}", ex);
            }
        }

        #region T_Reconciliation Management

        /// <summary>
        /// Met à jour la table T_Reconciliation pour les données importées via les API natives d'OfflineFirstService
        /// </summary>
        private async Task UpdateReconciliationTable(List<DataAmbre> newRecords, List<DataAmbre> updatedRecords,
            List<DataAmbre> deletedRecords, string countryId)
        {
            try
            {
                LogManager.Info($"[Direct] Insertion uniquement dans T_Reconciliation pour {countryId} - Candidats à l'insertion (issus de 'nouveaux'): {newRecords?.Count ?? 0}. Aucun UPDATE/DELETE ne sera effectué.");

                // Préparer uniquement les lignes à insérer (pas de mise à jour)
                var now = DateTime.UtcNow;
                var toInsert = new List<Reconciliation>();
                var country = await LoadCountryConfiguration(countryId);

                // Prepare helper services/context
                var countries = await _offlineFirstService.GetCountries();
                var recoSvc = new ReconciliationService(_offlineFirstService.GetCurrentLocalConnectionString(), _currentUser, countries, _offlineFirstService);
                var businessToday = DateTime.Today; // local time per spec

                // Preload DWINGS caches once (invoices; guarantees optional later)
                var dwInvoices = await recoSvc.GetDwingsInvoicesAsync();

                // Staging for later action assignment across Pivot/Receivable
                var staged = new List<(Reconciliation rec, DataAmbre ambre, bool isPivot, string bgi)>();

                foreach (var dataAmbre in newRecords ?? new List<DataAmbre>())
                {
                    var rec = CreateReconciliationFromDataAmbre(dataAmbre, country);
                    rec.CreationDate = now;
                    rec.LastModified = now;
                    rec.ModifiedBy = _currentUser;

                    bool isPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot);

                    // Resolve DWINGS references per rules
                    string resolvedBgi = null;
                    string resolvedBgpmt = null;
                    string resolvedGuarantee = null;
                    try
                    {
                        // Commission (BGPMT) from various fields
                        resolvedBgpmt = DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.Reconciliation_Num)
                                         ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.ReconciliationOrigin_Num)
                                         ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.RawLabel);

                        // Guarantee ID from various fields
                        resolvedGuarantee = DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.Reconciliation_Num)
                                            ?? DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.RawLabel);

                        if (!isPivot)
                        {
                            // Receivable: rely on BGI (prefer explicit field), fallback to regex in RawLabel then Reconciliation fields
                            resolvedBgi = (dataAmbre.Receivable_InvoiceFromAmbre?.Trim())
                                           ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel)
                                           ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                                           ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num);

                            var hit = DwingsLinkingHelper.ResolveInvoiceByBgiWithAmount(dwInvoices, resolvedBgi, dataAmbre.SignedAmount);
                            if (hit != null)
                            {
                                rec.DWINGS_InvoiceID = hit.INVOICE_ID;
                            }
                        }
                        else
                        {
                            // Pivot: search order for refs: Reconciliation_Num -> ReconciliationOrigin_Num -> RawLabel
                            // Prefer BGI; else try BGPMT
                            resolvedBgi = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                                           ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                                           ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel);
                            if (!string.IsNullOrWhiteSpace(resolvedBgi))
                            {
                                var hit = DwingsLinkingHelper.ResolveInvoiceByBgiWithAmount(dwInvoices, resolvedBgi, dataAmbre.SignedAmount);
                                if (hit != null)
                                {
                                    rec.DWINGS_InvoiceID = hit.INVOICE_ID;
                                }
                            }
                            if (string.IsNullOrWhiteSpace(rec.DWINGS_InvoiceID) && !string.IsNullOrWhiteSpace(resolvedBgpmt))
                            {
                                var hit = DwingsLinkingHelper.ResolveInvoiceByBgpmt(dwInvoices, resolvedBgpmt, dataAmbre.SignedAmount);
                                if (hit != null)
                                {
                                    rec.DWINGS_InvoiceID = hit.INVOICE_ID;
                                }
                            }
                            if (string.IsNullOrWhiteSpace(rec.DWINGS_InvoiceID) && !string.IsNullOrWhiteSpace(resolvedGuarantee))
                            {
                                var hits = DwingsLinkingHelper.ResolveInvoicesByGuarantee(dwInvoices, resolvedGuarantee, dataAmbre.Operation_Date ?? dataAmbre.Value_Date, dataAmbre.SignedAmount, take: 1);
                                var hit = (hits != null && hits.Count > 0) ? hits[0] : null;
                                if (hit != null)
                                {
                                    rec.DWINGS_InvoiceID = hit.INVOICE_ID;
                                }
                            }
                        }
                    }
                    catch (Exception linkEx)
                    {
                        LogManager.Warning($"DWINGS resolution failed for {dataAmbre?.ID}: {linkEx.Message}");
                    }

                    // As last resort, try the general suggestion chain to populate Invoice
                    if (string.IsNullOrWhiteSpace(rec.DWINGS_InvoiceID))
                    {
                        try
                        {
                            var suggestions = DwingsLinkingHelper.SuggestInvoicesForAmbre(dwInvoices,
                                rawLabel: dataAmbre.RawLabel,
                                reconciliationNum: dataAmbre.Reconciliation_Num,
                                reconciliationOriginNum: dataAmbre.ReconciliationOrigin_Num,
                                explicitBgi: resolvedBgi ?? dataAmbre.Receivable_InvoiceFromAmbre,
                                guaranteeId: resolvedGuarantee,
                                ambreDate: dataAmbre.Operation_Date ?? dataAmbre.Value_Date,
                                ambreAmount: dataAmbre.SignedAmount,
                                take: 1);
                            var hit = (suggestions != null && suggestions.Count > 0) ? suggestions[0] : null;
                            if (hit != null)
                            {
                                rec.DWINGS_InvoiceID = hit.INVOICE_ID;
                                if (string.IsNullOrWhiteSpace(resolvedBgi))
                                    resolvedBgi = hit.INVOICE_ID; // for grouping later
                            }
                        }
                        catch { }
                    }

                    // Persist extracted tokens onto reconciliation fields when present
                    if (!string.IsNullOrWhiteSpace(resolvedBgpmt)) rec.DWINGS_CommissionID = resolvedBgpmt;
                    if (!string.IsNullOrWhiteSpace(resolvedGuarantee)) rec.DWINGS_GuaranteeID = resolvedGuarantee;

                    // If we found an invoice but still no GuaranteeID, try to backfill from invoice business case fields
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(rec.DWINGS_InvoiceID) && string.IsNullOrWhiteSpace(rec.DWINGS_GuaranteeID))
                        {
                            var inv = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, rec.DWINGS_InvoiceID, StringComparison.OrdinalIgnoreCase));
                            var candidate = inv?.BUSINESS_CASE_REFERENCE ?? inv?.BUSINESS_CASE_ID;
                            if (!string.IsNullOrWhiteSpace(candidate)) rec.DWINGS_GuaranteeID = candidate;
                        }
                    }
                    catch { }

                    // KPI via mapping (independent of action assignment below)
                    var txType = _transformationService.DetermineTransactionType(dataAmbre.RawLabel, isPivot, dataAmbre.Category);
                    string guaranteeType = !isPivot ? ExtractGuaranteeType(dataAmbre.RawLabel) : null;
                    var (_, kpi) = _transformationService.ApplyAutomaticCategorization(txType, dataAmbre.SignedAmount, isPivot, guaranteeType);
                    rec.KPI = (int)kpi;

                    // Keep current auto action (ComputeAutoAction) as baseline; will be overridden by cross-side rules if applicable
                    try
                    {
                        string paymentMethod = null;
                        var dwRef = ExtractDwingsReference(dataAmbre);
                        if (dwRef != null)
                        {
                            try { paymentMethod = await TryGetPaymentMethodFromDwingsAsync(countryId, dwRef.Type, dwRef.Code).ConfigureAwait(false); } catch { }
                        }
                        var autoAction = recoSvc.ComputeAutoAction(txType, dataAmbre, rec, country, paymentMethod: paymentMethod, today: businessToday);
                        if (autoAction.HasValue)
                            rec.Action = (int)autoAction.Value;
                    }
                    catch { }

                    toInsert.Add(rec);
                    staged.Add((rec, dataAmbre, isPivot, string.IsNullOrWhiteSpace(resolvedBgi) ? null : resolvedBgi));
                }

                // Cross-side action rules based solely on shared DWINGS_InvoiceID (BGI)
                // Concept of "Matched": DWINGS_InvoiceID is set and there exists at least one Pivot and one Receivable
                // with the same InvoiceID. In this case: Pivot -> Match, Receivable -> Trigger.
                try
                {
                    var groups = staged.Where(s => !string.IsNullOrWhiteSpace(s.bgi))
                                       .GroupBy(s => s.bgi.Trim().ToUpperInvariant());
                    foreach (var g in groups)
                    {
                        var pivots = g.Where(x => x.isPivot).ToList();
                        var recvs = g.Where(x => !x.isPivot).ToList();
                        if (pivots.Count == 0 || recvs.Count == 0) continue;

                        // There is at least one pivot and one receivable sharing the same invoice id => matched set
                        foreach (var p in pivots)
                        {
                            try { p.rec.Action = (int)ActionType.Match; } catch { }
                        }
                        foreach (var rcv in recvs)
                        {
                            try { rcv.rec.Action = (int)ActionType.Trigger; } catch { }
                        }
                    }
                }
                catch (Exception assignEx)
                {
                    LogManager.Warning($"Cross-side action assignment failed: {assignEx.Message}");
                }

                // Diagnostics: log count and a sample of IDs
                if ((toInsert?.Count ?? 0) == 0)
                {
                    LogManager.Info($"[Direct] Aucun enregistrement à insérer dans T_Reconciliation pour {countryId} (liste vide).");
                }
                else
                {
                    var sampleIds = string.Join(", ", toInsert.Take(5).Select(r => r.ID));
                    LogManager.Debug($"[Direct] Préparation insertion T_Reconciliation - Total={toInsert.Count}, Exemples IDs=[{sampleIds}]");
                }

                // Connexion directe à la base du pays
                var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
                LogManager.Debug($"[Direct] Connexion T_Reconciliation via: {connectionString}");
                using (var conn = new OleDbConnection(connectionString))
                {
                    await conn.OpenAsync();

                    // 0) Désarchiver côté réconciliation les IDs "revenus" (présents en mises à jour)
                    int unarchivedCount = 0;
                    var toUnarchiveIds = (updatedRecords ?? new List<DataAmbre>()).
                        Select(d => d?.ID).
                        Where(id => !string.IsNullOrWhiteSpace(id)).
                        Distinct(StringComparer.OrdinalIgnoreCase).
                        ToList();
                    if (toUnarchiveIds.Count > 0)
                    {
                        using (var txU = conn.BeginTransaction())
                        {
                            try
                            {
                                var nowUtc = DateTime.UtcNow;
                                foreach (var id in toUnarchiveIds)
                                {
                                    using (var up = new OleDbCommand("UPDATE [T_Reconciliation] SET [DeleteDate]=NULL, [LastModified]=?, [ModifiedBy]=? WHERE [ID]=? AND [DeleteDate] IS NOT NULL", conn, txU))
                                    {
                                        var pMod = up.Parameters.Add("@LastModified", OleDbType.Date); pMod.Value = nowUtc;
                                        up.Parameters.AddWithValue("@ModifiedBy", (object)(_currentUser ?? "System") ?? DBNull.Value);
                                        up.Parameters.AddWithValue("@ID", id);
                                        unarchivedCount += await up.ExecuteNonQueryAsync();
                                    }
                                }
                                txU.Commit();
                            }
                            catch
                            {
                                txU.Rollback();
                                throw;
                            }
                        }
                        LogManager.Info($"[Direct] Désarchivage T_Reconciliation effectué: {unarchivedCount} ligne(s) réactivée(s)");
                    }

                    // 1) Archiver côté réconciliation les IDs supprimés côté AMBRE (DeleteDate)
                    int archivedCount = 0;
                    var toArchiveIds = (deletedRecords ?? new List<DataAmbre>())
                        .Select(d => d?.ID)
                        .Where(id => !string.IsNullOrWhiteSpace(id))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    if (toArchiveIds.Count > 0)
                    {
                        LogManager.Info($"[Direct] Archivage T_Reconciliation demandé pour {toArchiveIds.Count} ID(s) suite aux suppressions AMBRE");
                        using (var txA = conn.BeginTransaction())
                        {
                            try
                            {
                                var nowUtc = DateTime.UtcNow;
                                foreach (var id in toArchiveIds)
                                {
                                    using (var up = new OleDbCommand("UPDATE [T_Reconciliation] SET [DeleteDate]=?, [LastModified]=?, [ModifiedBy]=? WHERE [ID]=? AND [DeleteDate] IS NULL", conn, txA))
                                    {
                                        var pDel = up.Parameters.Add("@DeleteDate", OleDbType.Date); pDel.Value = nowUtc;
                                        var pMod = up.Parameters.Add("@LastModified", OleDbType.Date); pMod.Value = nowUtc;
                                        up.Parameters.AddWithValue("@ModifiedBy", (object)(_currentUser ?? "System") ?? DBNull.Value);
                                        up.Parameters.AddWithValue("@ID", id);
                                        archivedCount += await up.ExecuteNonQueryAsync();
                                    }
                                }
                                txA.Commit();
                            }
                            catch
                            {
                                txA.Rollback();
                                throw;
                            }
                        }
                        LogManager.Info($"[Direct] Archivage T_Reconciliation effectué: {archivedCount} ligne(s) marquée(s) supprimée(s)");
                    }

                    // 2) Construire un set d'IDs existants pour insert-only
                    var ids = toInsert.Select(r => r.ID).Where(id => !string.IsNullOrWhiteSpace(id)).Distinct().ToList();
                    var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (ids.Count > 0)
                    {
                        // Chunk pour éviter trop de paramètres
                        const int chunk = 500;
                        for (int i = 0; i < ids.Count; i += chunk)
                        {
                            var sub = ids.Skip(i).Take(chunk).ToList();
                            var placeholders = string.Join(",", Enumerable.Repeat("?", sub.Count));
                            var sql = $"SELECT [ID] FROM [T_Reconciliation] WHERE [ID] IN ({placeholders})";
                            using (var cmd = new OleDbCommand(sql, conn))
                            {
                                foreach (var id in sub) cmd.Parameters.AddWithValue("@ID", id);
                                using (var rdr = await cmd.ExecuteReaderAsync())
                                {
                                    while (await rdr.ReadAsync())
                                    {
                                        var id = rdr[0]?.ToString();
                                        if (!string.IsNullOrWhiteSpace(id)) existing.Add(id);
                                    }
                                }
                            }
                        }
                        LogManager.Debug($"[Direct] IDs existants détectés dans T_Reconciliation: {existing.Count} / {ids.Count}");
                    }

                    int insertedCount = 0;
                    using (var tx = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var r in toInsert)
                            {
                                if (existing.Contains(r.ID))
                                    continue; // insert-only

                                using (var cmd = new OleDbCommand(@"INSERT INTO [T_Reconciliation] (
    [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
    [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
    [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
    [IncidentType],[RiskyItem],[ReasonNonRisky],[CreationDate],[ModifiedBy],[LastModified]
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", conn, tx))
                                {
                                    // Paramètres (ordre strict pour OLE DB)
                                    cmd.Parameters.AddWithValue("@ID", (object)r.ID ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", (object)r.DWINGS_GuaranteeID ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", (object)r.DWINGS_InvoiceID ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@DWINGS_CommissionID", (object)r.DWINGS_CommissionID ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Action", (object)r.Action ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Comments", (object)r.Comments ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@InternalInvoiceReference", (object)r.InternalInvoiceReference ?? DBNull.Value);
                                    var pFirst = cmd.Parameters.Add("@FirstClaimDate", OleDbType.Date); pFirst.Value = r.FirstClaimDate.HasValue ? (object)r.FirstClaimDate.Value : DBNull.Value;
                                    var pLast  = cmd.Parameters.Add("@LastClaimDate",  OleDbType.Date); pLast.Value  = r.LastClaimDate.HasValue  ? (object)r.LastClaimDate.Value  : DBNull.Value;
                                    var pRem   = cmd.Parameters.Add("@ToRemind",      OleDbType.Boolean); pRem.Value   = r.ToRemind;
                                    var pRemD  = cmd.Parameters.Add("@ToRemindDate",  OleDbType.Date); pRemD.Value  = r.ToRemindDate.HasValue  ? (object)r.ToRemindDate.Value  : DBNull.Value;
                                    var pAck   = cmd.Parameters.Add("@ACK",          OleDbType.Boolean); pAck.Value   = r.ACK;
                                    cmd.Parameters.AddWithValue("@SwiftCode", (object)r.SwiftCode ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@PaymentReference", (object)r.PaymentReference ?? DBNull.Value);
                                    var pKpi   = cmd.Parameters.Add("@KPI",          OleDbType.Integer); pKpi.Value   = r.KPI.HasValue ? (object)r.KPI.Value : DBNull.Value;
                                    var pInc   = cmd.Parameters.Add("@IncidentType", OleDbType.Integer); pInc.Value   = r.IncidentType.HasValue ? (object)r.IncidentType.Value : DBNull.Value;
                                    var pRisky = cmd.Parameters.Add("@RiskyItem",    OleDbType.Boolean); pRisky.Value = r.RiskyItem.HasValue ? (object)r.RiskyItem.Value : DBNull.Value;
                                    var pReason= cmd.Parameters.Add("@ReasonNonRisky", OleDbType.Integer); pReason.Value = r.ReasonNonRisky.HasValue ? (object)r.ReasonNonRisky.Value : DBNull.Value;
                                    var pCre   = cmd.Parameters.Add("@CreationDate", OleDbType.Date); pCre.Value = r.CreationDate.HasValue ? (object)r.CreationDate.Value : DBNull.Value;
                                    cmd.Parameters.AddWithValue("@ModifiedBy", (object)(r.ModifiedBy ?? _currentUser) ?? DBNull.Value);
                                    var pMod   = cmd.Parameters.Add("@LastModified", OleDbType.Date); pMod.Value = r.LastModified.HasValue ? (object)r.LastModified.Value : DBNull.Value;

                                    insertedCount += await cmd.ExecuteNonQueryAsync();
                                }
                            }

                            tx.Commit();
                            LogManager.Info($"[Direct] T_Reconciliation (insert-only) pour {countryId}: Inserted={insertedCount}");
                        }
                        catch
                        {
                            tx.Rollback();
                            throw;
                        }
                    }
                }

                // Change-tracking désactivé pour Ambre: pas d'écriture ChangeLog
                LogManager.Info("[ChangeLog] Désactivé pour Ambre: aucune écriture dans ChangeLog pour T_Reconciliation.");
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la mise à jour de T_Reconciliation pour {countryId}: {ex.Message}", ex);
                throw new InvalidOperationException($"Erreur lors de la mise à jour de la table T_Reconciliation: {ex.Message}", ex);
            }
        }


        /// <summary>
        /// Filtre les lignes brutes pour ne conserver que celles dont Account_ID correspond
        /// aux comptes Pivot ou Receivable du pays référentiel.
        /// </summary>
        /// <param name="rawData">Lignes lues depuis Excel (clé = nom de champ, ex: "Account_ID")</param>
        /// <param name="country">Pays courant (doit contenir CNT_AmbrePivot / CNT_AmbreReceivable)</param>
        /// <returns>Liste filtrée</returns>
        private List<Dictionary<string, object>> FilterRowsByCountryAccounts(
            List<Dictionary<string, object>> rawData, Country country)
        {
            if (country == null)
                throw new InvalidOperationException("Configuration pays manquante.");

            var pivot = country.CNT_AmbrePivot?.Trim();
            var recv = country.CNT_AmbreReceivable?.Trim();

            if (string.IsNullOrWhiteSpace(pivot) && string.IsNullOrWhiteSpace(recv))
            {
                throw new InvalidOperationException(
                    $"Aucun compte Pivot/Receivable défini dans le référentiel pour le pays {country.CNT_Id}.");
            }

            bool Matches(string accountId)
            {
                if (string.IsNullOrWhiteSpace(accountId)) return false;
                var acc = accountId.Trim();
                return (!string.IsNullOrWhiteSpace(pivot) && string.Equals(acc, pivot, StringComparison.OrdinalIgnoreCase))
                       || (!string.IsNullOrWhiteSpace(recv) && string.Equals(acc, recv, StringComparison.OrdinalIgnoreCase));
            }

            return rawData.Where(row =>
            {
                if (!row.TryGetValue("Account_ID", out var val) || val == null)
                    return false;
                return Matches(val.ToString());
            }).ToList();
        }

        /// <summary>
        /// Crée un objet Reconciliation à partir d'un DataAmbre avec les règles métiers appliquées
        /// </summary>
        private Reconciliation CreateReconciliationFromDataAmbre(DataAmbre dataAmbre, Country country)
        {
            var reconciliation = new Reconciliation
            {
                ID = dataAmbre.ID,
                CreationDate = DateTime.UtcNow,
                ModifiedBy = _currentUser ?? "System",
                LastModified = DateTime.UtcNow,
                Version = 1
            };

            // Appliquer les règles métiers selon le type de compte
            bool isPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot);
            bool isReceivable = dataAmbre.IsReceivableAccount(country.CNT_AmbreReceivable);

            var transactionType = _transformationService.DetermineTransactionType(dataAmbre.RawLabel, isPivot);
            var creditDebit = _transformationService.DetermineCreditDebit(dataAmbre.SignedAmount);
            string guaranteeType = !isPivot ? ExtractGuaranteeType(dataAmbre.RawLabel) : null;

            // Defer Action/KPI assignment to UpdateReconciliationTable where we have reconciliation context (TriggerDate, reminders, etc.)
            // and can apply automatic action rules without overwriting user-set values.

            return reconciliation;
        }

        #region Méthodes utilitaires de conversion

        /// <summary>
        /// Récupère une valeur du dictionnaire Properties ou null si la clé n'existe pas
        /// </summary>
        private static object GetPropertyValue(Dictionary<string, object> properties, string key)
        {
            if (properties == null || string.IsNullOrEmpty(key))
                return null;
                
            return properties.TryGetValue(key, out object value) ? value : null;
        }

        /// <summary>
        /// Convertit une valeur en decimal nullable
        /// </summary>
        private static decimal ConvertToDecimal(object value)
        {
            if (decimal.TryParse(value?.ToString(), out decimal result))
                return result;
            return 0;
        }

        /// <summary>
        /// Convertit une valeur en DateTime nullable
        /// </summary>
        private static DateTime? ConvertToDateTime(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;
                
            if (DateTime.TryParse(value.ToString(), out DateTime result))
                return result;
                
            return null;
        }

        #endregion

        #endregion
    }
    #endregion
}