using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;
using RecoTool.Services.DTOs;
using RecoTool.Services.AmbreImport;

namespace RecoTool.Services
{
    /// <summary>
    /// Service principal d'import des données Ambre avec gestion offline-first
    /// </summary>
    public class AmbreImportService
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly AmbreDataProcessor _dataProcessor;
        private readonly AmbreConfigurationLoader _configurationLoader;
        private readonly AmbreDatabaseSynchronizer _databaseSynchronizer;
        private readonly AmbreImportValidator _validator;
        private readonly string _currentUser;

        public AmbreImportService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = Environment.UserName;
            
            _dataProcessor = new AmbreDataProcessor(offlineFirstService, _currentUser);
            _configurationLoader = new AmbreConfigurationLoader(offlineFirstService);
            _databaseSynchronizer = new AmbreDatabaseSynchronizer(offlineFirstService, _currentUser);
            _validator = new AmbreImportValidator();

            // Wire configuration loader so that TransformationService/CodeToCategory are available to the processor
            _dataProcessor.SetConfigurationLoader(_configurationLoader);
        }

        /// <summary>
        /// Importe plusieurs fichiers Excel Ambre (fusionnés) pour un pays donné
        /// </summary>
        public async Task<ImportResult> ImportAmbreFiles(
            string[] filePaths, 
            string countryId,
            Action<string, int> progressCallback = null)
        {
            if (filePaths == null || filePaths.Length == 0)
                return new ImportResult { CountryId = countryId, Errors = { "No files provided" } };

            var take = filePaths.Take(2).ToArray();
            return await ImportAmbreCoreAsync(take, countryId, isMultiFile: true, progressCallback);
        }

        /// <summary>
        /// Importe un fichier Excel Ambre pour un pays donné
        /// </summary>
        public async Task<ImportResult> ImportAmbreFile(
            string filePath, 
            string countryId,
            Action<string, int> progressCallback = null)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                return new ImportResult { CountryId = countryId, Errors = { "File path is required" } };

            return await ImportAmbreCoreAsync(new[] { filePath }, countryId, isMultiFile: false, progressCallback);
        }

        private async Task<ImportResult> ImportAmbreCoreAsync(
            string[] filePaths, 
            string countryId, 
            bool isMultiFile, 
            Action<string, int> progressCallback)
        {
            var result = new ImportResult { CountryId = countryId, StartTime = DateTime.UtcNow };
            
            try
            {
                // 1. Initialisation et validation
                await _configurationLoader.EnsureInitializedAsync();
                progressCallback?.Invoke(isMultiFile ? "Validating files..." : "Validating file...", 0);
                
                var files = _validator.ValidateFiles(filePaths, isMultiFile, result);
                if (result.Errors.Any()) return result;

                // 2. Chargement des configurations
                progressCallback?.Invoke("Loading configurations...", 10);
                var config = await _configurationLoader.LoadConfigurationsAsync(countryId, result);
                if (result.Errors.Any()) return result;

                // 3. Préparation de l'environnement
                progressCallback?.Invoke("Preparing environment...", 15);
                if (!await PrepareEnvironmentAsync(countryId, result))
                    return result;

                // 4. Import avec verrou global
                using (_offlineFirstService.BeginAmbreImportScope())
                using (var globalLock = await AcquireGlobalLockAsync(countryId, result))
                {
                    if (globalLock == null) return result;

                    // Set sync status now that we have the lock
                    try { await _offlineFirstService.SetSyncStatusAsync("Processing"); } catch { }

                    // 5. Lecture et traitement des données
                    var processedData = await ProcessDataAsync(
                        files, config, countryId, isMultiFile, result, progressCallback);
                    
                    if (!processedData.Any())
                    {
                        result.Errors.Add("No valid data after processing.");
                        return result;
                    }

                    // 6. Synchronisation avec la base de données
                    try { await _offlineFirstService.SetSyncStatusAsync("Synchronizing"); } catch { }
                    await _databaseSynchronizer.SynchronizeAsync(
                        processedData, countryId, result, progressCallback);
                }

                // 7. Finalisation
                await FinalizeImportAsync(countryId, result, progressCallback);
                
                // Force reconnection of TodoListSessionTracker to avoid lingering OleDbExceptions
                // after import completes (in case Access DB had lock contention issues)
                try
                {
                    TodoListSessionTracker.CloseAllConnections();
                    LogManager.Info("TodoListSessionTracker connections reset after import");
                }
                catch { /* best effort */ }
                
                result.IsSuccess = true;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Error during import: {ex.Message}");
                result.EndTime = DateTime.UtcNow;
                
                // Force reconnection even on error to avoid lingering OleDbExceptions
                try { TodoListSessionTracker.CloseAllConnections(); } catch { }
                
                return result;
            }
        }

        private async Task<bool> PrepareEnvironmentAsync(string countryId, ImportResult result)
        {
            var switched = await _offlineFirstService.SetCurrentCountryAsync(countryId, suppressPush: true);
            if (!switched)
            {
                result.Errors.Add($"Unable to initialize local database for {countryId}");
                return false;
            }

            // Note: SetSyncStatusAsync is now called AFTER acquiring the global lock to avoid DB contention

            var unsyncedCount = await _offlineFirstService.GetUnsyncedChangeCountAsync(countryId);
            if (unsyncedCount > 0)
            {
                LogManager.Info($"{unsyncedCount} unsynced change(s) found. Pushing to network...");
                try
                {
                    var pushed = await _offlineFirstService.PushPendingChangesToNetworkAsync(
                        countryId, assumeLockHeld: true);
                    LogManager.Info($"Pushed {pushed} local change(s) to network.");
                }
                catch (Exception pushEx)
                {
                    LogManager.Error("Error while pushing local changes before import", pushEx);
                    result.Errors.Add("Unable to push local changes before import.");
                    return false;
                }
            }

            await _offlineFirstService.CopyNetworkToLocalAsync(countryId);
            return true;
        }

        private async Task<IDisposable> AcquireGlobalLockAsync(string countryId, ImportResult result)
        {
            var lockTimeout = TimeSpan.FromMinutes(2);
            LogManager.Info($"Attempting to acquire global lock for {countryId}...");

            var acquireTask = _offlineFirstService.AcquireGlobalLockAsync(
                countryId, "AmbreImport", TimeSpan.FromMinutes(30));
            
            var completed = await Task.WhenAny(acquireTask, Task.Delay(lockTimeout)) == acquireTask;
            if (!completed)
            {
                result.Errors.Add($"Unable to obtain global lock within {lockTimeout.TotalSeconds} seconds.");
                return null;
            }

            var globalLock = await acquireTask;
            if (globalLock == null)
            {
                result.Errors.Add($"Unable to acquire global lock for {countryId}");
                return null;
            }

            LogManager.Info($"Global lock acquired for {countryId}");
            return globalLock;
        }

        private async Task<List<DataAmbre>> ProcessDataAsync(
            string[] files,
            AmbreImportConfiguration config,
            string countryId,
            bool isMultiFile,
            ImportResult result,
            Action<string, int> progressCallback)
        {
            // Lecture des fichiers Excel
            var rawData = await _dataProcessor.ReadExcelFilesAsync(
                files, config.ImportFields, isMultiFile, progressCallback);

            if (!rawData.Any())
            {
                result.Errors.Add("No data found in the Excel file(s).");
                return new List<DataAmbre>();
            }

            // Filtrage par comptes du pays
            var filtered = _dataProcessor.FilterRowsByCountryAccounts(rawData, config.Country);
            if (!filtered.Any())
            {
                result.Errors.Add($"No rows match the country's AMBRE accounts.");
                return new List<DataAmbre>();
            }

            // Validation des comptes requis
            if (!_validator.ValidateRequiredAccounts(filtered, config.Country, result))
                return new List<DataAmbre>();

            // Transformation et validation
            progressCallback?.Invoke("Transforming data...", 40);
            var transformed = await _dataProcessor.TransformDataAsync(
                filtered, config.Transforms, config.Country);

            progressCallback?.Invoke("Validating data...", 60);
            var validationResult = _validator.ValidateTransformedData(transformed, config.Country);
            result.ValidationErrors.AddRange(validationResult.errors);

            return validationResult.validData;
        }

        private async Task FinalizeImportAsync(
            string countryId, 
            ImportResult result, 
            Action<string, int> progressCallback)
        {
            progressCallback?.Invoke("Refreshing local configuration...", 90);
            try
            {
                await _offlineFirstService.RefreshConfigurationAsync();
                await _offlineFirstService.SetCurrentCountryAsync(countryId, suppressPush: true);
                await _offlineFirstService.SetLastSyncAnchorAsync(countryId, DateTime.UtcNow);
                
                // Invalidate all caches for this country after AMBRE import
                // This ensures fresh data is loaded for counts, status, and reconciliation views
                ReconciliationService.InvalidateReconciliationViewCache(countryId);
                LogManager.Info($"Cache invalidated for country {countryId} after AMBRE import");
            }
            catch (Exception refreshEx)
            {
                LogManager.Error("Error while refreshing configuration after import", refreshEx);
                result.Errors.Add($"Error while refreshing local configuration: {refreshEx.Message}");
            }

            progressCallback?.Invoke("Finalizing...", 100);
        }
    }
}