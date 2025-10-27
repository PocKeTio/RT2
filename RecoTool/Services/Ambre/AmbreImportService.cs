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
            var totalTimer = System.Diagnostics.Stopwatch.StartNew();
            LogManager.Info($"[PERF] ===== AMBRE IMPORT STARTED for {countryId} =====");
            
            try
            {
                // 1. Initialisation et validation
                var initTimer = System.Diagnostics.Stopwatch.StartNew();
                await _configurationLoader.EnsureInitializedAsync();
                progressCallback?.Invoke(isMultiFile ? "Validating files..." : "Validating file...", 0);
                
                var files = _validator.ValidateFiles(filePaths, isMultiFile, result);
                if (result.Errors.Any()) return result;
                initTimer.Stop();
                LogManager.Info($"[PERF] Initialization and validation completed in {initTimer.ElapsedMilliseconds}ms");

                // 2. Chargement des configurations
                var configTimer = System.Diagnostics.Stopwatch.StartNew();
                progressCallback?.Invoke("Loading configurations...", 10);
                var config = await _configurationLoader.LoadConfigurationsAsync(countryId, result);
                if (result.Errors.Any()) return result;
                configTimer.Stop();
                LogManager.Info($"[PERF] Configuration loading completed in {configTimer.ElapsedMilliseconds}ms");

                // 3. Préparation de l'environnement
                var prepTimer = System.Diagnostics.Stopwatch.StartNew();
                progressCallback?.Invoke("Preparing environment...", 15);
                // Determine whether to use a global lock for the import (default: disabled)
                bool useGlobalLock = false;
                try
                {
                    var param = _offlineFirstService.GetParameter("UseGlobalLockForAmbreImport");
                    if (!string.IsNullOrWhiteSpace(param)) bool.TryParse(param, out useGlobalLock);
                }
                catch { }

                if (!await PrepareEnvironmentAsync(countryId, result, useGlobalLock))
                    return result;
                prepTimer.Stop();
                LogManager.Info($"[PERF] Environment preparation completed in {prepTimer.ElapsedMilliseconds}ms");

                // 4. Import avec ou sans verrou global (par défaut: désactivé)
                if (useGlobalLock)
                {
                    var lockTimer = System.Diagnostics.Stopwatch.StartNew();
                    using (_offlineFirstService.BeginAmbreImportScope())
                    using (var globalLock = await AcquireGlobalLockAsync(countryId, result))
                    {
                        if (globalLock == null) return result;
                        lockTimer.Stop();
                        LogManager.Info($"[PERF] Global lock acquired in {lockTimer.ElapsedMilliseconds}ms");

                        // Set sync status now that we have the lock
                        try { await _offlineFirstService.SetSyncStatusAsync("Processing"); } catch { }

                        // 5. Lecture et traitement des données
                        var processTimer = System.Diagnostics.Stopwatch.StartNew();
                        var processedData = await ProcessDataAsync(
                            files, config, countryId, isMultiFile, result, progressCallback);
                        
                        if (!processedData.Any())
                        {
                            result.Errors.Add("No valid data after processing.");
                            return result;
                        }
                        processTimer.Stop();
                        LogManager.Info($"[PERF] Data processing completed: {processedData.Count} records in {processTimer.ElapsedMilliseconds}ms");

                        // 6. Synchronisation avec la base de données
                        try { await _offlineFirstService.SetSyncStatusAsync("Synchronizing"); } catch { }
                        var syncTimer = System.Diagnostics.Stopwatch.StartNew();
                        await _databaseSynchronizer.SynchronizeAsync(
                            processedData, countryId, result, progressCallback);
                        syncTimer.Stop();
                        LogManager.Info($"[PERF] Database synchronization completed in {syncTimer.ElapsedMilliseconds}ms");
                    }
                }
                else
                {
                    // No global lock: still run processing + sync, but do not assume lock
                    try { await _offlineFirstService.SetSyncStatusAsync("Processing"); } catch { }

                    var processTimer = System.Diagnostics.Stopwatch.StartNew();
                    var processedData = await ProcessDataAsync(
                        files, config, countryId, isMultiFile, result, progressCallback);
                    if (!processedData.Any())
                    {
                        result.Errors.Add("No valid data after processing.");
                        return result;
                    }
                    processTimer.Stop();
                    LogManager.Info($"[PERF] Data processing completed: {processedData.Count} records in {processTimer.ElapsedMilliseconds}ms");

                    try { await _offlineFirstService.SetSyncStatusAsync("Synchronizing"); } catch { }
                    var syncTimer = System.Diagnostics.Stopwatch.StartNew();
                    await _databaseSynchronizer.SynchronizeAsync(
                        processedData, countryId, result, progressCallback);
                    syncTimer.Stop();
                    LogManager.Info($"[PERF] Database synchronization completed in {syncTimer.ElapsedMilliseconds}ms");
                }

                // 7. Finalisation
                var finalTimer = System.Diagnostics.Stopwatch.StartNew();
                await FinalizeImportAsync(countryId, result, progressCallback);
                finalTimer.Stop();
                LogManager.Info($"[PERF] Finalization completed in {finalTimer.ElapsedMilliseconds}ms");
                
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
                totalTimer.Stop();
                LogManager.Info($"[PERF] ===== AMBRE IMPORT COMPLETED for {countryId} in {totalTimer.ElapsedMilliseconds}ms (total) =====");
                return result;
            }
            catch (Exception ex)
            {
                totalTimer.Stop();
                result.Errors.Add($"Error during import: {ex.Message}");
                result.EndTime = DateTime.UtcNow;
                LogManager.Error($"[PERF] ===== AMBRE IMPORT FAILED for {countryId} after {totalTimer.ElapsedMilliseconds}ms =====", ex);
                
                // Force reconnection even on error to avoid lingering OleDbExceptions
                try { TodoListSessionTracker.CloseAllConnections(); } catch { }
                
                return result;
            }
        }

        private async Task<bool> PrepareEnvironmentAsync(string countryId, ImportResult result, bool useGlobalLock)
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
                        countryId, assumeLockHeld: useGlobalLock);
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
            // Derive wait and lease from T_Param if present; defaults: wait=120s, lease=300s
            int waitSec = 120;
            int leaseSec = 300;
            try { var s = _offlineFirstService.GetParameter("ImportGlobalLockAcquireWaitSeconds"); if (!string.IsNullOrWhiteSpace(s)) int.TryParse(s, out waitSec); } catch { }
            try { var s = _offlineFirstService.GetParameter("ImportGlobalLockLeaseSeconds"); if (!string.IsNullOrWhiteSpace(s)) int.TryParse(s, out leaseSec); } catch { }

            if (waitSec < 30) waitSec = 30; if (waitSec > 600) waitSec = 600;
            if (leaseSec < 120) leaseSec = 120; if (leaseSec > 1800) leaseSec = 1800;

            var waitBudget = TimeSpan.FromSeconds(waitSec);
            var lease = TimeSpan.FromSeconds(leaseSec);

            LogManager.Info($"Attempting to acquire global lock for {countryId} (wait={waitSec}s, lease={leaseSec}s)...");

            using (var cts = new System.Threading.CancellationTokenSource(waitBudget))
            {
                try
                {
                    // Note: the OfflineFirstService method uses the timeout both as wait budget and lease.
                    // We pass the desired lease here and bound wait via CancellationToken to ensure in-process gate is released on timeout.
                    var handle = await _offlineFirstService.AcquireGlobalLockAsync(countryId, "AmbreImport", lease, cts.Token);
                    if (handle == null)
                    {
                        result.Errors.Add($"Unable to acquire global lock for {countryId}");
                        return null;
                    }
                    LogManager.Info($"Global lock acquired for {countryId}");
                    return handle;
                }
                catch (OperationCanceledException)
                {
                    result.Errors.Add($"Unable to obtain global lock within {waitSec} seconds.");
                    return null;
                }
            }
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