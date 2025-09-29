using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.AmbreImport
{
    /// <summary>
    /// Gestionnaire de synchronisation avec la base de données pour l'import Ambre
    /// </summary>
    public class AmbreDatabaseSynchronizer
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly string _currentUser;
        private ReconciliationService _reconciliationService;

        public AmbreDatabaseSynchronizer(OfflineFirstService offlineFirstService, string currentUser)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = currentUser;
        }

        /// <summary>
        /// Synchronise les données avec la base de données
        /// </summary>
        public async Task SynchronizeAsync(
            List<DataAmbre> validData,
            string countryId,
            ImportResult result,
            Action<string, int> progressCallback)
        {
            progressCallback?.Invoke("Synchronizing with database...", 80);
            
            var syncResult = await SynchronizeWithDatabaseAsync(
                validData, countryId, performNetworkSync: true, assumeGlobalLockHeld: true);
                
            result.NewRecords = syncResult.newCount;
            result.UpdatedRecords = syncResult.updatedCount;
            result.DeletedRecords = syncResult.deletedCount;
            result.ProcessedRecords = validData.Count;
        }

        private async Task<(int newCount, int updatedCount, int deletedCount)> SynchronizeWithDatabaseAsync(
            List<DataAmbre> newData, 
            string countryId, 
            bool performNetworkSync, 
            bool assumeGlobalLockHeld = false)
        {
            try
            {
                // 1. Charger les données existantes et calculer les changements
                var existingData = await LoadExistingDataAsync(countryId);
                var changes = CalculateChanges(existingData, newData);
                
                LogManager.Info($"Calculated changes for {countryId} - New: {changes.ToAdd.Count}, Updated: {changes.ToUpdate.Count}, Deleted: {changes.ToArchive.Count}");

                // 2. Appliquer les changements
                await ExecuteChangesAsync(changes, countryId, assumeGlobalLockHeld);

                // 3. Synchronisation réseau si nécessaire
                if (performNetworkSync && !assumeGlobalLockHeld)
                {
                    await PerformNetworkSyncAsync(countryId);
                }

                return (changes.ToAdd.Count, changes.ToUpdate.Count, changes.ToArchive.Count);
            }
            catch (Exception ex)
            {
                LogManager.Error($"Error during database synchronization for {countryId}", ex);
                throw new InvalidOperationException($"Database synchronization failed: {ex.Message}", ex);
            }
        }

        private async Task ExecuteChangesAsync(ImportChanges changes, string countryId, bool assumeGlobalLockHeld)
        {
            async Task ApplyChangesInternalAsync()
            {
                try
                {
                    // Backup
                    try { await _offlineFirstService.CreateLocalReconciliationBackupAsync(countryId, "PreImport"); } catch { }

                    // Apply changes
                    try { await _offlineFirstService.SetSyncStatusAsync("ApplyingChanges"); } catch { }
                    await ApplyChangesAsync(changes, countryId);

                    // Update reconciliation
                    try { await _offlineFirstService.SetSyncStatusAsync("Reconciling"); } catch { }
                    await UpdateReconciliationTableAsync(changes, countryId);

                    // Snapshot KPIs
                    await CreateKpiSnapshotAsync(countryId);

                    // Publish to network
                    try { await _offlineFirstService.SetSyncStatusAsync("Publishing"); } catch { }
                    // 1) Publish AMBRE as ZIP to network
                    try { await _offlineFirstService.CopyLocalToNetworkAmbreAsync(countryId).ConfigureAwait(false); } catch (Exception ex) { LogManager.Warning($"AMBRE: publish to network failed: {ex.Message}"); }
                    // 2) Publish country reconciliation DB to network
                    await _offlineFirstService.CopyLocalToNetworkAsync(countryId).ConfigureAwait(false);

                    // Finalize
                    try { await _offlineFirstService.SetSyncStatusAsync("Finalizing"); } catch { }
                    await _offlineFirstService.MarkAllLocalChangesAsSyncedAsync(countryId);
                    
                    // Cleanup
                    try { await _offlineFirstService.CleanupChangeLogAndCompactAsync(countryId); } catch { }
                }
                catch (Exception ex)
                {
                    LogManager.Error($"Error during local import for {countryId}", ex);
                    try { await _offlineFirstService.SetSyncStatusAsync("Error"); } catch { }
                    throw;
                }
            }

            if (assumeGlobalLockHeld)
            {
                await ApplyChangesInternalAsync();
            }
            else
            {
                using (var globalLock = await AcquireGlobalLockAsync(countryId))
                {
                    if (globalLock == null)
                        throw new InvalidOperationException($"Unable to acquire global lock for {countryId}");
                    
                    await ApplyChangesInternalAsync();
                }
            }
        }

        private async Task<IDisposable> AcquireGlobalLockAsync(string countryId)
        {
            var lockTimeout = TimeSpan.FromMinutes(2);
            var acquireTask = _offlineFirstService.AcquireGlobalLockAsync(countryId, "AmbreImport", TimeSpan.FromMinutes(30));
            
            if (await Task.WhenAny(acquireTask, Task.Delay(lockTimeout)) != acquireTask)
            {
                throw new TimeoutException($"Unable to obtain global lock within {lockTimeout.TotalSeconds} seconds");
            }
            
            return await acquireTask;
        }

        private async Task PerformNetworkSyncAsync(string countryId)
        {
            try
            {
                LogManager.Info($"Starting network synchronization for {countryId}");
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
            }
        }

        private async Task<List<DataAmbre>> LoadExistingDataAsync(string countryId)
        {
            LogManager.Info($"Loading existing Ambre data for {countryId}");
            
            var entities = await _offlineFirstService.GetEntitiesAsync(countryId, "T_Data_Ambre");
            var existingData = new List<DataAmbre>();
            
            if (entities != null)
            {
                foreach (var entity in entities)
                {
                    var dataAmbre = ConvertEntityToDataAmbre(entity);
                    existingData.Add(dataAmbre);
                }
            }
            
            LogManager.Info($"Loaded {existingData.Count} existing records");
            return existingData;
        }

        private DataAmbre ConvertEntityToDataAmbre(Entity entity)
        {
            return new DataAmbre
            {
                ID = GetPropertyValue<string>(entity.Properties, "ID"),
                Country = GetPropertyValue<string>(entity.Properties, "Country"),
                Account_ID = GetPropertyValue<string>(entity.Properties, "Account_ID"),
                CCY = GetPropertyValue<string>(entity.Properties, "CCY"),
                Event_Num = GetPropertyValue<string>(entity.Properties, "Event_Num"),
                Folder = GetPropertyValue<string>(entity.Properties, "Folder"),
                Pivot_MbawIDFromLabel = GetPropertyValue<string>(entity.Properties, "Pivot_MbawIDFromLabel"),
                Pivot_TransactionCodesFromLabel = GetPropertyValue<string>(entity.Properties, "Pivot_TransactionCodesFromLabel"),
                Pivot_TRNFromLabel = GetPropertyValue<string>(entity.Properties, "Pivot_TRNFromLabel"),
                RawLabel = GetPropertyValue<string>(entity.Properties, "RawLabel"),
                SignedAmount = GetPropertyValue<decimal>(entity.Properties, "SignedAmount"),
                LocalSignedAmount = GetPropertyValue<decimal>(entity.Properties, "LocalSignedAmount"),
                Operation_Date = GetPropertyValue<DateTime?>(entity.Properties, "Operation_Date"),
                Value_Date = GetPropertyValue<DateTime?>(entity.Properties, "Value_Date"),
                Category = GetPropertyValue<int?>(entity.Properties, "Category"),
                ReconciliationOrigin_Num = GetPropertyValue<string>(entity.Properties, "ReconciliationOrigin_Num"),
                Reconciliation_Num = GetPropertyValue<string>(entity.Properties, "Reconciliation_Num"),
                Receivable_InvoiceFromAmbre = GetPropertyValue<string>(entity.Properties, "Receivable_InvoiceFromAmbre"),
                Receivable_DWRefFromAmbre = GetPropertyValue<string>(entity.Properties, "Receivable_DWRefFromAmbre"),
                LastModified = GetPropertyValue<DateTime?>(entity.Properties, "LastModified"),
                DeleteDate = GetPropertyValue<DateTime?>(entity.Properties, "DeleteDate"),
                CreationDate = GetPropertyValue<DateTime?>(entity.Properties, "CreationDate"),
                ModifiedBy = GetPropertyValue<string>(entity.Properties, "ModifiedBy"),
                Version = GetPropertyValue<int>(entity.Properties, "Version", 1)
            };
        }

        private T GetPropertyValue<T>(Dictionary<string, object> properties, string key, T defaultValue = default)
        {
            if (properties == null || !properties.TryGetValue(key, out var value))
                return defaultValue;
                
            if (value == null || value == DBNull.Value)
                return defaultValue;
                
            try
            {
                if (typeof(T) == typeof(string))
                    return (T)(object)value.ToString();
                    
                if (typeof(T) == typeof(int) || typeof(T) == typeof(int?))
                {
                    if (int.TryParse(value.ToString(), out var intValue))
                        return (T)(object)intValue;
                }
                
                if (typeof(T) == typeof(decimal) || typeof(T) == typeof(decimal?))
                {
                    if (decimal.TryParse(value.ToString(), out var decimalValue))
                        return (T)(object)decimalValue;
                }
                
                if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTime?))
                {
                    if (DateTime.TryParse(value.ToString(), out var dateValue))
                        return (T)(object)dateValue;
                }
                
                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch
            {
                return defaultValue;
            }
        }

        private ImportChanges CalculateChanges(List<DataAmbre> existingData, List<DataAmbre> newData)
        {
            LogManager.Info($"Calculating changes - Existing: {existingData.Count}, New: {newData.Count}");
            
            var changes = new ImportChanges();
            var existingByKey = existingData.ToDictionary(d => d.GetUniqueKey(), d => d);
            var newByKey = newData.ToDictionary(d => d.GetUniqueKey(), d => d);
            
            // Identify additions and updates
            foreach (var newItem in newData)
            {
                var key = newItem.GetUniqueKey();
                
                if (existingByKey.TryGetValue(key, out var existingItem))
                {
                    ProcessExistingItem(newItem, existingItem, changes);
                }
                else
                {
                    ProcessNewItem(newItem, changes);
                }
            }
            
            // Identify deletions
            foreach (var existingItem in existingData)
            {
                var key = existingItem.GetUniqueKey();
                if (!newByKey.ContainsKey(key))
                {
                    ProcessDeletedItem(existingItem, changes);
                }
            }
            
            LogManager.Debug($"Changes calculated - Add: {changes.ToAdd.Count}, Update: {changes.ToUpdate.Count}, Delete: {changes.ToArchive.Count}");
            return changes;
        }

        private void ProcessExistingItem(DataAmbre newItem, DataAmbre existingItem, ImportChanges changes)
        {
            if (existingItem.DeleteDate.HasValue)
            {
                // Revival of archived item
                newItem.ID = existingItem.ID;
                newItem.Version = existingItem.Version + 1;
                newItem.CreationDate = existingItem.CreationDate;
                newItem.DeleteDate = null;
                newItem.LastModified = DateTime.UtcNow;
                newItem.ModifiedBy = _currentUser;
                changes.ToUpdate.Add(newItem);
            }
            else if (HasDataChanged(existingItem, newItem))
            {
                // Update needed
                newItem.ID = existingItem.ID;
                newItem.Version = existingItem.Version + 1;
                newItem.CreationDate = existingItem.CreationDate;
                newItem.LastModified = DateTime.UtcNow;
                newItem.ModifiedBy = _currentUser;
                changes.ToUpdate.Add(newItem);
            }
        }

        private void ProcessNewItem(DataAmbre newItem, ImportChanges changes)
        {
            newItem.ID = newItem.GetUniqueKey();
            newItem.Version = 1;
            newItem.CreationDate = DateTime.UtcNow;
            newItem.LastModified = DateTime.UtcNow;
            newItem.ModifiedBy = _currentUser;
            changes.ToAdd.Add(newItem);
        }

        private void ProcessDeletedItem(DataAmbre existingItem, ImportChanges changes)
        {
            existingItem.DeleteDate = DateTime.UtcNow;
            existingItem.LastModified = DateTime.UtcNow;
            existingItem.ModifiedBy = _currentUser;
            existingItem.Version += 1;
            changes.ToArchive.Add(existingItem);
        }

        private bool HasDataChanged(DataAmbre existing, DataAmbre newData)
        {
            return existing.Account_ID != newData.Account_ID ||
                   existing.CCY != newData.CCY ||
                   existing.Event_Num != newData.Event_Num ||
                   existing.Folder != newData.Folder ||
                   existing.Pivot_MbawIDFromLabel != newData.Pivot_MbawIDFromLabel ||
                   existing.Pivot_TransactionCodesFromLabel != newData.Pivot_TransactionCodesFromLabel ||
                   existing.Pivot_TRNFromLabel != newData.Pivot_TRNFromLabel ||
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

        private async Task ApplyChangesAsync(ImportChanges changes, string countryId)
        {
            LogManager.Info($"Applying changes (batch) - Add: {changes.ToAdd.Count}, Update: {changes.ToUpdate.Count}, Delete: {changes.ToArchive.Count}");

            var toAdd = changes.ToAdd.Select(ConvertDataAmbreToEntity).ToList();
            var toUpdate = changes.ToUpdate.Select(ConvertDataAmbreToEntity).ToList();
            var toArchive = changes.ToArchive.Select(ConvertDataAmbreToEntity).ToList();

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ok = await _offlineFirstService.ApplyEntitiesBatchAsync(countryId, toAdd, toUpdate, toArchive, suppressChangeLog: true);
            sw.Stop();

            if (!ok)
            {
                LogManager.Warning("Batch application returned false. Check error logs.");
            }

            LogManager.Info($"Changes applied in {sw.Elapsed.TotalSeconds:F1}s");
        }

        private Entity ConvertDataAmbreToEntity(DataAmbre dataAmbre)
        {
            return new Entity
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
                    ["Pivot_MbawIDFromLabel"] = dataAmbre.Pivot_MbawIDFromLabel,
                    ["Pivot_TransactionCodesFromLabel"] = dataAmbre.Pivot_TransactionCodesFromLabel,
                    ["Pivot_TRNFromLabel"] = dataAmbre.Pivot_TRNFromLabel,
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
                    ["Version"] = dataAmbre.Version,
                    ["CreationDate"] = dataAmbre.CreationDate,
                    ["LastModified"] = dataAmbre.LastModified,
                    ["ModifiedBy"] = dataAmbre.ModifiedBy,
                    ["DeleteDate"] = dataAmbre.DeleteDate
                }
            };
        }

        private async Task CreateKpiSnapshotAsync(string countryId)
        {
            try
            {
                await EnsureRecoServiceAsync();
                var kpiSvc = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
                
                await kpiSvc.FreezeLatestSnapshotAsync(countryId);
                
                var lastOpDate = await _reconciliationService.GetLastAmbreOperationDateAsync(countryId) ?? DateTime.Today;
                await kpiSvc.SaveDailyKpiSnapshotAsync(lastOpDate, countryId, sourceVersion: "PostAmbreImport");
            }
            catch (Exception snapEx)
            {
                LogManager.Warning($"Non-blocking KPI snapshot: {snapEx.Message}");
            }
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
                LogManager.Warning($"Unable to initialize ReconciliationService: {ex.Message}");
            }
        }

        private async Task UpdateReconciliationTableAsync(ImportChanges changes, string countryId)
        {
            // Branch to the dedicated updater that mirrors AMBRE changes into T_Reconciliation
            LogManager.Info($"Updating T_Reconciliation for {countryId}");

            if (changes == null)
            {
                LogManager.Warning("No changes provided to update reconciliation.");
                return;
            }

            // Ensure we have a ReconciliationService ready
            await EnsureRecoServiceAsync().ConfigureAwait(false);

            // Resolve Country by id
            var countries = await _offlineFirstService.GetCountries().ConfigureAwait(false);
            var country = countries?.FirstOrDefault(c => string.Equals(c.CNT_Id, countryId, StringComparison.OrdinalIgnoreCase));
            if (country == null)
            {
                LogManager.Warning($"Country not found: {countryId}. Skipping reconciliation update.");
                return;
            }

            // Execute reconciliation table updates (insert/unarchive/archive) using the dedicated updater
            var updater = new AmbreReconciliationUpdater(_offlineFirstService, _currentUser, _reconciliationService);
            await updater.UpdateReconciliationTableAsync(changes, countryId, country).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Représente les changements à appliquer lors de l'import
    /// </summary>
    public class ImportChanges
    {
        public List<DataAmbre> ToAdd { get; set; } = new List<DataAmbre>();
        public List<DataAmbre> ToUpdate { get; set; } = new List<DataAmbre>();
        public List<DataAmbre> ToArchive { get; set; } = new List<DataAmbre>();
    }
}
        