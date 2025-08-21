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
        private readonly TransformationService _transformationService;
        private readonly string _currentUser;

        /// <summary>
        /// Constructeur avec OfflineFirstService
        /// </summary>
        public AmbreImportService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = Environment.UserName;

            // Initialiser TransformationService avec les pays du service
            var countries = _offlineFirstService.GetCountries().Result;
            _transformationService = new TransformationService(countries);
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
            var result = new ImportResult { CountryId = countryId, StartTime = DateTime.Now };

            try
            {
                progressCallback?.Invoke("Validation du fichier...", 0);

                // Validation du fichier
                var validationErrors = ValidationHelper.ValidateImportFile(filePath);
                if (validationErrors.Any())
                {
                    result.Errors.AddRange(validationErrors);
                    return result;
                }

                progressCallback?.Invoke("Chargement des configurations...", 10);

                // Chargement des configurations
                var country = await LoadCountryConfiguration(countryId);
                var importFields = await LoadImportFieldsConfiguration();
                var transforms = await LoadTransformConfigurations();

                if (country == null)
                {
                    result.Errors.Add($"Configuration non trouvée pour le pays: {countryId}");
                    return result;
                }

                // Pré-validation: vérifier l'absence de changements locaux non synchronisés
                progressCallback?.Invoke("Vérification des modifications locales non synchronisées...", 15);
                try
                {
                    // S'assurer que le pays courant est correctement initialisé pour obtenir la chaîne locale
                    var switched = await _offlineFirstService.SetCurrentCountryAsync(countryId);
                    if (!switched)
                    {
                        result.Errors.Add($"Impossible d'initialiser la base locale pour {countryId}");
                        return result;
                    }

                    var unsyncedCount = await _offlineFirstService.GetUnsyncedChangeCountAsync(countryId);
                    if (unsyncedCount > 0)
                    {
                        LogManager.Warning($"Import annulé: {unsyncedCount} changement(s) non synchronisé(s) détecté(s) dans la base lock.");
                        result.Errors.Add($"Import annulé: {unsyncedCount} changement(s) non synchronisé(s) détecté(s). Veuillez synchroniser avant de relancer l'import.");
                        return result;
                    }
                }
                catch (Exception preCheckEx)
                {
                    LogManager.Error("Erreur lors de la vérification des changements non synchronisés", preCheckEx);
                    result.Errors.Add($"Erreur lors de la vérification des changements non synchronisés: {preCheckEx.Message}");
                    return result;
                }

                // Mise à jour d'état (best-effort) après initialisation réussie et pré-vérifications
                try { await _offlineFirstService.SetSyncStatusAsync("Importing"); } catch { }

                progressCallback?.Invoke("Lecture du fichier Excel...", 20);

                // Lecture du fichier Excel avec progression (20% -> 40%)
                var rawData = await ReadExcelFile(filePath, importFields, p =>
                {
                    int mapped = 20 + (p * 20 / 100); // map 0-100 => 20-40
                    if (mapped > 40) mapped = 40;
                    progressCallback?.Invoke($"Lecture du fichier Excel... ({p}%)", mapped);
                });

                // S'assurer d'atteindre 40% après la lecture
                progressCallback?.Invoke("Lecture du fichier Excel terminée", 40);
                if (!rawData.Any())
                {
                    result.Errors.Add("Aucune donnée trouvée dans le fichier Excel.");
                    return result;
                }

                // Filtrer les lignes pour ne conserver que celles du compte Pivot ou Receivable du pays
                var filteredRawData = FilterRowsByCountryAccounts(rawData, country);
                if (!filteredRawData.Any())
                {
                    var pivot = country?.CNT_AmbrePivot;
                    var recv = country?.CNT_AmbreReceivable;
                    result.Errors.Add($"Aucune ligne ne correspond aux comptes du pays ({country?.CNT_Id}): Pivot={pivot}, Receivable={recv}.");
                    return result;
                }

                progressCallback?.Invoke("Transformation des données...", 40);

                // Transformation des données
                var transformedData = await TransformData(filteredRawData, transforms, country);

                progressCallback?.Invoke("Validation des données...", 60);

                // Validation des données transformées
                var validationResult = ValidateTransformedData(transformedData, country);
                result.ValidationErrors.AddRange(validationResult.errors);
                var validData = validationResult.validData;

                if (!validData.Any())
                {
                    result.Errors.Add("Aucune donnée valide après transformation et validation.");
                    return result;
                }

                progressCallback?.Invoke("Synchronisation avec la base de données...", 80);

                // Synchronisation avec la base locale (offline-first) uniquement
                // La synchronisation réseau sera lancée en arrière-plan après l'import
                var syncResult = await SynchronizeWithDatabase(validData, countryId, performNetworkSync: false);
                result.NewRecords = syncResult.newCount;
                result.UpdatedRecords = syncResult.updatedCount;
                result.DeletedRecords = syncResult.deletedCount;

                // Rafraîchir la configuration/cache local(e) de façon atomique après import local réussi
                progressCallback?.Invoke("Rafraîchissement de la configuration locale...", 90);
                try
                {
                    await _offlineFirstService.RefreshConfigurationAsync();
                    // S'assurer que le pays courant reste celui de l'import après rafraîchissement
                    await _offlineFirstService.SetCurrentCountryAsync(countryId);
                }
                catch (Exception refreshEx)
                {
                    LogManager.Error("Erreur lors du rafraîchissement de la configuration après import", refreshEx);
                    result.Errors.Add($"Erreur lors du rafraîchissement de la configuration locale: {refreshEx.Message}");
                    result.EndTime = DateTime.Now;
                    return result;
                }

                progressCallback?.Invoke("Finalisation...", 100);

                result.IsSuccess = true;
                result.EndTime = DateTime.Now;
                result.ProcessedRecords = validData.Count;

                // Synchronisation réseau en arrière-plan supprimée (flux simplifié)

                return result;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Erreur durant l'import: {ex.Message}");
                try { await _offlineFirstService.SetSyncStatusAsync("Error"); } catch { }
                result.EndTime = DateTime.Now;
                return result;
            }
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

        /// <summary>
        /// Transforme les données selon les règles de transformation
        /// </summary>
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
            var transactionType = _transformationService.DetermineTransactionType(dataAmbre.RawLabel, isPivot);
            var creditDebit = _transformationService.DetermineCreditDebit(dataAmbre.SignedAmount);

            // Extraction du type de garantie pour les comptes Receivable
            string guaranteeType = null;
            if (!isPivot)
            {
                guaranteeType = ExtractGuaranteeType(dataAmbre.RawLabel);
            }

            var (action, kpi) = _transformationService.ApplyAutomaticCategorization(
                transactionType, creditDebit, isPivot, guaranteeType);

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
        private async Task<(int newCount, int updatedCount, int deletedCount)> SynchronizeWithDatabase(List<DataAmbre> newData, string countryId, bool performNetworkSync)
        {
            ImportChanges changes;
            try
            {
                // 1) Charger les données existantes et calculer les changements SANS verrou global
                var existingData = await LoadExistingDataAsync(countryId);
                changes = CalculateChanges(existingData, newData);
                LogManager.Info($"Changements calculés pour {countryId} - Nouveaux: {changes.ToAdd.Count}, Modifiés: {changes.ToUpdate.Count}, Supprimés: {changes.ToArchive.Count}");

                // 2) Protéger uniquement la phase d'écriture (apply + reconciliation)
                // Plus de synchronisation réseau concurrente: on acquiert directement le verrou global

                var lockWaitStart = DateTime.Now;
                var lockTimeout = TimeSpan.FromMinutes(2); // Timeout explicite pour éviter l'attente infinie
                LogManager.Info($"Tentative d'acquisition du verrou global pour {countryId} (timeout {lockTimeout.TotalSeconds} sec)...");

                var acquireTask = _offlineFirstService.AcquireGlobalLockAsync(countryId, "AmbreImport", TimeSpan.FromMinutes(30));
                var completed = await Task.WhenAny(acquireTask, Task.Delay(lockTimeout));
                if (completed != acquireTask)
                {
                    var waited = DateTime.Now - lockWaitStart;
                    var msg = $"Impossible d'obtenir le verrou global pour {countryId} dans le délai imparti ({lockTimeout.TotalSeconds} sec). Veuillez réessayer.";
                    var timeoutEx = new TimeoutException(msg);
                    LogManager.Error($"Timeout ({waited.TotalSeconds:F0}s) lors de l'attente du verrou global pour {countryId}. Opération annulée.", timeoutEx);
                    throw timeoutEx;
                }

                var globalLockHandle = await acquireTask;
                using (var globalLock = globalLockHandle)
                {
                    if (globalLock == null)
                    {
                        throw new InvalidOperationException($"Impossible d'acquérir le verrou global pour {countryId}. Import annulé.");
                    }

                    var waited = DateTime.Now - lockWaitStart;
                    LogManager.Info($"Verrou global acquis pour {countryId} après {waited.TotalSeconds:F0}s d'attente");

                    try
                    {
                        // Statut: application des changements
                        try { await _offlineFirstService.SetSyncStatusAsync("ApplyingChanges"); } catch { }

                        // Appliquer les changements via les API natives d'OfflineFirstService
                        await ApplyChangesAsync(changes);

                        // Statut: réconciliation
                        try { await _offlineFirstService.SetSyncStatusAsync("Reconciling"); } catch { }

                        // Mettre à jour T_Reconciliation
                        await UpdateReconciliationTable(changes.ToAdd, changes.ToUpdate, changes.ToArchive, countryId);

                        LogManager.Info($"Import local terminé avec succès pour {countryId}");

                        // Publier la base locale vers le réseau tant que le verrou global est détenu
                        try { await _offlineFirstService.SetSyncStatusAsync("Publishing"); } catch { }
                        await _offlineFirstService.CopyLocalToNetworkAsync(countryId);

                        // Finaliser: les changements de l'import sont déjà publiés (copie fichier). Marquer comme synchronisés dans le ChangeLog.
                        try { await _offlineFirstService.SetSyncStatusAsync("Finalizing"); } catch { }
                        await _offlineFirstService.MarkAllLocalChangesAsSyncedAsync(countryId);

                        // Rafraîchir la base locale depuis le réseau (copie atomique) pour garantir un état propre et incluant les pending appliqués
                        try { await _offlineFirstService.SetSyncStatusAsync("RefreshingLocal"); } catch { }
                        await _offlineFirstService.CopyNetworkToLocalAsync(countryId);

                        // Terminé
                        try { await _offlineFirstService.SetSyncStatusAsync("Completed"); } catch { }
                    }
                    catch (Exception ex)
                    {
                        LogManager.Error($"Erreur lors de l'import local pour {countryId}", ex);
                        try { await _offlineFirstService.SetSyncStatusAsync("Error"); } catch { }
                        throw new InvalidOperationException($"Erreur lors de la synchronisation avec la base de données: {ex.Message}", ex);
                    }
                    // Le verrou global est libéré automatiquement par Dispose() à la fin du using
                }

                if (performNetworkSync)
                {
                    try
                    {
                        LogManager.Info($"Démarrage de la synchronisation réseau pour {countryId}");

                        // Synchroniser avec la base réseau via OfflineFirstService
                        bool syncSuccess = await _offlineFirstService.SynchronizeData();

                        if (syncSuccess)
                        {
                            LogManager.Info($"Synchronisation réseau réussie pour {countryId}");
                        }
                        else
                        {
                            LogManager.Warning($"Synchronisation réseau échouée pour {countryId} - Les données restent en local");
                        }
                    }
                    catch (Exception syncEx)
                    {
                        LogManager.Error($"Erreur lors de la synchronisation réseau pour {countryId}", syncEx);
                        // Ne pas faire échouer l'import si seule la sync réseau échoue
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
                        if (entity.Properties.ContainsKey("DeleteDate") && entity.Properties["DeleteDate"] != null)
                            continue; // Ignorer les données archivées
                            
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
                            LastModified = ConvertToDateTime(GetPropertyValue(entity.Properties, "LastModified"))
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
                
                // Créer des dictionnaires pour accélérer les recherches
                var existingByKey = existingData.ToDictionary(d => d.ID, d => d);
                var newByKey = newData.ToDictionary(d => d.GetUniqueKey(), d => d);
                
                // Identifier les ajouts et modifications
                foreach (var newItem in newData)
                {
                    var key = newItem.GetUniqueKey();
                    
                    if (existingByKey.ContainsKey(key))
                    {
                        // Enregistrement existant - vérifier s'il a changé
                        var existingItem = existingByKey[key];
                        
                        if (HasDataChanged(existingItem, newItem))
                        {
                            // Mise à jour nécessaire - conserver l'ID et la version de l'existant
                            newItem.ID = existingItem.ID;
                            newItem.Version = existingItem.Version + 1; // Incrémenter la version
                            newItem.CreationDate = existingItem.CreationDate; // Conserver la date de création
                            newItem.LastModified = DateTime.Now;
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
                        newItem.CreationDate = DateTime.Now;
                        newItem.LastModified = DateTime.Now;
                        newItem.ModifiedBy = _currentUser;
                        
                        changes.ToAdd.Add(newItem);
                    }
                }
                
                // Identifier les suppressions (enregistrements présents dans existing mais pas dans new)
                foreach (var existingItem in existingData)
                {
                    var key = existingItem.ID;
                    
                    if (!newByKey.ContainsKey(key))
                    {
                        // Enregistrement à supprimer (archivage logique)
                        existingItem.DeleteDate = DateTime.Now;
                        existingItem.LastModified = DateTime.Now;
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
                  var ok = await _offlineFirstService.ApplyEntitiesBatchAsync(countryId, toAdd, toUpdate, toArchive);
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

        /// <summary>
        /// Charge la configuration des champs d'import
        /// </summary>
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

        /// <summary>
        /// Charge les configurations de transformation
        /// </summary>
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
                LogManager.Info($"[Staging] Mise à jour de T_Reconciliation pour {countryId} - Nouveaux: {newRecords.Count}, Modifiés: {updatedRecords.Count}, Supprimés: {deletedRecords.Count}");

                // 1) Préparer les lignes à upserter (nouveaux + modifiés)
                var now = DateTime.Now;
                var upserts = new List<Reconciliation>();

                foreach (var dataAmbre in newRecords)
                {
                    var rec = CreateReconciliationFromDataAmbre(dataAmbre);
                    rec.CreationDate = now;
                    rec.LastModified = now;
                    rec.ModifiedBy = _currentUser;
                    upserts.Add(rec);
                }

                foreach (var dataAmbre in updatedRecords)
                {
                    var rec = CreateReconciliationFromDataAmbre(dataAmbre);
                    rec.LastModified = now;
                    rec.ModifiedBy = _currentUser;
                    upserts.Add(rec);
                }

                // 2) Instancier le service de staging pour la base du pays
                var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
                var staging = new StagingImportService(connectionString, _currentUser);

                // 3) Assurer l'existence et l'indexation des tables nécessaires
                await staging.EnsureStagingForReconciliationAsync();
                await staging.EnsureTargetIndexesAsync();

                // 4) Charger le staging et fusionner
                await staging.TruncateStagingAsync();
                var staged = await staging.InsertStagingRowsAsync(upserts, chunkSize: 2000);
                var mergeRes = await staging.MergeStagingIntoReconciliationAsync();

                // 5) Archivage des enregistrements supprimés
                int archived = 0;
                if (deletedRecords != null && deletedRecords.Count > 0)
                {
                    var ids = deletedRecords.Select(d => d.ID).Where(id => !string.IsNullOrWhiteSpace(id));
                    archived = await staging.ArchiveByIdsAsync(ids);
                }

                LogManager.Info($"[Staging] T_Reconciliation mis à jour pour {countryId}: Staged={staged}, Updated={mergeRes.updated}, Inserted={mergeRes.inserted}, Archived={archived}");

                // 6) Change-tracking en lot (INSERT / UPDATE / DELETE)
                try
                {
                    var insertedIds = (newRecords ?? new List<DataAmbre>()).Select(r => r.ID)
                        .Where(id => !string.IsNullOrWhiteSpace(id));
                    var updatedIds = (updatedRecords ?? new List<DataAmbre>()).Select(r => r.ID)
                        .Where(id => !string.IsNullOrWhiteSpace(id));
                    var deletedIds = (deletedRecords ?? new List<DataAmbre>()).Select(r => r.ID)
                        .Where(id => !string.IsNullOrWhiteSpace(id));

                    // Éviter les doublons éventuels
                    var ins = new HashSet<string>(insertedIds);
                    var upd = new HashSet<string>(updatedIds);
                    var del = new HashSet<string>(deletedIds);

                    int totalToLog = ins.Count + upd.Count + del.Count;
                    if (totalToLog > 0)
                    {
                        using (var session = await _offlineFirstService.BeginChangeLogSessionAsync(countryId))
                        {
                            foreach (var id in ins)
                                await session.AddAsync("T_Reconciliation", id, "INSERT");
                            foreach (var id in upd)
                                await session.AddAsync("T_Reconciliation", id, "UPDATE");
                            foreach (var id in del)
                                await session.AddAsync("T_Reconciliation", id, "DELETE");

                            await session.CommitAsync();
                        }
                        LogManager.Info($"[ChangeLog] Enregistrements consignés: INSERT={ins.Count}, UPDATE={upd.Count}, DELETE={del.Count}");
                    }
                    else
                    {
                        LogManager.Info("[ChangeLog] Aucun changement à consigner pour T_Reconciliation.");
                    }
                }
                catch (Exception chEx)
                {
                    // Ne pas échouer l'import si la consignation échoue; journaliser uniquement
                    LogManager.Warning($"[ChangeLog] Échec de la consignation des changements: {chEx.Message}");
                }
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la mise à jour de T_Reconciliation pour {countryId}: {ex.Message}", ex);
                throw new InvalidOperationException($"Erreur lors de la mise à jour de la table T_Reconciliation: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Crée ou met à jour un enregistrement de réconciliation via les API natives d'OfflineFirstService
        /// </summary>
        private async Task CreateOrUpdateReconciliationAsync(DataAmbre dataAmbre, string countryId)
        {
            await CreateOrUpdateReconciliationAsync(dataAmbre, countryId, null);
        }

        // Overload that accepts a change-log session to batch change logging
        private async Task CreateOrUpdateReconciliationAsync(DataAmbre dataAmbre, string countryId, OfflineFirstAccess.ChangeTracking.IChangeLogSession changeLogSession)
        {
            try
            {
                // Vérifier si un enregistrement existe déjà via l'API native
                var existingEntity = await _offlineFirstService.GetEntityByIdAsync(countryId, "T_Reconciliation", "ID", dataAmbre.ID);
                
                var reconciliation = CreateReconciliationFromDataAmbre(dataAmbre);
                var reconciliationEntity = ConvertReconciliationToEntity(reconciliation);
                
                if (existingEntity != null)
                {
                    // Mise à jour - conserver la version existante et l'incrémenter
                    if (existingEntity.Properties.ContainsKey("Version") && existingEntity.Properties["Version"] != null)
                    {
                        reconciliationEntity.Properties["Version"] = Convert.ToInt32(existingEntity.Properties["Version"]) + 1;
                    }
                    else
                    {
                        reconciliationEntity.Properties["Version"] = 1;
                    }
                    
                    reconciliationEntity.Properties["LastModified"] = DateTime.Now.ToOADate();
                    reconciliationEntity.Properties["ModifiedBy"] = _currentUser;
                    
                    var success = changeLogSession != null
                        ? await _offlineFirstService.UpdateEntityAsync(countryId, reconciliationEntity, changeLogSession)
                        : await _offlineFirstService.UpdateEntityAsync(countryId, reconciliationEntity);
                    if (!success)
                    {
                        LogManager.Warning($"Échec de la mise à jour de réconciliation pour {dataAmbre.ID}");
                    }
                }
                else
                {
                    // Création - initialiser les champs de versionnage
                    reconciliationEntity.Properties["Version"] = 1;
                    reconciliationEntity.Properties["CreationDate"] = DateTime.Now.ToOADate();
                    reconciliationEntity.Properties["LastModified"] = DateTime.Now.ToOADate();
                    reconciliationEntity.Properties["ModifiedBy"] = _currentUser;
                    
                    var success = changeLogSession != null
                        ? await _offlineFirstService.AddEntityAsync(countryId, reconciliationEntity, changeLogSession)
                        : await _offlineFirstService.AddEntityAsync(countryId, reconciliationEntity);
                    if (!success)
                    {
                        LogManager.Warning($"Échec de la création de réconciliation pour {dataAmbre.ID}");
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la création/mise à jour de réconciliation pour {dataAmbre.ID}: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Convertit un objet Reconciliation en entité OfflineFirstAccess
        /// </summary>
        private OfflineFirstAccess.Models.Entity ConvertReconciliationToEntity(Reconciliation reconciliation)
        {
            var entity = new OfflineFirstAccess.Models.Entity
            {
                TableName = "T_Reconciliation",
                PrimaryKeyColumn = "ID",
                Properties = new Dictionary<string, object>
                {
                    ["ID"] = reconciliation.ID,
                    ["DWINGS_GuaranteeID"] = reconciliation.DWINGS_GuaranteeID,
                    ["DWINGS_InvoiceID"] = reconciliation.DWINGS_InvoiceID,
                    ["DWINGS_CommissionID"] = reconciliation.DWINGS_CommissionID,
                    ["Action"] = reconciliation.Action,
                    ["KPI"] = reconciliation.KPI,
                    ["CreationDate"] = reconciliation.CreationDate,
                    ["LastModified"] = reconciliation.LastModified,
                    ["ModifiedBy"] = reconciliation.ModifiedBy,
                    ["Version"] = reconciliation.Version,
                    ["DeleteDate"] = reconciliation.DeleteDate
                }
            };
            
            return entity;
        }

        /// <summary>
        /// Archive un enregistrement de réconciliation via les API natives d'OfflineFirstService
        /// </summary>
        private async Task ArchiveReconciliationAsync(DataAmbre dataAmbre, string countryId)
        {
            await ArchiveReconciliationAsync(dataAmbre, countryId, null);
        }

        // Overload that accepts a change-log session to batch change logging
        private async Task ArchiveReconciliationAsync(DataAmbre dataAmbre, string countryId, OfflineFirstAccess.ChangeTracking.IChangeLogSession changeLogSession)
        {
            try
            {
                // Récupérer l'enregistrement existant
                var existingEntity = await _offlineFirstService.GetEntityByIdAsync(countryId, "T_Reconciliation", "ID", dataAmbre.ID);
                
                if (existingEntity != null)
                {
                    // Archivage logique - définir DeleteDate
                    existingEntity.Properties["DeleteDate"] = DateTime.Now;
                    existingEntity.Properties["LastModified"] = DateTime.Now;
                    existingEntity.Properties["ModifiedBy"] = _currentUser;
                    
                    // Incrémenter la version
                    if (existingEntity.Properties.ContainsKey("Version") && existingEntity.Properties["Version"] != null)
                    {
                        existingEntity.Properties["Version"] = Convert.ToInt32(existingEntity.Properties["Version"]) + 1;
                    }
                    else
                    {
                        existingEntity.Properties["Version"] = 1;
                    }
                    
                    var success = changeLogSession != null
                        ? await _offlineFirstService.UpdateEntityAsync(countryId, existingEntity, changeLogSession)
                        : await _offlineFirstService.UpdateEntityAsync(countryId, existingEntity);
                    if (!success)
                    {
                        LogManager.Warning($"Échec de l'archivage de réconciliation pour {dataAmbre.ID}");
                    }
                }
                else
                {
                    LogManager.Warning($"Enregistrement de réconciliation non trouvé pour archivage: {dataAmbre.ID}");
                }
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de l'archivage de réconciliation pour {dataAmbre.ID}: {ex.Message}", ex);
                throw;
            }
        }



        /// <summary>
        /// Crée un objet Reconciliation à partir d'un DataAmbre avec les règles métiers appliquées
        /// </summary>
        private Reconciliation CreateReconciliationFromDataAmbre(DataAmbre dataAmbre)
        {
            var reconciliation = new Reconciliation
            {
                ID = dataAmbre.ID,
                CreationDate = DateTime.Now,
                ModifiedBy = _currentUser ?? "System",
                LastModified = DateTime.Now,
                Version = 1
            };

            // Appliquer les règles métiers selon le type de compte
            ApplyBusinessRulesForReconciliation(reconciliation, dataAmbre);

            return reconciliation;
        }

        /// <summary>
        /// Applique les règles métiers pour déterminer les valeurs Action et KPI
        /// </summary>
        private void ApplyBusinessRulesForReconciliation(Reconciliation reconciliation, DataAmbre dataAmbre)
        {
            var accountType = DetermineAccountType(dataAmbre);

            if (accountType == "Pivot")
            {
                ApplyPivotAccountRules(reconciliation, dataAmbre);
            }
            else if (accountType == "Receivable")
            {
                ApplyReceivableAccountRules(reconciliation, dataAmbre);
            }
        }

        /// <summary>
        /// Applique les règles pour les comptes Pivot
        /// </summary>
        private void ApplyPivotAccountRules(Reconciliation reconciliation, DataAmbre dataAmbre)
        {
            var transactionType = dataAmbre.Pivot_TransactionCodesFromLabel?.ToUpper();
            bool isCredit = dataAmbre.SignedAmount > 0;

            switch (transactionType)
            {
                case "COLLECTION":
                    reconciliation.Action = isCredit ? 1 : 2; // 1 = Collection reçue, 2 = Collection envoyée
                    reconciliation.KPI = 1; // KPI positif pour collections
                    break;
                case "REFUND":
                    reconciliation.Action = isCredit ? 3 : 4; // 3 = Remboursement reçu, 4 = Remboursement envoyé
                    reconciliation.KPI = 2; // KPI neutre pour remboursements
                    break;
                case "COMMISSION":
                    reconciliation.Action = 5; // Commission
                    reconciliation.KPI = 1; // KPI positif pour commissions
                    break;
                default:
                    reconciliation.Action = 0; // Action non déterminée
                    reconciliation.KPI = 0; // KPI neutre par défaut
                    break;
            }
        }

        /// <summary>
        /// Applique les règles pour les comptes Receivable
        /// </summary>
        private void ApplyReceivableAccountRules(Reconciliation reconciliation, DataAmbre dataAmbre)
        {
            bool isCredit = dataAmbre.SignedAmount > 0;
            bool hasInvoiceReference = !string.IsNullOrEmpty(dataAmbre.Receivable_InvoiceFromAmbre);
            bool hasDWReference = !string.IsNullOrEmpty(dataAmbre.Receivable_DWRefFromAmbre);

            if (isCredit)
            {
                reconciliation.Action = 6; // Paiement reçu
                reconciliation.KPI = 1; // KPI positif
            }
            else
            {
                reconciliation.Action = 7; // Charge/Provision
                reconciliation.KPI = 3; // KPI négatif
            }

            // Enrichir avec les références DWINGS si disponibles
            if (hasInvoiceReference)
            {
                reconciliation.DWINGS_InvoiceID = dataAmbre.Receivable_InvoiceFromAmbre;
            }

            if (hasDWReference)
            {
                reconciliation.DWINGS_GuaranteeID = dataAmbre.Receivable_DWRefFromAmbre;
            }
        }

        /// <summary>
        /// Détermine le type de compte (Pivot ou Receivable) basé sur l'Account_ID
        /// </summary>
        private string DetermineAccountType(DataAmbre dataAmbre)
        {
            var accountId = dataAmbre.Account_ID?.ToUpper();

            if (string.IsNullOrEmpty(accountId))
                return "Unknown";

            // Règles métier pour déterminer le type de compte
            if (accountId.Contains("PIVOT") || accountId.StartsWith("PIV"))
                return "Pivot";

            if (accountId.Contains("RECEIV") || accountId.StartsWith("REC") || accountId.Contains("SUSP"))
                return "Receivable";

            return "Unknown";
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