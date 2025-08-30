using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Service pour (re)créer uniquement la base DWINGS unifiée dans un répertoire donné.
    /// Le fichier créé est "{DWDatabasePrefix}{country}.accdb" et contient
    /// les tables T_DW_Guarantee et T_DW_Data.
    /// S'appuie sur les générateurs de templates Access existants (ADOX/EmptyTemplate/OleDb).
    /// </summary>
    public class DatabaseRecreationService
    {
        public class RecreationReport
        {
            public bool Success { get; set; }
            public string TargetDirectory { get; set; }
            public string AmbrePath { get; set; }
            // DWINGS unifiée: fichier unique par pays
            public string DwingsPath { get; set; }
            public string ReconciliationPath { get; set; }
            public string LockPath { get; set; }
            public List<string> Errors { get; set; } = new List<string>();
            public List<string> Logs { get; set; } = new List<string>();
        }

        /// <summary>
        /// (Re)crée uniquement la base de données DWINGS (unifiée)
        /// dans le répertoire spécifié. Le fichier est nommé: "{dwDatabasePrefix}{country}.accdb".
        /// </summary>
        /// <param name="directory">Répertoire cible</param>
        /// <param name="dwDatabasePrefix">Préfixe commun pour la base DWINGS (ex: "DWINGS" ou "Dwings")</param>
        /// <param name="country">Code pays (ex: "ES") à concaténer dans le nom de la base DWINGS</param>
        public async Task<RecreationReport> RecreateAllAsync(string directory, string dwDatabasePrefix, string country)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentNullException(nameof(directory));
            if (string.IsNullOrWhiteSpace(dwDatabasePrefix))
                throw new ArgumentNullException(nameof(dwDatabasePrefix));
            if (string.IsNullOrWhiteSpace(country))
                throw new ArgumentNullException(nameof(country));

            var report = new RecreationReport
            {
                TargetDirectory = directory,
            };

            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                report.Errors.Add($"Impossible de créer le répertoire: {ex.Message}");
                return FinalizeReport(report);
            }

            var cc = country.Trim();
            var dwingsPath = Path.Combine(directory, $"{dwDatabasePrefix}{cc}.accdb");

            report.DwingsPath = dwingsPath;

            // DWINGS (unifiée)
            if (!await CreateDwingsUnifiedDatabaseAsync(dwingsPath, report))
            {
                LogManager.Error($"Echec de création de {dwingsPath}", null);
            }

            return FinalizeReport(report);
        }

        /// <summary>
        /// Méthode statique utilitaire (obsolète): un code pays est désormais requis pour les bases DWINGS.
        /// </summary>
        public static Task<RecreationReport> RecreateAll(string directory)
            => throw new ArgumentException("Country is required. Use RecreateAll(directory, dwDatabasePrefix, country). Example: RecreateAll(path, \"Dwings\", \"ES\").");

        /// <summary>
        /// Variante statique (obsolète): un code pays est désormais requis pour les bases DWINGS.
        /// </summary>
        public static Task<RecreationReport> RecreateAll(string directory, string dwDatabasePrefix)
            => throw new ArgumentException("Country is required. Use RecreateAll(directory, dwDatabasePrefix, country). Example: RecreateAll(path, dwDatabasePrefix, \"ES\").");

        /// <summary>
        /// Variante statique avec préfixe DWINGS et code pays.
        /// </summary>
        public static Task<RecreationReport> RecreateAll(string directory, string dwDatabasePrefix, string country)
            => new DatabaseRecreationService().RecreateAllAsync(directory, dwDatabasePrefix, country);

        private RecreationReport FinalizeReport(RecreationReport report)
        {
            report.Success = report.Errors.Count == 0;
            return report;
        }


        private async Task<bool> CreateDwingsUnifiedDatabaseAsync(string path, RecreationReport report)
        {
            try
            {
                bool ok = await DatabaseTemplateFactory.CreateCustomTemplateAsync(
                    path,
                    builder =>
                    {
                        // Retirer les tables système non pertinentes pour DWINGS
                        var cfg = builder.GetConfiguration();
                        cfg.Tables.RemoveAll(t =>
                            string.Equals(t.Name, "SyncLocks", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(t.Name, "Sessions", StringComparison.OrdinalIgnoreCase));

                        // T_DW_Guarantee
                        builder.AddTable("T_DW_Guarantee")
                            .WithPrimaryKey("GUARANTEE_ID", typeof(string))
                            .WithColumn("BOOKING", typeof(string))
                            .WithColumn("GUARANTEE_STATUS", typeof(string))
                            .WithColumn("NATURE", typeof(string))
                            .WithColumn("EVENT_STATUS", typeof(string))
                            .WithColumn("EVENT_EFFECTIVEDATE", typeof(DateTime))
                            .WithColumn("ISSUEDATE", typeof(DateTime))
                            .WithColumn("OFFICIALREF", typeof(string))
                            .WithColumn("UNDERTAKINGEVENT", typeof(string))
                            .WithColumn("PROCESS", typeof(string))
                            .WithColumn("EXPIRYDATETYPE", typeof(string))
                            .WithColumn("EXPIRYDATE", typeof(DateTime))
                            .WithColumn("PARTY_ID", typeof(string))
                            .WithColumn("PARTY_REF", typeof(string))
                            .WithColumn("SECONDARY_OBLIGOR", typeof(string))
                            .WithColumn("SECONDARY_OBLIGOR_NATURE", typeof(string))
                            .WithColumn("ROLE", typeof(string))
                            .WithColumn("COUNTRY", typeof(string))
                            .WithColumn("CENTRAL_PARTY_CODE", typeof(string))
                            .WithColumn("NAME1", typeof(string))
                            .WithColumn("NAME2", typeof(string))
                            .WithColumn("GROUPE", typeof(string))
                            .WithColumn("PREMIUM", typeof(bool))
                            .WithColumn("BRANCH_CODE", typeof(string))
                            .WithColumn("BRANCH_NAME", typeof(string))
                            .WithColumn("OUTSTANDING_AMOUNT", typeof(double))
                            .WithColumn("OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY", typeof(double))
                            .WithColumn("CURRENCYNAME", typeof(string))
                            .WithColumn("CANCELLATIONDATE", typeof(DateTime))
                            .WithColumn("CONTROLER", typeof(bool))
                            .WithColumn("AUTOMATICBOOKOFF", typeof(bool))
                            .WithColumn("NATUREOFDEAL", typeof(string))
                            .EndTable();

                        // T_DW_Data
                        builder.AddTable("T_DW_Data")
                            .WithPrimaryKey("BGPMT", typeof(string))
                            .WithColumn("INVOICE_ID", typeof(string))
                            .WithColumn("BOOKING", typeof(string))
                            .WithColumn("REQUESTED_INVOICE_AMOUNT", typeof(string))
                            .WithColumn("SENDER_NAME", typeof(string))
                            .WithColumn("RECEIVER_NAME", typeof(string))
                            .WithColumn("SENDER_REFERENCE", typeof(string))
                            .WithColumn("RECEIVER_REFERENCE", typeof(string))
                            .WithColumn("T_INVOICE_STATUS", typeof(string))
                            .WithColumn("BILLING_AMOUNT", typeof(string))
                            .WithColumn("BILLING_CURRENCY", typeof(string))
                            .WithColumn("START_DATE", typeof(string))
                            .WithColumn("END_DATE", typeof(string))
                            .WithColumn("FINAL_AMOUNT", typeof(string))
                            .WithColumn("T_COMMISSION_PERIOD_STATUS", typeof(string))
                            .WithColumn("BUSINESS_CASE_REFERENCE", typeof(string))
                            .WithColumn("BUSINESS_CASE_ID", typeof(string))
                            .WithColumn("POSTING_PERIODICITY", typeof(string))
                            .WithColumn("EVENT_ID", typeof(string))
                            .WithColumn("COMMENTS", typeof(string))
                            .WithColumn("SENDER_ACCOUNT_NUMBER", typeof(string))
                            .WithColumn("SENDER_ACCOUNT_BIC", typeof(string))
                            .WithColumn("RECEIVER_ACCOUNT_NUMBER", typeof(string))
                            .WithColumn("RECEIVER_ACCOUNT_BIC", typeof(string))
                            .WithColumn("REQUESTED_AMOUNT", typeof(string))
                            .WithColumn("EXECUTED_AMOUNT", typeof(string))
                            .WithColumn("REQUESTED_EXECUTION_DATE", typeof(string))
                            .WithColumn("T_PAYMENT_REQUEST_STATUS", typeof(string))
                            .WithColumn("DEBTOR_ACCOUNT_ID", typeof(string))
                            .WithColumn("CREDITOR_ACCOUNT_ID", typeof(string))
                            .WithColumn("MT_STATUS", typeof(string))
                            .WithColumn("REMINDER_NUMBER", typeof(string))
                            .WithColumn("ERROR_MESSAGE", typeof(string))
                            .WithColumn("DEBTOR_PARTY_ID", typeof(string))
                            .WithColumn("PAYMENT_METHOD", typeof(string))
                            .WithColumn("PAYMENT_TYPE", typeof(string))
                            .WithColumn("DEBTOR_PARTY_NAME", typeof(string))
                            .WithColumn("DEBTOR_ACCOUNT_NUMBER", typeof(string))
                            .WithColumn("CREDITOR_PARTY_ID", typeof(string))
                            .WithColumn("CREDITOR_ACCOUNT_NUMBER", typeof(string))
                            .EndTable();
                    });

                if (!ok) report.Errors.Add($"DWINGS unifiée: échec de création ({Path.GetFileName(path)})");
                else report.Logs.Add($"DWINGS unifiée créée: {path}");
                return ok;
            }
            catch (Exception ex)
            {
                report.Errors.Add($"DWINGS unifiée: exception {ex.Message}");
                return false;
            }
        }


        /// <summary>
        /// Recrée une base AMBRE vide localement si le réseau ne contient pas encore la base/ZIP,
        /// puis publie un ZIP initial vers le réseau pour initialiser le pays donné.
        /// Utilise les helpers de template existants et les APIs d'OfflineFirstService pour la publication ZIP.
        /// </summary>
        /// <param name="offlineFirstService">Service offline-first (résolution des chemins et publication ZIP)</param>
        /// <param name="countryId">Code pays (ex: "ES")</param>
        /// <returns>Un rapport de (re)création</returns>
        public async Task<RecreationReport> RecreateAmbreAsync(OfflineFirstService offlineFirstService, string countryId)
        {
            if (offlineFirstService == null) throw new ArgumentNullException(nameof(offlineFirstService));
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentNullException(nameof(countryId));

            var report = new RecreationReport
            {
                TargetDirectory = null
            };

            // 1) Vérifier si une base/ZIP AMBRE existe déjà côté réseau: si oui, ne rien faire
            try
            {
                await offlineFirstService.CopyNetworkToLocalAmbreAsync(countryId);
                report.Logs.Add($"AMBRE: contenu réseau déjà présent pour {countryId}, aucune recréation nécessaire.");
                return FinalizeReport(report);
            }
            catch (FileNotFoundException)
            {
                // attendu: absence côté réseau -> on va créer localement puis publier
                report.Logs.Add($"AMBRE: aucun contenu réseau détecté pour {countryId}. Création locale d'une base vide...");
            }
            catch (InvalidOperationException ex)
            {
                // Cas typique: changements locaux non synchronisés bloquent le rafraîchissement.
                // Dans ce cas, ne pas tenter de recréer/publier pour éviter un écrasement accidentel.
                report.Errors.Add($"AMBRE: rafraîchissement bloqué ({ex.Message}). Annulation de la recréation/publish.");
                return FinalizeReport(report);
            }
            catch (Exception ex)
            {
                // Autre erreur imprévue -> arrêter et signaler plutôt que de risquer une publication incorrecte
                report.Errors.Add($"AMBRE: erreur lors de la détection réseau ({ex.Message}). Annulation de la recréation.");
                return FinalizeReport(report);
            }

            // 2) Créer la base AMBRE locale vide avec le schéma T_Data_Ambre
            string ambreLocalPath = offlineFirstService.GetLocalAmbreDatabasePath(countryId);
            if (string.IsNullOrWhiteSpace(ambreLocalPath))
            {
                report.Errors.Add("AMBRE: chemin local introuvable (GetLocalAmbreDatabasePath a renvoyé null/empty)");
                return FinalizeReport(report);
            }
            try
            {
                var dir = Path.GetDirectoryName(ambreLocalPath);
                if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);

                bool created = await DatabaseTemplateFactory.CreateCustomTemplateAsync(
                    ambreLocalPath,
                    builder =>
                    {
                        // Conserver les tables système par défaut (ChangeLog/SyncLocks/Sessions)
                        // Définir uniquement la table métier AMBRE requise
                        builder.AddTable("T_Data_Ambre")
                               .WithPrimaryKey("ID", typeof(string))
                               .WithColumn("Account_ID", typeof(string), "TEXT(50)")
                               .WithColumn("CCY", typeof(string), "TEXT(3)")
                               .WithColumn("Country", typeof(string), "TEXT(255)")
                               .WithColumn("Event_Num", typeof(string), "TEXT(50)")
                               .WithColumn("Folder", typeof(string), "TEXT(255)")
                               .WithColumn("Pivot_MbawIDFromLabel", typeof(string), "TEXT(255)")
                               .WithColumn("Pivot_TransactionCodesFromLabel", typeof(string), "TEXT(255)")
                               .WithColumn("Pivot_TRNFromLabel", typeof(string), "TEXT(255)")
                               .WithColumn("RawLabel", typeof(string), "TEXT(255)")
                               .WithColumn("Receivable_DWRefFromAmbre", typeof(string), "TEXT(255)")
                               .WithColumn("LocalSignedAmount", typeof(double), "DOUBLE", false)
                               .WithColumn("Operation_Date", typeof(DateTime), "DATETIME")
                               .WithColumn("Reconciliation_Num", typeof(string), "TEXT(255)")
                               .WithColumn("Receivable_InvoiceFromAmbre", typeof(string), "TEXT(255)")
                               .WithColumn("ReconciliationOrigin_Num", typeof(string), "TEXT(255)")
                               .WithColumn("SignedAmount", typeof(double), "DOUBLE", false)
                               .WithColumn("Value_Date", typeof(DateTime), "DATETIME")
                               // Champs BaseEntity
                               .WithColumn("CreationDate", typeof(DateTime), "DATETIME")
                               .WithColumn("DeleteDate", typeof(DateTime), "DATETIME")
                               .WithColumn("ModifiedBy", typeof(string), "TEXT(100)")
                               .WithColumn("LastModified", typeof(DateTime), "DATETIME")
                               .WithColumn("Version", typeof(long), "LONG", false)
                               .EndTable();
                    });

                if (!created)
                {
                    report.Errors.Add($"AMBRE: échec de création locale de la base vide ({Path.GetFileName(ambreLocalPath)})");
                    return FinalizeReport(report);
                }

                report.AmbrePath = ambreLocalPath;
                report.Logs.Add($"AMBRE: base locale vide créée -> {ambreLocalPath}");
            }
            catch (Exception ex)
            {
                report.Errors.Add($"AMBRE: exception lors de la création locale ({ex.Message})");
                return FinalizeReport(report);
            }

            // 3) Publier la base locale en ZIP vers le réseau
            try
            {
                await offlineFirstService.CopyLocalToNetworkAmbreAsync(countryId);
                report.Logs.Add($"AMBRE: ZIP initial publié vers le réseau pour {countryId}.");
            }
            catch (Exception ex)
            {
                report.Errors.Add($"AMBRE: publication ZIP vers réseau échouée ({ex.Message})");
                return FinalizeReport(report);
            }

            // 4) Optionnel: réaligner le local depuis le ZIP réseau (cache/local DB) pour cohérence avec l'enforcement ZIP
            try
            {
                await offlineFirstService.CopyNetworkToLocalAmbreAsync(countryId);
                report.Logs.Add("AMBRE: local réaligné depuis le ZIP réseau.");
            }
            catch (Exception ex)
            {
                // Non-bloquant
                report.Logs.Add($"AMBRE: réalignement local post-publication non effectué ({ex.Message}).");
            }

            return FinalizeReport(report);
        }

    }
}
