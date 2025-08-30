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


    }
}
