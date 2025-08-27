using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Service unique pour (re)créer les bases Access attendues par l'application
    /// dans un répertoire donné: Ambre.accdb, Dwings.accdb, Reconciliation.accdb, Lock.accdb.
    /// S'appuie sur les générateurs de templates Access existants (ADOX/EmptyTemplate/OleDb).
    /// </summary>
    public class DatabaseRecreationService
    {
        public class RecreationReport
        {
            public bool Success { get; set; }
            public string TargetDirectory { get; set; }
            public string AmbrePath { get; set; }
            public string DwingsPath { get; set; }
            public string ReconciliationPath { get; set; }
            public string LockPath { get; set; }
            public List<string> Errors { get; set; } = new List<string>();
            public List<string> Logs { get; set; } = new List<string>();
        }

        /// <summary>
        /// (Re)crée les 4 bases de données Access (Ambre, Dwings, Reconciliation, Lock)
        /// dans le répertoire spécifié. Les fichiers sont nommés exactement:
        /// Ambre.accdb, Dwings.accdb, Reconciliation.accdb, Lock.accdb
        /// </summary>
        public async Task<RecreationReport> RecreateAllAsync(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentNullException(nameof(directory));

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

            var ambrePath = Path.Combine(directory, "Ambre.accdb");
            var dwingsPath = Path.Combine(directory, "Dwings.accdb");
            var reconciliationPath = Path.Combine(directory, "Reconciliation.accdb");
            var lockPath = Path.Combine(directory, "Lock.accdb");

            report.AmbrePath = ambrePath;
            report.DwingsPath = dwingsPath;
            report.ReconciliationPath = reconciliationPath;
            report.LockPath = lockPath;

            // Ambre
            if (!await CreateAmbreDatabaseAsync(ambrePath, report))
            {
                LogManager.Error($"Echec de création de {ambrePath}", null);
            }

            // Reconciliation
            if (!await CreateReconciliationDatabaseAsync(reconciliationPath, report))
            {
                LogManager.Error($"Echec de création de {reconciliationPath}", null);
            }

            // DWINGS
            if (!await CreateDwingsDatabaseAsync(dwingsPath, report))
            {
                LogManager.Error($"Echec de création de {dwingsPath}", null);
            }

            // Lock
            if (!await CreateLockDatabaseAsync(lockPath, report))
            {
                LogManager.Error($"Echec de création de {lockPath}", null);
            }

            return FinalizeReport(report);
        }

        /// <summary>
        /// Méthode statique utilitaire si vous préférez ne pas instancier le service.
        /// </summary>
        public static Task<RecreationReport> RecreateAll(string directory)
            => new DatabaseRecreationService().RecreateAllAsync(directory);

        private RecreationReport FinalizeReport(RecreationReport report)
        {
            report.Success = report.Errors.Count == 0;
            return report;
        }

        private async Task<bool> CreateAmbreDatabaseAsync(string path, RecreationReport report)
        {
            try
            {
                // Partir de la config par défaut puis filtrer aux tables utiles pour Ambre
                bool ok = await DatabaseTemplateGenerator.CreateCustomTemplateAsync(
                    path,
                    config =>
                    {
                        // Tables à conserver: système + Ambre + paramètres
                        var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                        {
                            "SyncLocks", "Sessions", "ChangeLog", "T_ConfigParameters", "T_Data_Ambre"
                        };
                        config.Tables = config.Tables.Where(t => keep.Contains(t.Name)).ToList();
                    });

                if (!ok) report.Errors.Add($"Ambre: échec de création ({Path.GetFileName(path)})");
                else report.Logs.Add($"Ambre créé: {path}");
                return ok;
            }
            catch (Exception ex)
            {
                report.Errors.Add($"Ambre: exception {ex.Message}");
                return false;
            }
        }

        private async Task<bool> CreateReconciliationDatabaseAsync(string path, RecreationReport report)
        {
            try
            {
                // Partir de la config par défaut puis filtrer aux tables utiles pour Reconciliation
                bool ok = await DatabaseTemplateGenerator.CreateCustomTemplateAsync(
                    path,
                    config =>
                    {
                        var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                        {
                            "SyncLocks", "Sessions", "ChangeLog", "T_ConfigParameters", "T_Reconciliation"
                        };
                        config.Tables = config.Tables.Where(t => keep.Contains(t.Name)).ToList();
                    });

                if (!ok) report.Errors.Add($"Reconciliation: échec de création ({Path.GetFileName(path)})");
                else report.Logs.Add($"Reconciliation créé: {path}");
                return ok;
            }
            catch (Exception ex)
            {
                report.Errors.Add($"Reconciliation: exception {ex.Message}");
                return false;
            }
        }

        private async Task<bool> CreateDwingsDatabaseAsync(string path, RecreationReport report)
        {
            try
            {
                bool ok = await DatabaseTemplateFactory.CreateCustomTemplateAsync(
                    path,
                    builder =>
                    {
                        // T_DW_Guarantee
                        builder.AddTable("T_DW_Guarantee")
                            .WithPrimaryKey("GUARANTEE_ID", typeof(string))
                            .WithColumn("SYNDICATE", typeof(string))
                            .WithColumn("CURRENCY", typeof(string))
                            .WithColumn("AMOUNT", typeof(string))
                            .WithColumn("OfficialID", typeof(string))
                            .WithColumn("GuaranteeType", typeof(string))
                            .WithColumn("Client", typeof(string))
                            .WithColumn("_791Sent", typeof(string))
                            .WithColumn("InvoiceStatus", typeof(string))
                            .WithColumn("TriggerDate", typeof(string))
                            .WithColumn("FXRate", typeof(string))
                            .WithColumn("RMPM", typeof(string))
                            .WithColumn("GroupName", typeof(string))
                            .WithColumn("STATUS", typeof(string))
                            .EndTable();

                        // T_DW_Data
                        builder.AddTable("T_DW_Data")
                            .WithPrimaryKey("INVOICE_ID", typeof(string))
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
                            .WithColumn("T_COMMISSION_PERIOD_STAT", typeof(string))
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
                            .WithColumn("BGPMT", typeof(string))
                            .WithColumn("DEBTOR_ACCOUNT_ID", typeof(string))
                            .WithColumn("CREDITOR_ACCOUNT_ID", typeof(string))
                            .WithColumn("COMMISSION_ID", typeof(string))
                            .WithColumn("DEBTOR_PARTY_ID", typeof(string))
                            .WithColumn("DEBTOR_PARTY_NAME", typeof(string))
                            .WithColumn("DEBTOR_ACCOUNT_NUMBER", typeof(string))
                            .WithColumn("CREDITOR_PARTY_ID", typeof(string))
                            .WithColumn("CREDITOR_PARTY_NAME", typeof(string))
                            .WithColumn("CREDITOR_ACCOUNT_NUMBER", typeof(string))
                            .WithColumn("PAYMENT_METHOD", typeof(string))
                            .EndTable();
                    });

                if (!ok) report.Errors.Add($"DWINGS: échec de création ({Path.GetFileName(path)})");
                else report.Logs.Add($"DWINGS créé: {path}");
                return ok;
            }
            catch (Exception ex)
            {
                report.Errors.Add($"DWINGS: exception {ex.Message}");
                return false;
            }
        }

        private async Task<bool> CreateLockDatabaseAsync(string path, RecreationReport report)
        {
            try
            {
                bool ok = await DatabaseTemplateGenerator.CreateCustomTemplateAsync(
                    path,
                    config =>
                    {
                        // Démarre avec la config par défaut puis ne garde que SyncLocks & Sessions
                        var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                        {
                            "SyncLocks", "Sessions"
                        };
                        config.Tables = config.Tables.Where(t => keep.Contains(t.Name)).ToList();
                    });

                if (!ok) report.Errors.Add($"Lock: échec de création ({Path.GetFileName(path)})");
                else report.Logs.Add($"Lock créé: {path}");
                return ok;
            }
            catch (Exception ex)
            {
                report.Errors.Add($"Lock: exception {ex.Message}");
                return false;
            }
        }
    }
}
