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
    /// Gestionnaire de mise à jour de la table T_Reconciliation
    /// </summary>
    public class AmbreReconciliationUpdater
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly string _currentUser;
        private readonly ReconciliationService _reconciliationService;
        private readonly DwingsReferenceResolver _dwingsResolver;

        public AmbreReconciliationUpdater(
            OfflineFirstService offlineFirstService,
            string currentUser,
            ReconciliationService reconciliationService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = currentUser;
            _reconciliationService = reconciliationService;
            _dwingsResolver = new DwingsReferenceResolver(reconciliationService);
        }

        /// <summary>
        /// Met à jour la table T_Reconciliation avec les changements d'import
        /// </summary>
        public async Task UpdateReconciliationTableAsync(
            ImportChanges changes,
            string countryId,
            Country country)
        {
            LogManager.Info($"Updating T_Reconciliation for {countryId}");

            try
            {
                // Préparer les enregistrements de réconciliation
                var reconciliations = await PrepareReconciliationsAsync(
                    changes.ToAdd, country, countryId);

                // Appliquer les changements à la base de données
                await ApplyReconciliationChangesAsync(
                    reconciliations,
                    changes.ToUpdate,
                    changes.ToArchive,
                    countryId);

                LogManager.Info($"T_Reconciliation update completed for {countryId}");
            }
            catch (Exception ex)
            {
                LogManager.Error($"Error updating T_Reconciliation for {countryId}", ex);
                throw new InvalidOperationException($"Failed to update reconciliation table: {ex.Message}", ex);
            }
        }

        private async Task<List<Reconciliation>> PrepareReconciliationsAsync(
            List<DataAmbre> newRecords,
            Country country,
            string countryId)
        {
            var reconciliations = new List<Reconciliation>();
            var dwInvoices = await _reconciliationService.GetDwingsInvoicesAsync();
            var staged = new List<ReconciliationStaging>();

            foreach (var dataAmbre in newRecords)
            {
                var reconciliation = await CreateReconciliationAsync(
                    dataAmbre, country, countryId, dwInvoices.ToList());
                    
                reconciliations.Add(reconciliation);
                
                staged.Add(new ReconciliationStaging
                {
                    Reconciliation = reconciliation,
                    DataAmbre = dataAmbre,
                    IsPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot),
                    Bgi = reconciliation.DWINGS_InvoiceID
                });
            }

            // Apply cross-side action rules
            ApplyCrossSideActionRules(staged);

            return reconciliations;
        }

        private async Task<Reconciliation> CreateReconciliationAsync(
            DataAmbre dataAmbre,
            Country country,
            string countryId,
            List<DwingsInvoiceDto> dwInvoices)
        {
            var reconciliation = new Reconciliation
            {
                ID = dataAmbre.ID,
                CreationDate = DateTime.UtcNow,
                ModifiedBy = _currentUser,
                LastModified = DateTime.UtcNow,
                Version = 1
            };

            bool isPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot);

            // Resolve DWINGS references
            var dwingsRefs = await _dwingsResolver.ResolveReferencesAsync(
                dataAmbre, isPivot, dwInvoices);
                
            reconciliation.DWINGS_InvoiceID = dwingsRefs.InvoiceId;
            reconciliation.DWINGS_CommissionID = dwingsRefs.CommissionId;
            reconciliation.DWINGS_GuaranteeID = dwingsRefs.GuaranteeId;

            // Calculate KPI
            var kpi = CalculateKpi(dataAmbre, isPivot, country);
            reconciliation.KPI = (int)kpi;

            // Calculate auto action
            var action = await CalculateAutoActionAsync(
                dataAmbre, reconciliation, country, countryId, isPivot);
            if (action.HasValue)
                reconciliation.Action = (int)action.Value;

            return reconciliation;
        }

        private void ApplyCrossSideActionRules(List<ReconciliationStaging> staged)
        {
            try
            {
                var groups = staged
                    .Where(s => !string.IsNullOrWhiteSpace(s.Bgi))
                    .GroupBy(s => s.Bgi.Trim().ToUpperInvariant());

                foreach (var group in groups)
                {
                    var pivots = group.Where(x => x.IsPivot).ToList();
                    var receivables = group.Where(x => !x.IsPivot).ToList();
                    
                    if (pivots.Count == 0 || receivables.Count == 0) 
                        continue;

                    // Matched set: assign appropriate actions
                    foreach (var pivot in pivots)
                    {
                        pivot.Reconciliation.Action = (int)ActionType.Match;
                    }
                    
                    foreach (var receivable in receivables)
                    {
                        receivable.Reconciliation.Action = (int)ActionType.Trigger;
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Warning($"Cross-side action assignment failed: {ex.Message}");
            }
        }

        private KPIType CalculateKpi(DataAmbre dataAmbre, bool isPivot, Country country)
        {
            var transformationService = new TransformationService(new List<Country> { country });
            var transactionType = transformationService.DetermineTransactionType(
                dataAmbre.RawLabel, isPivot, dataAmbre.Category);
                
            string guaranteeType = !isPivot ? ExtractGuaranteeType(dataAmbre.RawLabel) : null;
            
            var (_, kpi) = transformationService.ApplyAutomaticCategorization(
                transactionType, dataAmbre.SignedAmount, isPivot, guaranteeType);
                
            return kpi;
        }

        private async Task<ActionType?> CalculateAutoActionAsync(
            DataAmbre dataAmbre,
            Reconciliation reconciliation,
            Country country,
            string countryId,
            bool isPivot)
        {
            try
            {
                var transformationService = new TransformationService(new List<Country> { country });
                var transactionType = transformationService.DetermineTransactionType(
                    dataAmbre.RawLabel, isPivot, dataAmbre.Category);

                string paymentMethod = await _dwingsResolver.GetPaymentMethodAsync(
                    dataAmbre, countryId);

                var autoAction = _reconciliationService.ComputeAutoAction(
                    transactionType, dataAmbre, reconciliation, country, 
                    paymentMethod: paymentMethod, today: DateTime.Today);
                    
                return autoAction;
            }
            catch (Exception ex)
            {
                LogManager.Warning($"Failed to calculate auto action: {ex.Message}");
                return null;
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

        private async Task ApplyReconciliationChangesAsync(
            List<Reconciliation> toInsert,
            List<DataAmbre> toUpdate,
            List<DataAmbre> toArchive,
            string countryId)
        {
            var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
            
            using (var conn = new OleDbConnection(connectionString))
            {
                await conn.OpenAsync();

                // Unarchive updated records
                if (toUpdate.Any())
                {
                    await UnarchiveRecordsAsync(conn, toUpdate);
                }

                // Archive deleted records
                if (toArchive.Any())
                {
                    await ArchiveRecordsAsync(conn, toArchive);
                }

                // Insert new reconciliations
                if (toInsert.Any())
                {
                    await InsertReconciliationsAsync(conn, toInsert);
                }
            }
        }

        private async Task UnarchiveRecordsAsync(OleDbConnection conn, List<DataAmbre> records)
        {
            var ids = records
                .Select(d => d?.ID)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (!ids.Any()) return;

            using (var tx = conn.BeginTransaction())
            {
                try
                {
                    var nowUtc = DateTime.UtcNow;
                    int count = 0;
                    
                    foreach (var id in ids)
                    {
                        using (var cmd = new OleDbCommand(
                            "UPDATE [T_Reconciliation] SET [DeleteDate]=NULL, [LastModified]=?, [ModifiedBy]=? " +
                            "WHERE [ID]=? AND [DeleteDate] IS NOT NULL", conn, tx))
                        {
                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                            cmd.Parameters.AddWithValue("@ID", id);
                            count += await cmd.ExecuteNonQueryAsync();
                        }
                    }
                    
                    tx.Commit();
                    LogManager.Info($"Unarchived {count} reconciliation record(s)");
                }
                catch
                {
                    tx.Rollback();
                    throw;
                }
            }
        }

        private async Task ArchiveRecordsAsync(OleDbConnection conn, List<DataAmbre> records)
        {
            var ids = records
                .Select(d => d?.ID)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (!ids.Any()) return;

            using (var tx = conn.BeginTransaction())
            {
                try
                {
                    var nowUtc = DateTime.UtcNow;
                    int count = 0;
                    
                    foreach (var id in ids)
                    {
                        using (var cmd = new OleDbCommand(
                            "UPDATE [T_Reconciliation] SET [DeleteDate]=?, [LastModified]=?, [ModifiedBy]=? " +
                            "WHERE [ID]=? AND [DeleteDate] IS NULL", conn, tx))
                        {
                            cmd.Parameters.Add("@DeleteDate", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                            cmd.Parameters.AddWithValue("@ID", id);
                            count += await cmd.ExecuteNonQueryAsync();
                        }
                    }
                    
                    tx.Commit();
                    LogManager.Info($"Archived {count} reconciliation record(s)");
                }
                catch
                {
                    tx.Rollback();
                    throw;
                }
            }
        }

        private async Task InsertReconciliationsAsync(OleDbConnection conn, List<Reconciliation> reconciliations)
        {
            // Get existing IDs to ensure insert-only
            var existingIds = await GetExistingIdsAsync(conn, reconciliations.Select(r => r.ID).ToList());
            
            using (var tx = conn.BeginTransaction())
            {
                try
                {
                    int insertedCount = 0;
                    
                    foreach (var rec in reconciliations.Where(r => !existingIds.Contains(r.ID)))
                    {
                        using (var cmd = CreateInsertCommand(conn, tx, rec))
                        {
                            insertedCount += await cmd.ExecuteNonQueryAsync();
                        }
                    }
                    
                    tx.Commit();
                    LogManager.Info($"Inserted {insertedCount} new reconciliation record(s)");
                }
                catch
                {
                    tx.Rollback();
                    throw;
                }
            }
        }

        private async Task<HashSet<string>> GetExistingIdsAsync(OleDbConnection conn, List<string> ids)
        {
            var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            if (!ids.Any()) return existing;

            const int chunkSize = 500;
            for (int i = 0; i < ids.Count; i += chunkSize)
            {
                var chunk = ids.Skip(i).Take(chunkSize).ToList();
                var placeholders = string.Join(",", Enumerable.Repeat("?", chunk.Count));
                
                using (var cmd = new OleDbCommand(
                    $"SELECT [ID] FROM [T_Reconciliation] WHERE [ID] IN ({placeholders})", conn))
                {
                    foreach (var id in chunk)
                        cmd.Parameters.AddWithValue("@ID", id);
                        
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var id = reader[0]?.ToString();
                            if (!string.IsNullOrWhiteSpace(id))
                                existing.Add(id);
                        }
                    }
                }
            }
            
            return existing;
        }

        private OleDbCommand CreateInsertCommand(OleDbConnection conn, OleDbTransaction tx, Reconciliation rec)
        {
            var cmd = new OleDbCommand(@"INSERT INTO [T_Reconciliation] (
                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                [IncidentType],[RiskyItem],[ReasonNonRisky],[CreationDate],[ModifiedBy],[LastModified]
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", conn, tx);

            // Add parameters in order
            cmd.Parameters.AddWithValue("@ID", (object)rec.ID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", (object)rec.DWINGS_GuaranteeID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", (object)rec.DWINGS_InvoiceID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_CommissionID", (object)rec.DWINGS_CommissionID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Action", (object)rec.Action ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Comments", (object)rec.Comments ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@InternalInvoiceReference", (object)rec.InternalInvoiceReference ?? DBNull.Value);
            
            cmd.Parameters.Add("@FirstClaimDate", OleDbType.Date).Value = 
                rec.FirstClaimDate.HasValue ? (object)rec.FirstClaimDate.Value : DBNull.Value;
            cmd.Parameters.Add("@LastClaimDate", OleDbType.Date).Value = 
                rec.LastClaimDate.HasValue ? (object)rec.LastClaimDate.Value : DBNull.Value;
            cmd.Parameters.Add("@ToRemind", OleDbType.Boolean).Value = rec.ToRemind;
            cmd.Parameters.Add("@ToRemindDate", OleDbType.Date).Value = 
                rec.ToRemindDate.HasValue ? (object)rec.ToRemindDate.Value : DBNull.Value;
            cmd.Parameters.Add("@ACK", OleDbType.Boolean).Value = rec.ACK;
            
            cmd.Parameters.AddWithValue("@SwiftCode", (object)rec.SwiftCode ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PaymentReference", (object)rec.PaymentReference ?? DBNull.Value);
            
            cmd.Parameters.Add("@KPI", OleDbType.Integer).Value = 
                rec.KPI.HasValue ? (object)rec.KPI.Value : DBNull.Value;
            cmd.Parameters.Add("@IncidentType", OleDbType.Integer).Value = 
                rec.IncidentType.HasValue ? (object)rec.IncidentType.Value : DBNull.Value;
            cmd.Parameters.Add("@RiskyItem", OleDbType.Boolean).Value = 
                rec.RiskyItem.HasValue ? (object)rec.RiskyItem.Value : DBNull.Value;
            cmd.Parameters.Add("@ReasonNonRisky", OleDbType.Integer).Value = 
                rec.ReasonNonRisky.HasValue ? (object)rec.ReasonNonRisky.Value : DBNull.Value;
            cmd.Parameters.Add("@CreationDate", OleDbType.Date).Value = 
                rec.CreationDate.HasValue ? (object)rec.CreationDate.Value : DBNull.Value;
                
            cmd.Parameters.AddWithValue("@ModifiedBy", (object)rec.ModifiedBy ?? DBNull.Value);
            
            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = 
                rec.LastModified.HasValue ? (object)rec.LastModified.Value : DBNull.Value;

            return cmd;
        }

        private class ReconciliationStaging
        {
            public Reconciliation Reconciliation { get; set; }
            public DataAmbre DataAmbre { get; set; }
            public bool IsPivot { get; set; }
            public string Bgi { get; set; }
        }
    }
}