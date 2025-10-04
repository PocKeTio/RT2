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
using RecoTool.Services;
using RecoTool.Services.Rules;
using RecoTool.Infrastructure.Logging;

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
        private readonly RulesEngine _rulesEngine;
        private TransformationService _transformationService; // Cached per import

        public AmbreReconciliationUpdater(
            OfflineFirstService offlineFirstService,
            string currentUser,
            ReconciliationService reconciliationService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = currentUser;
            _reconciliationService = reconciliationService;
            _dwingsResolver = new DwingsReferenceResolver(reconciliationService);
            _rulesEngine = new RulesEngine(_offlineFirstService);
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

                // Remplir les références DWINGS manquantes pour les enregistrements mis à jour (sans écraser les liens manuels)
                try
                {
                    await UpdateDwingsReferencesForUpdatesAsync(changes.ToUpdate, country, countryId);
                }
                catch (Exception fillEx)
                {
                    LogManager.Warning($"Non-blocking: failed to backfill DWINGS refs for updates: {fillEx.Message}");
                }

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
            // OPTIMIZATION: Load DWINGS invoices once and cache TransformationService
            var dwInvoices = (await _reconciliationService.GetDwingsInvoicesAsync()).ToList();
            _transformationService = new TransformationService(new List<Country> { country });
            
            var staged = new List<ReconciliationStaging>();

            // OPTIMIZATION: Parallel processing of reconciliation creation
            var tasks = newRecords.Select(async dataAmbre =>
            {
                var reconciliation = await CreateReconciliationAsync(
                    dataAmbre, country, countryId, dwInvoices);
                    
                return new ReconciliationStaging
                {
                    Reconciliation = reconciliation,
                    DataAmbre = dataAmbre,
                    IsPivot = dataAmbre.IsPivotAccount(country.CNT_AmbrePivot),
                    Bgi = reconciliation.DWINGS_InvoiceID
                };
            });
            
            staged = (await Task.WhenAll(tasks)).ToList();

            // Apply truth-table rules (import scope) - pass dwInvoices to avoid reloading
            await ApplyTruthTableRulesAsync(staged, country, countryId, dwInvoices);

            return staged.Select(s => s.Reconciliation).ToList();
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
            reconciliation.DWINGS_BGPMT = dwingsRefs.CommissionId;
            reconciliation.DWINGS_GuaranteeID = dwingsRefs.GuaranteeId;

            // KPI and Action are set by truth-table rules only

            return reconciliation;
        }


        private RuleContext BuildRuleContext(DataAmbre dataAmbre, Reconciliation reconciliation, Country country, string countryId, bool isPivot, List<DwingsInvoiceDto> dwInvoices)
        {
            // OPTIMIZATION: Use cached TransformationService instead of creating new instance
            var tx = _transformationService.DetermineTransactionType(dataAmbre.RawLabel, isPivot, dataAmbre.Category);
            string txName = tx.HasValue ? Enum.GetName(typeof(TransactionType), tx.Value) : null;

            // Guarantee type detectable primarily on receivable from label
            string guaranteeType = !isPivot ? ExtractGuaranteeType(dataAmbre.RawLabel) : null;

            // Sign from amount
            var sign = dataAmbre.SignedAmount >= 0 ? "C" : "D";

            // Presence of DWINGS links (any of Invoice/Guarantee/BGPMT)
            bool? hasDw = (!string.IsNullOrWhiteSpace(reconciliation?.DWINGS_InvoiceID)
                          || !string.IsNullOrWhiteSpace(reconciliation?.DWINGS_GuaranteeID)
                          || !string.IsNullOrWhiteSpace(reconciliation?.DWINGS_BGPMT));

            // Extended time/state inputs
            var today = DateTime.Today;
            bool? triggerDateIsNull = !(reconciliation?.TriggerDate.HasValue == true);
            int? daysSinceTrigger = reconciliation?.TriggerDate.HasValue == true
                ? (int?)(today - reconciliation.TriggerDate.Value.Date).TotalDays
                : null;
            // Transitory if a BGPMT token is present in reconciliation numbers or raw label
            var bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.Reconciliation_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.ReconciliationOrigin_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.RawLabel);
            bool? isTransitory = !string.IsNullOrWhiteSpace(bgpmt);
            int? operationDaysAgo = dataAmbre.Operation_Date.HasValue
                ? (int?)(today - dataAmbre.Operation_Date.Value.Date).TotalDays
                : null;
            bool? isMatched = hasDw; // consider matched when any DWINGS link is present
            bool? hasManualMatch = null; // unknown at import time
            bool? isFirstRequest = !(reconciliation?.FirstClaimDate.HasValue == true);
            int? daysSinceReminder = reconciliation?.LastClaimDate.HasValue == true
                ? (int?)(today - reconciliation.LastClaimDate.Value.Date).TotalDays
                : null;

            // OPTIMIZATION: Use passed dwInvoices instead of reloading
            bool? mtAcked = null;
            bool? hasCommEmail = null;
            bool? bgiInitiated = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(reconciliation?.DWINGS_InvoiceID))
                {
                    var inv = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, reconciliation.DWINGS_InvoiceID, StringComparison.OrdinalIgnoreCase));
                    if (inv != null)
                    {
                        if (!string.IsNullOrWhiteSpace(inv.MT_STATUS))
                            mtAcked = string.Equals(inv.MT_STATUS, "ACKED", StringComparison.OrdinalIgnoreCase);
                        hasCommEmail = inv.COMM_ID_EMAIL;
                        if (!string.IsNullOrWhiteSpace(inv.T_INVOICE_STATUS))
                            bgiInitiated = string.Equals(inv.T_INVOICE_STATUS, "INITIATED", StringComparison.OrdinalIgnoreCase);
                    }
                }
            }
            catch { }

            return new RuleContext
            {
                CountryId = countryId,
                IsPivot = isPivot,
                GuaranteeType = guaranteeType,
                TransactionType = txName,
                HasDwingsLink = hasDw,
                IsGrouped = null, // TODO: compute when grouping info is available
                IsAmountMatch = null, // TODO: compute when counterpart amounts available
                Sign = sign,
                Bgi = reconciliation?.DWINGS_InvoiceID,
                // Extended fields
                TriggerDateIsNull = triggerDateIsNull,
                DaysSinceTrigger = daysSinceTrigger,
                IsTransitory = isTransitory,
                OperationDaysAgo = operationDaysAgo,
                IsMatched = isMatched,
                HasManualMatch = hasManualMatch,
                IsFirstRequest = isFirstRequest,
                DaysSinceReminder = daysSinceReminder,
                CurrentActionId = reconciliation?.Action,
                // New DWINGS-derived
                IsMtAcked = mtAcked,
                HasCommIdEmail = hasCommEmail,
                IsBgiInitiated = bgiInitiated
            };
        }

        private async Task ApplyTruthTableRulesAsync(List<ReconciliationStaging> staged, Country country, string countryId, List<DwingsInvoiceDto> dwInvoices)
        {
            try
            {
                if (staged == null || staged.Count == 0) return;

                // First pass: apply SELF outputs immediately; gather counterpart intents by BGI
                var counterpartIntents = new List<(string Bgi, bool TargetIsPivot, string RuleId, int? ActionId, int? KpiId, int? IncidentTypeId, bool? RiskyItem, int? ReasonNonRiskyId, bool? ToRemind, int? ToRemindDays)>();

                // OPTIMIZATION: Parallel rule evaluation (rules engine is stateless)
                var ruleEvaluationTasks = staged.Select(async s =>
                {
                    var ctx = BuildRuleContext(s.DataAmbre, s.Reconciliation, country, countryId, s.IsPivot, dwInvoices);
                    var res = await _rulesEngine.EvaluateAsync(ctx, RuleScope.Import).ConfigureAwait(false);
                    return (Staging: s, Result: res);
                }).ToList();
                
                var evaluationResults = await Task.WhenAll(ruleEvaluationTasks);

                foreach (var (s, res) in evaluationResults)
                {
                    if (res == null || res.Rule == null) continue;

                    // Self application
                    // In Import scope, always apply rules (AutoApply only affects Edit scope)
                    if (res.Rule.ApplyTo == ApplyTarget.Self || res.Rule.ApplyTo == ApplyTarget.Both)
                    {
                        if (res.NewActionIdSelf.HasValue) s.Reconciliation.Action = res.NewActionIdSelf.Value;
                        if (res.NewKpiIdSelf.HasValue) s.Reconciliation.KPI = res.NewKpiIdSelf.Value;
                        if (res.NewIncidentTypeIdSelf.HasValue) s.Reconciliation.IncidentType = res.NewIncidentTypeIdSelf.Value;
                        if (res.NewRiskyItemSelf.HasValue) s.Reconciliation.RiskyItem = res.NewRiskyItemSelf.Value;
                        if (res.NewReasonNonRiskyIdSelf.HasValue) s.Reconciliation.ReasonNonRisky = res.NewReasonNonRiskyIdSelf.Value;
                        if (res.NewToRemindSelf.HasValue)
                        {
                            s.Reconciliation.ToRemind = res.NewToRemindSelf.Value;
                        }
                        if (res.NewToRemindDaysSelf.HasValue)
                        {
                            var days = res.NewToRemindDaysSelf.Value;
                            try { s.Reconciliation.ToRemindDate = DateTime.Today.AddDays(days); } catch { }
                        }
                        if (res.NewFirstClaimTodaySelf == true)
                        {
                            try { s.Reconciliation.FirstClaimDate = DateTime.Today; } catch { }
                        }
                        
                        if (!string.IsNullOrWhiteSpace(res.UserMessage))
                        {
                            // Prepend timestamped message to comments; keep non-blocking and avoid duplicates
                            try
                            {
                                var prefix = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {_currentUser}: ";
                                var msg = prefix + $"[Rule {res.Rule.RuleId ?? "(unnamed)"}] {res.UserMessage}";
                                if (string.IsNullOrWhiteSpace(s.Reconciliation.Comments))
                                {
                                    s.Reconciliation.Comments = msg;
                                }
                                else if (!s.Reconciliation.Comments.Contains(msg))
                                {
                                    s.Reconciliation.Comments = msg + Environment.NewLine + s.Reconciliation.Comments;
                                }
                            }
                            catch { }
                        }

                        // Log to file for import (SELF)
                        try
                        {
                            var outs = new System.Collections.Generic.List<string>();
                            if (res.NewActionIdSelf.HasValue) outs.Add($"Action={res.NewActionIdSelf.Value}");
                            if (res.NewKpiIdSelf.HasValue) outs.Add($"KPI={res.NewKpiIdSelf.Value}");
                            if (res.NewIncidentTypeIdSelf.HasValue) outs.Add($"IncidentType={res.NewIncidentTypeIdSelf.Value}");
                            if (res.NewRiskyItemSelf.HasValue) outs.Add($"RiskyItem={res.NewRiskyItemSelf.Value}");
                            if (res.NewReasonNonRiskyIdSelf.HasValue) outs.Add($"ReasonNonRisky={res.NewReasonNonRiskyIdSelf.Value}");
                            if (res.NewToRemindSelf.HasValue) outs.Add($"ToRemind={res.NewToRemindSelf.Value}");
                            if (res.NewToRemindDaysSelf.HasValue) outs.Add($"ToRemindDays={res.NewToRemindDaysSelf.Value}");
                            if (res.NewFirstClaimTodaySelf == true) outs.Add("FirstClaimDate=Today");
                            var outsStr = string.Join("; ", outs);
                            LogHelper.WriteRuleApplied("import", countryId, s.Reconciliation?.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                        }
                        catch { }
                    }

                    // Counterpart application intents
                    // In Import scope, always apply rules (AutoApply only affects Edit scope)
                    if ((res.Rule.ApplyTo == ApplyTarget.Counterpart || res.Rule.ApplyTo == ApplyTarget.Both)
                        && !string.IsNullOrWhiteSpace(s.Bgi))
                    {
                        var targetIsPivot = !s.IsPivot; // counterpart side
                        counterpartIntents.Add((
                            s.Bgi.Trim().ToUpperInvariant(),
                            targetIsPivot,
                            res.Rule.RuleId,
                            res.Rule.OutputActionId,
                            res.Rule.OutputKpiId,
                            res.Rule.OutputIncidentTypeId,
                            res.Rule.OutputRiskyItem,
                            res.Rule.OutputReasonNonRiskyId,
                            res.Rule.OutputToRemind,
                            res.Rule.OutputToRemindDays
                        ));
                    }
                }

                // Second pass: realize counterpart intents within the staged set using BGI grouping
                if (counterpartIntents.Count > 0)
                {
                    // Group staged by normalized BGI
                    var byBgi = staged
                        .Where(x => !string.IsNullOrWhiteSpace(x.Bgi))
                        .GroupBy(x => x.Bgi.Trim().ToUpperInvariant());

                    var map = byBgi.ToDictionary(g => g.Key, g => g.ToList());
                    foreach (var intent in counterpartIntents)
                    {
                        if (!map.TryGetValue(intent.Bgi, out var rows)) continue;
                        foreach (var row in rows)
                        {
                            if (row.IsPivot != intent.TargetIsPivot) continue;
                            if (intent.ActionId.HasValue) row.Reconciliation.Action = intent.ActionId.Value;
                            if (intent.KpiId.HasValue) row.Reconciliation.KPI = intent.KpiId.Value;
                            if (intent.IncidentTypeId.HasValue) row.Reconciliation.IncidentType = intent.IncidentTypeId.Value;
                            if (intent.RiskyItem.HasValue) row.Reconciliation.RiskyItem = intent.RiskyItem.Value;
                            if (intent.ReasonNonRiskyId.HasValue) row.Reconciliation.ReasonNonRisky = intent.ReasonNonRiskyId.Value;
                            if (intent.ToRemind.HasValue) row.Reconciliation.ToRemind = intent.ToRemind.Value;
                            if (intent.ToRemindDays.HasValue)
                            {
                                try { row.Reconciliation.ToRemindDate = DateTime.Today.AddDays(intent.ToRemindDays.Value); } catch { }
                            }

                            // Log counterpart application
                            try
                            {
                                var outs = new System.Collections.Generic.List<string>();
                                if (intent.ActionId.HasValue) outs.Add($"Action={intent.ActionId.Value}");
                                if (intent.KpiId.HasValue) outs.Add($"KPI={intent.KpiId.Value}");
                                if (intent.IncidentTypeId.HasValue) outs.Add($"IncidentType={intent.IncidentTypeId.Value}");
                                if (intent.RiskyItem.HasValue) outs.Add($"RiskyItem={intent.RiskyItem.Value}");
                                if (intent.ReasonNonRiskyId.HasValue) outs.Add($"ReasonNonRisky={intent.ReasonNonRiskyId.Value}");
                                if (intent.ToRemind.HasValue) outs.Add($"ToRemind={intent.ToRemind.Value}");
                                if (intent.ToRemindDays.HasValue) outs.Add($"ToRemindDays={intent.ToRemindDays.Value}");
                                var outsStr = string.Join("; ", outs);
                                LogHelper.WriteRuleApplied("import", countryId, row.Reconciliation?.ID, intent.RuleId, outsStr, "Counterpart application");
                            }
                            catch { }
                        }
                    }
                }

                // Finalize default ActionStatus and ActionDate values after all rule outputs are applied
                try
                {
                    var allUserFields = _offlineFirstService?.UserFields;
                    var nowLocal = DateTime.Now;
                    foreach (var s in staged)
                    {
                        var rec = s?.Reconciliation;
                        if (rec == null) continue;
                        try
                        {
                            bool isNa = !rec.Action.HasValue || UserFieldUpdateService.IsActionNA(rec.Action, allUserFields);
                            if (isNa)
                            {
                                rec.ActionStatus = null;
                                rec.ActionDate = null;
                            }
                            else
                            {
                                if (!rec.ActionStatus.HasValue) rec.ActionStatus = false; // default to PENDING
                                if (!rec.ActionDate.HasValue) rec.ActionDate = nowLocal;
                            }
                        }
                        catch { }
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                LogManager.Warning($"Truth-table rules application failed: {ex.Message}");
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

        private async Task UpdateDwingsReferencesForUpdatesAsync(
            List<DataAmbre> updatedRecords,
            Country country,
            string countryId)
        {
            try
            {
                if (updatedRecords == null || updatedRecords.Count == 0) return;
                var invoices = await _reconciliationService.GetDwingsInvoicesAsync();
                if (invoices == null || invoices.Count == 0) return;
                var dwList = invoices.ToList();

                var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
                using (var conn = new OleDbConnection(connectionString))
                {
                    await conn.OpenAsync();
                    using (var tx = conn.BeginTransaction())
                    {
                        try
                        {
                            var nowUtc = DateTime.UtcNow;
                            foreach (var amb in updatedRecords)
                            {
                                if (amb == null || string.IsNullOrWhiteSpace(amb.ID)) continue;
                                bool isPivot = amb.IsPivotAccount(country.CNT_AmbrePivot);
                                var refs = await _dwingsResolver.ResolveReferencesAsync(amb, isPivot, dwList);
                                if (refs != null)
                                {
                                    if (!string.IsNullOrWhiteSpace(refs.InvoiceId))
                                    {
                                        using (var cmd = new OleDbCommand(
                                            "UPDATE [T_Reconciliation] SET [DWINGS_InvoiceID]=?, [LastModified]=?, [ModifiedBy]=? " +
                                            "WHERE [ID]=? AND ([DWINGS_InvoiceID] IS NULL OR [DWINGS_InvoiceID] = '')",
                                            conn, tx))
                                        {
                                            cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", refs.InvoiceId);
                                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                                            cmd.Parameters.AddWithValue("@ID", amb.ID);
                                            await cmd.ExecuteNonQueryAsync();
                                        }
                                    }

                                    if (!string.IsNullOrWhiteSpace(refs.CommissionId))
                                    {
                                        using (var cmd = new OleDbCommand(
                                            "UPDATE [T_Reconciliation] SET [DWINGS_BGPMT]=?, [LastModified]=?, [ModifiedBy]=? " +
                                            "WHERE [ID]=? AND ([DWINGS_BGPMT] IS NULL OR [DWINGS_BGPMT] = '')",
                                            conn, tx))
                                        {
                                            cmd.Parameters.AddWithValue("@DWINGS_BGPMT", refs.CommissionId);
                                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                                            cmd.Parameters.AddWithValue("@ID", amb.ID);
                                            await cmd.ExecuteNonQueryAsync();
                                        }
                                    }

                                    if (!string.IsNullOrWhiteSpace(refs.GuaranteeId))
                                    {
                                        using (var cmd = new OleDbCommand(
                                            "UPDATE [T_Reconciliation] SET [DWINGS_GuaranteeID]=?, [LastModified]=?, [ModifiedBy]=? " +
                                            "WHERE [ID]=? AND ([DWINGS_GuaranteeID] IS NULL OR [DWINGS_GuaranteeID] = '')",
                                            conn, tx))
                                        {
                                            cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", refs.GuaranteeId);
                                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                                            cmd.Parameters.AddWithValue("@ID", amb.ID);
                                            await cmd.ExecuteNonQueryAsync();
                                        }
                                    }
                                }
                            }

                            tx.Commit();
                        }
                        catch
                        {
                            tx.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Warning($"Backfill DWINGS refs failed: {ex.Message}");
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
                    
                    // OPTIMIZATION: Batch update with IN clause (Access supports up to ~1000 items)
                    const int batchSize = 500;
                    int totalCount = 0;
                    
                    for (int i = 0; i < ids.Count; i += batchSize)
                    {
                        var batch = ids.Skip(i).Take(batchSize).ToList();
                        var inClause = string.Join(",", batch.Select((_, idx) => $"?"));
                        
                        using (var cmd = new OleDbCommand(
                            $"UPDATE [T_Reconciliation] SET [DeleteDate]=NULL, [LastModified]=?, [ModifiedBy]=? " +
                            $"WHERE [ID] IN ({inClause}) AND [DeleteDate] IS NOT NULL", conn, tx))
                        {
                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                            foreach (var id in batch)
                            {
                                cmd.Parameters.AddWithValue("@ID", id);
                            }
                            totalCount += await cmd.ExecuteNonQueryAsync();
                        }
                    }
                    
                    tx.Commit();
                    LogManager.Info($"Unarchived {totalCount} reconciliation record(s)");
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
                    
                    // OPTIMIZATION: Batch update with IN clause
                    const int batchSize = 500;
                    int totalCount = 0;
                    
                    for (int i = 0; i < ids.Count; i += batchSize)
                    {
                        var batch = ids.Skip(i).Take(batchSize).ToList();
                        var inClause = string.Join(",", batch.Select((_, idx) => $"?"));
                        
                        using (var cmd = new OleDbCommand(
                            $"UPDATE [T_Reconciliation] SET [DeleteDate]=?, [LastModified]=?, [ModifiedBy]=? " +
                            $"WHERE [ID] IN ({inClause}) AND [DeleteDate] IS NULL", conn, tx))
                        {
                            cmd.Parameters.Add("@DeleteDate", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.Add("@LastModified", OleDbType.Date).Value = nowUtc;
                            cmd.Parameters.AddWithValue("@ModifiedBy", _currentUser);
                            foreach (var id in batch)
                            {
                                cmd.Parameters.AddWithValue("@ID", id);
                            }
                            totalCount += await cmd.ExecuteNonQueryAsync();
                        }
                    }
                    
                    tx.Commit();
                    LogManager.Info($"Archived {totalCount} reconciliation record(s)");
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
                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_BGPMT],
                [Action],[ActionStatus],[ActionDate],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                [IncidentType],[RiskyItem],[ReasonNonRisky],[CreationDate],[ModifiedBy],[LastModified]
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", conn, tx);

            // Add parameters in order
            cmd.Parameters.AddWithValue("@ID", (object)rec.ID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", (object)rec.DWINGS_GuaranteeID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", (object)rec.DWINGS_InvoiceID ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_BGPMT", (object)rec.DWINGS_BGPMT ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Action", (object)rec.Action ?? DBNull.Value);
            cmd.Parameters.Add("@ActionStatus", OleDbType.Boolean).Value = rec.ActionStatus.HasValue ? (object)rec.ActionStatus.Value : DBNull.Value;
            cmd.Parameters.Add("@ActionDate", OleDbType.Date).Value = rec.ActionDate.HasValue ? (object)rec.ActionDate.Value : DBNull.Value;
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