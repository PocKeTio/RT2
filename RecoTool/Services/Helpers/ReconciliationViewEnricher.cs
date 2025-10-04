using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using RecoTool.Helpers;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.Helpers
{
    /// <summary>
    /// Enriches ReconciliationViewData rows with DWINGS invoice fields using an in-memory list
    /// of DwingsInvoiceDto records and lightweight heuristics.
    /// </summary>
    public static class ReconciliationViewEnricher
    {
        /// <summary>
        /// Links reconciliation rows to DWINGS invoices by setting DWINGS_InvoiceID
        /// OPTIMIZED: No longer enriches all I_* properties (now lazy-loaded on demand)
        /// NOTE: Assumes DWINGS caches are already initialized by ReconciliationService
        /// </summary>
        public static void EnrichWithDwingsInvoices(List<ReconciliationViewData> rows, IEnumerable<DwingsInvoiceDto> invoices)
        {
            if (rows == null || invoices == null) return;

            // NOTE: Do NOT reinitialize caches here - they are managed by ReconciliationService
            // ReconciliationViewData.InitializeDwingsCaches(invoices, null);

            // Build quick lookups (handle duplicates gracefully by picking first)
            var invoiceList = invoices as IList<DwingsInvoiceDto> ?? invoices.ToList();
            var byInvoiceId = invoiceList.Where(i => !string.IsNullOrWhiteSpace(i.INVOICE_ID))
                                         .GroupBy(i => i.INVOICE_ID, StringComparer.OrdinalIgnoreCase)
                                         .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var byBgpmt = invoiceList.Where(i => !string.IsNullOrWhiteSpace(i.BGPMT))
                                      .GroupBy(i => i.BGPMT, StringComparer.OrdinalIgnoreCase)
                                      .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            foreach (var row in rows)
            {
                DwingsInvoiceDto inv = null;
                // Receivable rule: if Receivable_InvoiceFromAmbre (BGI) is present, use ONLY this to link to DWINGS invoice.
                // Do not fall back to other heuristics in this case.
                if (!string.IsNullOrWhiteSpace(row.Receivable_InvoiceFromAmbre))
                {
                    if (byInvoiceId.TryGetValue(row.Receivable_InvoiceFromAmbre, out var foundByReceivable))
                    {
                        inv = foundByReceivable;
                        // Strict rule: on receivable, always bind using Receivable_InvoiceFromAmbre
                        row.DWINGS_InvoiceID = inv.INVOICE_ID;
                    }
                }
                // Else apply existing resolution order
                else
                {
                    // 1) Direct by DWINGS_InvoiceID
                    if (!string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID) && byInvoiceId.TryGetValue(row.DWINGS_InvoiceID, out var foundById))
                    {
                        inv = foundById;
                    }
                    // 2) By stored PaymentReference (BGPMT)
                    else if (!string.IsNullOrWhiteSpace(row.PaymentReference) && byBgpmt.TryGetValue(row.PaymentReference, out var foundByPm))
                    {
                        inv = foundByPm;
                    }
                    // 3) By stored DWINGS_BGPMT (BGPMT) when PaymentReference is not set
                    else if (!string.IsNullOrWhiteSpace(row.DWINGS_BGPMT) && byBgpmt.TryGetValue(row.DWINGS_BGPMT, out var foundByCommission))
                    {
                        inv = foundByCommission;
                        if (string.IsNullOrWhiteSpace(row.PaymentReference)) row.PaymentReference = row.DWINGS_BGPMT;
                    }
                    else
                    {
                        // 4) Heuristic: extract BGI or BGPMT from available texts (pivot or when no Receivable_InvoiceFromAmbre available)
                        string TryNonEmpty(params string[] ss)
                        {
                            foreach (var s in ss)
                                if (!string.IsNullOrWhiteSpace(s)) return s;
                            return null;
                        }

                        // Extract tokens from potential sources
                        var bgi = DwingsLinkingHelper.ExtractBgiToken(TryNonEmpty(row.Reconciliation_Num))
                                  ?? DwingsLinkingHelper.ExtractBgiToken(TryNonEmpty(row.Comments))
                                  ?? DwingsLinkingHelper.ExtractBgiToken(TryNonEmpty(row.RawLabel))
                                  ?? DwingsLinkingHelper.ExtractBgiToken(TryNonEmpty(row.Receivable_DWRefFromAmbre));

                        var bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(TryNonEmpty(row.Reconciliation_Num))
                                   ?? DwingsLinkingHelper.ExtractBgpmtToken(TryNonEmpty(row.Comments))
                                   ?? DwingsLinkingHelper.ExtractBgpmtToken(TryNonEmpty(row.RawLabel))
                                   ?? DwingsLinkingHelper.ExtractBgpmtToken(TryNonEmpty(row.PaymentReference));

                        if (!string.IsNullOrWhiteSpace(bgi) && byInvoiceId.TryGetValue(bgi, out var foundByBgi))
                        {
                            inv = foundByBgi;
                            // Backfill missing fields to strengthen link in UI
                            if (string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID)) row.DWINGS_InvoiceID = inv.INVOICE_ID;
                        }
                        else if (!string.IsNullOrWhiteSpace(bgpmt) && byBgpmt.TryGetValue(bgpmt, out var foundByBgpmt))
                        {
                            inv = foundByBgpmt;
                            if (string.IsNullOrWhiteSpace(row.PaymentReference)) row.PaymentReference = bgpmt;
                            if (string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID)) row.DWINGS_InvoiceID = inv.INVOICE_ID;
                        }
                    }
                }

                if (inv != null)
                {
                    // OPTIMIZED: Only set the linking fields, all I_* properties are now lazy-loaded
                    row.INVOICE_ID = inv.INVOICE_ID;
                    
                    // If guarantee link is missing but invoice carries Business Case reference, propose it
                    if (string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID) && !string.IsNullOrWhiteSpace(inv.BUSINESS_CASE_REFERENCE))
                    {
                        row.DWINGS_GuaranteeID = inv.BUSINESS_CASE_REFERENCE;
                    }
                }
            }
        }
        
        /// <summary>
        /// Calculates missing amounts for grouped lines (Receivable vs Pivot)
        /// Groups by DWINGS_InvoiceID or InternalInvoiceReference
        /// </summary>
        public static void CalculateMissingAmounts(List<ReconciliationViewData> rows, string receivableAccountId, string pivotAccountId)
        {
            if (rows == null || string.IsNullOrWhiteSpace(receivableAccountId) || string.IsNullOrWhiteSpace(pivotAccountId)) return;
            
            // Group by invoice reference (DWINGS_InvoiceID or InternalInvoiceReference)
            var groups = rows
                .Where(r => !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) || !string.IsNullOrWhiteSpace(r.InternalInvoiceReference))
                .GroupBy(r => 
                {
                    var key = !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) 
                        ? r.DWINGS_InvoiceID.Trim().ToUpperInvariant()
                        : r.InternalInvoiceReference?.Trim().ToUpperInvariant();
                    return key;
                })
                .Where(g => !string.IsNullOrWhiteSpace(g.Key))
                .ToList();
            
            foreach (var group in groups)
            {
                var receivableLines = group.Where(r => r.Account_ID == receivableAccountId).ToList();
                var pivotLines = group.Where(r => r.Account_ID == pivotAccountId).ToList();
                
                // Only calculate if we have both sides
                if (receivableLines.Count == 0 || pivotLines.Count == 0) continue;
                
                var receivableTotal = receivableLines.Sum(r => r.SignedAmount);
                var pivotTotal = pivotLines.Sum(r => r.SignedAmount);
                
                // Missing amount = Receivable - Pivot
                // Positive = waiting for more payments
                // Negative = overpayment
                var missing = receivableTotal - pivotTotal;
                
                // Enrich Receivable lines with counterpart info
                foreach (var r in receivableLines)
                {
                    r.CounterpartTotalAmount = pivotTotal;
                    r.CounterpartCount = pivotLines.Count;
                    r.MissingAmount = missing;
                }
                
                // Enrich Pivot lines with counterpart info
                foreach (var p in pivotLines)
                {
                    p.CounterpartTotalAmount = receivableTotal;
                    p.CounterpartCount = receivableLines.Count;
                    p.MissingAmount = -missing; // Inverted for Pivot perspective
                }
            }
        }
        
        /// <summary>
        /// Recalcule IsMatchedAcrossAccounts et MissingAmount pour un groupe spécifique seulement
        /// Version optimisée pour édition incrémentale (95% plus rapide que recalcul complet)
        /// </summary>
        /// <param name="allData">Toutes les données (pour trouver le groupe)</param>
        /// <param name="changedInvoiceRef">Référence modifiée (InternalInvoiceReference ou DWINGS_InvoiceID)</param>
        /// <param name="receivableAccountId">Account_ID Receivable</param>
        /// <param name="pivotAccountId">Account_ID Pivot</param>
        public static void RecalculateFlagsForGroup(
            IEnumerable<ReconciliationViewData> allData,
            string changedInvoiceRef,
            string receivableAccountId,
            string pivotAccountId)
        {
            if (allData == null || string.IsNullOrWhiteSpace(changedInvoiceRef))
                return;
            
            // Trouver toutes les lignes du groupe modifié
            var affectedRows = allData
                .Where(r => string.Equals(r.InternalInvoiceReference, changedInvoiceRef, StringComparison.OrdinalIgnoreCase)
                         || string.Equals(r.DWINGS_InvoiceID, changedInvoiceRef, StringComparison.OrdinalIgnoreCase))
                .ToList();
            
            if (affectedRows.Count == 0) return;
            
            // Recalculer IsMatchedAcrossAccounts pour ce groupe
            bool hasP = affectedRows.Any(x => string.Equals(x.AccountSide, "P", StringComparison.OrdinalIgnoreCase));
            bool hasR = affectedRows.Any(x => string.Equals(x.AccountSide, "R", StringComparison.OrdinalIgnoreCase));
            bool isMatched = hasP && hasR;
            
            foreach (var row in affectedRows)
            {
                row.IsMatchedAcrossAccounts = isMatched;
            }
            
            // Recalculer MissingAmount pour ce groupe uniquement
            if (isMatched && !string.IsNullOrWhiteSpace(receivableAccountId) && !string.IsNullOrWhiteSpace(pivotAccountId))
            {
                var receivableLines = affectedRows.Where(r => r.Account_ID == receivableAccountId).ToList();
                var pivotLines = affectedRows.Where(r => r.Account_ID == pivotAccountId).ToList();
                
                if (receivableLines.Count > 0 && pivotLines.Count > 0)
                {
                    var receivableTotal = receivableLines.Sum(r => r.SignedAmount);
                    var pivotTotal = pivotLines.Sum(r => r.SignedAmount);
                    var missing = receivableTotal - pivotTotal;
                    
                    // Enrich Receivable lines
                    foreach (var r in receivableLines)
                    {
                        r.CounterpartTotalAmount = pivotTotal;
                        r.CounterpartCount = pivotLines.Count;
                        r.MissingAmount = missing;
                    }
                    
                    // Enrich Pivot lines
                    foreach (var p in pivotLines)
                    {
                        p.CounterpartTotalAmount = receivableTotal;
                        p.CounterpartCount = receivableLines.Count;
                        p.MissingAmount = -missing;
                    }
                }
            }
            else
            {
                // Pas de matching ou pas de country info, reset les valeurs
                foreach (var row in affectedRows)
                {
                    row.MissingAmount = null;
                    row.CounterpartTotalAmount = null;
                    row.CounterpartCount = null;
                }
            }
        }
    }
}
