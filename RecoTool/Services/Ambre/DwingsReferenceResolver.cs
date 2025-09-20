using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using OfflineFirstAccess.Helpers;
using RecoTool.Helpers;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.AmbreImport
{
    /// <summary>
    /// Résolveur de références DWINGS pour l'import Ambre
    /// </summary>
    public class DwingsReferenceResolver
    {
        private readonly ReconciliationService _reconciliationService;

        public DwingsReferenceResolver(ReconciliationService reconciliationService)
        {
            _reconciliationService = reconciliationService;
        }

        /// <summary>
        /// Résout les références DWINGS pour une ligne Ambre
        /// </summary>
        public async Task<AmbreImport.DwingsTokens> ResolveReferencesAsync(
            DataAmbre dataAmbre,
            bool isPivot,
            List<DwingsInvoiceDto> dwInvoices)
        {
            var references = new AmbreImport.DwingsTokens();

            try
            {
                // Extract tokens from various fields
                var tokens = ExtractTokens(dataAmbre);
                
                // Build primary BGI candidate depending on side
                string bgiCandidate;
                if (!isPivot)
                {
                    // Receivable: prefer explicit, then Rec_Num -> RecOrigin_Num -> RawLabel
                    string ExtractBgiReceivableOrdered()
                    {
                        return DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                               ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                               ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel);
                    }
                    bgiCandidate = dataAmbre.Receivable_InvoiceFromAmbre?.Trim() ?? ExtractBgiReceivableOrdered();
                }
                else
                {
                    // Pivot: use extracted tokens (RawLabel -> Rec_Num -> RecOrigin_Num)
                    bgiCandidate = tokens.Bgi;
                }

                // Try to resolve an actual DW invoice (return full object so we can backfill)
                DwingsInvoiceDto hit = null;
                if (!string.IsNullOrWhiteSpace(bgiCandidate))
                {
                    hit = DwingsLinkingHelper.ResolveInvoiceByBgiWithAmount(dwInvoices, bgiCandidate, dataAmbre.SignedAmount);
                }

                // BGPMT path if not found yet
                if (hit == null && !string.IsNullOrWhiteSpace(tokens.Bgpmt))
                {
                    hit = DwingsLinkingHelper.ResolveInvoiceByBgpmt(dwInvoices, tokens.Bgpmt, dataAmbre.SignedAmount);
                }

                // OfficialRef path
                if (hit == null)
                {
                    var byOfficial = ResolveByOfficialRef(dataAmbre, dwInvoices);
                    if (!string.IsNullOrWhiteSpace(byOfficial))
                    {
                        hit = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, byOfficial, StringComparison.OrdinalIgnoreCase));
                    }
                }

                // Guarantee path
                if (hit == null && !string.IsNullOrWhiteSpace(tokens.GuaranteeId))
                {
                    var hits = DwingsLinkingHelper.ResolveInvoicesByGuarantee(
                        dwInvoices, tokens.GuaranteeId,
                        dataAmbre.Operation_Date ?? dataAmbre.Value_Date,
                        dataAmbre.SignedAmount, take: 1);
                    hit = hits?.FirstOrDefault();
                }

                // Suggestions
                if (hit == null)
                {
                    var suggested = GetSuggestedInvoice(dataAmbre, tokens, dwInvoices);
                    if (!string.IsNullOrWhiteSpace(suggested))
                        hit = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, suggested, StringComparison.OrdinalIgnoreCase));
                }

                // Fill references from tokens and resolved invoice
                references.InvoiceId = hit?.INVOICE_ID; // only if resolvable in DWINGS
                references.CommissionId = !string.IsNullOrWhiteSpace(tokens.Bgpmt) ? tokens.Bgpmt : hit?.BGPMT;
                references.GuaranteeId = !string.IsNullOrWhiteSpace(tokens.GuaranteeId) ? tokens.GuaranteeId : (hit?.BUSINESS_CASE_REFERENCE ?? hit?.BUSINESS_CASE_ID);
            }
            catch (Exception ex)
            {
                LogManager.Warning($"DWINGS resolution failed for {dataAmbre?.ID}: {ex.Message}");
            }

            return references;
        }

        /// <summary>
        /// Obtient la méthode de paiement pour une référence DWINGS
        /// </summary>
        public async Task<string> GetPaymentMethodAsync(DataAmbre dataAmbre, string countryId)
        {
            var dwRef = ExtractDwingsReference(dataAmbre);
            if (dwRef == null) return null;

            return await GetPaymentMethodFromDwingsAsync(countryId, dwRef.Type, dwRef.Code);
        }

        private DwingsTokens ExtractTokens(DataAmbre dataAmbre)
        {
            return new DwingsTokens
            {
                Bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.Reconciliation_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.ReconciliationOrigin_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.RawLabel),
                        
                GuaranteeId = DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.Reconciliation_Num)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.RawLabel),
                              
                Bgi = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel)
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
            };
        }

        private string ResolveReceivableInvoice(
            DataAmbre dataAmbre,
            DwingsTokens tokens,
            List<DwingsInvoiceDto> dwInvoices)
        {
            // Prefer explicit field, then extracted BGI with priority:
            // Reconciliation_Num -> ReconciliationOrigin_Num -> RawLabel (last)
            string ExtractBgiReceivableOrdered()
            {
                return DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                       ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                       ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel);
            }

            var bgi = dataAmbre.Receivable_InvoiceFromAmbre?.Trim() ?? ExtractBgiReceivableOrdered();
            
            if (string.IsNullOrWhiteSpace(bgi))
                return ResolveByOfficialRef(dataAmbre, dwInvoices); // try OfficialRef exact match if no BGI

            var hit = DwingsLinkingHelper.ResolveInvoiceByBgiWithAmount(
                dwInvoices, bgi, dataAmbre.SignedAmount);
            if (hit != null) return hit.INVOICE_ID;

            // Fallback: OfficialRef exact match
            return ResolveByOfficialRef(dataAmbre, dwInvoices);
        }

        private string ResolvePivotInvoice(
            DataAmbre dataAmbre,
            DwingsTokens tokens,
            List<DwingsInvoiceDto> dwInvoices)
        {
            // Try BGI first
            if (!string.IsNullOrWhiteSpace(tokens.Bgi))
            {
                var hit = DwingsLinkingHelper.ResolveInvoiceByBgiWithAmount(
                    dwInvoices, tokens.Bgi, dataAmbre.SignedAmount);
                if (hit != null)
                    return hit.INVOICE_ID;
            }

            // Try BGPMT
            if (!string.IsNullOrWhiteSpace(tokens.Bgpmt))
            {
                var hit = DwingsLinkingHelper.ResolveInvoiceByBgpmt(
                    dwInvoices, tokens.Bgpmt, dataAmbre.SignedAmount);
                if (hit != null)
                    return hit.INVOICE_ID;
            }

            // Try OfficialRef (exact match on BUSINESS_CASE_REFERENCE/ID) from Reconciliation fields/label
            var byOfficial = ResolveByOfficialRef(dataAmbre, dwInvoices);
            if (!string.IsNullOrWhiteSpace(byOfficial))
                return byOfficial;

            // Try guarantee
            if (!string.IsNullOrWhiteSpace(tokens.GuaranteeId))
            {
                var hits = DwingsLinkingHelper.ResolveInvoicesByGuarantee(
                    dwInvoices, tokens.GuaranteeId, 
                    dataAmbre.Operation_Date ?? dataAmbre.Value_Date, 
                    dataAmbre.SignedAmount, take: 1);
                    
                return hits?.FirstOrDefault()?.INVOICE_ID;
            }

            return null;
        }

        // OfficialRef exact match (SenderReference): extract alphanumeric tokens from Reconciliation_Num
        // and match equality (case-insensitive) against invoice SENDER_REFERENCE.
        private string ResolveByOfficialRef(DataAmbre dataAmbre, List<DwingsInvoiceDto> dwInvoices)
        {
            if (dwInvoices == null || dwInvoices.Count == 0 || dataAmbre == null) return null;

            // Build token set from Reconciliation_Num and fallback ReconciliationOrigin_Num
            var tokenSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            void AddTokens(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return;
                foreach (var t in Regex.Split(s, @"[^A-Za-z0-9]+").Where(p => !string.IsNullOrWhiteSpace(p)).Select(p => p.Trim()))
                    tokenSet.Add(t);
            }
            AddTokens(dataAmbre.Reconciliation_Num);
            AddTokens(dataAmbre.ReconciliationOrigin_Num);
            if (tokenSet.Count == 0) return null;

            var hits = dwInvoices.Where(i =>
                !string.IsNullOrWhiteSpace(i?.SENDER_REFERENCE) && tokenSet.Contains(i.SENDER_REFERENCE)
            ).ToList();

            if (hits.Count == 0) return null;

            // Rank by date then amount proximity
            var ambreDate = dataAmbre.Operation_Date ?? dataAmbre.Value_Date;
            var ambreAmt = dataAmbre.SignedAmount;

            double DateScore(DwingsInvoiceDto i)
            {
                if (!ambreDate.HasValue) return double.MaxValue;
                var best = i?.START_DATE ?? i?.END_DATE;
                if (!best.HasValue) return double.MaxValue;
                return Math.Abs((best.Value.Date - ambreDate.Value.Date).TotalDays);
            }

            decimal AmountScore(DwingsInvoiceDto i)
            {
                if (!i.BILLING_AMOUNT.HasValue) return decimal.MaxValue;
                return Math.Abs(ambreAmt - i.BILLING_AMOUNT.Value);
            }

            var chosen = hits.OrderBy(DateScore).ThenBy(AmountScore).FirstOrDefault();
            return chosen?.INVOICE_ID;
        }

        private string GetGuaranteeIdFromInvoice(string invoiceId, List<DwingsInvoiceDto> dwInvoices)
        {
            var invoice = dwInvoices?.FirstOrDefault(i => 
                string.Equals(i?.INVOICE_ID, invoiceId, StringComparison.OrdinalIgnoreCase));
                
            return invoice?.BUSINESS_CASE_REFERENCE ?? invoice?.BUSINESS_CASE_ID;
        }

        private string GetSuggestedInvoice(
            DataAmbre dataAmbre,
            DwingsTokens tokens,
            List<DwingsInvoiceDto> dwInvoices)
        {
            // For suggestions, prefer explicit Receivable_InvoiceFromAmbre, else BGI with same receivable order
            string bgiOrdered = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                                ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                                ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel);

            var suggestions = DwingsLinkingHelper.SuggestInvoicesForAmbre(
                dwInvoices,
                rawLabel: dataAmbre.RawLabel,
                reconciliationNum: dataAmbre.Reconciliation_Num,
                reconciliationOriginNum: dataAmbre.ReconciliationOrigin_Num,
                explicitBgi: dataAmbre.Receivable_InvoiceFromAmbre?.Trim() ?? bgiOrdered,
                guaranteeId: tokens.GuaranteeId,
                ambreDate: dataAmbre.Operation_Date ?? dataAmbre.Value_Date,
                ambreAmount: dataAmbre.SignedAmount,
                take: 1);
                
            return suggestions?.FirstOrDefault()?.INVOICE_ID;
        }

        private DwingsRef ExtractDwingsReference(DataAmbre ambre)
        {
            if (ambre == null) return null;
            
            // Try BGPMT first
            string bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(ambre.RawLabel) 
                          ?? DwingsLinkingHelper.ExtractBgpmtToken(ambre.Reconciliation_Num);
            if (!string.IsNullOrWhiteSpace(bgpmt))
                return new DwingsRef { Type = "BGPMT", Code = bgpmt };

            // Try BGI
            string bgi = DwingsLinkingHelper.ExtractBgiToken(ambre.RawLabel) 
                        ?? DwingsLinkingHelper.ExtractBgiToken(ambre.Reconciliation_Num);
            if (!string.IsNullOrWhiteSpace(bgi))
                return new DwingsRef { Type = "BGI", Code = bgi };

            return null;
        }

        private async Task<string> GetPaymentMethodFromDwingsAsync(
            string countryId,
            string refType,
            string refCode)
        {
            if (string.IsNullOrWhiteSpace(refType) || string.IsNullOrWhiteSpace(refCode))
                return null;

            var invoices = await _reconciliationService?.GetDwingsInvoicesAsync();
            if (invoices == null || invoices.Count == 0) 
                return null;

            var code = refCode?.Trim();
            if (string.IsNullOrEmpty(code)) 
                return null;

            DwingsInvoiceDto hit = null;
            
            if (string.Equals(refType, "BGPMT", StringComparison.OrdinalIgnoreCase))
            {
                hit = invoices.FirstOrDefault(i => 
                    !string.IsNullOrWhiteSpace(i?.BGPMT) &&
                    string.Equals(i.BGPMT, code, StringComparison.OrdinalIgnoreCase));
            }
            else // BGI
            {
                hit = FindInvoiceByBgi(invoices.ToList(), code);
            }

            return hit?.PAYMENT_METHOD;
        }

        private DwingsInvoiceDto FindInvoiceByBgi(List<DwingsInvoiceDto> invoices, string code)
        {
            return invoices.FirstOrDefault(i =>
                MatchesField(i.INVOICE_ID, code) ||
                MatchesField(i.SENDER_REFERENCE, code) ||
                MatchesField(i.RECEIVER_REFERENCE, code) ||
                MatchesField(i.BUSINESS_CASE_REFERENCE, code));
        }

        private bool MatchesField(string field, string value)
        {
            return field != null && 
                   string.Equals(field, value, StringComparison.OrdinalIgnoreCase);
        }

        private class DwingsRef
        {
            public string Type { get; set; } // "BGPMT" or "BGI"
            public string Code { get; set; }
        }

        private class DwingsTokens
        {
            public string Bgpmt { get; set; }
            public string GuaranteeId { get; set; }
            public string Bgi { get; set; }
        }
    }

    /// <summary>
    /// Références DWINGS résolues
    /// </summary>
    public class DwingsTokens
    {
        public string InvoiceId { get; set; }
        public string CommissionId { get; set; }
        public string GuaranteeId { get; set; }
    }
}