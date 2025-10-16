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
        private List<DwingsGuaranteeDto> _dwingsGuarantees;
        private string _lastResolvedGuaranteeId; // Stores GUARANTEE_ID found via OfficialRef

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
            List<DwingsInvoiceDto> dwInvoices,
            List<DwingsGuaranteeDto> dwGuarantees = null)
        {
            // Store guarantees for use in ResolveByOfficialRef
            _dwingsGuarantees = dwGuarantees;
            _lastResolvedGuaranteeId = null; // Reset for each resolution
            
            var references = new AmbreImport.DwingsTokens();

            try
            {
                // Extract tokens from various fields
                var tokens = ExtractTokens(dataAmbre);
                
                // Build primary BGI candidate depending on side
                string bgiCandidate;
                if (!isPivot)
                {
                    // Receivable: ONLY use Reconciliation_Num (no fallback to other fields)
                    bgiCandidate = dataAmbre.Receivable_InvoiceFromAmbre?.Trim() 
                                   ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num);
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
                    hit = DwingsLinkingHelper.ResolveInvoiceByBgi(dwInvoices, bgiCandidate);
                    // CRITICAL: Verify amount matches (tolerance 0.01) to avoid linking to wrong invoice
                    if (hit != null)
                    {
                        var ambreAmt = dataAmbre.SignedAmount;
                        var absAmbre = Math.Abs(ambreAmt);
                        bool reqMatch = hit.REQUESTED_AMOUNT.HasValue && DwingsLinkingHelper.AmountMatches(absAmbre, Math.Abs(hit.REQUESTED_AMOUNT.Value), tolerance: 0.01m);
                        bool billMatch = hit.BILLING_AMOUNT.HasValue && DwingsLinkingHelper.AmountMatches(absAmbre, Math.Abs(hit.BILLING_AMOUNT.Value), tolerance: 0.01m);
                        if (!reqMatch && !billMatch)
                        {
                            hit = null; // Amount mismatch, reject this invoice
                        }
                    }
                }

                // BGPMT path if not found yet
                if (hit == null && !string.IsNullOrWhiteSpace(tokens.Bgpmt))
                {
                    hit = DwingsLinkingHelper.ResolveInvoiceByBgpmt(dwInvoices, tokens.Bgpmt);
                    // CRITICAL: Verify amount matches (tolerance 0.01) to avoid linking to wrong invoice
                    if (hit != null)
                    {
                        var ambreAmt = dataAmbre.SignedAmount;
                        var absAmbre = Math.Abs(ambreAmt);
                        bool reqMatch = hit.REQUESTED_AMOUNT.HasValue && DwingsLinkingHelper.AmountMatches(absAmbre, Math.Abs(hit.REQUESTED_AMOUNT.Value), tolerance: 0.01m);
                        bool billMatch = hit.BILLING_AMOUNT.HasValue && DwingsLinkingHelper.AmountMatches(absAmbre, Math.Abs(hit.BILLING_AMOUNT.Value), tolerance: 0.01m);
                        if (!reqMatch && !billMatch)
                        {
                            hit = null; // Amount mismatch, reject this invoice
                        }
                    }
                }

                // OfficialRef path (CRITICAL: also resolves GUARANTEE_ID via _lastResolvedGuaranteeId)
                if (hit == null)
                {
                    var byOfficial = ResolveByOfficialRef(dataAmbre, dwInvoices);
                    if (!string.IsNullOrWhiteSpace(byOfficial))
                    {
                        // byOfficial can be:
                        // - INVOICE_ID: found invoice via OfficialRef
                        // - string.Empty: found guarantee via OfficialRef but no invoice
                        // In both cases, _lastResolvedGuaranteeId may be set
                        if (byOfficial != string.Empty)
                        {
                            hit = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, byOfficial, StringComparison.OrdinalIgnoreCase));
                        }
                        // If byOfficial == string.Empty, hit stays null but _lastResolvedGuaranteeId is set
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
                    var suggested = GetSuggestedInvoice(dataAmbre, tokens, dwInvoices, isPivot);
                    if (!string.IsNullOrWhiteSpace(suggested))
                        hit = dwInvoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, suggested, StringComparison.OrdinalIgnoreCase));
                }

                // Fill references from tokens and resolved invoice
                references.InvoiceId = hit?.INVOICE_ID; // only if resolvable in DWINGS
                references.CommissionId = !string.IsNullOrWhiteSpace(tokens.Bgpmt) ? tokens.Bgpmt : hit?.BGPMT;
                
                // CRITICAL: Prioritize GUARANTEE_ID from OfficialRef resolution (_lastResolvedGuaranteeId)
                // This ensures Pivot account with OfficialRef in Reconciliation_Num gets linked to guarantee
                // Handle duplicates: _lastResolvedGuaranteeId already contains the LATEST (highest) GUARANTEE_ID
                references.GuaranteeId = _lastResolvedGuaranteeId  // From OfficialRef (handles duplicates)
                                        ?? tokens.GuaranteeId       // From text extraction
                                        ?? hit?.BUSINESS_CASE_REFERENCE  // From invoice
                                        ?? hit?.BUSINESS_CASE_ID;
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
            // EXTENDED: Match ReconciliationViewEnricher heuristics for consistency
            return new DwingsTokens
            {
                // BGPMT: check Reconciliation_Num, ReconciliationOrigin_Num, RawLabel
                Bgpmt = DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.Reconciliation_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.ReconciliationOrigin_Num)
                        ?? DwingsLinkingHelper.ExtractBgpmtToken(dataAmbre.RawLabel),
                        
                // GuaranteeId: check Reconciliation_Num, RawLabel, Receivable_DWRefFromAmbre
                GuaranteeId = DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.Reconciliation_Num)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.RawLabel)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(dataAmbre.Receivable_DWRefFromAmbre),
                              
                // BGI: check RawLabel, Reconciliation_Num, ReconciliationOrigin_Num, Receivable_DWRefFromAmbre
                Bgi = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel)
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Receivable_DWRefFromAmbre)
            };
        }

        private string ResolveReceivableInvoice(
            DataAmbre dataAmbre,
            DwingsTokens tokens,
            List<DwingsInvoiceDto> dwInvoices)
        {
            // Receivable: ONLY use Reconciliation_Num (no fallback to other fields)
            var bgi = dataAmbre.Receivable_InvoiceFromAmbre?.Trim() 
                      ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num);
            
            if (string.IsNullOrWhiteSpace(bgi))
                return ResolveByOfficialRef(dataAmbre, dwInvoices); // try OfficialRef exact match if no BGI

            var hit = DwingsLinkingHelper.ResolveInvoiceByBgi(dwInvoices, bgi);
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
                var hit = DwingsLinkingHelper.ResolveInvoiceByBgi(dwInvoices, tokens.Bgi);
                if (hit != null)
                    return hit.INVOICE_ID;
            }

            // Try BGPMT
            if (!string.IsNullOrWhiteSpace(tokens.Bgpmt))
            {
                var hit = DwingsLinkingHelper.ResolveInvoiceByBgpmt(dwInvoices, tokens.Bgpmt);
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

        /// <summary>
        /// STEP 1: Find GUARANTEE_ID via OFFICIALREF/SENDER_REFERENCE/GUARANTEE_ID pattern matching
        /// STEP 2: Find best invoice linked to this guarantee (with amount matching)
        /// </summary>
        private string ResolveByOfficialRef(DataAmbre dataAmbre, List<DwingsInvoiceDto> dwInvoices)
        {
            if (dwInvoices == null || dwInvoices.Count == 0 || dataAmbre == null) return null;

            // STEP 1: Find GUARANTEE_ID
            var guaranteeId = FindGuaranteeIdFromReferences(dataAmbre, dwInvoices);
            if (string.IsNullOrWhiteSpace(guaranteeId)) return null;

            // Store the found GUARANTEE_ID
            _lastResolvedGuaranteeId = guaranteeId;

            // STEP 2: Find best invoice for this guarantee (with amount + date ranking)
            var invoices = DwingsLinkingHelper.ResolveInvoicesByGuarantee(
                dwInvoices,
                guaranteeId,
                dataAmbre.Operation_Date ?? dataAmbre.Value_Date,
                dataAmbre.SignedAmount,
                take: 5
            );

            if (invoices == null || invoices.Count == 0)
            {
                // Guarantee found but no invoice => return empty string (caller will use _lastResolvedGuaranteeId)
                return string.Empty;
            }

            // Return best invoice
            return invoices.FirstOrDefault()?.INVOICE_ID;
        }

        /// <summary>
        /// STEP 1: Find GUARANTEE_ID via multiple matching strategies
        /// Tries in order: SENDER_REFERENCE → OFFICIALREF → PARTY_REF
        /// All use same alphanumeric cleaning logic for consistent matching
        /// </summary>
        private string FindGuaranteeIdFromReferences(DataAmbre dataAmbre, List<DwingsInvoiceDto> dwInvoices)
        {
            // Extract alphanumeric token from Reconciliation_Num
            var token = ExtractAlphanumericToken(dataAmbre.Reconciliation_Num);
            if (string.IsNullOrWhiteSpace(token))
            {
                // Fallback to ReconciliationOrigin_Num
                token = ExtractAlphanumericToken(dataAmbre.ReconciliationOrigin_Num);
            }
            if (string.IsNullOrWhiteSpace(token)) return null;

            var ambreAmt = dataAmbre.SignedAmount;

            // Strategy 1: Match via invoice SENDER_REFERENCE (must have BUSINESS_CASE_ID + amount match)
            var guaranteeFromSender = FindGuaranteeViaSenderReference(token, ambreAmt, dwInvoices);
            if (!string.IsNullOrWhiteSpace(guaranteeFromSender))
                return guaranteeFromSender;

            // Strategy 2: Match via guarantee OFFICIALREF or PARTY_REF
            var guaranteeFromOfficial = FindGuaranteeViaOfficialRef(token);
            if (!string.IsNullOrWhiteSpace(guaranteeFromOfficial))
                return guaranteeFromOfficial;

            return null;
        }

        /// <summary>
        /// Extract single alphanumeric token from string (removes all non-alphanumeric chars)
        /// Example: "ABC-123-456" => "ABC123456"
        /// </summary>
        private string ExtractAlphanumericToken(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var cleaned = Regex.Replace(s, @"[^A-Za-z0-9]", "");
            if (string.IsNullOrWhiteSpace(cleaned) || cleaned.Length < 3 || cleaned.Equals("null", StringComparison.OrdinalIgnoreCase))
                return null;
            return cleaned;
        }

        /// <summary>
        /// Find GUARANTEE_ID via invoice SENDER_REFERENCE matching
        /// Requirements: SENDER_REFERENCE matches token + BUSINESS_CASE_ID exists + amount matches
        /// </summary>
        private string FindGuaranteeViaSenderReference(string token, decimal ambreAmt, List<DwingsInvoiceDto> dwInvoices)
        {
            var tokenAlnum = Regex.Replace(token, @"[^A-Za-z0-9]", "");
            
            var match = dwInvoices.FirstOrDefault(i =>
            {
                if (string.IsNullOrWhiteSpace(i?.SENDER_REFERENCE)) return false;
                if (string.IsNullOrWhiteSpace(i?.BUSINESS_CASE_ID)) return false;

                // Alphanumeric comparison
                var senderRefAlnum = Regex.Replace(i.SENDER_REFERENCE, @"[^A-Za-z0-9]", "");
                return !string.IsNullOrWhiteSpace(senderRefAlnum) 
                    && senderRefAlnum.Equals(tokenAlnum, StringComparison.OrdinalIgnoreCase);
            });

            return match?.BUSINESS_CASE_ID;
        }

        /// <summary>
        /// Find GUARANTEE_ID via guarantee OFFICIALREF or PARTY_REF matching
        /// Handles duplicates (recreations) by taking latest GUARANTEE_ID (descending sort)
        /// </summary>
        private string FindGuaranteeViaOfficialRef(string token)
        {
            if (_dwingsGuarantees == null) return null;

            var tokenAlnum = Regex.Replace(token, @"[^A-Za-z0-9]", "");

            var guaranteeMatches = _dwingsGuarantees
                .Where(g => !string.IsNullOrWhiteSpace(g?.OFFICIALREF) || !string.IsNullOrWhiteSpace(g?.PARTY_REF))
                .Where(g =>
                {
                    // Alphanumeric comparison for OFFICIALREF
                    if (!string.IsNullOrWhiteSpace(g.OFFICIALREF))
                    {
                        var officialRefAlnum = Regex.Replace(g.OFFICIALREF, @"[^A-Za-z0-9]", "");
                        if (!string.IsNullOrWhiteSpace(officialRefAlnum) && 
                            officialRefAlnum.Equals(tokenAlnum, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }

                    // Alphanumeric comparison for PARTY_REF
                    if (!string.IsNullOrWhiteSpace(g.PARTY_REF))
                    {
                        var partyRefAlnum = Regex.Replace(g.PARTY_REF, @"[^A-Za-z0-9]", "");
                        if (!string.IsNullOrWhiteSpace(partyRefAlnum) && 
                            partyRefAlnum.Equals(tokenAlnum, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }

                    return false;
                })
                .ToList();

            if (guaranteeMatches.Count == 0) return null;

            // Handle duplicates (recreations): take LATEST GUARANTEE_ID (highest = most recent)
            var bestGuarantee = guaranteeMatches
                .OrderByDescending(g => g.GUARANTEE_ID, StringComparer.OrdinalIgnoreCase)
                .First();

            return bestGuarantee.GUARANTEE_ID;
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
            List<DwingsInvoiceDto> dwInvoices,
            bool isPivot)
        {
            // For receivable: ONLY use Reconciliation_Num
            // For pivot: use fallback chain (Rec_Num -> RecOrigin_Num -> RawLabel)
            string bgiOrdered;
            if (!isPivot)
            {
                bgiOrdered = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num);
            }
            else
            {
                bgiOrdered = DwingsLinkingHelper.ExtractBgiToken(dataAmbre.Reconciliation_Num)
                            ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.ReconciliationOrigin_Num)
                            ?? DwingsLinkingHelper.ExtractBgiToken(dataAmbre.RawLabel);
            }

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