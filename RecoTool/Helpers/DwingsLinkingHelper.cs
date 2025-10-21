using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Helpers
{
    public static class DwingsLinkingHelper
    {
        // BGPMT token: e.g., BGPMTxxxxxxxx (8-20 alnum after BGPMT)
        public static string ExtractBgpmtToken(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            // Allow adjacent punctuation; avoid requiring classic word boundaries
            var m = Regex.Match(s, @"(?:^|[^A-Za-z0-9])(BGPMT[A-Za-z0-9]{8,20})(?![A-Za-z0-9])");
            return m.Success ? m.Groups[1].Value : null;
        }

        // BGI invoice id supported formats:
        //  - BGI + YYYYMM (6 digits) + 7 chars (digits or A-F)
        //  - BGI + YYMM (4 digits) + CountryCode (2 letters) + 7 chars (digits or A-F)
        public static string ExtractBgiToken(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(
                s,
                @"(?:^|[^A-Za-z0-9])(BGI(?:\d{6}[A-F0-9]{7}|\d{4}[A-Za-z]{2}[A-F0-9]{7}))(?![A-Za-z0-9])",
                RegexOptions.IgnoreCase);
            return m.Success ? m.Groups[1].Value.ToUpperInvariant() : null;
        }

        // Guarantee ID: G####AA######### or N####AA######### (G or N, 4 digits, 2 letters, 9 digits)
        public static string ExtractGuaranteeId(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(s, @"(?:^|[^A-Za-z0-9])([GN]\d{4}[A-Za-z]{2}\d{9})(?![A-Za-z0-9])");
            return m.Success ? m.Groups[1].Value : null;
        }

        // -------- Resolution helpers --------

        private static string Norm(string s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim().ToUpperInvariant();

        public static bool AmountMatches(decimal? ambreAmount, decimal? dwAmount, decimal tolerance = 0.01m)
        {
            if (!ambreAmount.HasValue || !dwAmount.HasValue) return false;
            return Math.Abs(ambreAmount.Value - dwAmount.Value) <= tolerance;
        }

        /// <summary>
        /// Resolve a DWINGS invoice by exact BGI match.
        /// BGI = INVOICE_ID (unique identifier), no matching needed.
        /// Returns the invoice found with this BGI, or NULL if ambiguous (multiple matches).
        /// </summary>
        public static DwingsInvoiceDto ResolveInvoiceByBgi(
            IEnumerable<DwingsInvoiceDto> invoices,
            string bgi)
        {
            if (invoices == null) return null;
            var key = Norm(bgi);
            if (string.IsNullOrWhiteSpace(key)) return null;

            var matches = invoices.Where(i => Norm(i?.INVOICE_ID) == key).ToList();
            
            // If ambiguous (multiple matches), return null (don't pick any)
            if (matches.Count > 1) return null;
            
            return matches.FirstOrDefault();
        }

        /// <summary>
        /// Return related invoices for a given Guarantee ID, ranked by date proximity then amount proximity.
        /// Matches against BUSINESS_CASE_REFERENCE and BUSINESS_CASE_ID, with exact match preferred over contains.
        /// UPDATED: Removed T_INVOICE_STATUS filter. Now matches by REQUESTED_EXECUTION_DATE and REQUESTED_AMOUNT (absolute value).
        /// </summary>
        public static List<DwingsInvoiceDto> ResolveInvoicesByGuarantee(
            IEnumerable<DwingsInvoiceDto> invoices,
            string guaranteeId,
            DateTime? ambreDate,
            decimal? ambreAmount,
            int take = 50)
        {
            var list = (invoices ?? Enumerable.Empty<DwingsInvoiceDto>()).ToList();
            var key = Norm(guaranteeId);
            if (string.IsNullOrWhiteSpace(key) || list.Count == 0) return new List<DwingsInvoiceDto>();

            bool MatchEq(DwingsInvoiceDto i) =>
                Norm(i?.BUSINESS_CASE_REFERENCE) == key || Norm(i?.BUSINESS_CASE_ID) == key;
            bool MatchContains(DwingsInvoiceDto i) =>
                (!string.IsNullOrEmpty(i?.BUSINESS_CASE_REFERENCE) && Norm(i.BUSINESS_CASE_REFERENCE)?.Contains(key) == true)
                || (!string.IsNullOrEmpty(i?.BUSINESS_CASE_ID) && Norm(i.BUSINESS_CASE_ID)?.Contains(key) == true);

            var exact = list.Where(MatchEq).ToList();
            var partial = exact.Count > 0 ? new List<DwingsInvoiceDto>() : list.Where(MatchContains).ToList();
            var candidates = exact.Count > 0 ? exact : partial;
            
            if (candidates.Count == 0) return new List<DwingsInvoiceDto>();
            
            // CRITICAL: Filter by amount match (tolerance 0.01) to avoid linking to wrong invoice
            if (ambreAmount.HasValue)
            {
                candidates = candidates.Where(i =>
                {
                    var absAmbre = Math.Abs(ambreAmount.Value);
                    // Check REQUESTED_AMOUNT or BILLING_AMOUNT
                    bool reqMatch = i?.REQUESTED_AMOUNT.HasValue == true && AmountMatches(absAmbre, Math.Abs(i.REQUESTED_AMOUNT.Value), tolerance: 0.01m);
                    bool billMatch = i?.BILLING_AMOUNT.HasValue == true && AmountMatches(absAmbre, Math.Abs(i.BILLING_AMOUNT.Value), tolerance: 0.01m);
                    return reqMatch || billMatch;
                }).ToList();
                
                if (candidates.Count == 0) return new List<DwingsInvoiceDto>();
            }

            Func<DwingsInvoiceDto, double> dateScore = (i) =>
            {
                if (!ambreDate.HasValue) return double.MaxValue;
                // Prioritize REQUESTED_EXECUTION_DATE, fallback to START_DATE/END_DATE
                var best = i?.REQUESTED_EXECUTION_DATE ?? i?.START_DATE ?? i?.END_DATE;
                if (!best.HasValue) return double.MaxValue;
                return Math.Abs((best.Value.Date - ambreDate.Value.Date).TotalDays);
            };

            Func<DwingsInvoiceDto, decimal> amountScore = (i) =>
            {
                if (!ambreAmount.HasValue) return decimal.MaxValue;
                var absAmbre = Math.Abs(ambreAmount.Value);
                // Prioritize REQUESTED_AMOUNT match (absolute value), fallback to BILLING_AMOUNT
                var reqDelta = i?.REQUESTED_AMOUNT.HasValue == true ? Math.Abs(absAmbre - i.REQUESTED_AMOUNT.Value) : decimal.MaxValue;
                var billDelta = i?.BILLING_AMOUNT.HasValue == true ? Math.Abs(absAmbre - i.BILLING_AMOUNT.Value) : decimal.MaxValue;
                return Math.Min(reqDelta, billDelta);
            };

            return candidates
                .OrderBy(i => dateScore(i))
                .ThenBy(i => amountScore(i))
                .Take(Math.Max(1, take))
                .ToList();
        }

        /// <summary>
        /// Suggest a best invoice for a given AMBRE item using BGI → BGPMT → Guarantee strategies.
        /// Returns a ranked list (best first).
        /// </summary>
        public static List<DwingsInvoiceDto> SuggestInvoicesForAmbre(
            IEnumerable<DwingsInvoiceDto> invoices,
            string rawLabel,
            string reconciliationNum,
            string reconciliationOriginNum,
            string explicitBgi,
            string guaranteeId,
            DateTime? ambreDate,
            decimal? ambreAmount,
            int take = 20)
        {
            var list = new List<DwingsInvoiceDto>();
            // 1) BGI direct
            var bgi = explicitBgi?.Trim()
                      ?? ExtractBgiToken(reconciliationNum)
                      ?? ExtractBgiToken(reconciliationOriginNum)
                      ?? ExtractBgiToken(rawLabel);
            if (!string.IsNullOrWhiteSpace(bgi))
            {
                var hit = ResolveInvoiceByBgi(invoices, bgi);
                if (hit != null) list.Add(hit);
            }
            if (list.Count >= take) return list;

            // 2) BGPMT
            var bgpmt = ExtractBgpmtToken(reconciliationNum)
                        ?? ExtractBgpmtToken(reconciliationOriginNum)
                        ?? ExtractBgpmtToken(rawLabel);
            if (!string.IsNullOrWhiteSpace(bgpmt))
            {
                var hit = ResolveInvoiceByBgpmt(invoices, bgpmt);
                if (hit != null && !list.Any(x => Norm(x.INVOICE_ID) == Norm(hit.INVOICE_ID))) list.Add(hit);
            }
            if (list.Count >= take) return list;

            // 3) Guarantee-based
            var gid = guaranteeId?.Trim() ?? ExtractGuaranteeId(reconciliationNum) ?? ExtractGuaranteeId(rawLabel);
            if (!string.IsNullOrWhiteSpace(gid))
            {
                var more = ResolveInvoicesByGuarantee(invoices, gid, ambreDate, ambreAmount, take: take - list.Count);
                foreach (var m in more)
                {
                    if (!list.Any(x => Norm(x.INVOICE_ID) == Norm(m.INVOICE_ID))) list.Add(m);
                }
            }

            return list.Take(take).ToList();
        }

        /// <summary>
        /// Resolve a DWINGS invoice by exact BGPMT match.
        /// BGPMT = commission identifier, no matching needed.
        /// Returns the invoice found with this BGPMT, or NULL if ambiguous (multiple matches).
        /// </summary>
        public static DwingsInvoiceDto ResolveInvoiceByBgpmt(
            IEnumerable<DwingsInvoiceDto> invoices,
            string bgpmt)
        {
            if (invoices == null) return null;
            var key = Norm(bgpmt);
            if (string.IsNullOrWhiteSpace(key)) return null;

            var matches = invoices.Where(i => Norm(i?.BGPMT) == key).ToList();
            
            // If ambiguous (multiple matches), return null (don't pick any)
            if (matches.Count > 1) return null;
            
            return matches.FirstOrDefault();
        }
    }
}
