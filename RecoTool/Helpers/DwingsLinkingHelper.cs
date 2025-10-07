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

        // Guarantee ID: G####AA######### (4 digits, 2 letters, 9 digits)
        public static string ExtractGuaranteeId(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(s, @"(?:^|[^A-Za-z0-9])(G\d{4}[A-Za-z]{2}\d{9})(?![A-Za-z0-9])");
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
        /// Resolve a DWINGS invoice by exact BGI match, refine by amount when multiple hits.
        /// CRITICAL: Only considers invoices with T_INVOICE_STATUS = 'GENERATED'.
        /// If only one GENERATED invoice exists with this BGI, return it.
        /// Otherwise, return the one with matching REQUESTED_AMOUNT (or closest BILLING_AMOUNT).
        /// </summary>
        public static DwingsInvoiceDto ResolveInvoiceByBgiWithAmount(
            IEnumerable<DwingsInvoiceDto> invoices,
            string bgi,
            decimal? ambreSignedAmount)
        {
            if (invoices == null) return null;
            var key = Norm(bgi);
            if (string.IsNullOrWhiteSpace(key)) return null;

            var candidates = invoices
                .Where(i => Norm(i?.INVOICE_ID) == key)
                .Where(i => string.Equals(Norm(i?.T_INVOICE_STATUS), "GENERATED", StringComparison.OrdinalIgnoreCase))
                .ToList();
            if (candidates.Count == 0) return null;
            
            // If only one invoice exists, return it
            if (candidates.Count == 1) return candidates[0];

            // Multiple invoices: prefer exact REQUESTED_AMOUNT match
            var exactReq = candidates.FirstOrDefault(i => AmountMatches(ambreSignedAmount, i?.REQUESTED_AMOUNT));
            if (exactReq != null) return exactReq;
            
            // Fallback: exact BILLING_AMOUNT match
            var exactBill = candidates.FirstOrDefault(i => AmountMatches(ambreSignedAmount, i?.BILLING_AMOUNT));
            if (exactBill != null) return exactBill;

            // Fallback: closest by REQUESTED_AMOUNT, then BILLING_AMOUNT
            return candidates
                .OrderBy(i => i?.REQUESTED_AMOUNT.HasValue == true ? Math.Abs((ambreSignedAmount ?? 0) - i.REQUESTED_AMOUNT.Value) : decimal.MaxValue)
                .ThenBy(i => i?.BILLING_AMOUNT.HasValue == true ? Math.Abs((ambreSignedAmount ?? 0) - i.BILLING_AMOUNT.Value) : decimal.MaxValue)
                .FirstOrDefault();
        }

        /// <summary>
        /// Return related invoices for a given Guarantee ID, ranked by date proximity then amount proximity.
        /// Matches against BUSINESS_CASE_REFERENCE and BUSINESS_CASE_ID, with exact match preferred over contains.
        /// IMPORTANT: Filters by T_INVOICE_STATUS = 'GENERATED' and matches REQUESTED_AMOUNT.
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
            
            // CRITICAL: Filter by INVOICE_STATUS = 'GENERATED' when resolving by Guarantee
            candidates = candidates.Where(i => 
                string.Equals(Norm(i?.T_INVOICE_STATUS), "GENERATED", StringComparison.OrdinalIgnoreCase)
            ).ToList();
            
            if (candidates.Count == 0) return new List<DwingsInvoiceDto>();

            Func<DwingsInvoiceDto, double> dateScore = (i) =>
            {
                if (!ambreDate.HasValue) return double.MaxValue;
                var best = i?.START_DATE ?? i?.END_DATE;
                if (!best.HasValue) return double.MaxValue;
                return Math.Abs((best.Value.Date - ambreDate.Value.Date).TotalDays);
            };

            Func<DwingsInvoiceDto, decimal> amountScore = (i) =>
            {
                if (!ambreAmount.HasValue) return decimal.MaxValue;
                // Prioritize REQUESTED_AMOUNT match, fallback to BILLING_AMOUNT
                var reqDelta = i?.REQUESTED_AMOUNT.HasValue == true ? Math.Abs(ambreAmount.Value - i.REQUESTED_AMOUNT.Value) : decimal.MaxValue;
                var billDelta = i?.BILLING_AMOUNT.HasValue == true ? Math.Abs(ambreAmount.Value - i.BILLING_AMOUNT.Value) : decimal.MaxValue;
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
                var hit = ResolveInvoiceByBgiWithAmount(invoices, bgi, ambreAmount);
                if (hit != null) list.Add(hit);
            }
            if (list.Count >= take) return list;

            // 2) BGPMT
            var bgpmt = ExtractBgpmtToken(reconciliationNum)
                        ?? ExtractBgpmtToken(reconciliationOriginNum)
                        ?? ExtractBgpmtToken(rawLabel);
            if (!string.IsNullOrWhiteSpace(bgpmt))
            {
                var hit = ResolveInvoiceByBgpmt(invoices, bgpmt, ambreAmount);
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
        /// Resolve a DWINGS invoice by BGPMT reference.
        /// CRITICAL: Only considers invoices with T_INVOICE_STATUS = 'GENERATED'.
        /// If only one GENERATED invoice exists with this BGPMT, return it.
        /// Otherwise, return the one with matching REQUESTED_AMOUNT (or closest amount).
        /// </summary>
        public static DwingsInvoiceDto ResolveInvoiceByBgpmt(
            IEnumerable<DwingsInvoiceDto> invoices,
            string bgpmt,
            decimal? ambreSignedAmount)
        {
            if (invoices == null) return null;
            var key = Norm(bgpmt);
            if (string.IsNullOrWhiteSpace(key)) return null;

            var candidates = invoices
                .Where(i => Norm(i?.BGPMT) == key)
                .Where(i => string.Equals(Norm(i?.T_INVOICE_STATUS), "GENERATED", StringComparison.OrdinalIgnoreCase))
                .ToList();
            if (candidates.Count == 0) return null;
            
            // If only one invoice exists, return it
            if (candidates.Count == 1) return candidates[0];
            
            // Multiple invoices: prefer exact REQUESTED_AMOUNT match
            var exactReq = candidates.FirstOrDefault(i => AmountMatches(ambreSignedAmount, i?.REQUESTED_AMOUNT));
            if (exactReq != null) return exactReq;
            
            // Fallback: exact BILLING_AMOUNT match
            var exactBill = candidates.FirstOrDefault(i => AmountMatches(ambreSignedAmount, i?.BILLING_AMOUNT));
            if (exactBill != null) return exactBill;
            
            // Fallback: closest by REQUESTED_AMOUNT, then BILLING_AMOUNT
            return candidates
                .OrderBy(i => i?.REQUESTED_AMOUNT.HasValue == true ? Math.Abs((ambreSignedAmount ?? 0) - i.REQUESTED_AMOUNT.Value) : decimal.MaxValue)
                .ThenBy(i => i?.BILLING_AMOUNT.HasValue == true ? Math.Abs((ambreSignedAmount ?? 0) - i.BILLING_AMOUNT.Value) : decimal.MaxValue)
                .FirstOrDefault();
        }
    }
}
