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
        public static void EnrichWithDwingsInvoices(List<ReconciliationViewData> rows, IEnumerable<DwingsInvoiceDto> invoices)
        {
            if (rows == null || invoices == null) return;

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
                    row.INVOICE_ID = inv.INVOICE_ID;
                    row.I_T_INVOICE_STATUS = inv.T_INVOICE_STATUS;
                    row.I_BILLING_AMOUNT = inv.BILLING_AMOUNT?.ToString(CultureInfo.InvariantCulture);
                    row.I_REQUESTED_INVOICE_AMOUNT = inv.REQUESTED_AMOUNT?.ToString(CultureInfo.InvariantCulture) ?? row.I_REQUESTED_INVOICE_AMOUNT;
                    row.I_FINAL_AMOUNT = inv.FINAL_AMOUNT?.ToString(CultureInfo.InvariantCulture) ?? row.I_FINAL_AMOUNT;
                    row.I_BILLING_CURRENCY = inv.BILLING_CURRENCY;
                    row.I_BGPMT = inv.BGPMT;
                    row.I_BUSINESS_CASE_REFERENCE = inv.BUSINESS_CASE_REFERENCE;
                    row.I_BUSINESS_CASE_ID = inv.BUSINESS_CASE_ID;
                    row.I_START_DATE = inv.START_DATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                    row.I_END_DATE = inv.END_DATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                    row.I_DEBTOR_PARTY_NAME = inv.DEBTOR_PARTY_NAME;
                    row.I_RECEIVER_NAME = inv.RECEIVER_NAME;
                    // Newly added invoice fields to surface MT status and error in the grid
                    row.I_MT_STATUS = inv.MT_STATUS ?? row.I_MT_STATUS;
                    row.I_ERROR_MESSAGE = inv.ERROR_MESSAGE ?? row.I_ERROR_MESSAGE;

                    // If guarantee link is missing but invoice carries Business Case reference, propose it
                    if (string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID) && !string.IsNullOrWhiteSpace(inv.BUSINESS_CASE_REFERENCE))
                    {
                        row.DWINGS_GuaranteeID = inv.BUSINESS_CASE_REFERENCE;
                    }
                }
            }
        }
    }
}
