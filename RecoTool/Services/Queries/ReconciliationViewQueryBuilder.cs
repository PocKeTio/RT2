using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using RecoTool.Domain.Filters;

namespace RecoTool.Services.Queries
{
    internal static class ReconciliationViewQueryBuilder
    {
        /// <summary>
        /// Builds the SQL string for the reconciliation view (dashboard or full) using escaped external DB paths.
        /// This mirrors the original inline construction in ReconciliationService.BuildReconciliationViewAsyncCore.
        /// </summary>
        public static string Build(string dwEsc, string ambreEsc, string filterSql, bool dashboardOnly)
        {
            string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";
            // Base AMBRE source for subqueries/aggregates (no alias)
            string ambreBase = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre" : $"[{ambreEsc}].T_Data_Ambre";

            // Detect PotentialDuplicates flag from optional JSON comment prefix (kept for parity; not used in SQL text)
            try
            {
                if (!string.IsNullOrWhiteSpace(filterSql))
                {
                    var mDup = Regex.Match(filterSql, @"^/\*JSON:(.*?)\*/", RegexOptions.Singleline);
                    if (mDup.Success)
                    {
                        var json = mDup.Groups[1].Value;
                        var preset = JsonSerializer.Deserialize<FilterPreset>(json);
                        var _ = preset?.PotentialDuplicates == true; // reserved for future use
                    }
                }
            }
            catch { }

            if (dashboardOnly)
            {
                return $@"
                        SELECT 
                            a.ID, 
                            a.Account_ID, 
                            a.CCY, 
                            a.SignedAmount, 
                            a.Operation_Date, 
                            a.Value_Date, 
                            a.CreationDate, 
                            a.DeleteDate,
                            r.Action, 
                            r.ActionStatus,
                            r.ActionDate,
                            r.KPI, 
                            r.RiskyItem,
                            r.DWINGS_GuaranteeID,
                            r.DWINGS_InvoiceID,
                            r.DWINGS_BGPMT,
                            IIF(dup.DupCount > 1, True, False) AS IsPotentialDuplicate
                        FROM 
                            (
                                (
                                    {ambreJoin}
                                    LEFT JOIN T_Reconciliation AS r 
                                        ON a.ID = r.ID
                                )
                                LEFT JOIN 
                                    (
                                        SELECT Event_Num, COUNT(*) AS DupCount 
                                        FROM {ambreBase} 
                                        GROUP BY Event_Num
                                    ) AS dup 
                                    ON dup.Event_Num = a.Event_Num
                            )
                        WHERE 
                            a.DeleteDate IS NULL 
                            AND (r.DeleteDate IS NULL) ";
            }

            // Full view query (DWINGS joins removed to avoid row multiplication; enrichment fills fields later)

            return $@"SELECT
                                   a.*,
                                   r.DWINGS_GuaranteeID,
                                   r.DWINGS_InvoiceID,
                                   r.DWINGS_BGPMT,
                                   r.CreationDate AS Reco_CreationDate,
                                   r.LastModified AS Reco_LastModified,
                                   r.Action,
                                   r.ActionStatus,
                                   r.ActionDate,
                                   r.Assignee,
                                   r.Comments,
                                   r.InternalInvoiceReference,
                                   r.FirstClaimDate,
                                   r.LastClaimDate,
                                   r.ToRemind,
                                   r.ToRemindDate,
                                   r.ACK,
                                   r.SwiftCode,
                                   r.PaymentReference,
                                   r.KPI,
                                   r.IncidentType,
                                   r.RiskyItem,
                                   r.ReasonNonRisky,
                                   r.ModifiedBy AS Reco_ModifiedBy,
                                   IIF(dup.DupCount > 1, True, False) AS IsPotentialDuplicate,

                                    NULL AS SYNDICATE,
                                    NULL AS GUARANTEE_AMOUNT,
                                    NULL AS GUARANTEE_CURRENCY,
                                    NULL AS GUARANTEE_STATUS,
                                    NULL AS GUARANTEE_TYPE,
                                    NULL AS COMMISSION_ID,
                                    NULL AS GUARANTEE_ID,

                                  NULL AS G_NATURE,
                                  NULL AS G_EVENT_STATUS,
                                  NULL AS G_EVENT_EFFECTIVEDATE,
                                  NULL AS G_ISSUEDATE,
                                  NULL AS G_OFFICIALREF,
                                  NULL AS G_UNDERTAKINGEVENT,
                                  NULL AS G_PROCESS,
                                  NULL AS G_EXPIRYDATETYPE,
                                  NULL AS G_EXPIRYDATE,
                                  NULL AS G_PARTY_ID,
                                  NULL AS G_PARTY_REF,
                                  NULL AS G_SECONDARY_OBLIGOR,
                                  NULL AS G_SECONDARY_OBLIGOR_NATURE,
                                  NULL AS G_ROLE,
                                  NULL AS G_COUNTRY,
                                  NULL AS G_CENTRAL_PARTY_CODE,
                                  NULL AS G_NAME1,
                                  NULL AS G_NAME2,
                                  NULL AS G_GROUPE,
                                  NULL AS G_PREMIUM,
                                  NULL AS G_BRANCH_CODE,
                                  NULL AS G_BRANCH_NAME,
                                  NULL AS G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY,
                                  NULL AS G_CANCELLATIONDATE,
                                  NULL AS G_CONTROLER,
                                  NULL AS G_AUTOMATICBOOKOFF,
                                  NULL AS G_NATUREOFDEAL,
                                  NULL AS G_GUARANTEE_TYPE,

                                  NULL AS INVOICE_ID,
                                  NULL AS I_T_INVOICE_STATUS,
                                  NULL AS I_BILLING_AMOUNT,
                                  NULL AS I_BILLING_CURRENCY,
                                  NULL AS I_START_DATE,
                                  NULL AS I_END_DATE,
                                  NULL AS I_FINAL_AMOUNT,
                                  NULL AS I_REQUESTED_INVOICE_AMOUNT,
                                  NULL AS I_PAYMENT_METHOD

                           FROM ({ambreJoin}
                           LEFT JOIN T_Reconciliation AS r ON a.ID = r.ID)
                           LEFT JOIN (SELECT Event_Num, COUNT(*) AS DupCount FROM {ambreBase} GROUP BY Event_Num) AS dup ON dup.Event_Num = a.Event_Num
                           WHERE 1=1";
        }
    }
}
