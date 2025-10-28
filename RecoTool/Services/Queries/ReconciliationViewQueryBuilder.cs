using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using RecoTool.Domain.Filters;

namespace RecoTool.Services.Queries
{
    internal static class ReconciliationViewQueryBuilder
    {
        /// <summary>
        /// Builds the SQL string for the reconciliation view using escaped external DB paths.
        /// REMOVED: dashboardOnly parameter - was incomplete (missing IsToReview, IsReviewedToday, Assignee)
        /// and created separate cache entries preventing cache reuse between HomePage and ReconciliationView.
        /// </summary>
        public static string Build(string dwEsc, string ambreEsc, string filterSql)
        {
            string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";
            // Base AMBRE source for subqueries/aggregates (no alias)
            string ambreBase = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre" : $"[{ambreEsc}].T_Data_Ambre";

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
                                   r.MbawData,
                                   r.SpiritData,
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
                           LEFT JOIN (SELECT Event_Num & Reconciliation_Num & Account_ID AS DupKey, COUNT(*) AS DupCount FROM {ambreBase} GROUP BY Event_Num & Reconciliation_Num & Account_ID) AS dup ON dup.DupKey = (a.Event_Num & a.Reconciliation_Num & a.Account_ID)
                           WHERE 1=1";
        }
    }
}
