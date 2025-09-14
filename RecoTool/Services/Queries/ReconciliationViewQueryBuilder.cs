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
                            r.KPI, 
                            r.RiskyItem,
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

            // Full view query
            string dwGuaranteeJoin = string.IsNullOrEmpty(dwEsc) ? "T_DW_Guarantee AS g" : $"(SELECT * FROM [{dwEsc}].T_DW_Guarantee) AS g";

            return $@"SELECT
                                   a.*,
                                   r.DWINGS_GuaranteeID,
                                   r.DWINGS_InvoiceID,
                                   r.DWINGS_CommissionID,
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
                                    g.OUTSTANDING_AMOUNT AS GUARANTEE_AMOUNT,
                                    g.CURRENCYNAME AS GUARANTEE_CURRENCY,
                                    g.GUARANTEE_STATUS AS GUARANTEE_STATUS,
                                    g.GUARANTEE_TYPE AS GUARANTEE_TYPE,
                                    NULL AS COMMISSION_ID,
                                    g.GUARANTEE_ID,
                                 
                                  g.NATURE AS G_NATURE,
                                  g.EVENT_STATUS AS G_EVENT_STATUS,
                                  g.EVENT_EFFECTIVEDATE AS G_EVENT_EFFECTIVEDATE,
                                  g.ISSUEDATE AS G_ISSUEDATE,
                                  g.OFFICIALREF AS G_OFFICIALREF,
                                  g.UNDERTAKINGEVENT AS G_UNDERTAKINGEVENT,
                                  g.PROCESS AS G_PROCESS,
                                  g.EXPIRYDATETYPE AS G_EXPIRYDATETYPE,
                                  g.EXPIRYDATE AS G_EXPIRYDATE,
                                  g.PARTY_ID AS G_PARTY_ID,
                                  g.PARTY_REF AS G_PARTY_REF,
                                  g.SECONDARY_OBLIGOR AS G_SECONDARY_OBLIGOR,
                                  g.SECONDARY_OBLIGOR_NATURE AS G_SECONDARY_OBLIGOR_NATURE,
                                  g.ROLE AS G_ROLE,
                                  g.COUNTRY AS G_COUNTRY,
                                  g.CENTRAL_PARTY_CODE AS G_CENTRAL_PARTY_CODE,
                                  g.NAME1 AS G_NAME1,
                                  g.NAME2 AS G_NAME2,
                                  g.GROUPE AS G_GROUPE,
                                  g.PREMIUM AS G_PREMIUM,
                                  g.BRANCH_CODE AS G_BRANCH_CODE,
                                  g.BRANCH_NAME AS G_BRANCH_NAME,
                                  g.OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY AS G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY,
                                  g.CANCELLATIONDATE AS G_CANCELLATIONDATE,
                                  g.CONTROLER AS G_CONTROLER,
                                  g.AUTOMATICBOOKOFF AS G_AUTOMATICBOOKOFF,
                                  g.NATUREOFDEAL AS G_NATUREOFDEAL,
                                  g.GUARANTEE_TYPE AS G_GUARANTEE_TYPE,

                                   NULL AS INVOICE_ID,
                                   NULL AS I_REQUESTED_INVOICE_AMOUNT,
                                   NULL AS I_SENDER_NAME,
                                   NULL AS I_RECEIVER_NAME,
                                   NULL AS I_SENDER_REFERENCE,
                                   NULL AS I_RECEIVER_REFERENCE,
                                   NULL AS I_T_INVOICE_STATUS,
                                   NULL AS I_BILLING_AMOUNT,
                                   NULL AS I_BILLING_CURRENCY,
                                   NULL AS I_START_DATE,
                                   NULL AS I_END_DATE,
                                   NULL AS I_FINAL_AMOUNT,
                                   NULL AS I_T_COMMISSION_PERIOD_STATUS,
                                   NULL AS I_BUSINESS_CASE_REFERENCE,
                                   NULL AS I_BUSINESS_CASE_ID,
                                   NULL AS I_POSTING_PERIODICITY,
                                   NULL AS I_EVENT_ID,
                                   NULL AS I_COMMENTS,
                                   NULL AS I_SENDER_ACCOUNT_NUMBER,
                                   NULL AS I_SENDER_ACCOUNT_BIC,
                                   NULL AS I_RECEIVER_ACCOUNT_NUMBER,
                                   NULL AS I_RECEIVER_ACCOUNT_BIC,
                                   NULL AS I_REQUESTED_AMOUNT,
                                   NULL AS I_EXECUTED_AMOUNT,
                                   NULL AS I_REQUESTED_EXECUTION_DATE,
                                   NULL AS I_T_PAYMENT_REQUEST_STATUS,
                                   NULL AS I_BGPMT,
                                   NULL AS I_DEBTOR_ACCOUNT_ID,
                                   NULL AS I_CREDITOR_ACCOUNT_ID,
                                   NULL AS I_MT_STATUS,
                                   NULL AS I_REMINDER_NUMBER,
                                   NULL AS I_ERROR_MESSAGE,
                                   NULL AS I_DEBTOR_PARTY_ID,
                                   NULL AS I_PAYMENT_METHOD,
                                   NULL AS I_PAYMENT_TYPE,
                                   NULL AS I_DEBTOR_PARTY_NAME,
                                   NULL AS I_DEBTOR_ACCOUNT_NUMBER,
                                   NULL AS I_CREDITOR_PARTY_ID,
                                   NULL AS I_CREDITOR_ACCOUNT_NUMBER

                           FROM (({ambreJoin}
                           LEFT JOIN T_Reconciliation AS r ON a.ID = r.ID)
                           LEFT JOIN {dwGuaranteeJoin} ON  g.GUARANTEE_ID = r.DWINGS_GuaranteeID)
                           LEFT JOIN (SELECT Event_Num, COUNT(*) AS DupCount FROM {ambreBase} GROUP BY Event_Num) AS dup ON dup.Event_Num = a.Event_Num
                           WHERE 1=1";
        }
    }
}
