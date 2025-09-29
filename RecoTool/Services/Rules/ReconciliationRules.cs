using System;
using RecoTool.Models;

namespace RecoTool.Services.Rules
{
    /// <summary>
    /// Encapsulates legacy reconciliation rule logic.
    /// Deprecated: replaced by the truth-table rules engine.
    /// </summary>
    [Obsolete("Deprecated: use RulesEngine and truth-table rules instead.")]
    internal static class ReconciliationRules
    {
        public static void ApplyPivotRules(Reconciliation reconciliation, DataAmbre data)
        {
            if (reconciliation == null || data == null) return;

            var transactionType = data.Pivot_TransactionCodesFromLabel?.ToUpper();
            bool isCredit = data.SignedAmount > 0;

            switch (transactionType)
            {
                case "COLLECTION":
                    reconciliation.Action = isCredit ? (int)ActionType.Match : (int)ActionType.NA;
                    reconciliation.KPI = isCredit ? (int)KPIType.PaidButNotReconciled : (int)KPIType.ITIssues;
                    break;

                case "PAYMENT":
                case "AUTOMATIC REFUND":
                    reconciliation.Action = !isCredit ? (int)ActionType.DoPricing : (int)ActionType.NA;
                    reconciliation.KPI = !isCredit ? (int)KPIType.CorrespondentChargesToBeInvoiced : (int)KPIType.ITIssues;
                    break;

                case "ADJUSTMENT":
                    reconciliation.Action = (int)ActionType.Adjust;
                    reconciliation.KPI = (int)KPIType.PaidButNotReconciled;
                    break;

                case "XCL LOADER":
                    reconciliation.Action = isCredit ? (int)ActionType.Match : (int)ActionType.Investigate;
                    reconciliation.KPI = isCredit ? (int)KPIType.PaidButNotReconciled : (int)KPIType.UnderInvestigation;
                    break;

                case "TRIGGER":
                    if (isCredit)
                    {
                        reconciliation.Action = (int)ActionType.Investigate;
                        reconciliation.KPI = (int)KPIType.UnderInvestigation;
                    }
                    else
                    {
                        reconciliation.Action = (int)ActionType.DoPricing;
                        reconciliation.KPI = (int)KPIType.CorrespondentChargesToBeInvoiced;
                    }
                    break;

                default:
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.UnderInvestigation;
                    break;
            }
        }

        public static void ApplyReceivableRules(Reconciliation reconciliation, DataAmbre data)
        {
            if (reconciliation == null || data == null) return;

            var transactionType = data.Pivot_TransactionCodesFromLabel?.ToUpper();
            var guaranteeType = ExtractGuaranteeTypeFromLabel(data.RawLabel);

            switch (transactionType)
            {
                case "INCOMING PAYMENT":
                    switch (guaranteeType?.ToUpper())
                    {
                        case "REISSUANCE":
                            reconciliation.Action = (int)ActionType.Request;
                            reconciliation.KPI = (int)KPIType.NotClaimed;
                            break;
                        case "ISSUANCE":
                            reconciliation.Action = (int)ActionType.NA;
                            reconciliation.KPI = (int)KPIType.ClaimedButNotPaid;
                            break;
                        case "ADVISING":
                            reconciliation.Action = (int)ActionType.Trigger;
                            reconciliation.KPI = (int)KPIType.PaidButNotReconciled;
                            break;
                        default:
                            reconciliation.Action = (int)ActionType.Investigate;
                            reconciliation.KPI = (int)KPIType.ITIssues;
                            break;
                    }
                    break;

                case "DIRECT DEBIT":
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.ITIssues;
                    break;

                case "MANUAL OUTGOING":
                case "OUTGOING PAYMENT":
                    reconciliation.Action = (int)ActionType.Trigger;
                    reconciliation.KPI = (int)KPIType.CorrespondentChargesPendingTrigger;
                    break;

                case "EXTERNAL DEBIT PAYMENT":
                    reconciliation.Action = (int)ActionType.Execute;
                    reconciliation.KPI = (int)KPIType.NotClaimed;
                    break;

                default:
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.ITIssues;
                    break;
            }
        }

        private static string ExtractGuaranteeTypeFromLabel(string rawLabel)
        {
            if (string.IsNullOrEmpty(rawLabel)) return null;

            var upperLabel = rawLabel.ToUpper();
            if (upperLabel.Contains("REISSUANCE")) return "REISSUANCE";
            if (upperLabel.Contains("ISSUANCE")) return "ISSUANCE";
            if (upperLabel.Contains("ADVISING")) return "ADVISING";

            return null;
        }
    }
}
