using System.ComponentModel;

namespace RecoTool.Services
{
    #region Enums and Helper Classes

    /// <summary>
    /// Types de KPI disponibles
    /// </summary>
    public enum KPIType
    {
        [Description("IT Issues")]
        ITIssues = 19,
        [Description("Paid But Not Reconciled")]
        PaidButNotReconciled = 18,
        [Description("Correspondent Charges To Be Invoiced")]
        CorrespondentChargesToBeInvoiced = 21,
        [Description("Under Investigation")]
        UnderInvestigation = 22,
        [Description("Not Claimed")]
        NotClaimed = 17,
        [Description("Claimed But Not Paid")]
        ClaimedButNotPaid = 16,
        [Description("Correspondent Charges Pending Trigger")]
        CorrespondentChargesPendingTrigger = 15,
        [Description("Not TFSC")]
        NotTFSC = 23
    }

    #endregion
}
