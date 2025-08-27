namespace RecoTool.Services
{
    #region Enums and Helper Classes

    /// <summary>
    /// Types de KPI disponibles
    /// </summary>
    public enum KPIType
    {
        ITIssues = 19,
        PaidButNotReconciled = 18,
        CorrespondentChargesToBeInvoiced = 21,
        UnderInvestigation = 22,
        NotClaimed = 17,
        ClaimedButNotPaid = 16,
        CorrespondentChargesPendingTrigger = 15,
        NotTFSC = 23
    }

    #endregion
}
