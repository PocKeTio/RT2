namespace RecoTool.Services
{
    #region Enums and Helper Classes

    /// <summary>
    /// Types de KPI disponibles
    /// </summary>
    public enum KPIType
    {
        ITIssues = 0,
        PaidButNotReconciled = 1,
        CorrespondentChargesToBeInvoiced = 2,
        UnderInvestigation = 3,
        NotClaimed = 4,
        ClaimedButNotPaid = 5,
        CorrespondentChargesPendingTrigger = 6
    }

    #endregion
}
