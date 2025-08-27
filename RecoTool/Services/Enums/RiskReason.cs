namespace RecoTool.Services
{
    using System.ComponentModel;

    /// <summary>
    /// Risk reasons (RISKY). Numeric IDs should match the referential if available.
    /// If your database defines specific IDs, adjust the values accordingly.
    /// </summary>
    public enum Risky
    {
        [Description("Commissions already collected and credit in account 67P")] CollectedCommissionsCredit67P = 32,
        [Description("Fees not yet invoiced")] FeesNotYetInvoiced = 33,
        [Description("We do not observe risk of non payment for this client; expected payment delay")] NoObservedRiskExpectedDelay = 35
    }
}
