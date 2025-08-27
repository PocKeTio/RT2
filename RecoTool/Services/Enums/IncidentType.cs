namespace RecoTool.Services
{
    using System.ComponentModel;

    /// <summary>
    /// Incident categories (INC). Numeric IDs should match the referential if available.
    /// If your database defines specific IDs, adjust the values accordingly.
    /// </summary>
    public enum INC
    {
        [Description("Duplicated lines")] DuplicatedLines = 24,
        [Description("Incorrect calculations")] IncorrectCalculations = 25,
        [Description("Concorde/Accruals gaps")] ConcordeOrAccrualsGaps = 26,
        [Description("Wrong triggers")] WrongTriggers = 27,
        [Description("Missing invoices")] MissingInvoices = 28,
        [Description("Others")] Others = 31
    }
}
