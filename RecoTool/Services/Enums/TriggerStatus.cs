namespace RecoTool.Services
{
    using System.ComponentModel;

    /// <summary>
    /// Status for receivable trigger lifecycle. Numeric values may align with referential later.
    /// </summary>
    public enum TriggerStatus
    {
        [Description("Unknown")] Unknown = 0,
        [Description("Pending")] Pending = 1,
        [Description("Triggered")] Triggered = 2,
        [Description("Dismissed")] Dismissed = 3
    }
}
