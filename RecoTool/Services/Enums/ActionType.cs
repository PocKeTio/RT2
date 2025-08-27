namespace RecoTool.Services
{
    using System.ComponentModel;

    #region Enums and Helper Classes

    /// <summary>
    /// Types d'actions disponibles
    /// </summary>
    public enum ActionType
    {
        [Description("Not Applicable")]
        NA = 1,
        [Description("Remind")]
        Remind = 3,
        [Description("Refund")]
        Refund = 9,
        [Description("Topaze")]
        Topaze = 8,
        [Description("Match")]
        Match = 6,
        [Description("Investigate")]
        Investigate = 7,
        [Description("Do Pricing")]
        DoPricing = 13,
        [Description("To Claim")]
        ToClaim = 11,
        [Description("Adjust")]
        Adjust = 12,
        [Description("Request")]
        Request = 2,
        [Description("Trigger")]
        Trigger = 4,
        [Description("Execute")]
        Execute = 5,
        [Description("To Do SDD")]
        ToDoSDD = 10,
        [Description("Triggered")]
        Triggered = 34
    }

    #endregion
}
