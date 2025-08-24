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
        NA = 0,
        [Description("Match")]
        Match = 1,
        [Description("Investigate")]
        Investigate = 2,
        [Description("Do Pricing")]
        DoPricing = 3,
        [Description("To Claim")]
        ToClaim = 4,
        [Description("Adjust")]
        Adjust = 5,
        [Description("Request")]
        Request = 6,
        [Description("Trigger")]
        Trigger = 7,
        [Description("Execute")]
        Execute = 8,
        [Description("To Do SDD")]
        ToDoSDD = 9
    }

    #endregion
}
