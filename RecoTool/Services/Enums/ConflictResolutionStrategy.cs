namespace RecoTool.Services
{
    #region Configuration Classes

    /// <summary>
    /// Stratégies de résolution de conflit
    /// </summary>
    public enum ConflictResolutionStrategy
    {
        KeepLocal,
        TakeServer,
        AskUser,
        MergeWhenPossible
    }

    #endregion
}
