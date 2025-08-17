namespace RecoTool.Services
{
    /// <summary>
    /// Stratégie de résolution de conflit
    /// </summary>
    public enum ConflictResolution
    {
        /// <summary>Garder la valeur locale</summary>
        KeepLocal,
        /// <summary>Prendre la valeur serveur</summary>
        TakeServer,
        /// <summary>Fusionner les valeurs</summary>
        Merge,
        /// <summary>Demander à l'utilisateur</summary>
        AskUser
    }
}
