using System;

namespace RecoTool.Services.Rules
{
    /// <summary>
    /// MT Status condition for rules: ACKED, NOT_ACKED, NULL (empty), or WILDCARD (don't check)
    /// </summary>
    public enum MtStatusCondition
    {
        /// <summary>
        /// Don't check MT status (wildcard)
        /// </summary>
        Wildcard = 0,
        
        /// <summary>
        /// MT status must be "ACKED"
        /// </summary>
        Acked = 1,
        
        /// <summary>
        /// MT status must be present but NOT "ACKED"
        /// </summary>
        NotAcked = 2,
        
        /// <summary>
        /// MT status must be NULL or empty
        /// </summary>
        Null = 3
    }
}
