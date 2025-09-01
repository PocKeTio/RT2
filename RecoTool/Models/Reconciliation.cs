using System;
using RecoTool.Services;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les informations de réconciliation
    /// Table T_Reconciliation
    /// </summary>
    public class Reconciliation : BaseEntity
    {
        public string ID { get; set; }
        public string DWINGS_GuaranteeID { get; set; }
        public string DWINGS_InvoiceID { get; set; }
        public string DWINGS_CommissionID { get; set; }
        public int? Action { get; set; }
        // New: assignee user ID (referential T_User.USR_ID)
        public string Assignee { get; set; }
        public string Comments { get; set; }
        public string InternalInvoiceReference { get; set; }
        public DateTime? FirstClaimDate { get; set; }
        public DateTime? LastClaimDate { get; set; }
        public bool ToRemind { get; set; }
        public DateTime? ToRemindDate { get; set; }
        public bool ACK { get; set; }
        public string SwiftCode { get; set; }
        public string PaymentReference { get; set; }
        // New long text fields for reconciliation notes
        public string MbawData { get; set; }
        public string SpiritData { get; set; }
        public int? KPI { get; set; }
        public int? IncidentType { get; set; }
        public bool? RiskyItem { get; set; }
        public int? ReasonNonRisky { get; set; }
        
        // Trigger date
        public DateTime? TriggerDate { get; set; }

        /// <summary>
        /// Effective risky flag for business logic: null is considered false.
        /// </summary>
        public bool IsRiskyEffective => RiskyItem == true;

        /// <summary>
        /// Crée une nouvelle réconciliation liée à une ligne Ambre
        /// </summary>
        /// <param name="ambreLineId">ID de la ligne Ambre correspondante</param>
        /// <returns>Nouvelle instance de Reconciliation</returns>
        public static Reconciliation CreateForAmbreLine(string ambreLineId)
        {
            return new Reconciliation
            {
                // Utiliser uniquement l'ID comme clé primaire stable
                ID = ambreLineId
            };
        }

        /// <summary>
        /// Indique si cette réconciliation nécessite un rappel
        /// </summary>
        public bool RequiresReminder => ToRemind && ToRemindDate.HasValue && ToRemindDate <= DateTime.Today;

        /// <summary>
        /// Indique si cette réconciliation a des informations DWINGS associées
        /// </summary>
        public bool HasDWINGSData => !string.IsNullOrEmpty(DWINGS_GuaranteeID) || 
                                     !string.IsNullOrEmpty(DWINGS_InvoiceID) || 
                                     !string.IsNullOrEmpty(DWINGS_CommissionID);
    }
}
