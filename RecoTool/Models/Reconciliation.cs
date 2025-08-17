using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les informations de réconciliation
    /// Table T_Reconciliation
    /// </summary>
    public class Reconciliation : BaseEntity
    {
        public string ID { get; set; }
        public string ROWGUID { get; set; }
        public string DWINGS_GuaranteeID { get; set; }
        public string DWINGS_InvoiceID { get; set; }
        public string DWINGS_CommissionID { get; set; }
        public int? Action { get; set; }
        public string Comments { get; set; }
        public string InternalInvoiceReference { get; set; }
        public DateTime? FirstClaimDate { get; set; }
        public DateTime? LastClaimDate { get; set; }
        public bool ToRemind { get; set; }
        public DateTime? ToRemindDate { get; set; }
        public bool ACK { get; set; }
        public string SwiftCode { get; set; }
        public string PaymentReference { get; set; }
        public int? KPI { get; set; }
        public int? IncidentType { get; set; }
        public int? RiskyItem { get; set; }
        public string ReasonNonRisky { get; set; }

        public Reconciliation()
        {
            ID = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Crée une nouvelle réconciliation liée à une ligne Ambre
        /// </summary>
        /// <param name="ambreLineId">ID de la ligne Ambre correspondante</param>
        /// <returns>Nouvelle instance de Reconciliation</returns>
        public static Reconciliation CreateForAmbreLine(string ambreLineId)
        {
            return new Reconciliation
            {
                // Historique: certains environnements lient via ID, d'autres via ROWGUID.
                // Pour compat, initialiser les deux si possible.
                ID = ambreLineId,
                ROWGUID = ambreLineId
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
