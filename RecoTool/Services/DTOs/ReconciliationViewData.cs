using System;
using RecoTool.Models;

namespace RecoTool.Services
{
    #region Enums and Helper Classes

    /// <summary>
    /// Vue combinée pour l'affichage des données de réconciliation
    /// </summary>
    public class ReconciliationViewData : DataAmbre
    {
        // Propriétés de Reconciliation
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
        // ModifiedBy from T_Reconciliation (avoid collision with BaseEntity.ModifiedBy coming from Ambre)
        public string Reco_ModifiedBy { get; set; }

        // Propriétés DWINGS
        public string GUARANTEE_ID { get; set; }
        public string INVOICE_ID { get; set; }
        public string COMMISSION_ID { get; set; }
        public string SYNDICATE { get; set; }
        public decimal? GUARANTEE_AMOUNT { get; set; }
        public string GUARANTEE_CURRENCY { get; set; }
        public string GUARANTEE_STATUS { get; set; }
    }

    #endregion
}
