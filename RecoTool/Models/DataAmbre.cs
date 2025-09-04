using System;
using System.Globalization;
using System.Windows.Markup;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les données importées depuis Ambre
    /// Table T_Data_Ambre
    /// </summary>
    public class DataAmbre : BaseEntity
    {
        public string ID { get; set; }
        public string Account_ID { get; set; }
        public string CCY { get; set; }
        public string Country { get; set; }
        public string Event_Num { get; set; }
        public string Folder { get; set; }
        public string Pivot_MbawIDFromLabel { get; set; }
        public string Pivot_TransactionCodesFromLabel { get; set; }
        public string Pivot_TRNFromLabel { get; set; }
        public string RawLabel { get; set; }
        public string Receivable_DWRefFromAmbre { get; set; }
        /// <summary>
        /// Catégorie Ambre résolue depuis T_Ref_Ambre_TransactionCodes (INTEGER)
        /// </summary>
        public int? Category { get; set; }
        public decimal LocalSignedAmount { get; set; }
        public DateTime? Operation_Date { get; set; }
        public string Reconciliation_Num { get; set; }
        public string Receivable_InvoiceFromAmbre { get; set; }
        public string ReconciliationOrigin_Num { get; set; }
        public decimal SignedAmount { get; set; }
        public DateTime? Value_Date { get; set; }

        /// <summary>
        /// Détermine si cette ligne appartient au compte Pivot ou Receivable
        /// </summary>
        public bool IsPivotAccount(string pivotAccountCode)
        {
            return Account_ID == pivotAccountCode;
        }

        /// <summary>
        /// Détermine si cette ligne appartient au compte Receivable
        /// </summary>
        public bool IsReceivableAccount(string receivableAccountCode)
        {
            return Account_ID == receivableAccountCode;
        }

        /// <summary>
        /// Génère la clé unique pour cette ligne (utilisée pour détecter les modifications/suppressions)
        /// </summary>
        public string GetUniqueKey()
        {
            return $"{Event_Num}_{RawLabel}_{ReconciliationOrigin_Num}_{Operation_Date?.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}_{SignedAmount}";
        }
    }
}
