using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les garanties DWINGS
    /// Table T_DW_Guarantee
    /// </summary>
    

    /// <summary>
    /// Modèle pour les données DWINGS
    /// Table T_DW_Data
    /// </summary>
    public class DWINGSInvoice
    {
        public string INVOICE_ID { get; set; }
        public string REQUESTED_INVOICE_AMOUNT { get; set; }
        public string SENDER_NAME { get; set; }
        public string RECEIVER_NAME { get; set; }
        public string SENDER_REFERENCE { get; set; }
        public string RECEIVER_REFERENCE { get; set; }
        public string T_INVOICE_STATUS { get; set; }
        public string BILLING_AMOUNT { get; set; }
        public string BILLING_CURRENCY { get; set; }
        public string START_DATE { get; set; }
        public string END_DATE { get; set; }
        public string FINAL_AMOUNT { get; set; }
        public string T_COMMISSION_PERIOD_STATUS { get; set; }
        public string BUSINESS_CASE_REFERENCE { get; set; }
        public string BUSINESS_CASE_ID { get; set; }
        public string POSTING_PERIODICITY { get; set; }
        public string EVENT_ID { get; set; }
        public string COMMENTS { get; set; }
        public string SENDER_ACCOUNT_NUMBER { get; set; }
        public string SENDER_ACCOUNT_BIC { get; set; }
        public string RECEIVER_ACCOUNT_NUMBER { get; set; }
        public string RECEIVER_ACCOUNT_BIC { get; set; }
        public string REQUESTED_AMOUNT { get; set; }
        public string EXECUTED_AMOUNT { get; set; }
        public string REQUESTED_EXECUTION_DATE { get; set; }
        public string T_PAYMENT_REQUEST_STATUS { get; set; }
        public string BGPMT { get; set; }
        public string DEBTOR_ACCOUNT_ID { get; set; }
        public string CREDITOR_ACCOUNT_ID { get; set; }
        public string MT_STATUS { get; set; }
        public string REMINDER_NUMBER { get; set; }
        public string ERROR_MESSAGE { get; set; }
        public string DEBTOR_PARTY_ID { get; set; }
        public string PAYMENT_METHOD { get; set; }
        public string PAYMENT_TYPE { get; set; }
        public string DEBTOR_PARTY_NAME { get; set; }
        public string DEBTOR_ACCOUNT_NUMBER { get; set; }
        public string CREDITOR_PARTY_ID { get; set; }
        public string CREDITOR_ACCOUNT_NUMBER { get; set; }

        /// <summary>
        /// Détermine si cette donnée DWINGS correspond à une invoice ID spécifique
        /// </summary>
        /// <param name="invoiceId">ID de l'invoice à rechercher</param>
        /// <returns>True si correspondance trouvée</returns>
        public bool MatchesInvoiceId(string invoiceId)
        {
            return INVOICE_ID == invoiceId ||
                   SENDER_REFERENCE == invoiceId ||
                   RECEIVER_REFERENCE == invoiceId ||
                   BUSINESS_CASE_REFERENCE == invoiceId;
        }
    }

    /// <summary>
    /// Classe utilitaire pour extraire l'ID d'invoice depuis les données Ambre
    /// Format attendu: BGIYYYYMMXXXXXXX
    /// </summary>
    public static class InvoiceIdExtractor
    {
        /// <summary>
        /// Extrait l'ID d'invoice depuis un libellé ou référence
        /// </summary>
        /// <param name="text">Texte contenant potentiellement un ID d'invoice</param>
        /// <returns>ID d'invoice si trouvé, null sinon</returns>
        public static string ExtractInvoiceId(string text)
        {
            if (string.IsNullOrEmpty(text))
                return null;

            // Pattern pour BGI + année (4 chiffres) + mois (2 chiffres) + numéro (7 chiffres)
            var regex = new System.Text.RegularExpressions.Regex(@"BGI\d{13}");
            var match = regex.Match(text);
            
            return match.Success ? match.Value : null;
        }
    }
}
