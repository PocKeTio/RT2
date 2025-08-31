using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les garanties DWINGS
    /// Table T_DW_Guarantee
    /// </summary>
    public class DWINGSGuarantee
    {
        public string GUARANTEE_ID { get; set; }
        public string GUARANTEE_STATUS { get; set; }
        public string GUARANTEE_TYPE { get; set; }
        public string NATURE { get; set; }
        public string EVENT_STATUS { get; set; }
        public DateTime? EVENT_EFFECTIVEDATE { get; set; }
        public DateTime? ISSUEDATE { get; set; }
        public string OFFICIALREF { get; set; }
        public string UNDERTAKINGEVENT { get; set; }
        public string PROCESS { get; set; }
        public string EXPIRYDATETYPE { get; set; }
        public DateTime? EXPIRYDATE { get; set; }
        public string PARTY_ID { get; set; }
        public string PARTY_REF { get; set; }
        public string SECONDARY_OBLIGOR { get; set; }
        public string SECONDARY_OBLIGOR_NATURE { get; set; }
        public string ROLE { get; set; }
        public string COUNTRY { get; set; }
        public string CENTRAL_PARTY_CODE { get; set; }
        public string NAME1 { get; set; }
        public string NAME2 { get; set; }
        public string GROUPE { get; set; }
        public bool? PREMIUM { get; set; }
        public string BRANCH_CODE { get; set; }
        public string BRANCH_NAME { get; set; }
        public double? OUTSTANDING_AMOUNT { get; set; }
        public double? OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY { get; set; }
        public string CURRENCYNAME { get; set; }
        public DateTime? CANCELLATIONDATE { get; set; }
        public bool? CONTROLER { get; set; }
        public bool? AUTOMATICBOOKOFF { get; set; }
        public string NATUREOFDEAL { get; set; }
    }

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

            // Pattern pour BGI + année (4 chiffres) + mois (2 chiffres) + numéro (X chiffres)
            var regex = new System.Text.RegularExpressions.Regex(@"BGI\d{6,}");
            var match = regex.Match(text);
            
            return match.Success ? match.Value : null;
        }
    }
}
