using System;

namespace RecoTool.Services.DTOs
{
    /// <summary>
    /// Lightweight DTO for DWINGS invoice rows used by UI assistance and linking.
    /// </summary>
    public class DwingsInvoiceDto
    {
        public string INVOICE_ID { get; set; }
        public string T_INVOICE_STATUS { get; set; }
        public decimal? BILLING_AMOUNT { get; set; }
        public decimal? REQUESTED_AMOUNT { get; set; }
        public decimal? FINAL_AMOUNT { get; set; }
        public string BILLING_CURRENCY { get; set; }
        public string BGPMT { get; set; }
        public string PAYMENT_METHOD { get; set; }
        public string T_PAYMENT_REQUEST_STATUS { get; set; }
        public string SENDER_REFERENCE { get; set; }
        public string RECEIVER_REFERENCE { get; set; }
        public string SENDER_NAME { get; set; }
        public string RECEIVER_NAME { get; set; }
        public string BUSINESS_CASE_REFERENCE { get; set; }
        public string BUSINESS_CASE_ID { get; set; }
        public string SENDER_ACCOUNT_NUMBER { get; set; }
        public string SENDER_ACCOUNT_BIC { get; set; }
        public DateTime? REQUESTED_EXECUTION_DATE { get; set; }
        public DateTime? START_DATE { get; set; }
        public DateTime? END_DATE { get; set; }
        public string DEBTOR_PARTY_NAME { get; set; }
        public string CREDITOR_PARTY_NAME { get; set; }
    }
}
