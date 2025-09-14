using System;

namespace RecoTool.Services.DTOs
{
    /// <summary>
    /// Lightweight DTO for DWINGS guarantee rows used by UI assistance and linking.
    /// </summary>
    public class DwingsGuaranteeDto
    {
        public string GUARANTEE_ID { get; set; }
        public string GUARANTEE_STATUS { get; set; }
        public decimal? OUTSTANDING_AMOUNT { get; set; }
        public string CURRENCYNAME { get; set; }
        public string NAME1 { get; set; }
        public string NAME2 { get; set; }
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
        public string GROUPE { get; set; }
        public string PREMIUM { get; set; }
        public string BRANCH_CODE { get; set; }
        public string BRANCH_NAME { get; set; }
        public string OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY { get; set; }
        public DateTime? CANCELLATIONDATE { get; set; }
        public string CONTROLER { get; set; }
        public string AUTOMATICBOOKOFF { get; set; }
        public string NATUREOFDEAL { get; set; }
    }
}
