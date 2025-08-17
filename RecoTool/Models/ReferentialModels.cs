using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Modèle pour les paramètres de configuration
    /// Table T_Param (clé/valeur)
    /// </summary>
    public class Param
    {
        public string PAR_Key { get; set; }
        public string PAR_Value { get; set; }
        public string PAR_Description { get; set; }
    }

    /// <summary>
    /// Modèle pour le mapping des champs d'import depuis Excel
    /// Table T_Ref_Ambre_ImportFields
    /// </summary>
    public class AmbreImportField
    {
        public string AMB_Source { get; set; }
        public string AMB_Destination { get; set; }
    }

    /// <summary>
    /// Modèle pour les codes de transaction
    /// Table T_Ref_Ambre_TransactionCodes
    /// </summary>
    public class AmbreTransactionCode
    {
        public int ATC_ID { get; set; }
        public string ATC_CODE { get; set; }
        public string ATC_TAG { get; set; }
    }

    /// <summary>
    /// Modèle pour les transformations de données
    /// Table T_Ref_Ambre_Transform
    /// </summary>
    public class AmbreTransform
    {
        public string AMB_Source { get; set; }
        public string AMB_Destination { get; set; }
        public string AMB_TransformationFunction { get; set; }
        public string AMB_Description { get; set; }
    }

    /// <summary>
    /// Modèle pour les pays/bookings
    /// Table T_Ref_Country
    /// </summary>
    public class Country
    {
        public string CNT_Id { get; set; }
        public string CNT_Name { get; set; }
        public int CNT_AmbrePivotCountryId { get; set; }
        public string CNT_AmbrePivot { get; set; }
        public string CNT_AmbreReceivable { get; set; }
        public int CNT_AmbreReceivableCountryId { get; set; }
        public string CNT_ServiceCode { get; set; }
        public string CNT_BIC { get; set; }
    }

    /// <summary>
    /// Modèle pour les champs utilisateur (combobox)
    /// Table T_Ref_User_Fields
    /// </summary>
    public class UserField
    {
        public int USR_ID { get; set; }
        public string USR_Category { get; set; }
        public string USR_FieldName { get; set; }
        public string USR_FieldDescription { get; set; }
        public bool USR_Pivot { get; set; }
        public bool USR_Receivable { get; set; }
        public string USR_Color { get; set; }
    }

    

    /// <summary>
    /// Modèle pour les filtres sauvegardés
    /// Table T_Ref_User_Filter
    /// </summary>
    public class UserFilter
    {
        public int UFI_id { get; set; }
        public string UFI_Name { get; set; }
        public string UFI_SQL { get; set; }
        public string UFI_CreatedBy { get; set; }
    }

    /// <summary>
    /// Modèle pour les garanties DWINGS
    /// Table T_DW_Guarantee
    /// </summary>
    public class DWGuarantee
    {
        public string GUARANTEE_ID { get; set; }
        public string SYNDICATE { get; set; }
        public string CURRENCY { get; set; }
        public string AMOUNT { get; set; }
        public string OfficialID { get; set; }
        public string GuaranteeType { get; set; }
        public string Client { get; set; }
        public string _791Sent { get; set; }
        public string InvoiceStatus { get; set; }
        public string TriggerDate { get; set; }
        public string FXRate { get; set; }
        public string RMPM { get; set; }
        public string GroupName { get; set; }
    }

    /// <summary>
    /// Modèle pour les données DWINGS
    /// Table T_DW_Data
    /// </summary>
    public class DWData
    {
        public string INVOICE_ID { get; set; }
        public string BOOKING { get; set; }
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
        public string T_COMMISSION_PERIOD_STAT { get; set; }
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
        public string COMMISSION_ID { get; set; }
        public string DEBTOR_PARTY_ID { get; set; }
        public string DEBTOR_PARTY_NAME { get; set; }
        public string DEBTOR_ACCOUNT_NUMBER { get; set; }
        public string CREDITOR_PARTY_ID { get; set; }
        public string CREDITOR_PARTY_NAME { get; set; }
        public string CREDITOR_ACCOUNT_NUMBER { get; set; }
    }
}
