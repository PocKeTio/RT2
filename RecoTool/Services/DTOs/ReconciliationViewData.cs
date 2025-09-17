using System;
using System.ComponentModel;
using RecoTool.Models;

namespace RecoTool.Services.DTOs
{
    #region Enums and Helper Classes

    /// <summary>
    /// Vue combinée pour l'affichage des données de réconciliation
    /// </summary>
    public class ReconciliationViewData : DataAmbre, INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;
        protected void OnPropertyChanged(string propertyName) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));

        // Propriétés de Reconciliation
        public string DWINGS_GuaranteeID { get; set; }
        public string DWINGS_InvoiceID { get; set; }
        public string DWINGS_CommissionID { get; set; }
        private int? _action;
        public int? Action
        {
            get => _action;
            set
            {
                if (_action != value)
                {
                    _action = value;
                    OnPropertyChanged(nameof(Action));
                }
            }
        }
        private string _comments;
        public string Comments
        {
            get => _comments;
            set
            {
                if (!string.Equals(_comments, value))
                {
                    _comments = value;
                    OnPropertyChanged(nameof(Comments));
                    OnPropertyChanged(nameof(LastComment));
                }
            }
        }

        /// <summary>
        /// Derived view property: returns the last non-empty line from Comments, used for compact display.
        /// </summary>
        public string LastComment
        {
            get
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(Comments)) return string.Empty;
                    var lines = Comments.Replace("\r\n", "\n").Split('\n');
                    for (int i = lines.Length - 1; i >= 0; i--)
                    {
                        var s = lines[i]?.Trim();
                        if (!string.IsNullOrEmpty(s)) return s;
                    }
                }
                catch { }
                return string.Empty;
            }
        }
        public string InternalInvoiceReference { get; set; }
        public DateTime? FirstClaimDate { get; set; }
        public DateTime? LastClaimDate { get; set; }
        private bool _toRemind;
        public bool ToRemind
        {
            get => _toRemind;
            set
            {
                if (_toRemind != value)
                {
                    _toRemind = value;
                    OnPropertyChanged(nameof(ToRemind));
                }
            }
        }

        private DateTime? _toRemindDate;
        public DateTime? ToRemindDate
        {
            get => _toRemindDate;
            set
            {
                if (_toRemindDate != value)
                {
                    _toRemindDate = value;
                    OnPropertyChanged(nameof(ToRemindDate));
                }
            }
        }
        public bool ACK { get; set; }
        public string SwiftCode { get; set; }
        public string PaymentReference { get; set; }
        private int? _kpi;
        public int? KPI
        {
            get => _kpi;
            set
            {
                if (_kpi != value)
                {
                    _kpi = value;
                    OnPropertyChanged(nameof(KPI));
                }
            }
        }
        private bool? _actionStatus;
        public bool? ActionStatus
        {
            get => _actionStatus;
            set
            {
                if (_actionStatus != value)
                {
                    _actionStatus = value;
                    OnPropertyChanged(nameof(ActionStatus));
                }
            }
        }
        private DateTime? _actionDate;
        public DateTime? ActionDate
        {
            get => _actionDate;
            set
            {
                if (_actionDate != value)
                {
                    _actionDate = value;
                    OnPropertyChanged(nameof(ActionDate));
                }
            }
        }
        private int? _incidentType;
        public int? IncidentType
        {
            get => _incidentType;
            set
            {
                if (_incidentType != value)
                {
                    _incidentType = value;
                    OnPropertyChanged(nameof(IncidentType));
                }
            }
        }
        private string _assignee;
        public string Assignee
        {
            get => _assignee;
            set
            {
                if (_assignee != value)
                {
                    _assignee = value;
                    OnPropertyChanged(nameof(Assignee));
                }
            }
        }
        public bool RiskyItem { get; set; }
        public int? ReasonNonRisky { get; set; }
        // ModifiedBy from T_Reconciliation (avoid collision with BaseEntity.ModifiedBy coming from Ambre)
        public string Reco_ModifiedBy { get; set; }

        /// <summary>
        /// Effective risky flag for analytics/filters: null is considered false.
        /// </summary>
        public bool IsRiskyEffective => RiskyItem == true;

        // Notes fields from T_Reconciliation
        public string MbawData { get; set; }
        public string SpiritData { get; set; }

        // Trigger date from T_Reconciliation
        public DateTime? TriggerDate { get; set; }

        // Reconciliation timestamps (used to compute UI indicators)
        public DateTime? Reco_CreationDate { get; set; }
        public DateTime? Reco_LastModified { get; set; }

        // Account side: 'P' for Pivot, 'R' for Receivable (used in matched popup)
        public string AccountSide { get; set; }

        // True if the reference (DWINGS_InvoiceID or InternalInvoiceReference) exists on both accounts
        public bool IsMatchedAcrossAccounts { get; set; }

        // Propriétés DWINGS
        public string GUARANTEE_ID { get; set; }
        public string G_GUARANTEE_TYPE { get; set; }
        public string INVOICE_ID { get; set; }
        public string COMMISSION_ID { get; set; }
        public string SYNDICATE { get; set; }
        public decimal? GUARANTEE_AMOUNT { get; set; }
        public string GUARANTEE_CURRENCY { get; set; }
        public string GUARANTEE_STATUS { get; set; }
        // DWINGS guarantee type (raw code)
        public string GUARANTEE_TYPE { get; set; }

        // DWINGS Guarantee extra fields (prefixed with G_ to avoid collisions)
        public string G_NATURE { get; set; }
        public string G_EVENT_STATUS { get; set; }
        public string G_EVENT_EFFECTIVEDATE { get; set; }
        public string G_ISSUEDATE { get; set; }
        public string G_OFFICIALREF { get; set; }
        public string G_UNDERTAKINGEVENT { get; set; }
        public string G_PROCESS { get; set; }
        public string G_EXPIRYDATETYPE { get; set; }
        public string G_EXPIRYDATE { get; set; }
        public string G_PARTY_ID { get; set; }
        public string G_PARTY_REF { get; set; }
        public string G_SECONDARY_OBLIGOR { get; set; }
        public string G_SECONDARY_OBLIGOR_NATURE { get; set; }
        public string G_ROLE { get; set; }
        public string G_COUNTRY { get; set; }
        public string G_CENTRAL_PARTY_CODE { get; set; }
        public string G_NAME1 { get; set; }
        public string G_NAME2 { get; set; }
        public string G_GROUPE { get; set; }
        public string G_PREMIUM { get; set; }
        public string G_BRANCH_CODE { get; set; }
        public string G_BRANCH_NAME { get; set; }
        public string G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY { get; set; }
        public string G_CANCELLATIONDATE { get; set; }
        public string G_CONTROLER { get; set; }
        public string G_AUTOMATICBOOKOFF { get; set; }
        public string G_NATUREOFDEAL { get; set; }

        // DWINGS Invoice extra fields (prefixed with I_)
        public string I_REQUESTED_INVOICE_AMOUNT { get; set; }
        public string I_SENDER_NAME { get; set; }
        public string I_RECEIVER_NAME { get; set; }
        public string I_SENDER_REFERENCE { get; set; }
        public string I_RECEIVER_REFERENCE { get; set; }
        public string I_T_INVOICE_STATUS { get; set; }
        public string I_BILLING_AMOUNT { get; set; }
        public string I_BILLING_CURRENCY { get; set; }
        public string I_START_DATE { get; set; }
        public string I_END_DATE { get; set; }
        public string I_FINAL_AMOUNT { get; set; }
        public string I_T_COMMISSION_PERIOD_STATUS { get; set; }
        public string I_BUSINESS_CASE_REFERENCE { get; set; }
        public string I_BUSINESS_CASE_ID { get; set; }
        public string I_POSTING_PERIODICITY { get; set; }
        public string I_EVENT_ID { get; set; }
        public string I_COMMENTS { get; set; }
        public string I_SENDER_ACCOUNT_NUMBER { get; set; }
        public string I_SENDER_ACCOUNT_BIC { get; set; }
        public string I_RECEIVER_ACCOUNT_NUMBER { get; set; }
        public string I_RECEIVER_ACCOUNT_BIC { get; set; }
        public string I_REQUESTED_AMOUNT { get; set; }
        public string I_EXECUTED_AMOUNT { get; set; }
        public string I_REQUESTED_EXECUTION_DATE { get; set; }
        public string I_T_PAYMENT_REQUEST_STATUS { get; set; }
        public string I_BGPMT { get; set; }
        public string I_DEBTOR_ACCOUNT_ID { get; set; }
        public string I_CREDITOR_ACCOUNT_ID { get; set; }
        public string I_MT_STATUS { get; set; }
        public string I_REMINDER_NUMBER { get; set; }
        public string I_ERROR_MESSAGE { get; set; }
        public string I_DEBTOR_PARTY_ID { get; set; }
        public string I_PAYMENT_METHOD { get; set; }
        public string I_PAYMENT_TYPE { get; set; }
        public string I_DEBTOR_PARTY_NAME { get; set; }
        public string I_DEBTOR_ACCOUNT_NUMBER { get; set; }
        public string I_CREDITOR_PARTY_ID { get; set; }
        public string I_CREDITOR_ACCOUNT_NUMBER { get; set; }

        // Transient UI highlight flags (not persisted). Used by DataGrid RowStyle triggers
        private bool _isNewlyAdded;
        public bool IsNewlyAdded
        {
            get => _isNewlyAdded;
            set
            {
                if (_isNewlyAdded != value)
                {
                    _isNewlyAdded = value;
                    OnPropertyChanged(nameof(IsNewlyAdded));
                }
            }
        }

        // Derived duplicate indicator from service query
        public bool IsPotentialDuplicate { get; set; }

        private bool _isUpdated;
        public bool IsUpdated
        {
            get => _isUpdated;
            set
            {
                if (_isUpdated != value)
                {
                    _isUpdated = value;
                    OnPropertyChanged(nameof(IsUpdated));
                }
            }
        }

        private bool _isHighlighted;
        public bool IsHighlighted
        {
            get => _isHighlighted;
            set
            {
                if (_isHighlighted != value)
                {
                    _isHighlighted = value;
                    OnPropertyChanged(nameof(IsHighlighted));
                }
            }
        }

        // Display-only: human-friendly label for TransactionType stored in DataAmbre.Category
        // Example: INCOMING_PAYMENT -> "INCOMING PAYMENT"
        public string CategoryLabel
        {
            get
            {
                if (!this.Category.HasValue) return string.Empty;
                try
                {
                    var name = Enum.GetName(typeof(TransactionType), this.Category.Value);
                    if (string.IsNullOrWhiteSpace(name)) return string.Empty;
                    return name.Replace('_', ' ');
                }
                catch
                {
                    return string.Empty;
                }
            }
        }
    }

    #endregion
}

