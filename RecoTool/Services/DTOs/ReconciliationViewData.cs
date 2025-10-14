using System;
using System.Collections.Generic;
using System.ComponentModel;
using RecoTool.Models;
using RecoTool.Services;

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
        
        // Static cache for DWINGS data (shared across all instances)
        private static Dictionary<string, DwingsInvoiceDto> _dwingsInvoiceCache;
        private static Dictionary<string, DwingsGuaranteeDto> _dwingsGuaranteeCache;
        
        /// <summary>
        /// Initialize DWINGS caches (called once during data load)
        /// </summary>
        public static void InitializeDwingsCaches(IEnumerable<DwingsInvoiceDto> invoices, IEnumerable<DwingsGuaranteeDto> guarantees)
        {
            _dwingsInvoiceCache = new Dictionary<string, DwingsInvoiceDto>(StringComparer.OrdinalIgnoreCase);
            _dwingsGuaranteeCache = new Dictionary<string, DwingsGuaranteeDto>(StringComparer.OrdinalIgnoreCase);
            
            if (invoices != null)
            {
                foreach (var inv in invoices)
                {
                    if (!string.IsNullOrWhiteSpace(inv.INVOICE_ID))
                    {
                        // NOTE: INVOICE_ID is NOT unique - keep first occurrence
                        if (!_dwingsInvoiceCache.ContainsKey(inv.INVOICE_ID))
                        {
                            _dwingsInvoiceCache[inv.INVOICE_ID] = inv;
                        }
                    }
                }
            }
            
            if (guarantees != null)
            {
                foreach (var guar in guarantees)
                {
                    if (!string.IsNullOrWhiteSpace(guar.GUARANTEE_ID))
                    {
                        // NOTE: GUARANTEE_ID should be unique, but check anyway
                        if (!_dwingsGuaranteeCache.ContainsKey(guar.GUARANTEE_ID))
                        {
                            _dwingsGuaranteeCache[guar.GUARANTEE_ID] = guar;
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Clear DWINGS caches (called when country changes)
        /// </summary>
        public static void ClearDwingsCaches()
        {
            _dwingsInvoiceCache?.Clear();
            _dwingsGuaranteeCache?.Clear();
        }
        
        /// <summary>
        /// Refresh DWINGS data when user manually changes DWINGS_InvoiceID or DWINGS_GuaranteeID
        /// Call this after user edits to recalculate all DWINGS-linked properties
        /// </summary>
        public void RefreshDwingsData()
        {
            // Reload invoice data
            DwingsInvoiceDto invoice = null;
            if (!string.IsNullOrWhiteSpace(DWINGS_InvoiceID) && _dwingsInvoiceCache != null)
            {
                _dwingsInvoiceCache.TryGetValue(DWINGS_InvoiceID, out invoice);
            }
            
            // Reload guarantee data
            DwingsGuaranteeDto guarantee = null;
            if (!string.IsNullOrWhiteSpace(DWINGS_GuaranteeID) && _dwingsGuaranteeCache != null)
            {
                _dwingsGuaranteeCache.TryGetValue(DWINGS_GuaranteeID, out guarantee);
            }
            
            // Update all invoice properties
            PopulateInvoiceProperties(invoice);
            
            // Update all guarantee properties
            PopulateGuaranteeProperties(guarantee);
            
            // Notify all dependent properties
            NotifyAllDwingsProperties();
        }
        
        /// <summary>
        /// Initialize DWINGS properties from invoice data (called once during load or after user edit)
        /// </summary>
        internal void PopulateInvoiceProperties(DwingsInvoiceDto invoice)
        {
            I_REQUESTED_INVOICE_AMOUNT = invoice?.REQUESTED_AMOUNT?.ToString();
            I_SENDER_NAME = invoice?.SENDER_NAME;
            I_RECEIVER_NAME = invoice?.RECEIVER_NAME;
            I_SENDER_REFERENCE = invoice?.SENDER_REFERENCE;
            I_RECEIVER_REFERENCE = invoice?.RECEIVER_REFERENCE;
            I_T_INVOICE_STATUS = invoice?.T_INVOICE_STATUS;
            _hasEmail = invoice?.COMM_ID_EMAIL;
            I_BILLING_AMOUNT = invoice?.BILLING_AMOUNT?.ToString();
            I_BILLING_CURRENCY = invoice?.BILLING_CURRENCY;
            I_START_DATE = invoice?.START_DATE?.ToString("yyyy-MM-dd");
            I_END_DATE = invoice?.END_DATE?.ToString("yyyy-MM-dd");
            I_FINAL_AMOUNT = invoice?.FINAL_AMOUNT?.ToString();
            I_BUSINESS_CASE_REFERENCE = invoice?.BUSINESS_CASE_REFERENCE;
            I_BUSINESS_CASE_ID = invoice?.BUSINESS_CASE_ID;
            I_SENDER_ACCOUNT_NUMBER = invoice?.SENDER_ACCOUNT_NUMBER;
            I_SENDER_ACCOUNT_BIC = invoice?.SENDER_ACCOUNT_BIC;
            I_REQUESTED_AMOUNT = invoice?.REQUESTED_AMOUNT?.ToString();
            I_REQUESTED_EXECUTION_DATE = invoice?.REQUESTED_EXECUTION_DATE?.ToString("yyyy-MM-dd");
            I_T_PAYMENT_REQUEST_STATUS = invoice?.T_PAYMENT_REQUEST_STATUS;
            I_BGPMT = invoice?.BGPMT;
            I_MT_STATUS = invoice?.MT_STATUS;
            I_ERROR_MESSAGE = invoice?.ERROR_MESSAGE;
            I_PAYMENT_METHOD = invoice?.PAYMENT_METHOD;
            I_DEBTOR_PARTY_NAME = invoice?.DEBTOR_PARTY_NAME;
        }
        
        /// <summary>
        /// Initialize DWINGS properties from guarantee data (called once during load or after user edit)
        /// </summary>
        internal void PopulateGuaranteeProperties(DwingsGuaranteeDto guarantee)
        {
            G_GUARANTEE_TYPE = guarantee?.GUARANTEE_TYPE;
            G_NATURE = guarantee?.NATURE;
            G_EVENT_STATUS = guarantee?.EVENT_STATUS;
            G_EVENT_EFFECTIVEDATE = guarantee?.EVENT_EFFECTIVEDATE?.ToString("yyyy-MM-dd");
            G_ISSUEDATE = guarantee?.ISSUEDATE?.ToString("yyyy-MM-dd");
            G_OFFICIALREF = guarantee?.OFFICIALREF;
            G_UNDERTAKINGEVENT = guarantee?.UNDERTAKINGEVENT;
            G_PROCESS = guarantee?.PROCESS;
            G_EXPIRYDATETYPE = guarantee?.EXPIRYDATETYPE;
            G_EXPIRYDATE = guarantee?.EXPIRYDATE?.ToString("yyyy-MM-dd");
            G_PARTY_ID = guarantee?.PARTY_ID;
            G_PARTY_REF = guarantee?.PARTY_REF;
            G_SECONDARY_OBLIGOR = guarantee?.SECONDARY_OBLIGOR;
            G_SECONDARY_OBLIGOR_NATURE = guarantee?.SECONDARY_OBLIGOR_NATURE;
            G_ROLE = guarantee?.ROLE;
            G_COUNTRY = guarantee?.COUNTRY;
            G_CENTRAL_PARTY_CODE = guarantee?.CENTRAL_PARTY_CODE;
            G_NAME1 = guarantee?.NAME1;
            G_NAME2 = guarantee?.NAME2;
            G_GROUPE = guarantee?.GROUPE;
            G_PREMIUM = guarantee?.PREMIUM?.ToString();
            G_BRANCH_CODE = guarantee?.BRANCH_CODE;
            G_BRANCH_NAME = guarantee?.BRANCH_NAME;
            G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY = guarantee?.OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY?.ToString();
            G_CANCELLATIONDATE = guarantee?.CANCELLATIONDATE?.ToString("yyyy-MM-dd");
            G_CONTROLER = guarantee?.CONTROLER;
            G_AUTOMATICBOOKOFF = guarantee?.AUTOMATICBOOKOFF;
            G_NATUREOFDEAL = guarantee?.NATUREOFDEAL;
        }
        
        private void NotifyAllDwingsProperties()
        {
            // Invoice properties
            OnPropertyChanged(nameof(I_REQUESTED_INVOICE_AMOUNT));
            OnPropertyChanged(nameof(I_SENDER_NAME));
            OnPropertyChanged(nameof(I_RECEIVER_NAME));
            OnPropertyChanged(nameof(I_SENDER_REFERENCE));
            OnPropertyChanged(nameof(I_RECEIVER_REFERENCE));
            OnPropertyChanged(nameof(I_T_INVOICE_STATUS));
            OnPropertyChanged(nameof(HasEmail));
            OnPropertyChanged(nameof(I_BILLING_AMOUNT));
            OnPropertyChanged(nameof(I_BILLING_CURRENCY));
            OnPropertyChanged(nameof(I_START_DATE));
            OnPropertyChanged(nameof(I_END_DATE));
            OnPropertyChanged(nameof(I_FINAL_AMOUNT));
            OnPropertyChanged(nameof(I_BUSINESS_CASE_REFERENCE));
            OnPropertyChanged(nameof(I_BUSINESS_CASE_ID));
            OnPropertyChanged(nameof(I_SENDER_ACCOUNT_NUMBER));
            OnPropertyChanged(nameof(I_SENDER_ACCOUNT_BIC));
            OnPropertyChanged(nameof(I_REQUESTED_AMOUNT));
            OnPropertyChanged(nameof(I_REQUESTED_EXECUTION_DATE));
            OnPropertyChanged(nameof(I_T_PAYMENT_REQUEST_STATUS));
            OnPropertyChanged(nameof(I_BGPMT));
            OnPropertyChanged(nameof(I_MT_STATUS));
            OnPropertyChanged(nameof(I_ERROR_MESSAGE));
            OnPropertyChanged(nameof(I_PAYMENT_METHOD));
            OnPropertyChanged(nameof(I_DEBTOR_PARTY_NAME));
            
            // Guarantee properties
            OnPropertyChanged(nameof(G_GUARANTEE_TYPE));
            OnPropertyChanged(nameof(G_NATURE));
            OnPropertyChanged(nameof(G_EVENT_STATUS));
            OnPropertyChanged(nameof(G_EVENT_EFFECTIVEDATE));
            OnPropertyChanged(nameof(G_ISSUEDATE));
            OnPropertyChanged(nameof(G_OFFICIALREF));
            OnPropertyChanged(nameof(G_UNDERTAKINGEVENT));
            OnPropertyChanged(nameof(G_PROCESS));
            OnPropertyChanged(nameof(G_EXPIRYDATETYPE));
            OnPropertyChanged(nameof(G_EXPIRYDATE));
            OnPropertyChanged(nameof(G_PARTY_ID));
            OnPropertyChanged(nameof(G_PARTY_REF));
            OnPropertyChanged(nameof(G_SECONDARY_OBLIGOR));
            OnPropertyChanged(nameof(G_SECONDARY_OBLIGOR_NATURE));
            OnPropertyChanged(nameof(G_ROLE));
            OnPropertyChanged(nameof(G_COUNTRY));
            OnPropertyChanged(nameof(G_CENTRAL_PARTY_CODE));
            OnPropertyChanged(nameof(G_NAME1));
            OnPropertyChanged(nameof(G_NAME2));
            OnPropertyChanged(nameof(G_GROUPE));
            OnPropertyChanged(nameof(G_PREMIUM));
            OnPropertyChanged(nameof(G_BRANCH_CODE));
            OnPropertyChanged(nameof(G_BRANCH_NAME));
            OnPropertyChanged(nameof(G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY));
            OnPropertyChanged(nameof(G_CANCELLATIONDATE));
            OnPropertyChanged(nameof(G_CONTROLER));
            OnPropertyChanged(nameof(G_AUTOMATICBOOKOFF));
            OnPropertyChanged(nameof(G_NATUREOFDEAL));
        }

        // Propriétés de Reconciliation
        public string DWINGS_GuaranteeID { get; set; }
        
        private string _dwingsInvoiceID;
        public string DWINGS_InvoiceID 
        { 
            get => _dwingsInvoiceID;
            set
            {
                if (_dwingsInvoiceID != value)
                {
                    _dwingsInvoiceID = value;
                    OnPropertyChanged(nameof(DWINGS_InvoiceID));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(StatusTooltip));
                }
            }
        }
        
        public string DWINGS_BGPMT { get; set; }
        private int? _action;
        public int? Action
        {
            get => _action;
            set
            {
                if (_action != value)
                {
                    _action = value;
                    _cachedIsToReview = null;
                    _cachedIsReviewed = null;
                    OnPropertyChanged(nameof(Action));
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsToReview));
                    OnPropertyChanged(nameof(IsReviewed));
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
        
        private string _internalInvoiceReference;
        public string InternalInvoiceReference 
        { 
            get => _internalInvoiceReference;
            set
            {
                if (_internalInvoiceReference != value)
                {
                    _internalInvoiceReference = value;
                    OnPropertyChanged(nameof(InternalInvoiceReference));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(StatusTooltip));
                }
            }
        }
        
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
                    _cachedHasActiveReminder = null;
                    OnPropertyChanged(nameof(ToRemind));
                    OnPropertyChanged(nameof(HasActiveReminder));
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
                    _cachedHasActiveReminder = null;
                    OnPropertyChanged(nameof(ToRemindDate));
                    OnPropertyChanged(nameof(HasActiveReminder));
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
                    _cachedIsReviewedToday = null;
                    _cachedIsToReview = null;
                    _cachedIsReviewed = null;
                    OnPropertyChanged(nameof(ActionStatus));
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsReviewedToday));
                    OnPropertyChanged(nameof(IsToReview));
                    OnPropertyChanged(nameof(IsReviewed));
                    OnPropertyChanged(nameof(CellBackgroundColor));
                    OnPropertyChanged(nameof(CellBorderBrush));
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
                    _cachedIsReviewedToday = null;
                    OnPropertyChanged(nameof(ActionDate));
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsReviewedToday));
                    OnPropertyChanged(nameof(CellBackgroundColor));
                    OnPropertyChanged(nameof(CellBorderBrush));
                }
            }
        }
        // DEPRECATED: ReviewDate removed - use ActionStatus/ActionDate instead
        // ToReview = Action.HasValue && ActionStatus != true
        // Reviewed = ActionStatus == true
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
        private bool _riskyItem;
        public bool RiskyItem
        {
            get => _riskyItem;
            set
            {
                if (_riskyItem != value)
                {
                    _riskyItem = value;
                    OnPropertyChanged(nameof(RiskyItem));
                }
            }
        }
        private int? _reasonNonRisky;
        public int? ReasonNonRisky
        {
            get => _reasonNonRisky;
            set
            {
                if (_reasonNonRisky != value)
                {
                    _reasonNonRisky = value;
                    OnPropertyChanged(nameof(ReasonNonRisky));
                }
            }
        }
        
        private string _incNumber;
        public string IncNumber
        {
            get => _incNumber;
            set
            {
                if (_incNumber != value)
                {
                    _incNumber = value;
                    OnPropertyChanged(nameof(IncNumber));
                }
            }
        }
        
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
        private bool _isMatchedAcrossAccounts;
        public bool IsMatchedAcrossAccounts
        {
            get => _isMatchedAcrossAccounts;
            set
            {
                if (_isMatchedAcrossAccounts != value)
                {
                    _isMatchedAcrossAccounts = value;
                    OnPropertyChanged(nameof(IsMatchedAcrossAccounts));
                    OnPropertyChanged(nameof(IsMatchedAcrossAccountsVisibility));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(StatusTooltip));
                }
            }
        }

        // Propriétés DWINGS
        public string GUARANTEE_ID { get; set; }
        public string INVOICE_ID { get; set; }
        public string COMMISSION_ID { get; set; }
        public string SYNDICATE { get; set; }
        public decimal? GUARANTEE_AMOUNT { get; set; }
        public string GUARANTEE_CURRENCY { get; set; }
        public string GUARANTEE_STATUS { get; set; }
        // DWINGS guarantee type (raw code)
        public string GUARANTEE_TYPE { get; set; }

        // DWINGS Guarantee extra fields (prefixed with G_ to avoid collisions) - PRE-CALCULATED
        public string G_GUARANTEE_TYPE { get; set; }
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

        // DWINGS Invoice extra fields (prefixed with I_) - PRE-CALCULATED
        public string I_REQUESTED_INVOICE_AMOUNT { get; set; }
        public string I_SENDER_NAME { get; set; }
        public string I_RECEIVER_NAME { get; set; }
        public string I_SENDER_REFERENCE { get; set; }
        public string I_RECEIVER_REFERENCE { get; set; }
        public string I_T_INVOICE_STATUS { get; set; }
        
        /// <summary>
        /// HasEmail: True if COMM_ID_EMAIL flag is set on the DWINGS invoice (used for rules)
        /// </summary>
        private bool? _hasEmail;
        public bool? HasEmail => _hasEmail;
        public string I_BILLING_AMOUNT { get; set; }
        public string I_BILLING_CURRENCY { get; set; }
        public string I_START_DATE { get; set; }
        public string I_END_DATE { get; set; }
        public string I_FINAL_AMOUNT { get; set; }
        public string I_T_COMMISSION_PERIOD_STATUS => null; // Not in DwingsInvoiceDto
        public string I_BUSINESS_CASE_REFERENCE { get; set; }
        public string I_BUSINESS_CASE_ID { get; set; }
        public string I_POSTING_PERIODICITY => null; // Not in DwingsInvoiceDto
        public string I_EVENT_ID => null; // Not in DwingsInvoiceDto
        public string I_COMMENTS => null; // Not in DwingsInvoiceDto
        public string I_SENDER_ACCOUNT_NUMBER { get; set; }
        public string I_SENDER_ACCOUNT_BIC { get; set; }
        public string I_RECEIVER_ACCOUNT_NUMBER => null; // Not in DwingsInvoiceDto
        public string I_RECEIVER_ACCOUNT_BIC => null; // Not in DwingsInvoiceDto
        public string I_REQUESTED_AMOUNT { get; set; }
        public string I_EXECUTED_AMOUNT => null; // Not in DwingsInvoiceDto
        public string I_REQUESTED_EXECUTION_DATE { get; set; }
        public string I_T_PAYMENT_REQUEST_STATUS { get; set; }
        public string I_BGPMT { get; set; }
        public string I_DEBTOR_ACCOUNT_ID => null; // Not in DwingsInvoiceDto
        public string I_CREDITOR_ACCOUNT_ID => null; // Not in DwingsInvoiceDto
        public string I_MT_STATUS { get; set; }
        public string I_REMINDER_NUMBER => null; // Not in DwingsInvoiceDto
        public string I_ERROR_MESSAGE { get; set; }
        public string I_DEBTOR_PARTY_ID => null; // Not in DwingsInvoiceDto
        public string I_PAYMENT_METHOD { get; set; }
        public string I_PAYMENT_TYPE => null; // Not in DwingsInvoiceDto
        public string I_DEBTOR_PARTY_NAME { get; set; }

        /// <summary>
        /// Gets the TransactionType for receivable based on PAYMENT_METHOD from BGI
        /// </summary>
        public TransactionType? GetReceivableTransactionType()
        {
            if (string.IsNullOrWhiteSpace(I_PAYMENT_METHOD)) return null;
            
            return I_PAYMENT_METHOD.ToUpperInvariant() switch
            {
                "INCOMING_PAYMENT" => TransactionType.INCOMING_PAYMENT,
                "DIRECT_DEBIT" => TransactionType.DIRECT_DEBIT,
                "MANUAL_OUTGOING" => TransactionType.MANUAL_OUTGOING,
                "OUTGOING_PAYMENT" => TransactionType.OUTGOING_PAYMENT,
                "EXTERNAL_DEBIT_PAYMENT" => TransactionType.EXTERNAL_DEBIT_PAYMENT,
                _ => null
            };
        }
        public string I_DEBTOR_ACCOUNT_NUMBER => null; // Not in DwingsInvoiceDto
        public string I_CREDITOR_PARTY_ID => null; // Not in DwingsInvoiceDto
        public string I_CREDITOR_ACCOUNT_NUMBER => null; // Not in DwingsInvoiceDto

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
                    OnPropertyChanged(nameof(StatusIndicator));
                    OnPropertyChanged(nameof(HasStatusIndicator));
                    OnPropertyChanged(nameof(HasStatusIndicatorVisibility));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusTooltip));
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
                    OnPropertyChanged(nameof(StatusIndicator));
                    OnPropertyChanged(nameof(HasStatusIndicator));
                    OnPropertyChanged(nameof(HasStatusIndicatorVisibility));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusTooltip));
                }
            }
        }

        /// <summary>
        /// Computed property for status indicator text
        /// New takes precedence over Updated (if new, it's inherently updated)
        /// </summary>
        public string StatusIndicator
        {
            get
            {
                if (_isNewlyAdded) return "N";
                if (_isUpdated) return "U";
                return string.Empty;
            }
        }

        /// <summary>
        /// Returns true if there's any status indicator to show
        /// </summary>
        public bool HasStatusIndicator => _isNewlyAdded || _isUpdated;
        
        /// <summary>
        /// Visibility for HasStatusIndicator (replaces BoolToVisibilityConverter)
        /// OPTIMIZED: Pre-calculated to avoid converter overhead on every scroll
        /// </summary>
        public System.Windows.Visibility HasStatusIndicatorVisibility => 
            HasStatusIndicator ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        
        /// <summary>
        /// Visibility for IsMatchedAcrossAccounts (replaces BoolToVisibilityConverter)
        /// OPTIMIZED: Pre-calculated to avoid converter overhead on every scroll
        /// </summary>
        public System.Windows.Visibility IsMatchedAcrossAccountsVisibility => 
            _isMatchedAcrossAccounts ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

        /// <summary>
        /// Computed property for status color based on reconciliation completeness
        /// Red: Not linked with DWINGS
        /// Orange: Not grouped (IsMatchedAcrossAccounts = false)
        /// Dark Amber: Large discrepancy (> 100)
        /// Yellow: Small discrepancy
        /// Green: Balanced and grouped
        /// </summary>
        public string StatusColor
        {
            get
            {
                if (_cachedStatusColor != null) return _cachedStatusColor;
                return _cachedStatusColor = CalculateStatusColor();
            }
        }

        private string _cachedStatusColor;
        private string CalculateStatusColor()
        {
            // Red: No DWINGS link
            if (string.IsNullOrWhiteSpace(DWINGS_InvoiceID) && string.IsNullOrWhiteSpace(InternalInvoiceReference))
                return "#F44336"; // Red

            // Orange: Not grouped (not matched across accounts)
            if (!_isMatchedAcrossAccounts)
                return "#FF9800"; // Orange

            // Yellow/Amber: Grouped but missing amount != 0
            if (_missingAmount.HasValue && _missingAmount.Value != 0)
            {
                // Dark amber for large discrepancies (> 100)
                if (Math.Abs(_missingAmount.Value) > 100)
                    return "#FF6F00"; // Dark Amber
                // Light yellow for small discrepancies
                return "#FFC107"; // Yellow
            }

            // Green: Balanced and grouped
            return "#4CAF50"; // Green
        }

        /// <summary>
        /// Tooltip explaining the status with actionable guidance
        /// </summary>
        public string StatusTooltip
        {
            get
            {
                if (_cachedStatusTooltip != null) return _cachedStatusTooltip;
                return _cachedStatusTooltip = CalculateStatusTooltip();
            }
        }

        private string _cachedStatusTooltip;
        private string CalculateStatusTooltip()
        {
            var status = _isNewlyAdded ? "New" : (_isUpdated ? "Updated" : "");
            var reconciliationStatus = "";
            var action = "";

            if (string.IsNullOrWhiteSpace(DWINGS_InvoiceID) && string.IsNullOrWhiteSpace(InternalInvoiceReference))
            {
                reconciliationStatus = "Not linked with DWINGS";
                action = "→ Add DWINGS Invoice ID or Internal Reference";
            }
            else if (!_isMatchedAcrossAccounts)
            {
                reconciliationStatus = "Not grouped";
                action = "→ Check if matching entry exists on other account side";
            }
            else if (_missingAmount.HasValue && _missingAmount.Value != 0)
            {
                reconciliationStatus = $"Missing amount: {_missingAmount.Value:N2}";
                action = Math.Abs(_missingAmount.Value) > 100
                    ? "→ Large discrepancy - verify amounts"
                    : "→ Small discrepancy - may need adjustment";
            }
            else
            {
                reconciliationStatus = "Balanced and grouped";
                action = "✓ Ready for review";
            }

            var tooltip = string.IsNullOrEmpty(status)
                ? $"{reconciliationStatus}\n{action}"
                : $"{status} - {reconciliationStatus}\n{action}";

            return tooltip;
        }

        /// <summary>
        /// Invalidate cached status values (call when properties change)
        /// </summary>
        private void InvalidateStatusCache()
        {
            _cachedStatusColor = null;
            _cachedStatusTooltip = null;
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
        // CACHED to avoid Enum.GetName + Replace on every scroll
        private string _cachedCategoryLabel;
        public string CategoryLabel
        {
            get
            {
                if (_cachedCategoryLabel != null) return _cachedCategoryLabel;
                
                if (!this.Category.HasValue) return _cachedCategoryLabel = string.Empty;
                try
                {
                    var name = Enum.GetName(typeof(TransactionType), this.Category.Value);
                    if (string.IsNullOrWhiteSpace(name)) return _cachedCategoryLabel = string.Empty;
                    return _cachedCategoryLabel = name.Replace('_', ' ');
                }
                catch
                {
                    return _cachedCategoryLabel = string.Empty;
                }
            }
        }

        /// <summary>
        /// Indicates if this row was reviewed today (based on ActionDate when status = Done)
        /// CACHED to avoid recalculating DateTime.Today on every scroll
        /// </summary>
        private bool? _cachedIsReviewedToday;
        public bool IsReviewedToday
        {
            get
            {
                if (_cachedIsReviewedToday.HasValue) return _cachedIsReviewedToday.Value;
                return (_cachedIsReviewedToday = ActionStatus == true && ActionDate.HasValue && ActionDate.Value.Date == DateTime.Today).Value;
            }
        }
        
        /// <summary>
        /// Background color for cells when reviewed today (replaces DataTrigger)
        /// OPTIMIZED: Pre-calculated to eliminate 200+ DataTrigger evaluations per scroll frame
        /// </summary>
        public string CellBackgroundColor => IsReviewedToday ? "#E8F5E9" : "Transparent";
        
        /// <summary>
        /// Border brush for cells when reviewed today (replaces DataTrigger)
        /// OPTIMIZED: Pre-calculated to eliminate DataTrigger overhead
        /// </summary>
        public string CellBorderBrush => IsReviewedToday ? "#A5D6A7" : "#DDDDDD";

        /// <summary>
        /// Indicates if this row is "To Review" (has an action AND status is Pending)
        /// NOTE: Action = null/empty is considered N/A (Done by default)
        /// CACHED to avoid recalculation on every scroll
        /// </summary>
        private bool? _cachedIsToReview;
        public bool IsToReview
        {
            get
            {
                if (_cachedIsToReview.HasValue) return _cachedIsToReview.Value;
                return (_cachedIsToReview = Action.HasValue && Action.Value > 0 && (ActionStatus == false || !ActionStatus.HasValue)).Value;
            }
        }

        /// <summary>
        /// Indicates if this row is "Reviewed" (action status is Done OR action is null/N/A)
        /// NOTE: Action = null/empty is considered N/A (Done by default)
        /// CACHED to avoid recalculation on every scroll
        /// </summary>
        private bool? _cachedIsReviewed;
        public bool IsReviewed
        {
            get
            {
                if (_cachedIsReviewed.HasValue) return _cachedIsReviewed.Value;
                return (_cachedIsReviewed = ActionStatus == true || !Action.HasValue || Action.Value == 0).Value;
            }
        }
        
        /// <summary>
        /// Indicates if this row has an active reminder (ToRemind = true and ToRemindDate <= today)
        /// CACHED to avoid recalculating DateTime.Today on every scroll
        /// </summary>
        private bool? _cachedHasActiveReminder;
        public bool HasActiveReminder
        {
            get
            {
                if (_cachedHasActiveReminder.HasValue) return _cachedHasActiveReminder.Value;
                return (_cachedHasActiveReminder = ToRemind && ToRemindDate.HasValue && ToRemindDate.Value.Date <= DateTime.Today).Value;
            }
        }
        
        /// <summary>
        /// Missing amount when Receivable is grouped with multiple Pivot lines
        /// Positive = Receivable > Pivot (waiting for more payments)
        /// Negative = Pivot > Receivable (overpayment)
        /// Null = not grouped or same account side
        /// </summary>
        private decimal? _missingAmount;
        public decimal? MissingAmount
        {
            get => _missingAmount;
            set
            {
                if (_missingAmount != value)
                {
                    _missingAmount = value;
                    // Invalidate cached colors/styles
                    _cachedMissingAmountBackgroundColor = null;
                    _cachedMissingAmountForegroundColor = null;
                    _cachedMissingAmountFontWeight = null;
                    OnPropertyChanged(nameof(MissingAmount));
                    OnPropertyChanged(nameof(MissingAmountBackgroundColor));
                    OnPropertyChanged(nameof(MissingAmountForegroundColor));
                    OnPropertyChanged(nameof(MissingAmountFontWeight));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(StatusTooltip));
                }
            }
        }
        
        /// <summary>
        /// Pre-calculated background color for MissingAmount column (avoids converter overhead during scroll)
        /// Green: Balanced (0), Orange: Waiting for payment (>0), Red: Overpayment (<0)
        /// </summary>
        private string _cachedMissingAmountBackgroundColor;
        public string MissingAmountBackgroundColor
        {
            get
            {
                if (_cachedMissingAmountBackgroundColor != null) return _cachedMissingAmountBackgroundColor;
                
                if (!_missingAmount.HasValue || _missingAmount.Value == 0)
                    return _cachedMissingAmountBackgroundColor = "#E8F5E9"; // Light green - balanced
                
                if (_missingAmount.Value > 0)
                    return _cachedMissingAmountBackgroundColor = "#FFF3E0"; // Orange - waiting for payment
                
                return _cachedMissingAmountBackgroundColor = "#FFEBEE"; // Red - overpayment
            }
        }
        
        /// <summary>
        /// Pre-calculated foreground color for MissingAmount column (avoids converter overhead during scroll)
        /// </summary>
        private string _cachedMissingAmountForegroundColor;
        public string MissingAmountForegroundColor
        {
            get
            {
                if (_cachedMissingAmountForegroundColor != null) return _cachedMissingAmountForegroundColor;
                
                if (!_missingAmount.HasValue || _missingAmount.Value == 0)
                    return _cachedMissingAmountForegroundColor = "#2E7D32"; // Green
                
                if (_missingAmount.Value > 0)
                    return _cachedMissingAmountForegroundColor = "#EF6C00"; // Orange
                
                return _cachedMissingAmountForegroundColor = "#C62828"; // Red
            }
        }
        
        /// <summary>
        /// Pre-calculated font weight for MissingAmount column (avoids converter overhead during scroll)
        /// </summary>
        private string _cachedMissingAmountFontWeight;
        public string MissingAmountFontWeight
        {
            get
            {
                if (_cachedMissingAmountFontWeight != null) return _cachedMissingAmountFontWeight;
                return _cachedMissingAmountFontWeight = "SemiBold";
            }
        }
        
        /// <summary>
        /// Total amount of counterpart lines in the same group
        /// </summary>
        private decimal? _counterpartTotalAmount;
        public decimal? CounterpartTotalAmount
        {
            get => _counterpartTotalAmount;
            set
            {
                if (_counterpartTotalAmount != value)
                {
                    _counterpartTotalAmount = value;
                    OnPropertyChanged(nameof(CounterpartTotalAmount));
                }
            }
        }
        
        /// <summary>
        /// Number of counterpart lines in the same group
        /// </summary>
        private int? _counterpartCount;
        public int? CounterpartCount
        {
            get => _counterpartCount;
            set
            {
                if (_counterpartCount != value)
                {
                    _counterpartCount = value;
                    OnPropertyChanged(nameof(CounterpartCount));
                }
            }
        }
    }

    #endregion
}

