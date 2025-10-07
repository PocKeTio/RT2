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
                        _dwingsInvoiceCache[inv.INVOICE_ID] = inv;
                    }
                }
            }
            
            if (guarantees != null)
            {
                foreach (var guar in guarantees)
                {
                    if (!string.IsNullOrWhiteSpace(guar.GUARANTEE_ID))
                    {
                        _dwingsGuaranteeCache[guar.GUARANTEE_ID] = guar;
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
        
        // Lazy-loaded DWINGS invoice data
        private DwingsInvoiceDto _cachedInvoice;
        private bool _invoiceLoaded;
        private DwingsInvoiceDto GetInvoiceData()
        {
            if (!_invoiceLoaded)
            {
                _invoiceLoaded = true;
                if (!string.IsNullOrWhiteSpace(DWINGS_InvoiceID) && _dwingsInvoiceCache != null)
                {
                    _dwingsInvoiceCache.TryGetValue(DWINGS_InvoiceID, out _cachedInvoice);
                }
            }
            return _cachedInvoice;
        }
        
        // Lazy-loaded DWINGS guarantee data
        private DwingsGuaranteeDto _cachedGuarantee;
        private bool _guaranteeLoaded;
        private DwingsGuaranteeDto GetGuaranteeData()
        {
            if (!_guaranteeLoaded)
            {
                _guaranteeLoaded = true;
                if (!string.IsNullOrWhiteSpace(DWINGS_GuaranteeID) && _dwingsGuaranteeCache != null)
                {
                    _dwingsGuaranteeCache.TryGetValue(DWINGS_GuaranteeID, out _cachedGuarantee);
                }
            }
            return _cachedGuarantee;
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
                    OnPropertyChanged(nameof(Action));
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsToReview));
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
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsReviewedToday));
                    OnPropertyChanged(nameof(IsToReview));
                    OnPropertyChanged(nameof(IsReviewed));
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
                    // Notify dependent properties
                    OnPropertyChanged(nameof(IsReviewedToday));
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

        // DWINGS Guarantee extra fields (prefixed with G_ to avoid collisions) - LAZY LOADED
        public string G_GUARANTEE_TYPE => GetGuaranteeData()?.GUARANTEE_TYPE;
        public string G_NATURE => GetGuaranteeData()?.NATURE;
        public string G_EVENT_STATUS => GetGuaranteeData()?.EVENT_STATUS;
        public string G_EVENT_EFFECTIVEDATE => GetGuaranteeData()?.EVENT_EFFECTIVEDATE?.ToString("yyyy-MM-dd");
        public string G_ISSUEDATE => GetGuaranteeData()?.ISSUEDATE?.ToString("yyyy-MM-dd");
        public string G_OFFICIALREF => GetGuaranteeData()?.OFFICIALREF;
        public string G_UNDERTAKINGEVENT => GetGuaranteeData()?.UNDERTAKINGEVENT;
        public string G_PROCESS => GetGuaranteeData()?.PROCESS;
        public string G_EXPIRYDATETYPE => GetGuaranteeData()?.EXPIRYDATETYPE;
        public string G_EXPIRYDATE => GetGuaranteeData()?.EXPIRYDATE?.ToString("yyyy-MM-dd");
        public string G_PARTY_ID => GetGuaranteeData()?.PARTY_ID;
        public string G_PARTY_REF => GetGuaranteeData()?.PARTY_REF;
        public string G_SECONDARY_OBLIGOR => GetGuaranteeData()?.SECONDARY_OBLIGOR;
        public string G_SECONDARY_OBLIGOR_NATURE => GetGuaranteeData()?.SECONDARY_OBLIGOR_NATURE;
        public string G_ROLE => GetGuaranteeData()?.ROLE;
        public string G_COUNTRY => GetGuaranteeData()?.COUNTRY;
        public string G_CENTRAL_PARTY_CODE => GetGuaranteeData()?.CENTRAL_PARTY_CODE;
        public string G_NAME1 => GetGuaranteeData()?.NAME1;
        public string G_NAME2 => GetGuaranteeData()?.NAME2;
        public string G_GROUPE => GetGuaranteeData()?.GROUPE;
        public string G_PREMIUM => GetGuaranteeData()?.PREMIUM?.ToString();
        public string G_BRANCH_CODE => GetGuaranteeData()?.BRANCH_CODE;
        public string G_BRANCH_NAME => GetGuaranteeData()?.BRANCH_NAME;
        public string G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY => GetGuaranteeData()?.OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY?.ToString();
        public string G_CANCELLATIONDATE => GetGuaranteeData()?.CANCELLATIONDATE?.ToString("yyyy-MM-dd");
        public string G_CONTROLER => GetGuaranteeData()?.CONTROLER;
        public string G_AUTOMATICBOOKOFF => GetGuaranteeData()?.AUTOMATICBOOKOFF;
        public string G_NATUREOFDEAL => GetGuaranteeData()?.NATUREOFDEAL;

        // DWINGS Invoice extra fields (prefixed with I_) - LAZY LOADED
        public string I_REQUESTED_INVOICE_AMOUNT => GetInvoiceData()?.REQUESTED_AMOUNT?.ToString();
        public string I_SENDER_NAME => GetInvoiceData()?.SENDER_NAME;
        public string I_RECEIVER_NAME => GetInvoiceData()?.RECEIVER_NAME;
        public string I_SENDER_REFERENCE => GetInvoiceData()?.SENDER_REFERENCE;
        public string I_RECEIVER_REFERENCE => GetInvoiceData()?.RECEIVER_REFERENCE;
        public string I_T_INVOICE_STATUS => GetInvoiceData()?.T_INVOICE_STATUS;
        public string I_BILLING_AMOUNT => GetInvoiceData()?.BILLING_AMOUNT?.ToString();
        public string I_BILLING_CURRENCY => GetInvoiceData()?.BILLING_CURRENCY;
        public string I_START_DATE => GetInvoiceData()?.START_DATE?.ToString("yyyy-MM-dd");
        public string I_END_DATE => GetInvoiceData()?.END_DATE?.ToString("yyyy-MM-dd");
        public string I_FINAL_AMOUNT => GetInvoiceData()?.FINAL_AMOUNT?.ToString();
        public string I_T_COMMISSION_PERIOD_STATUS => null; // Not in DwingsInvoiceDto
        public string I_BUSINESS_CASE_REFERENCE => GetInvoiceData()?.BUSINESS_CASE_REFERENCE;
        public string I_BUSINESS_CASE_ID => GetInvoiceData()?.BUSINESS_CASE_ID;
        public string I_POSTING_PERIODICITY => null; // Not in DwingsInvoiceDto
        public string I_EVENT_ID => null; // Not in DwingsInvoiceDto
        public string I_COMMENTS => null; // Not in DwingsInvoiceDto
        public string I_SENDER_ACCOUNT_NUMBER => GetInvoiceData()?.SENDER_ACCOUNT_NUMBER;
        public string I_SENDER_ACCOUNT_BIC => GetInvoiceData()?.SENDER_ACCOUNT_BIC;
        public string I_RECEIVER_ACCOUNT_NUMBER => null; // Not in DwingsInvoiceDto
        public string I_RECEIVER_ACCOUNT_BIC => null; // Not in DwingsInvoiceDto
        public string I_REQUESTED_AMOUNT => GetInvoiceData()?.REQUESTED_AMOUNT?.ToString();
        public string I_EXECUTED_AMOUNT => null; // Not in DwingsInvoiceDto
        public string I_REQUESTED_EXECUTION_DATE => GetInvoiceData()?.REQUESTED_EXECUTION_DATE?.ToString("yyyy-MM-dd");
        public string I_T_PAYMENT_REQUEST_STATUS => GetInvoiceData()?.T_PAYMENT_REQUEST_STATUS;
        public string I_BGPMT => GetInvoiceData()?.BGPMT;
        public string I_DEBTOR_ACCOUNT_ID => null; // Not in DwingsInvoiceDto
        public string I_CREDITOR_ACCOUNT_ID => null; // Not in DwingsInvoiceDto
        public string I_MT_STATUS => GetInvoiceData()?.MT_STATUS;
        public string I_REMINDER_NUMBER => null; // Not in DwingsInvoiceDto
        public string I_ERROR_MESSAGE => GetInvoiceData()?.ERROR_MESSAGE;
        public string I_DEBTOR_PARTY_ID => null; // Not in DwingsInvoiceDto
        public string I_PAYMENT_METHOD => GetInvoiceData()?.PAYMENT_METHOD;
        public string I_PAYMENT_TYPE => null; // Not in DwingsInvoiceDto
        public string I_DEBTOR_PARTY_NAME => GetInvoiceData()?.DEBTOR_PARTY_NAME;

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

        /// <summary>
        /// Indicates if this row was reviewed today (based on ActionDate when status = Done)
        /// </summary>
        public bool IsReviewedToday => ActionStatus == true && ActionDate.HasValue && ActionDate.Value.Date == DateTime.Today;

        /// <summary>
        /// Indicates if this row is "To Review" (has an action but status is Pending)
        /// </summary>
        public bool IsToReview => Action.HasValue && (ActionStatus == false || !ActionStatus.HasValue);

        /// <summary>
        /// Indicates if this row is "Reviewed" (action status is Done)
        /// </summary>
        public bool IsReviewed => ActionStatus == true;
        
        /// <summary>
        /// Indicates if this row has an active reminder (ToRemind = true and ToRemindDate <= today)
        /// </summary>
        public bool HasActiveReminder => ToRemind && ToRemindDate.HasValue && ToRemindDate.Value.Date <= DateTime.Today;
        
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
                    OnPropertyChanged(nameof(MissingAmount));
                    InvalidateStatusCache();
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(StatusTooltip));
                }
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

