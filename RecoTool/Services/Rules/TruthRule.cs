using System;
using System.Collections.Generic;

namespace RecoTool.Services.Rules
{
    public enum RuleScope
    {
        Import,
        Edit,
        Both
    }

    public enum ApplyTarget
    {
        Self,
        Counterpart,
        Both
    }

    /// <summary>
    /// Declarative rule loaded from the referential truth-table or JSON fallback.
    /// Each condition is optional: when null/empty, it does not restrict matching.
    /// </summary>
    public class TruthRule
    {
        public string RuleId { get; set; }
        public bool Enabled { get; set; } = true;
        public int Priority { get; set; } = 100;
        public RuleScope Scope { get; set; } = RuleScope.Both;

        // Conditions
        // 'P' (Pivot), 'R' (Receivable), or '*' for any
        public string AccountSide { get; set; } = "*";
        // Semi-colon or comma separated list (e.g., "ISSU;REISSU;NOTIF"). '*' for any.
        public string GuaranteeType { get; set; }
        // Semi-colon or comma separated enum names from TransactionType (e.g., "INCOMING_PAYMENT;PAYMENT") or '*'
        public string TransactionType { get; set; }
        // Booking code(s): '*' for any, or a list like "FR;DE;ES". Matched against RuleContext.CountryId
        public string Booking { get; set; }
        // null => don't care; true/false => must match
        public bool? HasDwingsLink { get; set; }
        public bool? IsGrouped { get; set; }
        public bool? IsAmountMatch { get; set; }
        // 'C' (credit), 'D' (debit), or '*'
        public string Sign { get; set; } = "*";

        // New DWINGS-related input conditions
        // True when DWINGS invoice MT status is "ACKED", false when present and not ACKED, null to ignore
        public bool? MTStatusAcked { get; set; }
        // True when COMM_ID_EMAIL flag is set on the DWINGS invoice
        public bool? CommIdEmail { get; set; }
        // True when DWINGS invoice status is "INITIATED"
        public bool? BgiStatusInitiated { get; set; }

        // Time/state conditions
        public bool? TriggerDateIsNull { get; set; }
        public int? DaysSinceTriggerMin { get; set; }
        public int? DaysSinceTriggerMax { get; set; }
        public bool? IsTransitory { get; set; }
        public int? OperationDaysAgoMin { get; set; }
        public int? OperationDaysAgoMax { get; set; }
        public bool? IsMatched { get; set; }
        public bool? HasManualMatch { get; set; }
        public bool? IsFirstRequest { get; set; }
        public int? DaysSinceReminderMin { get; set; }
        public int? DaysSinceReminderMax { get; set; }

        // Current state conditions
        public int? CurrentActionId { get; set; }

        // Outputs
        public int? OutputActionId { get; set; }
        public int? OutputKpiId { get; set; }
        public int? OutputIncidentTypeId { get; set; }
        public bool? OutputRiskyItem { get; set; }
        public int? OutputReasonNonRiskyId { get; set; }
        public bool? OutputToRemind { get; set; }
        public int? OutputToRemindDays { get; set; }
        // New: set FirstClaimDate to today when true (self only)
        public bool? OutputFirstClaimToday { get; set; }
        public ApplyTarget ApplyTo { get; set; } = ApplyTarget.Self;
        public bool AutoApply { get; set; } = true;
        public string Message { get; set; }
    }

    /// <summary>
    /// Minimal evaluation context provided to the engine.
    /// </summary>
    public class RuleContext
    {
        public string CountryId { get; set; }
        public bool IsPivot { get; set; }
        public string GuaranteeType { get; set; } // e.g., ISSUANCE/REISSUANCE/ADVISING
        public string TransactionType { get; set; } // enum name from Services.Enums.TransactionType
        public bool? HasDwingsLink { get; set; }
        public bool? IsGrouped { get; set; }
        public bool? IsAmountMatch { get; set; }
        public string Sign { get; set; } // 'C' or 'D'
        public string Bgi { get; set; } // DWINGS_InvoiceID

        // New DWINGS-derived inputs
        public bool? IsMtAcked { get; set; }
        public bool? HasCommIdEmail { get; set; }
        public bool? IsBgiInitiated { get; set; }

        // Extended inputs for time/state rules
        public bool? TriggerDateIsNull { get; set; }
        public int? DaysSinceTrigger { get; set; }
        public bool? IsTransitory { get; set; }
        public int? OperationDaysAgo { get; set; }
        public bool? IsMatched { get; set; }
        public bool? HasManualMatch { get; set; }
        public bool? IsFirstRequest { get; set; }
        public int? DaysSinceReminder { get; set; }
        public int? CurrentActionId { get; set; }
    }

    public class RuleEvaluationResult
    {
        public TruthRule Rule { get; set; }
        public int? NewActionIdSelf { get; set; }
        public int? NewKpiIdSelf { get; set; }
        public int? NewIncidentTypeIdSelf { get; set; }
        public bool? NewRiskyItemSelf { get; set; }
        public int? NewReasonNonRiskyIdSelf { get; set; }
        public bool? NewToRemindSelf { get; set; }
        public int? NewToRemindDaysSelf { get; set; }
        // New: set FirstClaimDate to today when true
        public bool? NewFirstClaimTodaySelf { get; set; }
        public List<(string ReconciliationId, int? ActionId, int? KpiId)> CounterpartUpdates { get; set; } = new List<(string, int?, int?)>();
        public bool RequiresUserConfirm { get; set; }
        public string UserMessage { get; set; }
    }
}
