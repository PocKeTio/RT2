using System;

namespace RecoTool.Domain.Filters
{
    /// <summary>
    /// Snapshot of filter values for Reconciliation view to build backend WHERE clauses.
    /// This will be populated from the ViewModel in a later step.
    /// </summary>
    public sealed class FilterState
    {
        public string AccountId { get; set; }
        public string Currency { get; set; }
        public decimal? Amount { get; set; } // Fixed amount filter
        public bool AmountWithTolerance { get; set; } // If true, apply +/- 1 tolerance
        public bool? PotentialDuplicates { get; set; }
        // New flags
        public bool? Unmatched { get; set; } // Dwings: no invoice linked
        public bool? NewLines { get; set; } // Ambre: newly appeared lines
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public int? ActionId { get; set; }
        public int? KpiId { get; set; }
        public string Status { get; set; } // All | Matched | Unmatched | Live | Archived
        public string ReconciliationNum { get; set; }
        public string RawLabel { get; set; }
        public string EventNum { get; set; }
        public string DwGuaranteeId { get; set; }
        public string DwCommissionId { get; set; }
        public string DwInvoiceId { get; set; }
        public int? TransactionTypeId { get; set; }
        public string TransactionType { get; set; }
        public string GuaranteeStatus { get; set; }
        public string GuaranteeType { get; set; }
        public string Comments { get; set; }
        public string AssigneeId { get; set; }
        public bool? RiskyItem { get; set; }
        public bool? Ack { get; set; }
        public int? IncidentTypeId { get; set; }
        public bool? ActionDone { get; set; }
        public DateTime? ActionDate { get; set; } // Specific action date filter
        public DateTime? DeletedDate { get; set; }
        public bool? ToRemind { get; set; } // Filter by ToRemind flag
        public DateTime? RemindDate { get; set; } // Filter by specific remind date
        public string LastReviewed { get; set; } // Never | Today | 1week | 1month
    }
}
