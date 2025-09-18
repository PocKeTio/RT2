using System;

namespace RecoTool.Domain.Filters
{
    /// <summary>
    /// Serializable snapshot of filter values for saving/loading presets from the UI.
    /// Mirrors ReconciliationView filter surface.
    /// </summary>
    public sealed class FilterPreset
    {
        public string AccountId { get; set; }
        public string Currency { get; set; }
        public string Country { get; set; }
        public decimal? MinAmount { get; set; }
        public decimal? MaxAmount { get; set; }
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public int? Action { get; set; }
        public int? KPI { get; set; }
        public int? IncidentType { get; set; }
        public string Status { get; set; }
        public string ReconciliationNum { get; set; }
        public string RawLabel { get; set; }
        public string EventNum { get; set; }
        public string DwGuaranteeId { get; set; }
        public string DwCommissionId { get; set; }
        public string GuaranteeType { get; set; }
        public string Comments { get; set; }
        public bool? PotentialDuplicates { get; set; }
        public bool? Unmatched { get; set; }
        public bool? NewLines { get; set; }
        public bool? ActionDone { get; set; }
        public DateTime? ActionDateFrom { get; set; }
        public DateTime? ActionDateTo { get; set; }
    }
}
