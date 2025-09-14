using System;

namespace RecoTool.Services.DTOs
{
    /// <summary>
    /// Snapshot of HomePage KPIs persisted in Control DB (KpiDailySnapshot).
    /// </summary>
    public class KpiDailySnapshotDto
    {
        public DateTime SnapshotDate { get; set; }
        public string CountryId { get; set; }

        public long MissingInvoices { get; set; }
        public long PaidNotReconciled { get; set; }
        public long UnderInvestigation { get; set; }

        public decimal ReceivableAmount { get; set; }
        public long ReceivableCount { get; set; }
        public decimal PivotAmount { get; set; }
        public long PivotCount { get; set; }

        public long NewCount { get; set; }
        public long DeletedCount { get; set; }

        public string DeletionDelayBucketsJson { get; set; }
        public string ReceivablePivotByActionJson { get; set; }
        public string KpiDistributionJson { get; set; }
        public string KpiRiskMatrixJson { get; set; }
        public string CurrencyDistributionJson { get; set; }
        public string ActionDistributionJson { get; set; }

        public DateTime CreatedAtUtc { get; set; }
        public string SourceVersion { get; set; }
        public DateTime? FrozenAt { get; set; }
    }
}
