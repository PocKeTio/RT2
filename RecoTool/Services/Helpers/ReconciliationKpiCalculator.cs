using System;
using System.Collections.Generic;
using System.Linq;
using RecoTool.Models;

namespace RecoTool.Services.Helpers
{
    /// <summary>
    /// Calculates KPIs for reconciliation lines during import/processing.
    /// Computes IsGrouped (matched across accounts) and MissingAmount (discrepancy).
    /// </summary>
    public static class ReconciliationKpiCalculator
    {
        /// <summary>
        /// Represents a staging item for KPI calculation
        /// </summary>
        public class ReconciliationStaging
        {
            public DataAmbre DataAmbre { get; set; }
            public Reconciliation Reconciliation { get; set; }
            public bool IsPivot { get; set; }
            
            // Calculated KPIs
            public bool IsGrouped { get; set; }
            public decimal? MissingAmount { get; set; }
            public decimal? CounterpartTotalAmount { get; set; }
            public int? CounterpartCount { get; set; }
        }
        
        /// <summary>
        /// Calculates grouping and missing amounts for a list of reconciliation staging items.
        /// Groups by DWINGS_InvoiceID or InternalInvoiceReference, checks if both Pivot and Receivable exist.
        /// </summary>
        public static void CalculateKpis(List<ReconciliationStaging> items)
        {
            if (items == null || items.Count == 0) return;
            
            // Group by invoice reference (DWINGS_InvoiceID or InternalInvoiceReference)
            var groups = items
                .Where(r => !string.IsNullOrWhiteSpace(r.Reconciliation?.DWINGS_InvoiceID) 
                         || !string.IsNullOrWhiteSpace(r.Reconciliation?.InternalInvoiceReference))
                .GroupBy(r => 
                {
                    var rec = r.Reconciliation;
                    var key = !string.IsNullOrWhiteSpace(rec.DWINGS_InvoiceID) 
                        ? rec.DWINGS_InvoiceID.Trim().ToUpperInvariant()
                        : rec.InternalInvoiceReference?.Trim().ToUpperInvariant();
                    return key;
                })
                .Where(g => !string.IsNullOrWhiteSpace(g.Key))
                .ToList();
            
            foreach (var group in groups)
            {
                var receivableLines = group.Where(r => !r.IsPivot).ToList();
                var pivotLines = group.Where(r => r.IsPivot).ToList();
                
                // IsGrouped = true if we have both sides
                bool isGrouped = receivableLines.Count > 0 && pivotLines.Count > 0;
                
                if (isGrouped)
                {
                    // Calculate amounts
                    var receivableTotal = receivableLines.Sum(r => r.DataAmbre?.SignedAmount ?? 0);
                    var pivotTotal = pivotLines.Sum(r => r.DataAmbre?.SignedAmount ?? 0);
                    
                    // Missing amount = Receivable + Pivot (should sum to 0 when balanced)
                    // Receivable is typically negative, Pivot is positive
                    var missing = receivableTotal + pivotTotal;
                    
                    // Enrich Receivable lines
                    foreach (var r in receivableLines)
                    {
                        r.IsGrouped = true;
                        r.MissingAmount = missing;
                        r.CounterpartTotalAmount = pivotTotal;
                        r.CounterpartCount = pivotLines.Count;
                    }
                    
                    // Enrich Pivot lines
                    foreach (var p in pivotLines)
                    {
                        p.IsGrouped = true;
                        p.MissingAmount = missing; // Same value for both sides
                        p.CounterpartTotalAmount = receivableTotal;
                        p.CounterpartCount = receivableLines.Count;
                    }
                }
                else
                {
                    // Not grouped: set flags accordingly
                    foreach (var item in group)
                    {
                        item.IsGrouped = false;
                        item.MissingAmount = null;
                        item.CounterpartTotalAmount = null;
                        item.CounterpartCount = null;
                    }
                }
            }
            
            // Set IsGrouped = false for items without any invoice reference
            foreach (var item in items)
            {
                if (string.IsNullOrWhiteSpace(item.Reconciliation?.DWINGS_InvoiceID) 
                    && string.IsNullOrWhiteSpace(item.Reconciliation?.InternalInvoiceReference))
                {
                    item.IsGrouped = false;
                    item.MissingAmount = null;
                    item.CounterpartTotalAmount = null;
                    item.CounterpartCount = null;
                }
            }
        }
    }
}
