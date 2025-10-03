using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.Analytics
{
    /// <summary>
    /// Service for calculating dashboard analytics and trends
    /// </summary>
    public class DashboardAnalyticsService
    {
        /// <summary>
        /// Gets daily review trend for the last N days (based on ActionDate when status = Done)
        /// </summary>
        public static List<DailyTrendPoint> GetReviewTrend(List<ReconciliationViewData> data, int days = 7)
        {
            var today = DateTime.Today;
            var startDate = today.AddDays(-days + 1);
            
            var trend = new List<DailyTrendPoint>();
            
            for (int i = 0; i < days; i++)
            {
                var date = startDate.AddDays(i);
                var nextDate = date.AddDays(1);
                
                // Count items marked as Done (ActionStatus = true) on this date
                var reviewedCount = data.Count(r => 
                    r.ActionStatus == true &&
                    r.ActionDate.HasValue && 
                    r.ActionDate.Value.Date >= date && 
                    r.ActionDate.Value.Date < nextDate);
                
                trend.Add(new DailyTrendPoint
                {
                    Date = date,
                    Count = reviewedCount,
                    Label = date.ToString("MM/dd")
                });
            }
            
            return trend;
        }

        /// <summary>
        /// Gets daily matched rate trend
        /// </summary>
        public static List<DailyTrendPoint> GetMatchedRateTrend(List<ReconciliationViewData> data, int days = 7)
        {
            var today = DateTime.Today;
            var startDate = today.AddDays(-days + 1);
            
            var trend = new List<DailyTrendPoint>();
            
            for (int i = 0; i < days; i++)
            {
                var date = startDate.AddDays(i);
                var nextDate = date.AddDays(1);
                
                var dayData = data.Where(r => 
                    r.CreationDate.HasValue && 
                    r.CreationDate.Value.Date >= date && 
                    r.CreationDate.Value.Date < nextDate).ToList();
                
                if (dayData.Count > 0)
                {
                    var matched = dayData.Count(r => 
                        !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) ||
                        !string.IsNullOrWhiteSpace(r.DWINGS_GuaranteeID) ||
                        !string.IsNullOrWhiteSpace(r.DWINGS_BGPMT));
                    
                    var rate = (matched * 100.0) / dayData.Count;
                    
                    trend.Add(new DailyTrendPoint
                    {
                        Date = date,
                        Count = (int)rate,
                        Label = date.ToString("MM/dd"),
                        Percentage = rate
                    });
                }
                else
                {
                    trend.Add(new DailyTrendPoint
                    {
                        Date = date,
                        Count = 0,
                        Label = date.ToString("MM/dd"),
                        Percentage = 0
                    });
                }
            }
            
            return trend;
        }

        /// <summary>
        /// Gets assignee leaderboard for the current week (based on ActionStatus = Done)
        /// </summary>
        public static List<AssigneeStats> GetAssigneeLeaderboard(List<ReconciliationViewData> data)
        {
            var weekStart = DateTime.Today.AddDays(-(int)DateTime.Today.DayOfWeek);
            
            var stats = data
                .Where(r => r.ActionStatus == true && r.ActionDate.HasValue && r.ActionDate.Value >= weekStart)
                .GroupBy(r => r.Assignee ?? "Unassigned")
                .Select(g => new AssigneeStats
                {
                    Assignee = g.Key,
                    ReviewedThisWeek = g.Count(),
                    TotalAssigned = data.Count(r => (r.Assignee ?? "Unassigned") == g.Key),
                    MatchedCount = g.Count(r => 
                        !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) ||
                        !string.IsNullOrWhiteSpace(r.DWINGS_GuaranteeID)),
                    AverageReviewTime = CalculateAverageReviewTime(g.ToList())
                })
                .OrderByDescending(s => s.ReviewedThisWeek)
                .Take(10)
                .ToList();
            
            return stats;
        }

        /// <summary>
        /// Gets urgent alerts requiring immediate attention
        /// </summary>
        public static List<AlertItem> GetUrgentAlerts(List<ReconciliationViewData> data)
        {
            var alerts = new List<AlertItem>();
            var today = DateTime.Today;
            
            // Old items to review (>30 days with Pending action)
            var oldUnreviewed = data.Count(r => 
                r.IsToReview && 
                r.Operation_Date.HasValue && 
                (today - r.Operation_Date.Value.Date).TotalDays > 30);
            
            if (oldUnreviewed > 0)
            {
                alerts.Add(new AlertItem
                {
                    Type = AlertType.Warning,
                    Title = "Old Unreviewed Items",
                    Message = $"{oldUnreviewed} items older than 30 days need review",
                    Count = oldUnreviewed,
                    Priority = 2
                });
            }
            
            // Unmatched high amounts (>10000)
            var unmatchedHighValue = data.Count(r => 
                string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) && 
                Math.Abs(r.SignedAmount) > 10000);
            
            if (unmatchedHighValue > 0)
            {
                alerts.Add(new AlertItem
                {
                    Type = AlertType.Critical,
                    Title = "High-Value Unmatched",
                    Message = $"{unmatchedHighValue} high-value items (>10k) not matched to DWINGS",
                    Count = unmatchedHighValue,
                    Priority = 1
                });
            }
            
            // Potential duplicates
            var duplicates = data.Count(r => r.IsPotentialDuplicate);
            if (duplicates > 0)
            {
                alerts.Add(new AlertItem
                {
                    Type = AlertType.Info,
                    Title = "Potential Duplicates",
                    Message = $"{duplicates} items flagged as potential duplicates",
                    Count = duplicates,
                    Priority = 3
                });
            }
            
            // Overdue triggers
            var overdueTriggers = data.Count(r => 
                r.Action == (int)ActionType.Trigger && 
                r.TriggerDate.HasValue && 
                r.TriggerDate.Value.Date < today);
            
            if (overdueTriggers > 0)
            {
                alerts.Add(new AlertItem
                {
                    Type = AlertType.Warning,
                    Title = "Overdue Triggers",
                    Message = $"{overdueTriggers} trigger actions are overdue",
                    Count = overdueTriggers,
                    Priority = 2
                });
            }
            
            return alerts.OrderBy(a => a.Priority).ToList();
        }

        /// <summary>
        /// Estimates time to complete based on current review rate
        /// </summary>
        public static CompletionEstimate GetCompletionEstimate(List<ReconciliationViewData> data)
        {
            var today = DateTime.Today;
            var last7Days = today.AddDays(-7);
            
            // Count items to review (Action Pending)
            var unreviewed = data.Count(r => r.IsToReview);
            
            // Calculate review rate (last 7 days - ActionStatus Done)
            var reviewedLast7Days = data.Count(r => 
                r.ActionStatus == true &&
                r.ActionDate.HasValue && 
                r.ActionDate.Value >= last7Days);
            
            var dailyRate = reviewedLast7Days / 7.0;
            
            var estimate = new CompletionEstimate
            {
                UnreviewedCount = unreviewed,
                DailyReviewRate = dailyRate,
                EstimatedDaysToComplete = dailyRate > 0 ? (int)Math.Ceiling(unreviewed / dailyRate) : 0,
                ReviewedLast7Days = reviewedLast7Days,
                CompletionPercentage = data.Count > 0 
                    ? ((data.Count - unreviewed) * 100.0) / data.Count 
                    : 0
            };
            
            if (estimate.EstimatedDaysToComplete > 0)
            {
                estimate.EstimatedCompletionDate = today.AddDays(estimate.EstimatedDaysToComplete);
            }
            
            return estimate;
        }

        private static double CalculateAverageReviewTime(List<ReconciliationViewData> items)
        {
            // Estimate based on creation to action done date
            var times = items
                .Where(r => r.ActionStatus == true && r.ActionDate.HasValue && r.CreationDate.HasValue)
                .Select(r => (r.ActionDate.Value - r.CreationDate.Value).TotalHours)
                .Where(h => h >= 0 && h < 720) // Max 30 days
                .ToList();
            
            return times.Any() ? times.Average() : 0;
        }
    }

    #region DTOs

    public class DailyTrendPoint
    {
        public DateTime Date { get; set; }
        public int Count { get; set; }
        public string Label { get; set; }
        public double Percentage { get; set; }
    }

    public class AssigneeStats
    {
        public string Assignee { get; set; }
        public int ReviewedThisWeek { get; set; }
        public int TotalAssigned { get; set; }
        public int MatchedCount { get; set; }
        public double AverageReviewTime { get; set; }
        public double CompletionRate => TotalAssigned > 0 
            ? (ReviewedThisWeek * 100.0) / TotalAssigned 
            : 0;
    }

    public class AlertItem
    {
        public AlertType Type { get; set; }
        public string Title { get; set; }
        public string Message { get; set; }
        public int Count { get; set; }
        public int Priority { get; set; }
    }

    public enum AlertType
    {
        Info,
        Warning,
        Critical
    }

    public class CompletionEstimate
    {
        public int UnreviewedCount { get; set; }
        public double DailyReviewRate { get; set; }
        public int EstimatedDaysToComplete { get; set; }
        public DateTime? EstimatedCompletionDate { get; set; }
        public int ReviewedLast7Days { get; set; }
        public double CompletionPercentage { get; set; }
    }

    #endregion
}
