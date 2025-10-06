using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Media;
using LiveCharts;
using LiveCharts.Wpf;
using RecoTool.Models;
using RecoTool.Services.DTOs;
using RecoTool.Helpers;

namespace RecoTool.Services.Charts
{
    /// <summary>
    /// Service responsible for building LiveCharts series from reconciliation data
    /// Centralizes chart logic previously scattered in code-behind files
    /// </summary>
    public class ChartBuilderService
    {
        private readonly OfflineFirstService _offlineFirstService;

        public ChartBuilderService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
        }

        #region Currency Distribution

        /// <summary>
        /// Builds a pie chart showing top 10 currencies by absolute amount
        /// </summary>
        public SeriesCollection BuildCurrencyDistributionChart(List<ReconciliationViewData> data, int topN = 10)
        {
            if (data == null || !data.Any())
                return new SeriesCollection();

            var grouped = data
                .Where(r => !string.IsNullOrWhiteSpace(r.CCY))
                .GroupBy(r => r.CCY.Trim().ToUpperInvariant())
                .Select(g => new
                {
                    Currency = g.Key,
                    Amount = g.Sum(x => Math.Abs(x.SignedAmount))
                })
                .OrderByDescending(x => x.Amount)
                .Take(topN)
                .ToList();

            var series = new SeriesCollection();
            foreach (var item in grouped)
            {
                series.Add(new PieSeries
                {
                    Title = item.Currency,
                    Values = new ChartValues<double> { Convert.ToDouble(item.Amount) },
                    DataLabels = true,
                    LabelPoint = chartPoint => $"{chartPoint.Y:N2}"
                });
            }

            return series;
        }

        #endregion

        #region Action Distribution

        /// <summary>
        /// Builds a pie chart showing distribution by Action
        /// </summary>
        public SeriesCollection BuildActionDistributionChart(List<ReconciliationViewData> data)
        {
            if (data == null || !data.Any())
                return new SeriesCollection();

            var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();
            var actionFieldMap = userFields
                .Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(u => u.USR_ID, u => u);

            var grouped = data
                .Where(r => r.Action.HasValue)
                .GroupBy(r => r.Action.Value)
                .Select(g => new
                {
                    ActionId = g.Key,
                    Count = g.Count(),
                    Label = GetActionLabel(g.Key, actionFieldMap)
                })
                .OrderBy(x => x.Label)
                .ToList();

            var series = new SeriesCollection();
            foreach (var item in grouped)
            {
                series.Add(new PieSeries
                {
                    Title = item.Label,
                    Values = new ChartValues<int> { item.Count },
                    DataLabels = true,
                    LabelPoint = chartPoint => $"{chartPoint.Y:N0}"
                });
            }

            return series;
        }

        #endregion

        #region Receivable vs Pivot by Currency

        /// <summary>
        /// Builds a side-by-side column chart comparing Receivable vs Pivot by Currency
        /// </summary>
        public (SeriesCollection Series, List<string> Labels) BuildReceivablePivotByCurrencyChart(
            List<ReconciliationViewData> data, 
            int receivableAccountId, 
            int pivotAccountId, 
            int topN = 10)
        {
            if (data == null || !data.Any())
                return (new SeriesCollection(), new List<string>());

            var grouped = data
                .Where(r => !string.IsNullOrWhiteSpace(r.CCY) && r.SignedAmount != 0)
                .GroupBy(r => r.CCY.Trim().ToUpperInvariant())
                .Select(g => new
                {
                    CCY = g.Key,
                    RecAmount = g.Where(x => x.Account_ID == receivableAccountId).Sum(x => Math.Abs(x.SignedAmount)),
                    PivAmount = g.Where(x => x.Account_ID == pivotAccountId).Sum(x => Math.Abs(x.SignedAmount))
                })
                .OrderByDescending(x => x.RecAmount + x.PivAmount)
                .Take(topN)
                .ToList();

            if (!grouped.Any())
                return (new SeriesCollection(), new List<string>());

            var labels = grouped.Select(x => x.CCY).ToList();
            var recValues = new ChartValues<double>(grouped.Select(x => Convert.ToDouble(x.RecAmount)));
            var pivValues = new ChartValues<double>(grouped.Select(x => Convert.ToDouble(x.PivAmount)));

            var series = new SeriesCollection
            {
                new ColumnSeries
                {
                    Title = "Receivable",
                    Values = recValues,
                    DataLabels = true,
                    LabelPoint = cp => cp.Y.ToString("N2")
                },
                new ColumnSeries
                {
                    Title = "Pivot",
                    Values = pivValues,
                    DataLabels = true,
                    LabelPoint = cp => cp.Y.ToString("N2")
                }
            };

            return (series, labels);
        }

        #endregion

        #region Receivable vs Pivot by Action

        /// <summary>
        /// Builds a stacked column chart comparing Receivable vs Pivot by Action
        /// </summary>
        public (SeriesCollection Series, List<string> Labels) BuildReceivablePivotByActionChart(
            List<ReconciliationViewData> data,
            int receivableAccountId,
            int pivotAccountId)
        {
            if (data == null || !data.Any())
                return (new SeriesCollection(), new List<string>());

            var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();
            var actionFieldMap = userFields
                .Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(u => u.USR_ID, u => u);

            var actions = data
                .Where(r => r.Action.HasValue)
                .Select(r => r.Action.Value)
                .Distinct()
                .OrderBy(a => GetActionLabel(a, actionFieldMap))
                .ToList();

            var labels = actions.Select(a => GetActionLabel(a, actionFieldMap)).ToList();
            var receivableValues = new ChartValues<int>();
            var pivotValues = new ChartValues<int>();

            foreach (var action in actions)
            {
                var rCount = data.Count(x => x.Action == action && x.Account_ID == receivableAccountId);
                var pCount = data.Count(x => x.Action == action && x.Account_ID == pivotAccountId);
                receivableValues.Add(rCount);
                pivotValues.Add(pCount);
            }

            var series = new SeriesCollection
            {
                new StackedColumnSeries
                {
                    Title = "Receivable",
                    Values = receivableValues,
                    DataLabels = true,
                    LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : ""
                },
                new StackedColumnSeries
                {
                    Title = "Pivot",
                    Values = pivotValues,
                    DataLabels = true,
                    LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : ""
                }
            };

            return (series, labels);
        }

        #endregion

        #region KPI Risk Distribution

        /// <summary>
        /// Builds a stacked column chart showing Risky vs Non-Risky items by KPI
        /// </summary>
        public (SeriesCollection Series, List<string> Labels) BuildKpiRiskChart(List<ReconciliationViewData> data)
        {
            if (data == null || !data.Any())
                return (new SeriesCollection(), new List<string>());

            var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();

            var grouped = data
                .Where(r => r.KPI.HasValue)
                .GroupBy(r => r.KPI.Value)
                .OrderBy(g => g.Key)
                .ToList();

            var labels = new List<string>();
            var riskyValues = new ChartValues<int>();
            var nonRiskyValues = new ChartValues<int>();

            foreach (var g in grouped)
            {
                labels.Add(EnumHelper.GetKPIName(g.Key, userFields));
                riskyValues.Add(g.Count(x => x.RiskyItem == true));
                nonRiskyValues.Add(g.Count(x => x.RiskyItem != true));
            }

            var series = new SeriesCollection
            {
                new StackedColumnSeries
                {
                    Title = "Risky",
                    Values = riskyValues,
                    DataLabels = true,
                    Fill = new SolidColorBrush(Color.FromRgb(244, 67, 54)), // Red
                    LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : ""
                },
                new StackedColumnSeries
                {
                    Title = "Non-Risky",
                    Values = nonRiskyValues,
                    DataLabels = true,
                    Fill = new SolidColorBrush(Color.FromRgb(76, 175, 80)), // Green
                    LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : ""
                }
            };

            return (series, labels);
        }

        #endregion

        #region Deletion Delay Distribution

        /// <summary>
        /// Builds a column chart showing average days before reconciliation by time buckets
        /// </summary>
        public (SeriesCollection Series, List<string> Labels) BuildDeletionDelayChart(List<ReconciliationViewData> data)
        {
            if (data == null || !data.Any())
                return (new SeriesCollection(), new List<string>());

            var items = data
                .Where(r => r.CreationDate.HasValue && r.DeleteDate.HasValue)
                .Select(r => (int)(r.DeleteDate.Value.Date - r.CreationDate.Value.Date).TotalDays)
                .Where(d => d >= 0)
                .ToList();

            if (!items.Any())
                return (new SeriesCollection(), new List<string>());

            var buckets = new[]
            {
                new { Key = "0-14j", Min = 0, Max = 14 },
                new { Key = "15-30j", Min = 15, Max = 30 },
                new { Key = "1-3 mois", Min = 31, Max = 92 },
                new { Key = ">3 mois", Min = 93, Max = int.MaxValue }
            };

            var labels = new List<string>();
            var avgDaysValues = new ChartValues<double>();

            foreach (var bucket in buckets)
            {
                var inBucket = items.Where(d => d >= bucket.Min && d <= bucket.Max).ToList();
                double avg = inBucket.Any() ? inBucket.Average() : 0;
                labels.Add(bucket.Key);
                avgDaysValues.Add(avg);
            }

            var series = new SeriesCollection
            {
                new ColumnSeries
                {
                    Title = "Average duration (days)",
                    Values = avgDaysValues,
                    DataLabels = true,
                    LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : ""
                }
            };

            return (series, labels);
        }

        #endregion

        #region Helper Methods

        private string GetActionLabel(int actionId, Dictionary<int, UserField> actionFieldMap)
        {
            if (actionFieldMap.TryGetValue(actionId, out var uf) && uf != null)
            {
                if (!string.IsNullOrWhiteSpace(uf.USR_FieldName))
                    return uf.USR_FieldName;
                if (!string.IsNullOrWhiteSpace(uf.USR_FieldDescription))
                    return uf.USR_FieldDescription;
            }
            return EnumHelper.GetActionName(actionId, _offlineFirstService?.UserFields);
        }

        #endregion
    }
}
