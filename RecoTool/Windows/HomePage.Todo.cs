using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using RecoTool.Domain.Filters;
using RecoTool.Models;
using RecoTool.Services;

namespace RecoTool.Windows
{
    public partial class HomePage
    {
        /// <summary>
        /// Load shared/global ToDo items and compute Live counts for the current country.
        /// </summary>
        private async Task LoadTodoCardsAsync()
        {
            try
            {
                if (_offlineFirstService == null || _reconciliationService == null) return;
                var cid = _offlineFirstService.CurrentCountryId;
                if (string.IsNullOrWhiteSpace(cid)) return;

                if (TodoCards == null) TodoCards = new ObservableCollection<TodoCard>();

                var refCs = _offlineFirstService.ReferentialConnectionString ?? RecoTool.Properties.Settings.Default.ReferentialDB;
                var todoSvc = new UserTodoListService(refCs);
                try { await todoSvc.EnsureTableAsync().ConfigureAwait(false); } catch { }
                var list = await todoSvc.ListAsync(cid).ConfigureAwait(false);

                var curUser = _offlineFirstService.CurrentUser ?? Environment.UserName;
                var filterSvc = new UserFilterService(refCs, curUser);

                var country = _offlineFirstService.CurrentCountry;
                var pivotId = country?.CNT_AmbrePivot;
                var recvId = country?.CNT_AmbreReceivable;

                // Compute total Live count to derive share per ToDo
                int totalLive = 0;
                try { totalLive = await _reconciliationService.GetReconciliationCountAsync(cid, null).ConfigureAwait(false); } catch { totalLive = 0; }

                var tasks = new List<Task<TodoCard>>();
                foreach (var t in list)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            string where = null;
                            try { where = filterSvc.LoadUserFilterWhere(t.TDL_FilterName); } catch { }
                            // Strip Account/Status parts like in ApplyTodoToNextViewAsync
                            try { where = UserFilterService.SanitizeWhereClause(where); } catch { }

                            // Map a single account token exactly like ApplyTodoToNextViewAsync
                            string accId = null;
                            var token = t.TDL_Account?.Trim();
                            if (!string.IsNullOrWhiteSpace(token))
                            {
                                if (token.StartsWith("Pivot", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(pivotId)) accId = pivotId;
                                else if (token.StartsWith("Receivable", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(recvId)) accId = recvId;
                                else accId = token;
                            }

                            // Human-readable account label for the card
                            string accountLabel = "All Accounts";
                            if (!string.IsNullOrWhiteSpace(token))
                            {
                                if (token.StartsWith("Pivot", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(pivotId))
                                    accountLabel = $"Pivot ({pivotId})";
                                else if (token.StartsWith("Receivable", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(recvId))
                                    accountLabel = $"Receivable ({recvId})";
                                else
                                    accountLabel = token;
                            }

                            string accSql = null;
                            if (!string.IsNullOrWhiteSpace(accId))
                            {
                                var esc = accId.Replace("'", "''");
                                accSql = $"a.Account_ID = '{esc}'";
                            }

                            var pred = FilterSqlHelper.ExtractNormalizedPredicate(where);
                            
                            // Build base filter (same as ApplyTodoToNextViewAsync - no ActionStatus filter)
                            var baseParts = new List<string>();
                            baseParts.Add("a.DeleteDate IS NULL AND (r.DeleteDate IS NULL)");
                            if (!string.IsNullOrWhiteSpace(accSql)) baseParts.Add(accSql);
                            if (!string.IsNullOrWhiteSpace(pred)) baseParts.Add($"({pred})");
                            string baseCombined = baseParts.Count > 0 ? ("WHERE " + string.Join(" AND ", baseParts)) : null;
                            
                            // Get total count (all items matching filter, regardless of ActionStatus)
                            int totalCount = await _reconciliationService.GetReconciliationCountAsync(cid, baseCombined).ConfigureAwait(false);
                            
                            // Build To Review filter (add ActionStatus = Pending)
                            var toReviewParts = new List<string>(baseParts);
                            toReviewParts.Add("(r.Action IS NOT NULL AND (r.ActionStatus = 0 OR r.ActionStatus IS NULL))");
                            string toReviewCombined = toReviewParts.Count > 0 ? ("WHERE " + string.Join(" AND ", toReviewParts)) : null;
                            
                            int count = await _reconciliationService.GetReconciliationCountAsync(cid, toReviewCombined).ConfigureAwait(false);
                            // Currency sums for display (use base filter to show all amounts)
                            var sums = await _reconciliationService.GetCurrencySumsAsync(cid, baseCombined).ConfigureAwait(false);
                            string amountsText = string.Empty;
                            if (sums != null && sums.Count > 0)
                            {
                                var top = sums.OrderByDescending(kv => kv.Value)
                                              .Select(kv => $"{kv.Key} {kv.Value:N0}")
                                              .Take(3)
                                              .ToList();
                                if (top.Count > 0) amountsText = string.Join(" | ", top);
                                var remaining = sums.Count - top.Count;
                                if (remaining > 0) amountsText += $" (+{remaining} more)";
                            }

                            // Compute reviewed count (ActionStatus = Done)
                            var reviewedParts = new List<string>(baseParts);
                            reviewedParts.Add("(r.ActionStatus = -1)"); // True in Access
                            string reviewedCombined = reviewedParts.Count > 0 ? ("WHERE " + string.Join(" AND ", reviewedParts)) : null;
                            int reviewedCount = await _reconciliationService.GetReconciliationCountAsync(cid, reviewedCombined).ConfigureAwait(false);

                            // Percent based on total items in this filter
                            double percent = (totalLive > 0) ? (totalCount * 100.0 / totalLive) : 0.0;
                            
                            // Compute status indicators (fetch actual data to calculate)
                            var statusCounts = await GetStatusCountsAsync(cid, baseCombined).ConfigureAwait(false);
                            
                            return new TodoCard 
                            { 
                                Item = t, 
                                Count = count,                    // To Review
                                ReviewedCount = reviewedCount,    // Reviewed
                                ActualTotal = totalCount,         // Real total (includes items without action)
                                Percent = percent, 
                                AccountLabel = accountLabel, 
                                AmountsText = amountsText,
                                NewCount = statusCounts.NewCount,
                                UpdatedCount = statusCounts.UpdatedCount,
                                NotLinkedCount = statusCounts.NotLinkedCount,
                                NotGroupedCount = statusCounts.NotGroupedCount,
                                DiscrepancyCount = statusCounts.DiscrepancyCount,
                                BalancedCount = statusCounts.BalancedCount
                            };
                        }
                        catch
                        {
                            return new TodoCard { Item = t, Count = 0, Percent = 0 };
                        }
                    }));
                }

                var cards = await Task.WhenAll(tasks).ConfigureAwait(false);

                await Dispatcher.InvokeAsync(() =>
                {
                    try
                    {
                        TodoCards.Clear();
                        foreach (var c in cards.OrderBy(x => x.Item?.TDL_Order ?? int.MaxValue).ThenBy(x => x.Item?.TDL_Name))
                            TodoCards.Add(c);
                    }
                    catch { }
                });
            }
            catch { }
        }

        /// <summary>
        /// Helper class to hold status counts
        /// </summary>
        private class StatusCounts
        {
            public int NewCount { get; set; }
            public int UpdatedCount { get; set; }
            public int NotLinkedCount { get; set; }
            public int NotGroupedCount { get; set; }
            public int DiscrepancyCount { get; set; }
            public int BalancedCount { get; set; }
        }

        /// <summary>
        /// Get status indicator counts for a TodoCard filter
        /// </summary>
        private async Task<StatusCounts> GetStatusCountsAsync(string countryId, string whereClause)
        {
            var result = new StatusCounts();
            try
            {
                // Fetch the actual data to calculate status colors
                var data = await _reconciliationService.GetReconciliationViewAsync(countryId, whereClause).ConfigureAwait(false);
                if (data == null || !data.Any()) return result;

                // Calculate counts based on StatusColor property
                result.NewCount = data.Count(a => a.IsNewlyAdded);
                result.UpdatedCount = data.Count(a => a.IsUpdated);
                result.NotLinkedCount = data.Count(a => a.StatusColor == "#F44336"); // Red
                result.NotGroupedCount = data.Count(a => a.StatusColor == "#FF9800"); // Orange
                result.DiscrepancyCount = data.Count(a => a.StatusColor == "#FFC107" || a.StatusColor == "#FF6F00"); // Yellow or Dark Amber
                result.BalancedCount = data.Count(a => a.StatusColor == "#4CAF50"); // Green
            }
            catch
            {
                // Return empty counts on error
            }
            return result;
        }
    }
}
