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
                            try 
                            { 
                                where = filterSvc.LoadUserFilterWhere(t.TDL_FilterName); 
                                System.Diagnostics.Debug.WriteLine($"[TodoCard] {t.TDL_Name}: FilterName='{t.TDL_FilterName}', LoadedWhere='{where}'");
                            } 
                            catch (Exception ex) 
                            { 
                                System.Diagnostics.Debug.WriteLine($"[TodoCard] {t.TDL_Name}: ERROR loading filter '{t.TDL_FilterName}': {ex.Message}");
                            }
                            
                            // Strip JSON comment if present (Access doesn't support SQL comments)
                            if (!string.IsNullOrWhiteSpace(where) && where.Contains("/*JSON:"))
                            {
                                try
                                {
                                    if (FilterSqlHelper.TryExtractPreset(where, out _, out var pureWhere))
                                        where = pureWhere;
                                }
                                catch { }
                            }
                            
                            // CRITICAL FIX: Use SanitizeWhereClause ONLY (same as direct filter selection and ApplyTodoToNextViewAsync)
                            // Do NOT use ExtractNormalizedPredicate which strips WHERE and causes double-processing
                            try { where = UserFilterService.SanitizeWhereClause(where); } catch { }
                            System.Diagnostics.Debug.WriteLine($"[TodoCard] {t.TDL_Name}: SanitizedWhere='{where}'");

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

                            // Build base filter using sanitized WHERE directly
                            string baseCombined = where;
                            
                            // If we have an account filter, we need to add it to the WHERE clause
                            if (!string.IsNullOrWhiteSpace(accSql))
                            {
                                if (string.IsNullOrWhiteSpace(baseCombined))
                                {
                                    baseCombined = $"WHERE {accSql}";
                                }
                                else if (baseCombined.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                                {
                                    baseCombined = baseCombined + $" AND {accSql}";
                                }
                                else
                                {
                                    baseCombined = $"WHERE {baseCombined} AND {accSql}";
                                }
                            }
                            else if (!string.IsNullOrWhiteSpace(baseCombined) && !baseCombined.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                            {
                                baseCombined = "WHERE " + baseCombined;
                            }
                            
                            System.Diagnostics.Debug.WriteLine($"[TodoCard] {t.TDL_Name}: FinalFilter='{baseCombined}'");
                            
                            // Get total count (all items matching filter, regardless of ActionStatus)
                            int totalCount = await _reconciliationService.GetReconciliationCountAsync(cid, baseCombined).ConfigureAwait(false);
                            
                            // Build To Review filter (add ActionStatus = Pending to base filter)
                            string toReviewCombined = baseCombined;
                            if (!string.IsNullOrWhiteSpace(toReviewCombined))
                            {
                                if (toReviewCombined.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                                    toReviewCombined += " AND (r.Action IS NOT NULL AND (r.ActionStatus = 0 OR r.ActionStatus IS NULL))";
                                else
                                    toReviewCombined = "WHERE " + toReviewCombined + " AND (r.Action IS NOT NULL AND (r.ActionStatus = 0 OR r.ActionStatus IS NULL))";
                            }
                            else
                            {
                                toReviewCombined = "WHERE (r.Action IS NOT NULL AND (r.ActionStatus = 0 OR r.ActionStatus IS NULL))";
                            }
                            
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
                            string reviewedCombined = baseCombined;
                            if (!string.IsNullOrWhiteSpace(reviewedCombined))
                            {
                                if (reviewedCombined.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                                    reviewedCombined += " AND (r.ActionStatus = -1)";
                                else
                                    reviewedCombined = "WHERE " + reviewedCombined + " AND (r.ActionStatus = -1)";
                            }
                            else
                            {
                                reviewedCombined = "WHERE (r.ActionStatus = -1)";
                            }
                            int reviewedCount = await _reconciliationService.GetReconciliationCountAsync(cid, reviewedCombined).ConfigureAwait(false);

                            // Percent based on total items in this filter
                            double percent = (totalLive > 0) ? (totalCount * 100.0 / totalLive) : 0.0;
                            
                            // Compute status indicators (uses cached ReconciliationService method)
                            var statusCounts = await _reconciliationService.GetStatusCountsAsync(cid, baseCombined).ConfigureAwait(false);
                            
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

        // Removed local StatusCounts class and GetStatusCountsAsync method
        // Now using ReconciliationService.GetStatusCountsAsync which is cached
    }
}
