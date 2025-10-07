using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Services;

namespace RecoTool.Services.Rules
{
    /// <summary>
    /// Central rules engine evaluating truth-table rules.
    /// </summary>
    public class RulesEngine
    {
        // DEBUG FLAG: Set to true to log detailed rule matching failures
        private const bool DEBUG_RULES = true;
        
        private readonly TruthTableRepository _repo;
        private List<TruthRule> _cache;
        private DateTime _cacheTimeUtc;
        private readonly TimeSpan _cacheTtl = TimeSpan.FromMinutes(2);

        public RulesEngine(OfflineFirstService offlineFirstService)
        {
            if (offlineFirstService == null) throw new ArgumentNullException(nameof(offlineFirstService));
            _repo = new TruthTableRepository(offlineFirstService);
        }

        /// <summary>
        /// Invalidate the in-memory rules cache so next evaluation reloads from repository.
        /// </summary>
        public void InvalidateCache()
        {
            _cache = null;
            _cacheTimeUtc = DateTime.MinValue;
        }

        private async Task<List<TruthRule>> GetRulesAsync(CancellationToken token)
        {
            if (_cache != null && DateTime.UtcNow - _cacheTimeUtc < _cacheTtl)
                return _cache;
            var rules = await _repo.LoadRulesAsync(token).ConfigureAwait(false);
            _cache = rules ?? new List<TruthRule>();
            _cacheTimeUtc = DateTime.UtcNow;
            return _cache;
        }

        /// <summary>
        /// Evaluate a single row context against rules for the desired scope.
        /// Returns the best matching rule (lowest Priority) with its outputs packaged in a result.
        /// Returns null when no rule matches.
        /// </summary>
        public async Task<RuleEvaluationResult> EvaluateAsync(RuleContext ctx, RuleScope scope, CancellationToken token = default)
        {
            if (ctx == null) return null;
            var rules = await GetRulesAsync(token).ConfigureAwait(false);
            if (rules == null || rules.Count == 0) return null;

            // Pre-normalize context
            var c = NormalizeContext(ctx);

            foreach (var r in rules)
            {
                if (r == null || !r.Enabled) continue;
                if (r.Scope != RuleScope.Both && r.Scope != scope) continue;
                
                bool matches;
                
                // DEBUG MODE: Show detailed failure reasons (commented for performance)
                // List<string> failures = null;
                // if (DEBUG_RULES)
                // {
                //     matches = MatchesWithDebug(r, c, out failures);
                //     if (!matches)
                //     {
                //         System.Diagnostics.Debug.WriteLine($"[RulesEngine] Rule {r.RuleId} ({r.Name}) did NOT match. Failures:");
                //         foreach (var fail in failures)
                //             System.Diagnostics.Debug.WriteLine($"  - {fail}");
                //     }
                // }
                // else
                // {
                //     matches = Matches(r, c);
                // }
                
                matches = Matches(r, c);
                
                if (!matches) continue;

                // Rule matched!
                if (DEBUG_RULES)
                {
                    System.Diagnostics.Debug.WriteLine($"[RulesEngine] ✓ Rule APPLIED: RuleId={r.RuleId}, Kpi='{r.OutputKpiId}', Action={r.OutputActionId}");
                }

                var res = new RuleEvaluationResult
                {
                    Rule = r,
                    NewActionIdSelf = r.OutputActionId,
                    NewKpiIdSelf = r.OutputKpiId,
                    NewIncidentTypeIdSelf = r.OutputIncidentTypeId,
                    NewRiskyItemSelf = r.OutputRiskyItem,
                    NewReasonNonRiskyIdSelf = r.OutputReasonNonRiskyId,
                    NewToRemindSelf = r.OutputToRemind,
                    NewToRemindDaysSelf = r.OutputToRemindDays,
                    NewFirstClaimTodaySelf = r.OutputFirstClaimToday,
                    RequiresUserConfirm = !string.IsNullOrWhiteSpace(r.Message),
                    UserMessage = r.Message
                };
                return res;
            }
            
            // No rule matched
            if (DEBUG_RULES)
            {
                System.Diagnostics.Debug.WriteLine($"[RulesEngine] ✗ NO RULE APPLIED (IsPivot={c.IsPivot}, TxType={c.TransactionType}, IsGrouped={c.IsGrouped})");
            }
            
            return null;
        }

        private static RuleContext NormalizeContext(RuleContext ctx)
        {
            var n = new RuleContext
            {
                CountryId = ctx.CountryId,
                IsPivot = ctx.IsPivot,
                HasDwingsLink = ctx.HasDwingsLink,
                IsGrouped = ctx.IsGrouped,
                IsAmountMatch = ctx.IsAmountMatch,
                MissingAmount = ctx.MissingAmount,
                Bgi = string.IsNullOrWhiteSpace(ctx.Bgi) ? null : ctx.Bgi.Trim(),
                Sign = NormalizeSign(ctx.Sign),
                GuaranteeType = NormalizeGuaranteeType(ctx.GuaranteeType),
                TransactionType = NormalizeTransactionType(ctx.TransactionType),
                // pass-through extended fields
                TriggerDateIsNull = ctx.TriggerDateIsNull,
                DaysSinceTrigger = ctx.DaysSinceTrigger,
                OperationDaysAgo = ctx.OperationDaysAgo,
                IsMatched = ctx.IsMatched,
                HasManualMatch = ctx.HasManualMatch,
                IsFirstRequest = ctx.IsFirstRequest,
                DaysSinceReminder = ctx.DaysSinceReminder,
                CurrentActionId = ctx.CurrentActionId,
                // new DWINGS-derived inputs
                IsMtAcked = ctx.IsMtAcked,
                HasCommIdEmail = ctx.HasCommIdEmail,
                IsBgiInitiated = ctx.IsBgiInitiated
            };
            return n;
        }

        private static string NormalizeSign(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            s = s.Trim().ToUpperInvariant();
            if (s.StartsWith("D")) return "D";
            if (s.StartsWith("C")) return "C";
            return s;
        }

        private static string NormalizeGuaranteeType(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            s = s.Trim().ToUpperInvariant();
            // Map common synonyms
            if (s.StartsWith("REISSU")) return "REISSUANCE";
            if (s.StartsWith("ISSU")) return "ISSUANCE";
            if (s.StartsWith("NOTIF") || s.StartsWith("ADVISING")) return "ADVISING";
            return s;
        }

        private static string NormalizeTransactionType(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            s = s.Trim().ToUpperInvariant().Replace(' ', '_');
            return s;
        }

        private static bool Matches(TruthRule r, RuleContext c)
        {
            // Account side
            if (!IsWildcard(r.AccountSide))
            {
                bool needPivot = r.AccountSide.Equals("P", StringComparison.OrdinalIgnoreCase);
                bool needRecv = r.AccountSide.Equals("R", StringComparison.OrdinalIgnoreCase);
                if (!(needPivot && c.IsPivot) && !(needRecv && !c.IsPivot)) return false;
            }

            // Booking (country)
            if (!IsWildcard(r.Booking))
            {
                if (string.IsNullOrWhiteSpace(c.CountryId)) return false;
                if (!MatchesSet(r.Booking, c.CountryId)) return false;
            }

            // Guarantee Type
            if (!IsWildcard(r.GuaranteeType))
            {
                if (string.IsNullOrWhiteSpace(c.GuaranteeType)) return false;
                if (!MatchesSet(r.GuaranteeType, c.GuaranteeType)) return false;
            }

            // Transaction Type (enum name)
            if (!IsWildcard(r.TransactionType))
            {
                if (string.IsNullOrWhiteSpace(c.TransactionType)) return false;
                if (!MatchesSet(r.TransactionType, c.TransactionType)) return false;
            }

            // DWINGS link
            if (r.HasDwingsLink.HasValue && c.HasDwingsLink != r.HasDwingsLink.Value) return false;

            // Grouped
            if (r.IsGrouped.HasValue && c.IsGrouped != r.IsGrouped.Value) return false;

            // Amount match
            if (r.IsAmountMatch.HasValue && c.IsAmountMatch != r.IsAmountMatch.Value) return false;

            // Missing amount range
            if (r.MissingAmountMin.HasValue || r.MissingAmountMax.HasValue)
            {
                if (!c.MissingAmount.HasValue) return false;
                var amount = c.MissingAmount.Value;
                if (r.MissingAmountMin.HasValue && amount < r.MissingAmountMin.Value) return false;
                if (r.MissingAmountMax.HasValue && amount > r.MissingAmountMax.Value) return false;
            }

            // Sign
            if (!IsWildcard(r.Sign))
            {
                if (string.IsNullOrWhiteSpace(c.Sign)) return false;
                if (!r.Sign.Equals(c.Sign, StringComparison.OrdinalIgnoreCase)) return false;
            }

            // DWINGS: MT status acked
            if (r.MTStatusAcked.HasValue)
            {
                if (!c.IsMtAcked.HasValue) return false;
                if (c.IsMtAcked.Value != r.MTStatusAcked.Value) return false;
            }

            // DWINGS: COMM_ID_EMAIL flag
            if (r.CommIdEmail.HasValue)
            {
                if (!c.HasCommIdEmail.HasValue) return false;
                if (c.HasCommIdEmail.Value != r.CommIdEmail.Value) return false;
            }

            // DWINGS: BGI status initiated
            if (r.BgiStatusInitiated.HasValue)
            {
                if (!c.IsBgiInitiated.HasValue) return false;
                if (c.IsBgiInitiated.Value != r.BgiStatusInitiated.Value) return false;
            }

            // TriggerDateIsNull
            if (r.TriggerDateIsNull.HasValue)
            {
                if (!c.TriggerDateIsNull.HasValue) return false;
                if (c.TriggerDateIsNull.Value != r.TriggerDateIsNull.Value) return false;
            }

            // DaysSinceTrigger range
            if (r.DaysSinceTriggerMin.HasValue || r.DaysSinceTriggerMax.HasValue)
            {
                if (!c.DaysSinceTrigger.HasValue) return false;
                var d = c.DaysSinceTrigger.Value;
                if (r.DaysSinceTriggerMin.HasValue && d < r.DaysSinceTriggerMin.Value) return false;
                if (r.DaysSinceTriggerMax.HasValue && d > r.DaysSinceTriggerMax.Value) return false;
            }

            // OperationDaysAgo range
            if (r.OperationDaysAgoMin.HasValue || r.OperationDaysAgoMax.HasValue)
            {
                if (!c.OperationDaysAgo.HasValue) return false;
                var d = c.OperationDaysAgo.Value;
                if (r.OperationDaysAgoMin.HasValue && d < r.OperationDaysAgoMin.Value) return false;
                if (r.OperationDaysAgoMax.HasValue && d > r.OperationDaysAgoMax.Value) return false;
            }

            // IsMatched
            if (r.IsMatched.HasValue)
            {
                if (!c.IsMatched.HasValue) return false;
                if (c.IsMatched.Value != r.IsMatched.Value) return false;
            }

            // HasManualMatch
            if (r.HasManualMatch.HasValue)
            {
                if (!c.HasManualMatch.HasValue) return false;
                if (c.HasManualMatch.Value != r.HasManualMatch.Value) return false;
            }

            // IsFirstRequest
            if (r.IsFirstRequest.HasValue)
            {
                if (!c.IsFirstRequest.HasValue) return false;
                if (c.IsFirstRequest.Value != r.IsFirstRequest.Value) return false;
            }

            // DaysSinceReminder range
            if (r.DaysSinceReminderMin.HasValue || r.DaysSinceReminderMax.HasValue)
            {
                if (!c.DaysSinceReminder.HasValue) return false;
                var d = c.DaysSinceReminder.Value;
                if (r.DaysSinceReminderMin.HasValue && d < r.DaysSinceReminderMin.Value) return false;
                if (r.DaysSinceReminderMax.HasValue && d > r.DaysSinceReminderMax.Value) return false;
            }

            // CurrentActionId (input filter on existing Action)
            if (r.CurrentActionId.HasValue)
            {
                if (!c.CurrentActionId.HasValue) return false;
                if (c.CurrentActionId.Value != r.CurrentActionId.Value) return false;
            }

            return true;
        }

        private static bool MatchesWithDebug(TruthRule r, RuleContext c, out List<string> failures)
        {
            failures = new List<string>();
            
            // Account side
            if (!IsWildcard(r.AccountSide))
            {
                bool needPivot = r.AccountSide.Equals("P", StringComparison.OrdinalIgnoreCase);
                bool needRecv = r.AccountSide.Equals("R", StringComparison.OrdinalIgnoreCase);
                if (!(needPivot && c.IsPivot) && !(needRecv && !c.IsPivot))
                {
                    failures.Add($"AccountSide: Expected '{r.AccountSide}', Context IsPivot={c.IsPivot}");
                    return false;
                }
            }

            // Booking (country)
            if (!IsWildcard(r.Booking))
            {
                if (string.IsNullOrWhiteSpace(c.CountryId))
                {
                    failures.Add($"Booking: Expected '{r.Booking}', Context CountryId is null/empty");
                    return false;
                }
                if (!MatchesSet(r.Booking, c.CountryId))
                {
                    failures.Add($"Booking: Expected '{r.Booking}', Context CountryId='{c.CountryId}'");
                    return false;
                }
            }

            // Guarantee Type
            if (!IsWildcard(r.GuaranteeType))
            {
                if (string.IsNullOrWhiteSpace(c.GuaranteeType))
                {
                    failures.Add($"GuaranteeType: Expected '{r.GuaranteeType}', Context GuaranteeType is null/empty");
                    return false;
                }
                if (!MatchesSet(r.GuaranteeType, c.GuaranteeType))
                {
                    failures.Add($"GuaranteeType: Expected '{r.GuaranteeType}', Context GuaranteeType='{c.GuaranteeType}'");
                    return false;
                }
            }

            // Transaction Type
            if (!IsWildcard(r.TransactionType))
            {
                if (string.IsNullOrWhiteSpace(c.TransactionType))
                {
                    failures.Add($"TransactionType: Expected '{r.TransactionType}', Context TransactionType is null/empty");
                    return false;
                }
                if (!MatchesSet(r.TransactionType, c.TransactionType))
                {
                    failures.Add($"TransactionType: Expected '{r.TransactionType}', Context TransactionType='{c.TransactionType}'");
                    return false;
                }
            }

            // DWINGS link
            if (r.HasDwingsLink.HasValue && c.HasDwingsLink != r.HasDwingsLink.Value)
            {
                failures.Add($"HasDwingsLink: Expected {r.HasDwingsLink.Value}, Context={c.HasDwingsLink}");
                return false;
            }

            // Grouped
            if (r.IsGrouped.HasValue && c.IsGrouped != r.IsGrouped.Value)
            {
                failures.Add($"IsGrouped: Expected {r.IsGrouped.Value}, Context={c.IsGrouped}");
                return false;
            }

            // Amount match
            if (r.IsAmountMatch.HasValue && c.IsAmountMatch != r.IsAmountMatch.Value)
            {
                failures.Add($"IsAmountMatch: Expected {r.IsAmountMatch.Value}, Context={c.IsAmountMatch}");
                return false;
            }

            // Missing amount range
            if (r.MissingAmountMin.HasValue || r.MissingAmountMax.HasValue)
            {
                if (!c.MissingAmount.HasValue)
                {
                    failures.Add($"MissingAmount: Expected range [{r.MissingAmountMin}..{r.MissingAmountMax}], Context MissingAmount is null");
                    return false;
                }
                var amount = c.MissingAmount.Value;
                if (r.MissingAmountMin.HasValue && amount < r.MissingAmountMin.Value)
                {
                    failures.Add($"MissingAmount: Expected >= {r.MissingAmountMin.Value}, Context={amount}");
                    return false;
                }
                if (r.MissingAmountMax.HasValue && amount > r.MissingAmountMax.Value)
                {
                    failures.Add($"MissingAmount: Expected <= {r.MissingAmountMax.Value}, Context={amount}");
                    return false;
                }
            }

            // Sign
            if (!IsWildcard(r.Sign))
            {
                if (string.IsNullOrWhiteSpace(c.Sign))
                {
                    failures.Add($"Sign: Expected '{r.Sign}', Context Sign is null/empty");
                    return false;
                }
                if (!r.Sign.Equals(c.Sign, StringComparison.OrdinalIgnoreCase))
                {
                    failures.Add($"Sign: Expected '{r.Sign}', Context='{c.Sign}'");
                    return false;
                }
            }

            // MT status acked
            if (r.MTStatusAcked.HasValue)
            {
                if (!c.IsMtAcked.HasValue)
                {
                    failures.Add($"MTStatusAcked: Expected {r.MTStatusAcked.Value}, Context IsMtAcked is null");
                    return false;
                }
                if (c.IsMtAcked.Value != r.MTStatusAcked.Value)
                {
                    failures.Add($"MTStatusAcked: Expected {r.MTStatusAcked.Value}, Context={c.IsMtAcked.Value}");
                    return false;
                }
            }

            // COMM_ID_EMAIL flag
            if (r.CommIdEmail.HasValue)
            {
                if (!c.HasCommIdEmail.HasValue)
                {
                    failures.Add($"CommIdEmail: Expected {r.CommIdEmail.Value}, Context HasCommIdEmail is null");
                    return false;
                }
                if (c.HasCommIdEmail.Value != r.CommIdEmail.Value)
                {
                    failures.Add($"CommIdEmail: Expected {r.CommIdEmail.Value}, Context={c.HasCommIdEmail.Value}");
                    return false;
                }
            }

            // BGI status initiated
            if (r.BgiStatusInitiated.HasValue)
            {
                if (!c.IsBgiInitiated.HasValue)
                {
                    failures.Add($"BgiStatusInitiated: Expected {r.BgiStatusInitiated.Value}, Context IsBgiInitiated is null");
                    return false;
                }
                if (c.IsBgiInitiated.Value != r.BgiStatusInitiated.Value)
                {
                    failures.Add($"BgiStatusInitiated: Expected {r.BgiStatusInitiated.Value}, Context={c.IsBgiInitiated.Value}");
                    return false;
                }
            }

            // TriggerDateIsNull
            if (r.TriggerDateIsNull.HasValue)
            {
                if (!c.TriggerDateIsNull.HasValue)
                {
                    failures.Add($"TriggerDateIsNull: Expected {r.TriggerDateIsNull.Value}, Context TriggerDateIsNull is null");
                    return false;
                }
                if (c.TriggerDateIsNull.Value != r.TriggerDateIsNull.Value)
                {
                    failures.Add($"TriggerDateIsNull: Expected {r.TriggerDateIsNull.Value}, Context={c.TriggerDateIsNull.Value}");
                    return false;
                }
            }

            // DaysSinceTrigger range
            if (r.DaysSinceTriggerMin.HasValue || r.DaysSinceTriggerMax.HasValue)
            {
                if (!c.DaysSinceTrigger.HasValue)
                {
                    failures.Add($"DaysSinceTrigger: Expected range [{r.DaysSinceTriggerMin}..{r.DaysSinceTriggerMax}], Context is null");
                    return false;
                }
                var d = c.DaysSinceTrigger.Value;
                if (r.DaysSinceTriggerMin.HasValue && d < r.DaysSinceTriggerMin.Value)
                {
                    failures.Add($"DaysSinceTrigger: Expected >= {r.DaysSinceTriggerMin.Value}, Context={d}");
                    return false;
                }
                if (r.DaysSinceTriggerMax.HasValue && d > r.DaysSinceTriggerMax.Value)
                {
                    failures.Add($"DaysSinceTrigger: Expected <= {r.DaysSinceTriggerMax.Value}, Context={d}");
                    return false;
                }
            }

            // OperationDaysAgo range
            if (r.OperationDaysAgoMin.HasValue || r.OperationDaysAgoMax.HasValue)
            {
                if (!c.OperationDaysAgo.HasValue)
                {
                    failures.Add($"OperationDaysAgo: Expected range [{r.OperationDaysAgoMin}..{r.OperationDaysAgoMax}], Context is null");
                    return false;
                }
                var d = c.OperationDaysAgo.Value;
                if (r.OperationDaysAgoMin.HasValue && d < r.OperationDaysAgoMin.Value)
                {
                    failures.Add($"OperationDaysAgo: Expected >= {r.OperationDaysAgoMin.Value}, Context={d}");
                    return false;
                }
                if (r.OperationDaysAgoMax.HasValue && d > r.OperationDaysAgoMax.Value)
                {
                    failures.Add($"OperationDaysAgo: Expected <= {r.OperationDaysAgoMax.Value}, Context={d}");
                    return false;
                }
            }

            // IsMatched
            if (r.IsMatched.HasValue)
            {
                if (!c.IsMatched.HasValue)
                {
                    failures.Add($"IsMatched: Expected {r.IsMatched.Value}, Context IsMatched is null");
                    return false;
                }
                if (c.IsMatched.Value != r.IsMatched.Value)
                {
                    failures.Add($"IsMatched: Expected {r.IsMatched.Value}, Context={c.IsMatched.Value}");
                    return false;
                }
            }

            // HasManualMatch
            if (r.HasManualMatch.HasValue)
            {
                if (!c.HasManualMatch.HasValue)
                {
                    failures.Add($"HasManualMatch: Expected {r.HasManualMatch.Value}, Context HasManualMatch is null");
                    return false;
                }
                if (c.HasManualMatch.Value != r.HasManualMatch.Value)
                {
                    failures.Add($"HasManualMatch: Expected {r.HasManualMatch.Value}, Context={c.HasManualMatch.Value}");
                    return false;
                }
            }

            // IsFirstRequest
            if (r.IsFirstRequest.HasValue)
            {
                if (!c.IsFirstRequest.HasValue)
                {
                    failures.Add($"IsFirstRequest: Expected {r.IsFirstRequest.Value}, Context IsFirstRequest is null");
                    return false;
                }
                if (c.IsFirstRequest.Value != r.IsFirstRequest.Value)
                {
                    failures.Add($"IsFirstRequest: Expected {r.IsFirstRequest.Value}, Context={c.IsFirstRequest.Value}");
                    return false;
                }
            }

            // DaysSinceReminder range
            if (r.DaysSinceReminderMin.HasValue || r.DaysSinceReminderMax.HasValue)
            {
                if (!c.DaysSinceReminder.HasValue)
                {
                    failures.Add($"DaysSinceReminder: Expected range [{r.DaysSinceReminderMin}..{r.DaysSinceReminderMax}], Context is null");
                    return false;
                }
                var d = c.DaysSinceReminder.Value;
                if (r.DaysSinceReminderMin.HasValue && d < r.DaysSinceReminderMin.Value)
                {
                    failures.Add($"DaysSinceReminder: Expected >= {r.DaysSinceReminderMin.Value}, Context={d}");
                    return false;
                }
                if (r.DaysSinceReminderMax.HasValue && d > r.DaysSinceReminderMax.Value)
                {
                    failures.Add($"DaysSinceReminder: Expected <= {r.DaysSinceReminderMax.Value}, Context={d}");
                    return false;
                }
            }

            // CurrentActionId
            if (r.CurrentActionId.HasValue)
            {
                if (!c.CurrentActionId.HasValue)
                {
                    failures.Add($"CurrentActionId: Expected {r.CurrentActionId.Value}, Context CurrentActionId is null");
                    return false;
                }
                if (c.CurrentActionId.Value != r.CurrentActionId.Value)
                {
                    failures.Add($"CurrentActionId: Expected {r.CurrentActionId.Value}, Context={c.CurrentActionId.Value}");
                    return false;
                }
            }

            return true;
        }
        
        private static bool IsWildcard(string s)
        {
            return string.IsNullOrWhiteSpace(s) || s.Trim() == "*";
        }

        private static bool MatchesSet(string ruleValue, string ctxValue)
        {
            if (string.IsNullOrWhiteSpace(ruleValue)) return false;
            var parts = ruleValue.Split(new[] { ';', ',', '|' }, StringSplitOptions.RemoveEmptyEntries)
                                 .Select(x => x.Trim().ToUpperInvariant()).ToList();
            var val = (ctxValue ?? string.Empty).Trim().ToUpperInvariant();
            return parts.Contains(val);
        }
    }
}
