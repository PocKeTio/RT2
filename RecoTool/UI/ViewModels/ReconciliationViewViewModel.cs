using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Linq;
using RecoTool.Services.DTOs;
using RecoTool.Domain.Filters;
using RecoTool.UI.Models;

namespace RecoTool.UI.ViewModels
{
    /// <summary>
    /// MVVM skeleton for ReconciliationView. Holds the list bound to the grid and future filter state.
    /// </summary>
    public sealed class ReconciliationViewViewModel : INotifyPropertyChanged
    {
        private ObservableCollection<ReconciliationViewData> _viewData = new ObservableCollection<ReconciliationViewData>();
        public ObservableCollection<ReconciliationViewData> ViewData
        {
            get => _viewData;
            set { _viewData = value ?? new ObservableCollection<ReconciliationViewData>(); OnPropertyChanged(); }
        }

        // Centralized filter state for the view
        private FilterState _currentFilter = new FilterState();
        public FilterState CurrentFilter
        {
            get => _currentFilter;
            set { _currentFilter = value ?? new FilterState(); OnPropertyChanged(); }
        }

        // Wrapper properties for XAML binding (TwoWay) that proxy to CurrentFilter
        public string FilterReconciliationNum
        {
            get => CurrentFilter.ReconciliationNum;
            set { if (CurrentFilter.ReconciliationNum != value) { CurrentFilter.ReconciliationNum = value; OnPropertyChanged(); } }
        }

        public string FilterRawLabel
        {
            get => CurrentFilter.RawLabel;
            set { if (CurrentFilter.RawLabel != value) { CurrentFilter.RawLabel = value; OnPropertyChanged(); } }
        }

        public string FilterEventNum
        {
            get => CurrentFilter.EventNum;
            set { if (CurrentFilter.EventNum != value) { CurrentFilter.EventNum = value; OnPropertyChanged(); } }
        }

        public string FilterComments
        {
            get => CurrentFilter.Comments;
            set { if (CurrentFilter.Comments != value) { CurrentFilter.Comments = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterFromDate
        {
            get => CurrentFilter.FromDate;
            set { if (CurrentFilter.FromDate != value) { CurrentFilter.FromDate = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterToDate
        {
            get => CurrentFilter.ToDate;
            set { if (CurrentFilter.ToDate != value) { CurrentFilter.ToDate = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterDeletedDate
        {
            get => CurrentFilter.DeletedDate;
            set { if (CurrentFilter.DeletedDate != value) { CurrentFilter.DeletedDate = value; OnPropertyChanged(); } }
        }

        public string FilterCurrency
        {
            get => CurrentFilter.Currency;
            set { if (CurrentFilter.Currency != value) { CurrentFilter.Currency = value; OnPropertyChanged(); } }
        }

        public decimal? FilterMinAmount
        {
            get => CurrentFilter.MinAmount;
            set { if (CurrentFilter.MinAmount != value) { CurrentFilter.MinAmount = value; OnPropertyChanged(); } }
        }

        public decimal? FilterMaxAmount
        {
            get => CurrentFilter.MaxAmount;
            set { if (CurrentFilter.MaxAmount != value) { CurrentFilter.MaxAmount = value; OnPropertyChanged(); } }
        }

        public string FilterDwGuaranteeId
        {
            get => CurrentFilter.DwGuaranteeId;
            set { if (CurrentFilter.DwGuaranteeId != value) { CurrentFilter.DwGuaranteeId = value; OnPropertyChanged(); } }
        }

        public string FilterDwCommissionId
        {
            get => CurrentFilter.DwCommissionId;
            set { if (CurrentFilter.DwCommissionId != value) { CurrentFilter.DwCommissionId = value; OnPropertyChanged(); } }
        }

        public string FilterDwInvoiceId
        {
            get => CurrentFilter.DwInvoiceId;
            set { if (CurrentFilter.DwInvoiceId != value) { CurrentFilter.DwInvoiceId = value; OnPropertyChanged(); } }
        }

        public string FilterGuaranteeType
        {
            get => CurrentFilter.GuaranteeType;
            set { if (CurrentFilter.GuaranteeType != value) { CurrentFilter.GuaranteeType = value; OnPropertyChanged(); } }
        }

        public int? FilterTransactionTypeId
        {
            get => CurrentFilter.TransactionTypeId;
            set { if (CurrentFilter.TransactionTypeId != value) { CurrentFilter.TransactionTypeId = value; OnPropertyChanged(); } }
        }

        public string FilterGuaranteeStatus
        {
            get => CurrentFilter.GuaranteeStatus;
            set { if (CurrentFilter.GuaranteeStatus != value) { CurrentFilter.GuaranteeStatus = value; OnPropertyChanged(); } }
        }

        public int? FilterActionId
        {
            get => CurrentFilter.ActionId;
            set { if (CurrentFilter.ActionId != value) { CurrentFilter.ActionId = value; OnPropertyChanged(); } }
        }

        public int? FilterKpiId
        {
            get => CurrentFilter.KpiId;
            set { if (CurrentFilter.KpiId != value) { CurrentFilter.KpiId = value; OnPropertyChanged(); } }
        }

        public int? FilterIncidentTypeId
        {
            get => CurrentFilter.IncidentTypeId;
            set { if (CurrentFilter.IncidentTypeId != value) { CurrentFilter.IncidentTypeId = value; OnPropertyChanged(); } }
        }

        public bool? FilterActionDone
        {
            get => CurrentFilter.ActionDone;
            set { if (CurrentFilter.ActionDone != value) { CurrentFilter.ActionDone = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterActionDateFrom
        {
            get => CurrentFilter.ActionDateFrom;
            set { if (CurrentFilter.ActionDateFrom != value) { CurrentFilter.ActionDateFrom = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterActionDateTo
        {
            get => CurrentFilter.ActionDateTo;
            set { if (CurrentFilter.ActionDateTo != value) { CurrentFilter.ActionDateTo = value; OnPropertyChanged(); } }
        }

        public bool FilterPotentialDuplicates
        {
            get => CurrentFilter.PotentialDuplicates == true;
            set { var v = (bool?)value; if (CurrentFilter.PotentialDuplicates != v) { CurrentFilter.PotentialDuplicates = v; OnPropertyChanged(); } }
        }

        // New filters
        public bool FilterUnmatched
        {
            get => CurrentFilter.Unmatched == true;
            set { var v = (bool?)value; if (CurrentFilter.Unmatched != v) { CurrentFilter.Unmatched = v; OnPropertyChanged(); } }
        }

        public bool FilterNewLines
        {
            get => CurrentFilter.NewLines == true;
            set { var v = (bool?)value; if (CurrentFilter.NewLines != v) { CurrentFilter.NewLines = v; OnPropertyChanged(); } }
        }

        public string FilterAssigneeId
        {
            get => CurrentFilter.AssigneeId;
            set { if (CurrentFilter.AssigneeId != value) { CurrentFilter.AssigneeId = value; OnPropertyChanged(); } }
        }

        public string FilterAccountId
        {
            get => CurrentFilter.AccountId;
            set { if (CurrentFilter.AccountId != value) { CurrentFilter.AccountId = value; OnPropertyChanged(); } }
        }

        public string FilterStatus
        {
            get => CurrentFilter.Status;
            set { if (CurrentFilter.Status != value) { CurrentFilter.Status = value; OnPropertyChanged(); } }
        }

        public string FilterLastReviewed
        {
            get => CurrentFilter.LastReviewed;
            set { if (CurrentFilter.LastReviewed != value) { CurrentFilter.LastReviewed = value; OnPropertyChanged(); } }
        }

        /// <summary>
        /// Apply filters held in CurrentFilter to the provided dataset.
        /// If excludeTransactionType is true, the TransactionTypeId step is skipped (used to refresh options).
        /// OPTIMIZED: Uses single-pass filtering with combined predicate instead of chained LINQ Where() calls
        /// </summary>
        public List<ReconciliationViewData> ApplyFilters(IEnumerable<ReconciliationViewData> source, bool excludeTransactionType = false)
        {
            if (source == null) return new List<ReconciliationViewData>();
            var f = CurrentFilter ?? new FilterState();
            
            // Pre-compute filter values once
            var accountId = f.AccountId?.Trim();
            var currency = f.Currency;
            var reconciliationNum = f.ReconciliationNum;
            var rawLabel = f.RawLabel;
            var eventNum = f.EventNum;
            var comments = f.Comments;
            var guaranteeStatus = f.GuaranteeStatus?.Trim();
            var dwInvoiceId = f.DwInvoiceId;
            var dwGuaranteeId = f.DwGuaranteeId;
            var dwCommissionId = f.DwCommissionId;
            var assigneeId = f.AssigneeId;
            var status = f.Status;
            var deletedDay = f.DeletedDate?.Date;
            var deletedNext = deletedDay?.AddDays(1);
            var today = DateTime.Today;
            var oneWeekAgo = today.AddDays(-7);
            var oneMonthAgo = today.AddMonths(-1);
            
            // Single-pass filter with combined predicate
            var result = new List<ReconciliationViewData>();
            foreach (var x in source)
            {
                // Account
                if (!string.IsNullOrWhiteSpace(accountId) && 
                    !string.Equals(x.Account_ID?.Trim(), accountId, System.StringComparison.OrdinalIgnoreCase))
                    continue;

                // Currency contains
                if (!string.IsNullOrWhiteSpace(currency) && 
                    (x.CCY == null || x.CCY.IndexOf(currency, System.StringComparison.OrdinalIgnoreCase) < 0))
                    continue;

                // Amount range
                if (f.MinAmount.HasValue && x.SignedAmount < f.MinAmount.Value) continue;
                if (f.MaxAmount.HasValue && x.SignedAmount > f.MaxAmount.Value) continue;

                // Date range (operation)
                if (f.FromDate.HasValue && (!x.Operation_Date.HasValue || x.Operation_Date.Value < f.FromDate.Value)) continue;
                if (f.ToDate.HasValue && (!x.Operation_Date.HasValue || x.Operation_Date.Value > f.ToDate.Value)) continue;

                // Reference contains
                if (!string.IsNullOrWhiteSpace(reconciliationNum) && 
                    (x.Reconciliation_Num ?? string.Empty).IndexOf(reconciliationNum, System.StringComparison.OrdinalIgnoreCase) < 0)
                    continue;
                if (!string.IsNullOrWhiteSpace(rawLabel) && 
                    (x.RawLabel ?? string.Empty).IndexOf(rawLabel, System.StringComparison.OrdinalIgnoreCase) < 0)
                    continue;
                if (!string.IsNullOrWhiteSpace(eventNum) && 
                    (x.Event_Num ?? string.Empty).IndexOf(eventNum, System.StringComparison.OrdinalIgnoreCase) < 0)
                    continue;

                // Comments contains
                if (!string.IsNullOrWhiteSpace(comments) && 
                    (x.Comments ?? string.Empty).IndexOf(comments, System.StringComparison.OrdinalIgnoreCase) < 0)
                    continue;

                // Guarantee status contains
                if (!string.IsNullOrWhiteSpace(guaranteeStatus) && 
                    (x.GUARANTEE_STATUS ?? string.Empty).IndexOf(guaranteeStatus, System.StringComparison.OrdinalIgnoreCase) < 0)
                    continue;

                // DW ids
                if (!string.IsNullOrWhiteSpace(dwInvoiceId))
                {
                    if ((x.DWINGS_InvoiceID ?? string.Empty).IndexOf(dwInvoiceId, System.StringComparison.OrdinalIgnoreCase) < 0 &&
                        (x.INVOICE_ID ?? string.Empty).IndexOf(dwInvoiceId, System.StringComparison.OrdinalIgnoreCase) < 0)
                        continue;
                }
                if (!string.IsNullOrWhiteSpace(dwGuaranteeId))
                {
                    if ((x.DWINGS_GuaranteeID ?? string.Empty).IndexOf(dwGuaranteeId, System.StringComparison.OrdinalIgnoreCase) < 0 &&
                        (x.GUARANTEE_ID ?? string.Empty).IndexOf(dwGuaranteeId, System.StringComparison.OrdinalIgnoreCase) < 0)
                        continue;
                }
                if (!string.IsNullOrWhiteSpace(dwCommissionId))
                {
                    if ((x.DWINGS_BGPMT ?? string.Empty).IndexOf(dwCommissionId, System.StringComparison.OrdinalIgnoreCase) < 0 &&
                        (x.COMMISSION_ID ?? string.Empty).IndexOf(dwCommissionId, System.StringComparison.OrdinalIgnoreCase) < 0)
                        continue;
                }

                // Deleted date exact day
                if (deletedDay.HasValue)
                {
                    if (!x.DeleteDate.HasValue || x.DeleteDate.Value < deletedDay.Value || x.DeleteDate.Value >= deletedNext.Value)
                        continue;
                }

                // Status (Live/Archived)
                if (!string.IsNullOrWhiteSpace(status) && !string.Equals(status, "All", System.StringComparison.OrdinalIgnoreCase))
                {
                    if (string.Equals(status, "Live", System.StringComparison.OrdinalIgnoreCase) && x.DeleteDate.HasValue)
                        continue;
                    if (string.Equals(status, "Archived", System.StringComparison.OrdinalIgnoreCase) && !x.DeleteDate.HasValue)
                        continue;
                }

                // Transaction type by enum id (Category)
                if (!excludeTransactionType && f.TransactionTypeId.HasValue)
                {
                    if (!x.Category.HasValue || x.Category.Value != f.TransactionTypeId.Value)
                        continue;
                }

                // Referential filters
                if (f.ActionId.HasValue && x.Action != f.ActionId) continue;
                if (f.KpiId.HasValue && x.KPI != f.KpiId) continue;
                if (f.IncidentTypeId.HasValue && x.IncidentType != f.IncidentTypeId) continue;
                if (!string.IsNullOrWhiteSpace(assigneeId) && 
                    !string.Equals(x.Assignee, assigneeId, System.StringComparison.OrdinalIgnoreCase))
                    continue;

                // Potential Duplicates
                if (f.PotentialDuplicates == true && !x.IsPotentialDuplicate)
                    continue;

                // Unmatched: no invoice linked (DWINGS invoice id AND InternalInvoiceReference are blank)
                if (f.Unmatched == true)
                {
                    if (!string.IsNullOrWhiteSpace(x.DWINGS_InvoiceID) || !string.IsNullOrWhiteSpace(x.InternalInvoiceReference))
                        continue;
                }

                // New lines: appeared in Ambre today (based on AMBRE CreationDate)
                if (f.NewLines == true)
                {
                    if (!x.CreationDate.HasValue || x.CreationDate.Value.Date != today)
                        continue;
                }

                // Action Done and Action Date
                if (f.ActionDone.HasValue)
                {
                    if (f.ActionDone.Value) 
                    {
                        // Done = ActionStatus is true
                        if (x.ActionStatus != true)
                            continue;
                    }
                    else 
                    {
                        // Pending = has Action AND ActionStatus is false (or null)
                        if (!x.Action.HasValue || x.ActionStatus == true)
                            continue;
                    }
                }
                if (f.ActionDateFrom.HasValue && (!x.ActionDate.HasValue || x.ActionDate.Value < f.ActionDateFrom.Value))
                    continue;
                if (f.ActionDateTo.HasValue && (!x.ActionDate.HasValue || x.ActionDate.Value > f.ActionDateTo.Value))
                    continue;

                // Last Reviewed filter (based on ActionStatus = Done and ActionDate)
                if (!string.IsNullOrWhiteSpace(f.LastReviewed))
                {
                    switch (f.LastReviewed)
                    {
                        case "Never":
                            // Not reviewed = no action or action status is Pending
                            if (x.Action.HasValue && x.ActionStatus == true)
                                continue;
                            break;
                        case "Today":
                            // Reviewed today = ActionStatus Done today
                            if (x.ActionStatus != true || !x.ActionDate.HasValue || x.ActionDate.Value.Date != today)
                                continue;
                            break;
                        case "1week":
                            // Reviewed in last week
                            if (x.ActionStatus != true || !x.ActionDate.HasValue || x.ActionDate.Value.Date < oneWeekAgo)
                                continue;
                            break;
                        case "1month":
                            // Reviewed in last month
                            if (x.ActionStatus != true || !x.ActionDate.HasValue || x.ActionDate.Value.Date < oneMonthAgo)
                                continue;
                            break;
                    }
                }

                // Passed all filters - add to result
                result.Add(x);
            }

            return result;
        }

        // TransactionType options for UI binding
        private ObservableCollection<OptionItem> _transactionTypeOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> TransactionTypeOptions
        {
            get => _transactionTypeOptions;
            private set { _transactionTypeOptions = value ?? new ObservableCollection<OptionItem>(); OnPropertyChanged(nameof(TransactionTypeOptions)); }
        }

        public void LoadTransactionTypeOptions()
        {
            try
            {
                var list = new List<OptionItem> { new OptionItem { Id = -1, Name = string.Empty } };
                foreach (var v in Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>())
                {
                    list.Add(new OptionItem { Id = (int)v, Name = v.ToString().Replace('_', ' ') });
                }
                TransactionTypeOptions = new ObservableCollection<OptionItem>(list);
            }
            catch { }
        }

        public void UpdateTransactionTypeOptionsForData(IEnumerable<ReconciliationViewData> data)
        {
            try
            {
                if (data == null) return;
                var ids = data.Where(x => x?.Category != null)
                              .Select(x => x.Category.Value)
                              .Distinct()
                              .OrderBy(i => i)
                              .ToList();
                var target = new List<OptionItem> { new OptionItem { Id = -1, Name = string.Empty } };
                foreach (var id in ids)
                {
                    var name = Enum.GetName(typeof(TransactionType), id)?.Replace('_', ' ') ?? id.ToString();
                    target.Add(new OptionItem { Id = id, Name = name });
                }
                var same = (TransactionTypeOptions?.Count ?? 0) == target.Count
                           && TransactionTypeOptions.Zip(target, (a, b) => a.Id == b.Id && string.Equals(a.Name, b.Name, StringComparison.Ordinal)).All(eq => eq);
                if (!same)
                    TransactionTypeOptions = new ObservableCollection<OptionItem>(target);

                // Coerce selection to All if not available
                if (FilterTransactionTypeId.HasValue && !ids.Contains(FilterTransactionTypeId.Value))
                    FilterTransactionTypeId = null;
            }
            catch { }
        }

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged([CallerMemberName] string name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
