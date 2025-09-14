using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Diagnostics;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    // Partial: Paging and scroll handling for ReconciliationView
    public partial class ReconciliationView
    {
        // Wire the DataGrid's ScrollViewer for incremental loading
        private void TryHookResultsGridScroll(DataGrid dg)
        {
            try
            {
                if (_scrollHooked || dg == null) return;
                _resultsScrollViewer = VisualTreeHelpers.FindDescendant<ScrollViewer>(dg);
                if (_resultsScrollViewer != null)
                {
                    _resultsScrollViewer.ScrollChanged -= ResultsScrollViewer_ScrollChanged;
                    _resultsScrollViewer.ScrollChanged += ResultsScrollViewer_ScrollChanged;
                    _scrollHooked = true;
                }
                // Cache the footer button once
                if (_loadMoreFooterButton == null)
                {
                    _loadMoreFooterButton = this.FindName("LoadMoreFooterButton") as Button;
                }
            }
            catch { }
        }

        private void ResultsScrollViewer_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                if (_filteredData == null || _filteredData.Count == 0) return;
                var sv = sender as ScrollViewer;
                if (sv == null) return;
                // Ignore horizontal-only scroll changes to avoid unnecessary UI work
                if (e != null && Math.Abs(e.VerticalChange) < double.Epsilon && Math.Abs(e.ExtentHeightChange) < double.Epsilon && Math.Abs(e.ViewportHeightChange) < double.Epsilon)
                {
                    return;
                }
                // Show footer button when user reaches bottom (android-like behavior)
                bool atBottom = sv.ScrollableHeight > 0 && sv.VerticalOffset >= (sv.ScrollableHeight * 0.9);
                int remaining = Math.Max(0, _filteredData.Count - _loadedCount);
                if (_loadMoreFooterButton == null)
                {
                    _loadMoreFooterButton = this.FindName("LoadMoreFooterButton") as Button;
                }
                if (_loadMoreFooterButton != null)
                {
                    var desired = (atBottom && remaining > 0) ? Visibility.Visible : Visibility.Collapsed;
                    if (_loadMoreFooterButton.Visibility != desired)
                    {
                        _loadMoreFooterButton.Visibility = desired;
                    }
                }

                sw.Stop();
                // Throttle perf log to once every ScrollLogThrottleMs
                var now = DateTime.Now;
                if ((now - _lastScrollPerfLog).TotalMilliseconds >= ScrollLogThrottleMs)
                {
                    try
                    {
                        // place for perf diagnostics if needed
                    }
                    catch { }
                    _lastScrollPerfLog = now;
                }
            }
            catch { }
        }

        private void LoadMorePage()
        {
            if (_isLoadingMore) return;
            _isLoadingMore = true;
            try
            {
                if (_filteredData == null) return;
                int remaining = _filteredData.Count - _loadedCount;
                if (remaining <= 0) return;
                int take = Math.Min(InitialPageSize, remaining);
                foreach (var item in _filteredData.Skip(_loadedCount).Take(take))
                {
                    ViewData.Add(item);
                }
                _loadedCount += take;
                UpdateStatusInfo($"{ViewData.Count} / {_filteredData.Count} lines displayed");
                // After load, hide footer if no more data, otherwise keep visible when still at bottom
                if (_loadMoreFooterButton == null)
                {
                    _loadMoreFooterButton = this.FindName("LoadMoreFooterButton") as Button;
                }
                if (_loadMoreFooterButton != null)
                {
                    int newRemaining = _filteredData.Count - _loadedCount;
                    if (newRemaining <= 0 && _loadMoreFooterButton.Visibility != Visibility.Collapsed)
                        _loadMoreFooterButton.Visibility = Visibility.Collapsed;
                }
            }
            catch { }
            finally
            {
                _isLoadingMore = false;
            }
        }

        // Footer button click handler to load the next page of rows
        private void LoadMoreButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                LoadMorePage();
                e.Handled = true;
            }
            catch { }
        }
    }
}
