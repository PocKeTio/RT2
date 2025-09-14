using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using RecoTool.UI.Helpers;

namespace RecoTool.Windows
{
    // Partial: Column layout capture/apply and header context menu for ReconciliationView
    public partial class ReconciliationView
    {
        private class ColumnSetting
        {
            public string Header { get; set; }
            public string SortMemberPath { get; set; }
            public int DisplayIndex { get; set; }
            public double? WidthValue { get; set; } // store as pixel width when possible
            public string WidthType { get; set; } // Auto, SizeToCells, SizeToHeader, Pixel
            public bool Visible { get; set; }
        }

        private class GridLayout
        {
            public List<ColumnSetting> Columns { get; set; } = new List<ColumnSetting>();
            public List<SortDescriptor> Sorts { get; set; } = new List<SortDescriptor>();
        }

        private class SortDescriptor
        {
            public string Member { get; set; }
            public ListSortDirection Direction { get; set; }
        }

        private GridLayout CaptureGridLayout()
        {
            var layout = new GridLayout();
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return layout;

                foreach (var col in dg.Columns)
                {
                    var st = new ColumnSetting
                    {
                        Header = Convert.ToString(col.Header),
                        SortMemberPath = col.SortMemberPath,
                        DisplayIndex = col.DisplayIndex,
                        Visible = col.Visibility == Visibility.Visible
                    };
                    if (col.Width.IsAbsolute)
                    {
                        st.WidthType = "Pixel";
                        st.WidthValue = col.Width.Value;
                    }
                    else if (col.Width.IsAuto)
                    {
                        st.WidthType = "Auto";
                    }
                    else if (col.Width.UnitType == DataGridLengthUnitType.SizeToCells)
                    {
                        st.WidthType = "SizeToCells";
                    }
                    else if (col.Width.UnitType == DataGridLengthUnitType.SizeToHeader)
                    {
                        st.WidthType = "SizeToHeader";
                    }
                    layout.Columns.Add(st);
                }

                var view = CollectionViewSource.GetDefaultView((this as UserControl).DataContext == this ? ViewData : dg.ItemsSource) as ICollectionView;
                if (view != null)
                {
                    foreach (var sd in view.SortDescriptions)
                    {
                        layout.Sorts.Add(new SortDescriptor { Member = sd.PropertyName, Direction = sd.Direction });
                    }
                }
            }
            catch { }
            return layout;
        }

        private void ApplyGridLayout(GridLayout layout)
        {
            try
            {
                if (layout == null) return;
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;

                // Map by header text
                foreach (var setting in layout.Columns)
                {
                    var col = dg.Columns.FirstOrDefault(c => string.Equals(Convert.ToString(c.Header), setting.Header, StringComparison.OrdinalIgnoreCase));
                    if (col == null) continue;
                    try { col.DisplayIndex = Math.Max(0, Math.Min(setting.DisplayIndex, dg.Columns.Count - 1)); } catch { }
                    try { col.Visibility = setting.Visible ? Visibility.Visible : Visibility.Collapsed; } catch { }
                    try
                    {
                        switch (setting.WidthType)
                        {
                            case "Pixel":
                                if (setting.WidthValue.HasValue && setting.WidthValue.Value > 0)
                                    col.Width = new DataGridLength(setting.WidthValue.Value);
                                break;
                            case "Auto":
                                col.Width = DataGridLength.Auto;
                                break;
                            case "SizeToCells":
                                col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToCells);
                                break;
                            case "SizeToHeader":
                                col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToHeader);
                                break;
                        }
                    }
                    catch { }
                }

                // Apply sorting
                var view = CollectionViewSource.GetDefaultView(dg.ItemsSource);
                if (view != null)
                {
                    using (view.DeferRefresh())
                    {
                        view.SortDescriptions.Clear();
                        foreach (var s in layout.Sorts)
                        {
                            if (!string.IsNullOrWhiteSpace(s.Member))
                                view.SortDescriptions.Add(new SortDescription(s.Member, s.Direction));
                        }
                    }
                }
            }
            catch { }
        }

        private void ResultsDataGrid_PreviewMouseRightButtonUp(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;
                var dep = e.OriginalSource as DependencyObject;
                var header = VisualTreeHelpers.FindAncestor<DataGridColumnHeader>(dep);
                if (header != null)
                {
                    e.Handled = true;
                    var cm = new ContextMenu();
                    foreach (var col in dg.Columns)
                    {
                        var mi = new MenuItem { Header = Convert.ToString(col.Header), IsCheckable = true, IsChecked = col.Visibility == Visibility.Visible };
                        mi.Click += (s, ev) =>
                        {
                            try
                            {
                                col.Visibility = mi.IsChecked ? Visibility.Visible : Visibility.Collapsed;
                            }
                            catch { }
                        };
                        cm.Items.Add(mi);
                    }
                    cm.IsOpen = true;
                }
            }
            catch { }
        }

        // Public helper to apply a saved grid layout from its JSON representation.
        public void ApplyLayoutJson(string layoutJson)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(layoutJson)) return;
                var layout = System.Text.Json.JsonSerializer.Deserialize<GridLayout>(layoutJson);
                ApplyGridLayout(layout);
            }
            catch { /* ignore invalid layout JSON */ }
        }
    }
}
