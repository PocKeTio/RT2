using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using RecoTool.Services;

namespace RecoTool.Windows
{
    public partial class FilterPickerWindow : Window
    {
        private readonly UserFilterService _service;
        // Provider returns plain names (for compatibility) which we wrap into PickerItem
        private readonly Func<string, IEnumerable<string>> _listProvider;
        // Optional detailed provider returning (Name, Creator)
        private readonly Func<string, IEnumerable<(string Name, string Creator)>> _detailedProvider;
        private readonly Func<string, bool> _deleteProvider;
        public string SelectedFilterName { get; private set; }

        private class PickerItem
        {
            public string Name { get; set; }
            public string Creator { get; set; }
            public string Display { get; set; }
        }

        public FilterPickerWindow()
        {
            InitializeComponent();
            // Default mode = Filters
            _service = new UserFilterService(RecoTool.Properties.Settings.Default.ReferentialDB, Environment.UserName);
            // Default list shows filters with creator
            _listProvider = (contains) => string.IsNullOrWhiteSpace(contains)
                ? _service.ListUserFilterNames()
                : _service.ListUserFilterNames(contains);
            _deleteProvider = (name) => _service.DeleteUserFilter(name);
            LoadList();
        }

        /// <summary>
        /// Generic constructor allowing re-use for other pickable entities (e.g., Saved Views).
        /// Provide a title, a list provider filtered by search text, and a delete provider.
        /// </summary>
        public FilterPickerWindow(string title,
                                  Func<string, IEnumerable<string>> listProvider,
                                  Func<string, bool> deleteProvider)
            : this()
        {
            // Override providers and title
            this.Title = string.IsNullOrWhiteSpace(title) ? this.Title : title;
            _listProvider = listProvider ?? _listProvider;
            _deleteProvider = deleteProvider ?? _deleteProvider;
            // In generic mode (e.g., Saved Views), do not use default filter service
            _service = null;
            LoadList();
        }

        /// <summary>
        /// Overload allowing a detailed provider (Name, Creator) to populate list with creator.
        /// </summary>
        public FilterPickerWindow(string title,
                                  Func<string, IEnumerable<string>> listProvider,
                                  Func<string, bool> deleteProvider,
                                  Func<string, IEnumerable<(string Name, string Creator)>> detailedProvider)
            : this(title, listProvider, deleteProvider)
        {
            _detailedProvider = detailedProvider;
            LoadList();
        }

        private void LoadList(string contains = null)
        {
            // Prefer detailed listing (name + creator) when available
            IEnumerable<PickerItem> items;
            if (_detailedProvider != null)
            {
                var detailed = _detailedProvider(contains) ?? Enumerable.Empty<(string Name, string Creator)>();
                items = detailed.Select(d => new PickerItem
                {
                    Name = d.Name,
                    Creator = d.Creator,
                    Display = string.IsNullOrWhiteSpace(d.Creator) ? d.Name : ($"{d.Name} — {d.Creator}")
                });
            }
            else if (_service != null)
            {
                var detailed = _service.ListUserFiltersDetailed(contains);
                items = detailed.Select(d => new PickerItem
                {
                    Name = d.Name,
                    Creator = d.CreatedBy,
                    Display = string.IsNullOrWhiteSpace(d.CreatedBy) ? d.Name : ($"{d.Name} — {d.CreatedBy}")
                });
            }
            else
            {
                // Generic mode: wrap plain names from provider
                var names = _listProvider != null ? _listProvider(contains) : Enumerable.Empty<string>();
                items = names.Select(n => new PickerItem { Name = n, Creator = string.Empty, Display = n });
            }

            FiltersList.ItemsSource = items.ToList();
        }

        private void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            LoadList(SearchBox.Text);
        }

        private void Ok_Click(object sender, RoutedEventArgs e)
        {
            var item = FiltersList.SelectedItem as PickerItem;
            var name = item?.Name;
            if (string.IsNullOrWhiteSpace(name))
            {
                MessageBox.Show("Please select an item.");
                return;
            }
            SelectedFilterName = name;
            DialogResult = true;
        }

        private void Delete_Click(object sender, RoutedEventArgs e)
        {
            var item = FiltersList.SelectedItem as PickerItem;
            var name = item?.Name;
            if (string.IsNullOrWhiteSpace(name)) return;
            var res = MessageBox.Show($"Delete '{name}'?", "Confirmation", MessageBoxButton.YesNo, MessageBoxImage.Question);
            if (res != MessageBoxResult.Yes) return;
            if (_deleteProvider != null ? _deleteProvider(name) : false)
            {
                LoadList(SearchBox.Text);
            }
            else
            {
                MessageBox.Show("Deletion failed.");
            }
        }
    }
}
