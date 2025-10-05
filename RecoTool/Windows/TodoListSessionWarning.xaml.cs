using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using RecoTool.Services;

namespace RecoTool.Windows
{
    public partial class TodoListSessionWarning : UserControl, INotifyPropertyChanged
    {
        private TodoListSessionTracker _sessionTracker;
        private int _currentTodoId;
        private DispatcherTimer _refreshTimer;
        private ObservableCollection<SessionViewModel> _activeSessions;
        private bool _hasActiveSessions;
        private bool _hasEditingSessions;

        public event PropertyChangedEventHandler PropertyChanged;

        public TodoListSessionWarning()
        {
            InitializeComponent();
            DataContext = this;

            _activeSessions = new ObservableCollection<SessionViewModel>();
            
            // Refresh every 10 seconds (aligned with session check)
            _refreshTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(10)
            };
            _refreshTimer.Tick += async (s, e) => await RefreshSessionsAsync();
        }

        public ObservableCollection<SessionViewModel> ActiveSessions
        {
            get => _activeSessions;
            set
            {
                _activeSessions = value;
                OnPropertyChanged(nameof(ActiveSessions));
            }
        }

        public bool HasActiveSessions
        {
            get => _hasActiveSessions;
            set
            {
                _hasActiveSessions = value;
                OnPropertyChanged(nameof(HasActiveSessions));
            }
        }

        public bool HasEditingSessions
        {
            get => _hasEditingSessions;
            set
            {
                _hasEditingSessions = value;
                OnPropertyChanged(nameof(HasEditingSessions));
            }
        }

        /// <summary>
        /// Initializes the warning control for a specific TodoList item
        /// </summary>
        public async Task InitializeAsync(TodoListSessionTracker sessionTracker, int todoId)
        {
            _sessionTracker = sessionTracker;
            _currentTodoId = todoId;

            await RefreshSessionsAsync();
            
            // Always start auto-refresh to detect new sessions
            if (!_refreshTimer.IsEnabled)
            {
                _refreshTimer.Start();
            }
        }

        /// <summary>
        /// Loads and displays active sessions for a TodoList item
        /// </summary>
        public async Task LoadSessionsAsync(TodoListSessionTracker sessionTracker, int todoId)
        {
            _sessionTracker = sessionTracker;
            _currentTodoId = todoId;

            await RefreshSessionsAsync();
            
            // Update UI visibility
            UpdateVisibility();
            
            // Always start auto-refresh to detect new sessions
            if (!_refreshTimer.IsEnabled)
            {
                _refreshTimer.Start();
            }
        }

        /// <summary>
        /// Stops monitoring and cleans up
        /// </summary>
        public void Stop()
        {
            _refreshTimer?.Stop();
        }

        /// <summary>
        /// Refreshes the list of active sessions
        /// </summary>
        private async Task RefreshSessionsAsync()
        {
            if (_sessionTracker == null || _currentTodoId == 0) return;

            try
            {
                var sessions = await _sessionTracker.GetActiveSessionsAsync(_currentTodoId);
                
                Application.Current?.Dispatcher.Invoke(() =>
                {
                    ActiveSessions.Clear();
                    foreach (var session in sessions.Where(s => s.IsActive))
                    {
                        ActiveSessions.Add(new SessionViewModel(session));
                    }

                    HasActiveSessions = ActiveSessions.Count > 0;
                    HasEditingSessions = ActiveSessions.Any(s => s.IsEditing);
                    
                    UpdateVisibility();
                });
            }
            catch { /* Silently fail */ }
        }

        /// <summary>
        /// Updates the visibility of the control and its child elements
        /// </summary>
        private void UpdateVisibility()
        {
            try
            {
                // Show/hide the entire control
                Visibility = HasActiveSessions ? Visibility.Visible : Visibility.Collapsed;
                
                // Show/hide the editing warning
                var editingWarning = FindName("EditingWarning") as TextBlock;
                if (editingWarning != null)
                {
                    editingWarning.Visibility = HasEditingSessions ? Visibility.Visible : Visibility.Collapsed;
                }
            }
            catch { /* Best effort */ }
        }

        protected void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    /// <summary>
    /// ViewModel for displaying session information
    /// </summary>
    public class SessionViewModel
    {
        private readonly TodoSessionInfo _session;

        public SessionViewModel(TodoSessionInfo session)
        {
            _session = session;
        }

        public string UserName => _session.UserName ?? _session.UserId;
        public bool IsEditing => _session.IsEditing;

        public string StatusText => IsEditing ? "editing" : "viewing";

        public Brush StatusColor => IsEditing 
            ? new SolidColorBrush(Color.FromRgb(220, 53, 69))  // Red for editing
            : new SolidColorBrush(Color.FromRgb(255, 193, 7)); // Yellow for viewing

        public string DurationText
        {
            get
            {
                var duration = _session.Duration;
                if (duration.TotalMinutes < 1)
                    return "(just now)";
                if (duration.TotalMinutes < 60)
                    return $"({(int)duration.TotalMinutes}m ago)";
                return $"({(int)duration.TotalHours}h ago)";
            }
        }
    }
}
