using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using RecoTool.Services;

namespace RecoTool.Helpers
{
    /// <summary>
    /// Helper class for multi-user conflict prevention and warnings
    /// </summary>
    public static class MultiUserHelper
    {
        /// <summary>
        /// Checks if a TodoList item is being edited by another user and shows a warning dialog
        /// Returns true if the user should proceed, false if they should cancel
        /// </summary>
        public static async Task<bool> CheckAndWarnBeforeEditAsync(
            TodoListSessionTracker sessionTracker, 
            int todoId, 
            string itemName)
        {
            if (sessionTracker == null) return true;

            try
            {
                var sessions = await sessionTracker.GetActiveSessionsAsync(todoId);
                var activeSessions = sessions.Where(s => s.IsActive).ToList();

                if (activeSessions.Count == 0)
                    return true; // No other users, proceed

                var editingSessions = activeSessions.Where(s => s.IsEditing).ToList();
                var viewingSessions = activeSessions.Where(s => !s.IsEditing).ToList();

                // Build warning message
                var sb = new StringBuilder();
                sb.AppendLine($"⚠️ Multi-User Warning for: {itemName}");
                sb.AppendLine();

                if (editingSessions.Count > 0)
                {
                    sb.AppendLine("🔴 CURRENTLY BEING EDITED BY:");
                    foreach (var session in editingSessions)
                    {
                        sb.AppendLine($"   • {session.UserName ?? session.UserId} (for {FormatDuration(session.Duration)})");
                    }
                    sb.AppendLine();
                }

                if (viewingSessions.Count > 0)
                {
                    sb.AppendLine("👁️ Currently being viewed by:");
                    foreach (var session in viewingSessions)
                    {
                        sb.AppendLine($"   • {session.UserName ?? session.UserId} (for {FormatDuration(session.Duration)})");
                    }
                    sb.AppendLine();
                }

                if (editingSessions.Count > 0)
                {
                    sb.AppendLine("⚠️ WARNING: Editing this item now may cause conflicts!");
                    sb.AppendLine("Your changes might overwrite theirs or vice versa.");
                    sb.AppendLine();
                    sb.AppendLine("Do you want to proceed anyway?");

                    var result = MessageBox.Show(
                        sb.ToString(),
                        "Multi-User Conflict Warning",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Warning,
                        MessageBoxResult.No);

                    return result == MessageBoxResult.Yes;
                }
                else
                {
                    sb.AppendLine("ℹ️ Other users are viewing this item.");
                    sb.AppendLine("Proceed with caution to avoid surprising them.");
                    sb.AppendLine();
                    sb.AppendLine("Do you want to continue?");

                    var result = MessageBox.Show(
                        sb.ToString(),
                        "Multi-User Information",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Information,
                        MessageBoxResult.Yes);

                    return result == MessageBoxResult.Yes;
                }
            }
            catch
            {
                // On error, allow the operation (fail open)
                return true;
            }
        }

        /// <summary>
        /// Shows a notification that another user started editing while you were viewing
        /// </summary>
        public static void ShowConcurrentEditNotification(string userName, string itemName)
        {
            var message = $"⚠️ {userName} has started editing \"{itemName}\".\n\n" +
                         "Your view may become outdated. Consider refreshing or coordinating with them.";

            MessageBox.Show(
                message,
                "Concurrent Edit Detected",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
        }

        /// <summary>
        /// Formats a duration for display
        /// </summary>
        private static string FormatDuration(TimeSpan duration)
        {
            if (duration.TotalSeconds < 30)
                return "just now";
            if (duration.TotalMinutes < 1)
                return $"{(int)duration.TotalSeconds} seconds";
            if (duration.TotalMinutes < 60)
                return $"{(int)duration.TotalMinutes} minute{((int)duration.TotalMinutes > 1 ? "s" : "")}";
            if (duration.TotalHours < 24)
                return $"{(int)duration.TotalHours} hour{((int)duration.TotalHours > 1 ? "s" : "")}";
            return $"{(int)duration.TotalDays} day{((int)duration.TotalDays > 1 ? "s" : "")}";
        }

        /// <summary>
        /// Gets a summary of active sessions for display in tooltips or status bars
        /// </summary>
        public static async Task<string> GetSessionSummaryAsync(TodoListSessionTracker sessionTracker, int todoId)
        {
            if (sessionTracker == null) return string.Empty;

            try
            {
                var sessions = await sessionTracker.GetActiveSessionsAsync(todoId);
                var activeSessions = sessions.Where(s => s.IsActive).ToList();

                if (activeSessions.Count == 0)
                    return string.Empty;

                var editing = activeSessions.Count(s => s.IsEditing);
                var viewing = activeSessions.Count(s => !s.IsEditing);

                var parts = new System.Collections.Generic.List<string>();
                if (editing > 0)
                    parts.Add($"{editing} editing");
                if (viewing > 0)
                    parts.Add($"{viewing} viewing");

                return $"👥 {string.Join(", ", parts)}";
            }
            catch
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Shows an edit warning dialog when other users are editing
        /// Returns true if the user wants to proceed, false to cancel
        /// </summary>
        public static async Task<bool> ShowEditWarningAsync(System.Collections.Generic.List<TodoSessionInfo> editingSessions)
        {
            if (editingSessions == null || editingSessions.Count == 0)
                return true;

            var sb = new StringBuilder();
            sb.AppendLine("⚠️ MULTI-USER CONFLICT WARNING");
            sb.AppendLine();
            sb.AppendLine("The following users are currently editing this TodoList:");
            sb.AppendLine();
            
            foreach (var session in editingSessions)
            {
                sb.AppendLine($"   🔴 {session.UserName ?? session.UserId} (for {FormatDuration(session.Duration)})");
            }
            
            sb.AppendLine();
            sb.AppendLine("⚠️ WARNING: Making changes now may cause conflicts!");
            sb.AppendLine("Your changes might overwrite theirs or vice versa.");
            sb.AppendLine();
            sb.AppendLine("Do you want to proceed anyway?");

            var result = MessageBox.Show(
                sb.ToString(),
                "Multi-User Edit Conflict",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning,
                MessageBoxResult.No);

            return result == MessageBoxResult.Yes;
        }
    }
}
