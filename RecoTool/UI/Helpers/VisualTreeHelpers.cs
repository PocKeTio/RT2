using System.Windows;
using System.Windows.Media;

namespace RecoTool.UI.Helpers
{
    public static class VisualTreeHelpers
    {
        // Find first ancestor of type T by walking up the visual/logical tree
        public static T FindAncestor<T>(DependencyObject current) where T : DependencyObject
        {
            while (current != null)
            {
                if (current is T match)
                    return match;

                if (current is FrameworkElement fe)
                {
                    // Prefer logical/templated parent if available
                    var logical = fe.Parent ?? fe.TemplatedParent as DependencyObject;
                    if (logical != null)
                    {
                        current = logical;
                        continue;
                    }
                }

                current = VisualTreeHelper.GetParent(current);
            }
            return null;
        }

        // Depth-first search for a descendant of type T
        public static T FindDescendant<T>(DependencyObject root) where T : DependencyObject
        {
            if (root == null) return null;
            int count = VisualTreeHelper.GetChildrenCount(root);
            for (int i = 0; i < count; i++)
            {
                var child = VisualTreeHelper.GetChild(root, i);
                if (child is T t) return t;
                var found = FindDescendant<T>(child);
                if (found != null) return found;
            }
            return null;
        }

        // Alias for FindAncestor to match older code signature
        public static T FindParent<T>(DependencyObject child) where T : DependencyObject
            => FindAncestor<T>(child);
    }
}
