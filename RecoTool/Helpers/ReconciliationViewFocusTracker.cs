using System;

namespace RecoTool.Helpers
{
    public static class ReconciliationViewFocusTracker
    {
        private static WeakReference<Windows.ReconciliationView> _lastFocused = new WeakReference<Windows.ReconciliationView>(null);

        public static Windows.ReconciliationView GetLastFocused()
        {
            if (_lastFocused.TryGetTarget(out var view)) return view;
            return null;
        }

        public static void SetLastFocused(Windows.ReconciliationView view)
        {
            if (view == null) return;
            _lastFocused.SetTarget(view);
        }
    }
}
