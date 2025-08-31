using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Centralizes conversions from IDs/enums to display names for Actions and KPIs.
    /// Uses referential UserFields when provided, with graceful fallbacks.
    /// </summary>
    public static class EnumHelper
    {
        public static string GetActionName(int actionId, IEnumerable<UserField> userFields = null)
        {
            try
            {
                if (TryGetUserFieldLabel(actionId, new[] { "Action" }, userFields, out var label))
                    return label;

                try
                {
                    var value = (ActionType)actionId;
                    return GetEnumDescription(value) ?? $"Action {actionId}";
                }
                catch { return $"Action {actionId}"; }
            }
            catch { return $"Action {actionId}"; }
        }

        public static string GetKPIName(int kpiId, IEnumerable<UserField> userFields = null)
        {
            if (TryGetUserFieldLabel(kpiId, new[] { "KPI" }, userFields, out var label))
                return label;

            try
            {
                var value = (KPIType)kpiId;
                switch (value)
                {
                    case KPIType.ITIssues: return "IT Issues";
                    case KPIType.PaidButNotReconciled: return "Paid but not reconciled";
                    case KPIType.CorrespondentChargesToBeInvoiced: return "Corr. charges to be invoiced";
                    case KPIType.UnderInvestigation: return "Under investigation";
                    case KPIType.NotClaimed: return "Not claimed";
                    case KPIType.ClaimedButNotPaid: return "Claimed but not paid";
                    case KPIType.CorrespondentChargesPendingTrigger: return "Corr. charges pending trigger";
                    case KPIType.NotTFSC: return "Not TFSC";
                    default: return SplitCamelCase(value.ToString());
                }
            }
            catch { return $"KPI {kpiId}"; }
        }

        public static string GetIncidentName(int incidentId, IEnumerable<UserField> userFields = null)
        {
            if (TryGetUserFieldLabel(incidentId, new[] { "INC", "Incident Type" }, userFields, out var label))
                return label;

            try
            {
                var value = (INC)incidentId;
                return GetEnumDescription(value) ?? $"Incident {incidentId}";
            }
            catch { return $"Incident {incidentId}"; }
        }

        public static string GetRiskReasonName(int reasonId, IEnumerable<UserField> userFields = null)
        {
            if (TryGetUserFieldLabel(reasonId, new[] { "RISKY", "ReasonNonRisky" }, userFields, out var label))
                return label;

            try
            {
                var value = (Risky)reasonId;
                return GetEnumDescription(value) ?? $"Reason {reasonId}";
            }
            catch { return $"Reason {reasonId}"; }
        }

        // Alias for the renamed category: ReasonNonRisky => Risky
        public static string GetRiskyName(int reasonId, IEnumerable<UserField> userFields = null)
        {
            return GetRiskReasonName(reasonId, userFields);
        }

        private static bool TryGetUserFieldLabel(int id, string[] categories, IEnumerable<UserField> userFields, out string label)
        {
            label = null;
            try
            {
                if (userFields != null)
                {
                    var uf = userFields.FirstOrDefault(u =>
                        u.USR_ID == id &&
                        categories.Any(c => string.Equals(u.USR_Category, c, StringComparison.OrdinalIgnoreCase)));
                    if (uf != null)
                    {
                        if (!string.IsNullOrWhiteSpace(uf.USR_FieldName))
                        {
                            label = uf.USR_FieldName;
                            return true;
                        }
                        if (!string.IsNullOrWhiteSpace(uf.USR_FieldDescription))
                        {
                            label = uf.USR_FieldDescription;
                            return true;
                        }
                    }
                }
            }
            catch { }
            return false;
        }

        public static string GetEnumDescription(Enum value)
        {
            try
            {
                var fi = value.GetType().GetField(value.ToString());
                if (fi != null)
                {
                    var attrs = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                    if (attrs != null && attrs.Length > 0)
                        return attrs[0].Description;
                }
            }
            catch { }
            return SplitCamelCase(value.ToString());
        }

        public static string SplitCamelCase(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return s;
            return System.Text.RegularExpressions.Regex.Replace(s.Replace('_', ' '), "([a-z])([A-Z])", "$1 $2");
        }
    }
}
