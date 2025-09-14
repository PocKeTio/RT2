using System;
using System.Collections.Generic;
using System.Linq;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Services
{
    /// <summary>
    /// Centralizes updates to user-field related values (Action, KPI, Incident Type, ActionStatus).
    /// Keeps View code-behind thin and makes logic reusable from both row actions and editors.
    /// </summary>
    public static class UserFieldUpdateService
    {
        public static bool IsActionNA(int? actionId, IReadOnlyList<UserField> allUserFields)
        {
            try
            {
                if (!actionId.HasValue) return true; // treat null as N/A for our rule
                if (allUserFields == null) return false;
                var uf = allUserFields.FirstOrDefault(u => u.USR_ID == actionId.Value);
                var name = uf?.USR_FieldName?.Trim();
                if (string.IsNullOrEmpty(name)) return false;
                return string.Equals(name, "N/A", StringComparison.OrdinalIgnoreCase)
                       || string.Equals(name, "NA", StringComparison.OrdinalIgnoreCase)
                       || string.Equals(name, "NOT APPLICABLE", StringComparison.OrdinalIgnoreCase);
            }
            catch { return false; }
        }

        public static void ApplyAction(ReconciliationViewData row, Reconciliation reco, int? newId, IReadOnlyList<UserField> allUserFields)
        {
            row.Action = newId; reco.Action = newId;
            if (!newId.HasValue || IsActionNA(newId, allUserFields))
            {
                row.ActionStatus = null;
                row.ActionDate = null;
                reco.ActionStatus = null;
                reco.ActionDate = null;
            }
            else
            {
                row.ActionStatus = false; // PENDING
                row.ActionDate = DateTime.Now;
                reco.ActionStatus = false;
                reco.ActionDate = row.ActionDate;
            }
        }

        public static void ApplyActionStatus(ReconciliationViewData row, Reconciliation reco, bool? newStatus)
        {
            var oldStatus = row.ActionStatus;
            row.ActionStatus = newStatus; reco.ActionStatus = newStatus;
            if (newStatus.HasValue)
            {
                if (oldStatus != newStatus)
                {
                    row.ActionDate = DateTime.Now; reco.ActionDate = row.ActionDate;
                }
            }
            else
            {
                row.ActionDate = null; reco.ActionDate = null;
            }
        }

        public static void ApplyKpi(ReconciliationViewData row, Reconciliation reco, int? newId)
        {
            row.KPI = newId; reco.KPI = newId;
        }

        public static void ApplyIncidentType(ReconciliationViewData row, Reconciliation reco, int? newId)
        {
            row.IncidentType = newId; reco.IncidentType = newId;
        }
    }
}
