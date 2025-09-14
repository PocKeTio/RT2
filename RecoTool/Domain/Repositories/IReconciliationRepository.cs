using System.Collections.Generic;
using System.Threading.Tasks;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Domain.Repositories
{
    public interface IReconciliationRepository
    {
        Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null, bool dashboardOnly = false);
        Task<List<Reconciliation>> GetTriggerReconciliationsAsync(string countryId);
        Task<bool> SaveReconciliationAsync(Reconciliation reconciliation);
        Task<bool> SaveReconciliationsAsync(IEnumerable<Reconciliation> reconciliations);
    }
}
