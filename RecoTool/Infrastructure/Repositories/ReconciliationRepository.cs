using System.Collections.Generic;
using System.Threading.Tasks;
using RecoTool.Domain.Repositories;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Infrastructure.Repositories
{
    /// <summary>
    /// Transitional repository that delegates to existing ReconciliationService.
    /// This allows us to decouple UI from the service, and later migrate SQL to pure infra.
    /// </summary>
    public sealed class ReconciliationRepository : IReconciliationRepository
    {
        private readonly ReconciliationService _service;

        public ReconciliationRepository(ReconciliationService service)
        {
            _service = service;
        }

        public Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null, bool dashboardOnly = false)
            => _service.GetReconciliationViewAsync(countryId, filterSql, dashboardOnly);

        public Task<List<Reconciliation>> GetTriggerReconciliationsAsync(string countryId)
            => _service.GetTriggerReconciliationsAsync(countryId);

        public Task<bool> SaveReconciliationAsync(Reconciliation reconciliation)
            => _service.SaveReconciliationAsync(reconciliation);

        public Task<bool> SaveReconciliationsAsync(IEnumerable<Reconciliation> reconciliations)
            => _service.SaveReconciliationsAsync(reconciliations);
    }
}
