using System;
using System.Threading.Tasks;

namespace RecoTool.Services.External
{
    public interface IFreeApiClient
    {
        // Authenticate once at startup to obtain a session/token (real impl is external; provide a mock here)
        Task<bool> AuthenticateAsync();
        // Expose current authentication state (best-effort)
        bool IsAuthenticated { get; }
        // Mockable search: returns a raw detail string for the given day/reference/serviceCode
        Task<string> SearchAsync(DateTime day, string reference, string cntServiceCode);
    }
}
