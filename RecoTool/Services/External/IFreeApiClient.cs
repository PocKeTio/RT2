using System;
using System.Threading.Tasks;

namespace RecoTool.Services.External
{
    public interface IFreeApiClient
    {
        // Mockable search: returns a raw detail string for the given day/reference/serviceCode
        Task<string> SearchAsync(DateTime day, string reference, string cntServiceCode);
    }
}
