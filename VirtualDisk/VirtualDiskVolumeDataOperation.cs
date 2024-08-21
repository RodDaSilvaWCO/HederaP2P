namespace UnoSysKernel
{
    using System;
    using System.Threading.Tasks;
    using UnoSys.Api;
    using UnoSys.Api.Exceptions;
    using UnoSys.Api.Models;
    using UnoSysCore;

    internal partial class ApiManager : SecuredKernelService, IApiManager
    {
        public async Task<string> VirtualDiskVolumeDataOperationAsync(string userSessionToken, string base64Operation )
        {
            ThrowIfParameterNullOrEmpty("UserSessionToken", userSessionToken);
            ThrowIfParameterNullOrEmpty("Operation", base64Operation);
            var ust = new UserSessionToken(userSessionToken);
            

            //if (!wcContext.CheckResourceOwnerContext(ust, rst))
            //{
            //    throw new UnoSysUnauthorizedAccessException();
            //}

            return await virtualDiskManager.VolumeDataOperationAsync(base64Operation).ConfigureAwait(false);
        }

        public string VirtualDiskVolumeDataOperation(string userSessionToken, string base64Operation)
        {
            return VirtualDiskVolumeDataOperationAsync(userSessionToken, base64Operation).Result;
        }
    }
}