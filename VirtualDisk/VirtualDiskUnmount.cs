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
        public async Task VirtualDiskUnmountAsync(string userSessionToken, string volumeID )
        {
            ThrowIfParameterNullOrEmpty("UserSessionToken", userSessionToken);
            ThrowIfParameterNoValidIDString("VolumeID", volumeID);
            var ust = new UserSessionToken(userSessionToken);
            

            //if (!wcContext.CheckResourceOwnerContext(ust, rst))
            //{
            //    throw new UnoSysUnauthorizedAccessException();
            //}

            await virtualDiskManager.UnmountAsync(new Guid(volumeID)).ConfigureAwait(false);
        }

        public void VirtualDiskUnmount(string userSessionToken, string virutalDiskID)
        {
            VirtualDiskUnmountAsync(userSessionToken, virutalDiskID).Wait();
        }
    }
}