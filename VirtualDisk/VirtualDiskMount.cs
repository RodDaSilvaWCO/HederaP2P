namespace UnoSysKernel
{
    using System;
    using System.Threading.Tasks;
    using UnoSys.Api.Exceptions;

    internal partial class ApiManager : SecuredKernelService, IApiManager
    {
        public async Task<string> VirtualDiskMountAsync(string userSessionToken, string virtualDiskID, uint blockSize)
        {
            ThrowIfParameterNullOrEmpty("UserSessionToken", userSessionToken);
            ThrowIfParameterNoValidIDString("VirtualDiskID", virtualDiskID);
            ThrowIfParameterNotInIntegerRange("BlockSize", Convert.ToInt32(blockSize), 512, (64 * 1024));
            if( blockSize % 512 != 0)
            {
                throw new UnoSysArgumentException("BlockSize not a multiple of 512");
            }
            var ust = new UserSessionToken(userSessionToken);
            

            //if (!wcContext.CheckResourceOwnerContext(ust, rst))
            //{
            //    throw new UnoSysUnauthorizedAccessException();
            //}

            return await virtualDiskManager.MountAsync(new Guid(virtualDiskID), blockSize).ConfigureAwait(false);
        }

        public string VirtualDiskMount(string userSessionToken, string virutalDiskID, uint blockSize )
        {
            return VirtualDiskMountAsync(userSessionToken, virutalDiskID, blockSize).Result;
        }
    }
}