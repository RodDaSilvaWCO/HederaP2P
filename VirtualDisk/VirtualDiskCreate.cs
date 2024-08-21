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
        public async Task<string> VirtualDiskCreateAsync(string userSessionToken, string subjectSessionToken, int clusterSize, int replicationFactor)
        {
            ThrowIfParameterNullOrEmpty("UserSessionToken", userSessionToken);
            ThrowIfParameterNullOrEmpty("SubjectSessionToken", subjectSessionToken);
            ThrowIfParameterNotInIntegerRange("ClusterSize", clusterSize, 1, 100);
            ThrowIfParameterNotInIntegerRange("ReplicationFactor", replicationFactor, 1, 32);
            var ust = new UserSessionToken(userSessionToken);
            var sessionType = SessionToken.GetSessionTokenType(subjectSessionToken);
            if ( sessionType != SessionType.User && sessionType != SessionType.Application)
            {
                throw new UnoSysArgumentException($"Invalid SubjectSessionToken - Must be a User or an Application");
            }
            SessionToken rst = null!;
            if (sessionType == SessionType.Application)
            {
                rst = new ApplicationSessionToken(subjectSessionToken);
            }
            else
            {
                rst = new UserSessionToken(subjectSessionToken);
            }

            //if (!wcContext.CheckResourceOwnerContext(ust, rst))
            //{
            //    throw new UnoSysUnauthorizedAccessException();
            //}

            return await virtualDiskManager.CreateAsync(clusterSize, replicationFactor).ConfigureAwait(false);
        }

        public string VirtualDiskCreate(string userSessionToken, string subjectSessionToken, int clusterSize, int replicationFactor)
        {
            return VirtualDiskCreateAsync(userSessionToken, subjectSessionToken, clusterSize, replicationFactor).Result;
        }
    }
}