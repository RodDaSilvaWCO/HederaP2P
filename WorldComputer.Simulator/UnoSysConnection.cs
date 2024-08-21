namespace WorldComputer.Simulator
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Net.Security;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading.Tasks;

    public class UnoSysConnection : IDisposable
    {
        #region Field Members
        private const string API_REQUEST_CONTENT_TYPE = "application/json";
        private const string API_CONTROLLER = "UnoSysApi";
        //const string APIURI = "https://localhost:50060/";
        private HttpClient httpsPostClient = null!;
        private HttpClient httpsGetClient = null!;
        private Dictionary<string, bool> validIssuers = new Dictionary<string, bool>();
        #endregion

        #region Constructors
        public  UnoSysConnection(string url)
        {
            httpsPostClient = new HttpClient(HttpsPostClientHandler(), true);
            httpsGetClient = new HttpClient(HttpsGetClientHandler(), true);
            httpsPostClient.BaseAddress = new Uri(url);
            httpsGetClient.BaseAddress = new Uri(url);
        }
        #endregion


        #region IDisposable Implementation
        public void Dispose()
        {
            if (httpsPostClient != null)
            {
                httpsPostClient.Dispose();
                httpsPostClient = null;
            }
            if (httpsGetClient != null)
            {
                httpsGetClient.Dispose();
                httpsGetClient = null;
            }
        }
        #endregion

        #region Public Methods
        public Task<HttpResponseMessage> PostAsync( HttpContent content)
        {
            return httpsPostClient.PostAsync(API_CONTROLLER, content);
        }
        #endregion 

        #region Helpers
        private HttpClientHandler HttpsPostClientHandler()
        {
            var handler = new HttpClientHandler();
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;
            handler.SslProtocols = SslProtocols.None;
            handler.ServerCertificateCustomValidationCallback = ServerCertificateCustomValidation;
            return handler;
        }


        private bool ServerCertificateCustomValidation(HttpRequestMessage requestMessage,
                X509Certificate2 certificate, X509Chain chain, SslPolicyErrors sslErrors)
        {
            return true;


            var issuer = certificate.Issuer;
            if (validIssuers.ContainsKey(issuer))
            {
                return validIssuers[issuer];
            }
            else
            {
                string[] parts = issuer.Split(new char[] { ',' });
                string encryptedOrgUnit = null;
                foreach (var part in parts)
                {
                    if (part.Trim().Substring(0, 3) == "OU=")
                    {
                        encryptedOrgUnit = part.Trim().Substring(3).Trim();
                        break;
                    }
                }
                if (!string.IsNullOrEmpty(encryptedOrgUnit))
                {
                    var response = httpsGetClient.GetAsync(API_CONTROLLER + $"?v={encryptedOrgUnit}").Result;
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        bool isValid = false;
                        bool.TryParse(response.Content.ReadAsStringAsync().Result, out isValid);
                        validIssuers.Add(issuer, isValid);
                        return isValid;
                    }
                }
            }
            return false;
        }

        private HttpClientHandler HttpsGetClientHandler()
        {
            var handler = new HttpClientHandler();
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;
            handler.SslProtocols = SslProtocols.None;
            handler.ServerCertificateCustomValidationCallback = ServerCertificateCustomValidation2;
            return handler;
        }

        private bool ServerCertificateCustomValidation2(HttpRequestMessage requestMessage,
                   X509Certificate2 certificate, X509Chain chain, SslPolicyErrors sslErrors)
        {
            return true;
        }

        
        #endregion 
    }
}
