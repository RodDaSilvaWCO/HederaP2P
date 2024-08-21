using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace WorldComputer.Simulator
{

	public class ClusterCommandContext : ICommandContext
	{
        #region Field Members
        const int MAX_CLUSTER_COUNT = 63;
        const int MAX_NODES = 100;
        const int DEFAULT_CLIENT_NODE_NUMBER = 1;


        Hashtable switchSet = new Hashtable( 20 );
		string[] cmdArgs;
		int clusterSize = 1;
		bool createSimulator = true;
		bool deleteSimulator = false;
        bool clientSimulator = false;
        bool simulatorHelp = false;
        int clientNodeNumber = DEFAULT_CLIENT_NODE_NUMBER;
        NetworkSpec NetworkSpec = null;
        VDriveInfo VirtualDriveSpec = null!;

		#endregion

		#region Constructors
		public ClusterCommandContext( string[] commandContextArgs )
		{
			cmdArgs = commandContextArgs;
			// Define valid context switches
			string[] allowedSwitches = new string[4];
			allowedSwitches[0] = "/CREATE:";
			allowedSwitches[1] = "/DELETE";
            allowedSwitches[2] = "/CLIENTNODE:";
            allowedSwitches[3] = "/?";
			CommandLineTool.ParseCommandContextSwitches( commandContextArgs, allowedSwitches, switchSet, this );
			simulatorHelp = (bool) switchSet["/?"];
		}
		#endregion

		#region Public Methods
		public void ProcessCommand()
		{
			if (simulatorHelp)
			{
				CommandLineTool.DisplayBanner();
				DisplayUsage();
			}
			else
			{
                Program.UnoSysApiConnection = new UnoSysConnection(string.Format(Program.UnoSysApiUrlTemplate,Program.BasePort + ((clientNodeNumber - 1) * Program.BasePortSpacing)));

                #region Create Cluster
                if (createSimulator)
                {
                    CreateCluster();

                }
                #endregion 

                #region Delete Cluster
                if (deleteSimulator)
				{
                    DeleteCluster();
				}
				#endregion 
			}
        }

		public bool ValidateContext()
		{
			bool invalidSwitch = false;
			object switchValue = null;

            #region Switch Processing
            #region Create switch processing
            //Examples of possible legal / Create switches:  / Create, / Create:, / Create:4
            switchValue = switchSet["/CREATE"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    string[] values = ((string)switchValue).Split(new char[] { ',' }, StringSplitOptions.None); // NOTE: We do NOT throw away empty values in order to check for invalid switches
                    switch (values.Length)
                    {
                        case 0:
                            // NOP - defaults are good
                            break;

                        case 1:
                            if (!Int32.TryParse(values[0], out clusterSize))
                            {
                                invalidSwitch = true;
                            }
                            else
                            {
                                if (clusterSize < 0 || clusterSize > MAX_CLUSTER_COUNT)
                                {
                                    throw new CommandLineToolInvalidSwitchArgumentException($"/CREATE:{switchValue} - # of clusters must be >= 1 and <= {MAX_CLUSTER_COUNT}");
                                }
                            }
                            break;
                       
                        default:
                            invalidSwitch = true;
                            break;
                    }
                    if (invalidSwitch)
                    {
                        throw new CommandLineToolInvalidSwitchException("/CREATE:" + switchValue);
                    }
                }
            }
            else
            {
                if (!(bool)switchValue)
                {
                    createSimulator = false;
                }
            }
            if (clusterSize < 1)
                throw new CommandLineToolInvalidSwitchException($"Cluster size must be an integer >= 1 and <= {MAX_CLUSTER_COUNT}");

            
            #endregion

            #region Delete switch processing
            switchValue = switchSet["/DELETE"];
			if (! (switchValue is Boolean))
			{
				throw new CommandLineToolInvalidSwitchException( "/DELETE:" + switchValue );
			}
			if( (bool)switchValue)
			{
				deleteSimulator = true;
			}
            #endregion

            #region Client switch processing
            switchValue = switchSet["/CLIENTNODE"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    clientNodeNumber = int.Parse(((string)switchValue).ToUpper());
                }
                else
                {
                    clientNodeNumber = DEFAULT_CLIENT_NODE_NUMBER;
                }
            }
            else
            {
                if((bool)switchValue)
                {
                    clientSimulator = true;
                    clientNodeNumber = DEFAULT_CLIENT_NODE_NUMBER;
                }
            }

            #endregion
            #endregion 


            #region Invalid Switch Combination Check
            if (clientSimulator && !createSimulator )
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/CLIENTNODE can only be used with the /CREATE switch.");
            }

            if ( createSimulator && deleteSimulator)
		    {
			    throw new CommandLineToolInvalidSwitchCombinationException( "/CREATE and /DELETE cannot be used together.");
		    }



            #endregion

            bool result= (createSimulator|| deleteSimulator|| clientSimulator || simulatorHelp);
			if (!result)
			{
                CommandLineTool.DisplayBanner();
                this.DisplayUsage();
                throw new CommandLineToolInvalidCommandException("At least one switch must be specified.");
            }
            return result;
		}

		public void DisplayUsage()
		{
			Console.Error.WriteLine( "WorldComputer CLUSTER command usage:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "      WCSim CLUSTER | C  [<switches>]" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "where <switches> are one or more of:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( " [/CREATE[[:<# of Clusters]]]\tCreate a WorldComputer cluster of messh connected nodes - (1)" );
			Console.Error.WriteLine( " [/DELETE]\t\t\tDelete a WorldComputer cluster of mesh connected nodes");
            Console.Error.WriteLine($" [/CLIENTNODE:<url>]\t\tClient Node # to connect to - ({DEFAULT_CLIENT_NODE_NUMBER})");
            Console.Error.WriteLine( " [/?]\t\t\t\tUsage information" );
			Console.Error.WriteLine( "" );
            Console.WriteLine("============================================================================");
        }
		#endregion


		#region Helpers
		private bool NetworkSimulationAlreadyCreated()
		{
			return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
		}


        private void CreateCluster()
        {
            string vDiskID = null!;
            #region  Step #1:  Check if a simulation has already been created and if so error
            if (!NetworkAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer node network does not exist. Use 'WCSim NODE /Create' to create one first.");
            }
            #endregion

            #region Step #2: Deserialize the SIMULATOR_SETTINGS_FILE_NAME settings file
            using (FileStream fs = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                NetworkSpec = (NetworkSpec)JsonSerializer.Deserialize<NetworkSpec>(fs);
            }
            if (clusterSize > NetworkSpec.NodeList.Count)
            {
                throw new CommandLineToolInvalidOperationException($"The # of Clusters: {clusterSize} must be <= # of Nodes {NetworkSpec.NodeList.Count}");
            }
            #endregion

            #region Step #3:  Ensure only one cluster exists at a time
            if (ClusterAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException("A World Computer Cluster has already been created.  Use 'WCSim CLUSTER /Delete' to delete the existing one before attempting to create a new one.");
            }
            #endregion

            #region Step #4: Check if a node network is not already running and if so error
            if (!NetworkCommandContext.NetworkAlreadyRunning(NetworkSpec))
            {
                throw new CommandLineToolInvalidOperationException("A World Computer node network is not already running.  Use NODE /START to start one running one first.");
            }
            #endregion

            #region  Step #5:  Call the VirtualDiskCreate() Api
            vDiskID = CallVirtualDiskCreateApiOnNode(1, clusterSize );  // NOTE:  In this case the "cluster size" is actaully the repliation factor of a single cluster.  Same first step as creating a VirtualDisk except replication factor is always 1 and we won't mount the resultant virtual disk
            //}
            #endregion

            #region  Step #6: Create a VDriveInfo object to capture details of the Cluster  (Hack - shouldn't be using a VDriveInfo object to describe a cluster but it works)
            VDriveInfo vDriveInfo = new VDriveInfo();
            vDriveInfo.DriveLetter = '*';  // not used for a cluster
            vDriveInfo.VDiskID = new Guid(vDiskID);
            vDriveInfo.SectorSize = 0; // not used for a cluster
            vDriveInfo.BlockSize = 0; // not used for a cluster
            vDriveInfo.MaxWriteBlockSize = 0; // not used for a cluster
            vDriveInfo.MaxReadBlockSize = 0; // not used for a cluster
            #endregion

            #region  Step #7: Persist the VDriveInfo object to disk so it can be automatically remounted when the nodes are next started back up
            using (var vDriveStream = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_CLUSTER_FILE_NAME), FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize<VDriveInfo>(vDriveStream, vDriveInfo);
                vDriveStream.Flush();
            }
            #endregion

            #region Step #8 - Connect up the nodes
            var mountResult = WCClusterMountApi(vDiskID);

            if (!string.IsNullOrEmpty(mountResult))
            {
                #region Display VDisk Clusters
                string[] clusters = mountResult.Substring(32).Split('|');
                var numClusters = clusters.Length;
                var numReplicas = clusters[0].Split(',').Length;
                var orgColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"");
                Console.WriteLine($"Cluster of {numReplicas} Nodes agreed to by all Nodes in network:");
                Console.WriteLine($"");
                var cluster = new int[numClusters, numReplicas];
                //foreach ( var c in clusters)
                for (int c = 0; c < numClusters; c++)
                {
                    string[] replicas = clusters[c].Split(",");
                    Console.Write($"[ ");
                    for (int r = 0; r < numReplicas; r++)
                    {
                        string[] node = replicas[r].Split(":");
                        cluster[c, r] = Program.ComputerNodeNumberFromPort(int.Parse(node[1]));
                        Console.Write($"{Program.ComputerNodeNumberFromPort(int.Parse(node[1])).ToString().PadLeft(3)}");
                        if (r < numReplicas - 1) Console.Write(", ");
                    }
                    Console.WriteLine($"   ]");
                }
                Console.ForegroundColor = orgColor;
                #endregion
            }

            #endregion

        }


        private void DeleteCluster()
        {
            #region  Step #1:  Check if a vdrive has already been created and if so error
            if (!ClusterAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer Cluster does not exist to delete. Use 'CLUSTER /Create' to create one first.");
            }
            #endregion

            #region Step #2:  Delete the VDrive file
            DeleteClusterFileSpec();
            #endregion
        }

        private string CallVirtualDiskCreateApiOnNode( int clusterCount, int replicationFactor)
        {
            string vDiskID = null!;
            const string WC_OSUSER_SESSION_TOKEN = "U00000000000000000000000000000000";
            var json = @$"
            {{
                ""V"" : 1,
                ""F"" : 0,
                ""A"" : {/*(int)ApiIdentifier.VirtualDiskCreate*/720},
                ""I"" : [{{
                            ""D"" : {{
                                        ""N"" : ""UserSessionToken"",
                                        ""T"" : ""System.String"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{WC_OSUSER_SESSION_TOKEN}"",
                            ""N""  : false
                        }},{{
                            ""D"" : {{
                                        ""N"" : ""SubjectSessionToken"",
                                        ""T"" : ""System.String"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{WC_OSUSER_SESSION_TOKEN}"",
                            ""N""  : false
                        }},{{
                            ""D"" : {{
                                        ""N"" : ""ClusterSize"",
                                        ""T"" : ""System.Int32"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{clusterCount}"",
                            ""N""  : false
                        }},{{
                            ""D"" : {{
                                        ""N"" : ""ReplicationFactor"",
                                        ""T"" : ""System.Int32"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{replicationFactor}"",
                            ""N""  : false
                        }}]
            }}";

            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var nodeName = $"{Program.NodeExecutableName} #{clientNodeNumber}";
            //Console.WriteLine($"Calling Node Api:  VirtualDiskCreate() on Node: {nodeName}...");
            try
            {

                var response = Program.UnoSysApiConnection.PostAsync(content).Result;
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // Retrieve result of call
                    var responseJson = response.Content.ReadAsStringAsync().Result;
                    using var doc = JsonDocument.Parse(responseJson);
                    var element = doc.RootElement;
                    vDiskID = element.GetProperty("O")[0].GetProperty("V").GetString();
                    //Console.WriteLine($"VDiskID = {result}");
                }
                else
                {
                    Console.WriteLine($"Error calling VirtualDiskCreate() - response.StatusCode={response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error calling VirtualDiskCreate(): {nodeName} - {ex} ");
            }
            return vDiskID;

        }

        private string WCClusterMountApi(string vdiskID)  
        {
            string mountResult = null!;
            var json = @$"
            {{
                ""V"" : 1,
                ""F"" : 0,
                ""A"" : {/*(int)ApiIdentifier.VirtualDiskMount*/ 722},
                ""I"" : [{{
                            ""D"" : {{
                                        ""N"" : ""UserSessionToken"",
                                        ""T"" : ""System.String"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{Program.WC_OSUSER_SESSION_TOKEN}"",
                            ""N""  : false
                        }},{{
                            ""D"" : {{
                                        ""N"" : ""VirtualDiskID"",
                                        ""T"" : ""System.String"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{vdiskID}"",
                            ""N""  : false
                        }},{{
                            ""D"" : {{
                                        ""N"" : ""BlockSize"",
                                        ""T"" : ""System.UInt32"",
                                        ""MV"" : false,
                                        ""IN"" : false
                                    }},
                            ""V""  : ""{512}"", 
                            ""N""  : false
                        }}]
            }}";

            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = Program.UnoSysApiConnection.PostAsync(content).Result;
            if (response.StatusCode == HttpStatusCode.OK)
            {
                // Retrieve result of call
                var responseJson = response.Content.ReadAsStringAsync().Result;
                using var doc = JsonDocument.Parse(responseJson);
                var element = doc.RootElement;
                //mWorldComputerVirtualDiskDefinition = element.GetProperty("O")[0].GetProperty("V").GetString()!;
                //mWorldComputerVirtualDiskVolumeID = mWorldComputerVirtualDiskDefinition.Substring(0, 32);
                mountResult = element.GetProperty("O")[0].GetProperty("V").GetString()!;
            }
            else
            {
                Console.WriteLine($"Error calling VirtualDiskMount() - response.StatusCode={response.StatusCode}");
            }
            return mountResult;
        }


        private bool NetworkAlreadyCreated()
        {
            return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
        }

        private bool ClusterAlreadyCreated()
        {
            return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_CLUSTER_FILE_NAME));
        }

        private void DeleteClusterFileSpec()
        {
            File.Delete(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_CLUSTER_FILE_NAME));
        }

        private async Task<bool> IsNodeOffline( int port)
        {
            // NOTE:  We check the connection using the TcpClient since that class allows us to set the LingerState
            //		  where as using ClientWebSocket to poll for a connection does not.  This means we won't wait 4 minutes with
            //		  the socket in a TIME_WAIT state every time we fail to connect, using TcpClient, as we would with ClientWebSocket. 
            //		  Therefore the failing socket will be torn down and returned to the socket pool immediately, allowing us to poll 
            //		  rapidly as part of the connection "healing" process.
            port = port + 2;
            bool isNodeOffLine = true;
            using (TcpClient client = new TcpClient())
            {
                // We aren't going to use the socket created as part of the TcpClient() other than to see if we can create one and if so send a small HTTP GET message.
                // We don't have to worry about "partial" data being received after it is closed (the entire purpose of entering the TIME_WAIT state) because the response to GET message is small.  
                // This allows us to set the LingerState to no time, which returns the socket to the pool of available sockets on the machine immediately.
                client.LingerState = new LingerOption(true, 0);
                try
                {
                    // NOTE:  We wish to test if the remote peer node is "up" from a Unosys perspective.
                    //        This means:
                    //			i) we can connect to it (i.e.; it is listening on IPv4Address:Port)
                    //			ii) it will return a 403 Forbidden when we try and send an HTTP GET message to a well known and up endpoint - but that does not allow GETs


                    // i)
                    // Attempt to connect to the external node
                    await client.ConnectAsync("127.0.0.1", port).ConfigureAwait(false);
                    // Prepare HTTP GET message 
                    //connectionUpDataBuffer = System.Text.Encoding.ASCII.GetBytes( string.Format( "GET http://{0}:{1}/UnosysNode/{2}/ExternalNodeManager/ HTTP/1.1\r\nHost: localhost\r\n\r\n", IPv4Address, Port, ProcessorId.ToString() ) );
                    //Debug.Print("ProcessorConnection.IsRemotePeerOffLine() {0}", string.Format("GET http://{0}:{1}/worldcomputernode/{2}/ HTTP/1.1\r\nHost: {0}\r\n\r\n\r\n", IPv4Address, Port, ProcessorId.ToString()));
                    var connectionUpDataBuffer = Encoding.UTF8.GetBytes(string.Format("GET http://{0}:{1}/worldcomputernode/{2}/ HTTP/1.1\r\nHost: {0}\r\n\r\n\r\n", "127.0.0.1", port, ""));
                    
                    using (NetworkStream stream = client.GetStream())
                    {
                        // Send the message to the connected TcpServer. 
                        stream.Write(connectionUpDataBuffer, 0, connectionUpDataBuffer.Length);
                        // Read the first batch of the TcpServer response bytes.
                        Int32 bytesRead = stream.Read(connectionUpDataBuffer, 0, connectionUpDataBuffer.Length);
                        //String responseData = System.Text.Encoding.ASCII.GetString( connectionUpDataBuffer, 0, bytesRead );
                        String responseData = System.Text.Encoding.UTF8.GetString(connectionUpDataBuffer, 0, bytesRead).ToUpper();
                        if (!string.IsNullOrEmpty(responseData))
                        {
                            // Debug.Print("A {0}", responseData);
                            int indexPos = responseData.IndexOf("403 FORBIDDEN");
                            // Debug.Print("B {0}", indexPos);
                            if (indexPos >= 0)
                            {
                                isNodeOffLine = false;  // we are online!
                                //Debug.Print($"ProcessorConnection.IsRemotePeerOffline - remote node is online!!");
                            }
                            //else
                            //{
                            //                         Debug.Print($"$ProcessorConnection.IsRemotePeerOffline - response={responseData}");
                            //                     }
                        }
                        //else
                        //{
                        //	Debug.Print($"$ProcessorConnection.IsRemotePeerOffline - response is NULL");
                        //}
                    }
                }
                catch (Exception)
                {
                    // NOP - If we make it here then we failed to connect to the remote node presumably because it is not reachable.
                    //		 We simply ignore the error and fall out of the using() statement block
                    //		 which will close the socket and immediately return it to the available pool
                    //Debug.Print( $"ProcessorConnection.IsRemotePeerOffline - true - {ex.Message}" );
                }
                //Debug.Print( "ProcessorConnection.IsRemotePeerOffline - {0}", isRemotePeerOffline );
            } 
            return isNodeOffLine;
        }
        #endregion
    }
}
