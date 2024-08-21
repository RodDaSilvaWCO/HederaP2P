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

	public class VirtualDriveCommandContext : ICommandContext
	{
        #region Field Members
        const int MAX_CLUSTER_COUNT = 63;
        const int MAX_REPLICATION_FACTOR = 63;
        const int MAX_NODES = 100;
        const int DEFAULT_CLIENT_NODE_NUMBER = 1;
        const int DEFAULT_BLOCKSIZE = 64*1024;
        const int DEFAULT_SECTOR_SIZE = 4096;
        const int MAX_READ_BLOCK_SIZE = 1024 * 1024;
        const int MAX_WRITE_BLOCK_SIZE = 1024 * 1024;

        const string DEFAULT_DRIVE_LETTER = "Z";


        Hashtable switchSet = new Hashtable( 20 );
		string[] cmdArgs;
		int clusterSize = 1;
		int replicationFactor = 1;
		bool createSimulator = true;
		bool deleteSimulator = false;
		bool blocksizeSimulator = false;
        bool clientSimulator = false;
        bool letterSimulator = false;
        bool attachSimulator = false;
        string vDriveLetter = DEFAULT_DRIVE_LETTER;
        bool simulatorHelp = false;
        uint blocksize = DEFAULT_BLOCKSIZE;
        int clientNodeNumber = DEFAULT_CLIENT_NODE_NUMBER;
        NetworkSpec NetworkSpec = null;
        VDriveInfo VirtualDriveSpec = null!;

		#endregion

		#region Constructors
		public VirtualDriveCommandContext( string[] commandContextArgs )
		{
			cmdArgs = commandContextArgs;
			// Define valid context switches
			string[] allowedSwitches = new string[7];
			allowedSwitches[0] = "/CREATE:";
			allowedSwitches[1] = "/DELETE";
            allowedSwitches[2] = "/ATTACH";
            allowedSwitches[3] = "/LETTER:";
			allowedSwitches[4] = "/BLOCKSIZE:";
            allowedSwitches[5] = "/CLIENTNODE:";
            allowedSwitches[6] = "/?";
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
                //Console.WriteLine($"CLIENT URL={string.Format(Program.UnoSysApiUrlTemplate, Program.BasePort + ((clientNodeNumber - 1) * Program.BasePortSpacing))}");
                Program.UnoSysApiConnection = new UnoSysConnection(string.Format(Program.UnoSysApiUrlTemplate,Program.BasePort + ((clientNodeNumber - 1) * Program.BasePortSpacing)));

                #region Create or Attach a VDrive
                if (createSimulator)
                {
                    CreateVirtualDrive();

                }
                else if (attachSimulator)
                {
                    AttachVirtualDrive();
                }
                #endregion

                #region Delete existing VDrive
                if (deleteSimulator)
				{
                    DeleteVirtualDrive();
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
            //Examples of possible legal / Create switches:  / Create, / Create:, / Create:4, / Create:4,2
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
                        case 2:
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
                            if (!Int32.TryParse(values[1], out replicationFactor))
                            {
                                invalidSwitch = true;
                            }
                            else
                            {
                                if (replicationFactor < 0 || replicationFactor > MAX_REPLICATION_FACTOR)
                                {
                                    throw new CommandLineToolInvalidSwitchArgumentException($"/CREATE:{switchValue} - # of replicas must be >= 1 and <= {MAX_REPLICATION_FACTOR}");
                                }
                            }
                            if( clusterSize * replicationFactor > 100)
                            {
                                throw new CommandLineToolInvalidSwitchArgumentException($"/CREATE:{switchValue} - # of clusters * # of replicas must be <= {MAX_NODES}");
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

            if (replicationFactor < 1 || replicationFactor > MAX_REPLICATION_FACTOR)
                {
                    throw new CommandLineToolInvalidSwitchException($"Replication factor must be an integer >= 1 and <= {MAX_REPLICATION_FACTOR}");
                }
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

            #region Attach switch processing
            switchValue = switchSet["/ATTACH"];
            if (!(switchValue is Boolean))
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH:" + switchValue);
            }
            if ((bool)switchValue)
            {
                attachSimulator = true;
            }
            #endregion

            #region Letter switch processing
            switchValue = switchSet["/LETTER"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    vDriveLetter = ((string)switchValue).ToUpper();
                    if (vDriveLetter.Length > 1)
                    {
                        throw new CommandLineToolInvalidSwitchException($"Invalid drive letter: {vDriveLetter}");
                    }
                    if("ABCDEFGHIJKLMNOPQRSTUVWXYZ".IndexOf(vDriveLetter.ToUpper()) < 0)
                    {
                        throw new CommandLineToolInvalidSwitchException($"Invalid drive letter: {vDriveLetter}");
                    }

                }
                else
                {
                    vDriveLetter = DEFAULT_DRIVE_LETTER;
                }
                letterSimulator = true;
            }
            else
            {
                if ((bool)switchValue)
                {
                    letterSimulator = true;
                    vDriveLetter = DEFAULT_DRIVE_LETTER;
                }
            }
            if( !CheckIfDriveLetterAvailable(vDriveLetter))
            {
                throw new CommandLineToolInvalidSwitchException($"Drive {vDriveLetter}: is not available.  Please choose another letter.");
            }

            #endregion

            #region BlockSize switch processing
            switchValue = switchSet["/BLOCKSIZE"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    string svalue = ((string)switchValue).ToUpper();
                    switch (svalue)
                    {
                        //case "1K":
                        //    blocksize = 1024;
                        //    break;

                        //case "2K":
                        //    blocksize = 2048;
                        //    break;
                        case "4K":
                            blocksize = 4096;
                            break;
                        case "8K":
                            blocksize = 8192;
                            break;

                        case "16K":
                            blocksize = 16384;
                            break;
                        case "32K":
                            blocksize = 32768;
                            break;
                        case "64K":
                            blocksize = 65536;
                            break;

                        default:
                            invalidSwitch = true;

                            break;
                    }
                    if (invalidSwitch)
                    {
                        throw new CommandLineToolInvalidSwitchException("/BLOCKSIZE:" + switchValue);
                    }
                }
                else
                {
                    // not specified so default to 64k
                    blocksize = DEFAULT_BLOCKSIZE;
                }
            }
            else
            {
                if ((bool)switchValue)
                {
                    blocksizeSimulator = true;
                    blocksize = DEFAULT_BLOCKSIZE;
                }
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
            if (attachSimulator && createSimulator)
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH cannot be used with the /CREATE switch.");
            }

            if (attachSimulator && deleteSimulator)
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH cannot be used with the /DELETE switch.");
            }

            if (attachSimulator && blocksizeSimulator)
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH cannot be used with the /BLOCKSIZE switch.");
            }

            if (attachSimulator && !NetworkAlreadyCreated() )
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH cannot be used because no Node network has been started.  Use NODE /START[:<# of nodes>] to start one first.");
            }

            if ( attachSimulator && !VirtualDriveAlreadyCreated())
            {
                throw new CommandLineToolInvalidSwitchException("/ATTACH cannot be used because no virtual drive currently exists.  Use VDRIVE /CREATE to create one first.");
            }
            if (clientSimulator && !createSimulator )
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/CLIENTNODE can only be used with the /CREATE switch.");
            }
            if (letterSimulator && deleteSimulator)
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/LETTER cannot be used with the /DELETE switch.");
            }
            if ( blocksizeSimulator && !createSimulator )
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/BLOCKSIZE can only be used with the /CREATE switch.");
            }

            if ( createSimulator && deleteSimulator)
		    {
			    throw new CommandLineToolInvalidSwitchCombinationException( "/CREATE and /DELETE cannot be used together.");
		    }



            #endregion

            bool result= (createSimulator|| attachSimulator || deleteSimulator|| letterSimulator || blocksizeSimulator|| clientSimulator || simulatorHelp);
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
			Console.Error.WriteLine( "WorldComputer VDRIVE command usage:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "      WCSim VDRIVE | VD  [<switches>]" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "where <switches> are one or more of:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( " [/CREATE[[:<# of Clusters][,<# of Replicas>]]]\tCreate a Virtual Drive of (CxR) nodes - (1,1)" );
			Console.Error.WriteLine( " [/DELETE]\t\t\t\t\tDelete a Virtual Drive");
            Console.Error.WriteLine( " [/ATTACH]\t\t\t\t\tAttach to an existing Virtual Drive");
            Console.Error.WriteLine( " [/LETTER:<drive letter>]\t\t\tDrive letter to assign the Virtual Disk - (Z)");
            Console.Error.WriteLine($" [/BLOCKSIZE:4K|8K|16K|32K|64K]\t\t\tBlock size of Virtual Disk - (64K)");
            Console.Error.WriteLine($" [/CLIENTNODE:<url>]\t\t\t\tClient Node # to connect to - ({DEFAULT_CLIENT_NODE_NUMBER})");
            Console.Error.WriteLine( " [/?]\t\t\t\t\t\tUsage information" );
			Console.Error.WriteLine( "" );
            Console.WriteLine("============================================================================");
        }
		#endregion


		#region Helpers
		private bool NetworkSimulationAlreadyCreated()
		{
			return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
		}


        private void CreateVirtualDrive()
        {
            // try
            // {
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

            if (clusterSize * replicationFactor > NetworkSpec.NodeList.Count)
            {
                throw new CommandLineToolInvalidOperationException($"The # of Clusters: {clusterSize} x # of Replicas: {replicationFactor} must be <= # of Nodes {NetworkSpec.NodeList.Count}");
            }
            #endregion

            #region Step #3:	Check if a node network is not already running and if so error
            if (!NetworkCommandContext.NetworkAlreadyRunning(NetworkSpec))
            {
                throw new CommandLineToolInvalidOperationException("A World Computer node network is not already running.  Use NODE /START to start one running first.");
            }
            #endregion

            // Step #3:	Check if an of the nodes are already running and if so error
            //if (!NetworkAlreadyRunning())
            //{
            //    throw new CommandLineToolInvalidOperationException("The World Computer node network is not running. Use 'WCSim NODE /Create' to create one first.");
            //}

            //#region Step #4:  Find first active node in the Simulation
            //Node node = null!;
            //for (int i = 0; i < NetworkSpec.NodeList.Count; i++)
            //{
            //    if( ! IsNodeOffline(Program.BasePort + ((NetworkSpec.NodeList[i].Number - 1) * Program.BasePortSpacing)).Result )
            //    {
            //        node = NetworkSpec.NodeList[i];
            //        break;
            //    }
            //}
            //#endregion

            #region  Step #4:  Call the VirtualDiskCreate() Api

            //if (node != null)
            //{
            vDiskID = CallVirtualDiskCreateApiOnNode(clusterSize, replicationFactor);

            //}
            #endregion

            #region  Step #5: Create a VDriveInfo object to capture details of the VDrive
            VDriveInfo vDriveInfo = new VDriveInfo();
            vDriveInfo.DriveLetter = vDriveLetter[0];
            vDriveInfo.VDiskID = new Guid(vDiskID);
            vDriveInfo.SectorSize = DEFAULT_SECTOR_SIZE;
            vDriveInfo.BlockSize = blocksize;
            vDriveInfo.MaxWriteBlockSize = MAX_WRITE_BLOCK_SIZE;
            vDriveInfo.MaxReadBlockSize = MAX_READ_BLOCK_SIZE;
            #endregion

            #region  Step #6: Persist the VDriveInfo object to disk so it can be automatically remounted when the nodes are next started back up
            using (var vDriveStream = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME), FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize<VDriveInfo>(vDriveStream, vDriveInfo);
                vDriveStream.Flush();
            }
            #endregion 


            #region  Step #7:  Mount the VDrive to the vDriveLetter
            if (vDiskID != null && vDriveLetter != null)
            {
                //VDrive.Init(vDriveInfo, $"./{Program.LocalStoreDirectoryName}/Node{clientNodeNumber}{{0}}{vDiskID}");
                VDrive.Init(vDriveInfo, Path.Combine(Program.NodeDirectory, $"{Program.LocalStoreDirectoryName}/Node{clientNodeNumber}_Root_{vDiskID}"),
                                            Path.Combine(Program.NodeDirectory, $"{Program.LocalStoreDirectoryName}/Node{{0}}_Data_{vDiskID}"));
            }
            #endregion

            //}
            //catch( Exception ex)
            //{
            //    Console.WriteLine($"Operation failed due to unexpected error creating VDrive - {ex.Message}.  ");
            //}
        }

        private void AttachVirtualDrive()
        {
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

            if (clusterSize * replicationFactor > NetworkSpec.NodeList.Count)
            {
                throw new CommandLineToolInvalidOperationException($"The # of Clusters: {clusterSize} x # of Replicas: {replicationFactor} must be <= # of Nodes {NetworkSpec.NodeList.Count}");
            }
            #endregion 

            #region  Step #3:  Check if a vdrive has already been created and if so error
            if (!VirtualDriveAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer Virtual Drive '{Simulator.SIMULATOR_VDRIVE_FILE_NAME}' settings file cannot be located. Use 'WCSim VDRIVE /Create' to create a Virtual Drive first.");
            }
            #endregion 

            #region Step #4: Deserialize the SIMULATOR_VDRIVE_FILE_NAME settings file
            using (FileStream fs = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                VirtualDriveSpec = (VDriveInfo)JsonSerializer.Deserialize<VDriveInfo>(fs);
            }
            #endregion

            #region Step #5:  Set the new DriveLetter for this VDrive
            VirtualDriveSpec.DriveLetter = vDriveLetter[0];
            #endregion 

            #region  Step #5:  Mount the VDrive to the vDriveLetter
            if (VirtualDriveSpec != null )
            {
                VDrive.Init(VirtualDriveSpec, Path.Combine(Program.NodeDirectory, $"{Program.LocalStoreDirectoryName}/Node{clientNodeNumber}_Root_{VirtualDriveSpec.VDiskID.ToString("N").ToUpper()}"),
                                               Path.Combine(Program.NodeDirectory, $"{Program.LocalStoreDirectoryName}/Node{{0}}_Data_{VirtualDriveSpec.VDiskID.ToString("N").ToUpper()}"));
            }
            #endregion
        }

        private void DeleteVirtualDrive()
        {
            #region  Step #1:  Check if a vdrive has already been created and if so error
            if (!VirtualDriveAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer Virtual Drive does not exist to delete. Use 'VDRIVE /Create' to create a one first.");
            }
            #endregion

            #region Step #2:  Delete the VDrive file
            DeleteVirtualDriveFileSpec();
            #endregion
        }
        private bool CheckIfDriveLetterAvailable(string driveLetter)
        {
            #region Read local drive info
            bool isDriveLetterAvailable = true;
            DriveInfo[] allDrives = DriveInfo.GetDrives();
            foreach (DriveInfo d in allDrives)
            {
                if (d.Name.ToUpper().Substring(0, 1) == driveLetter.ToUpper())
                {
                    isDriveLetterAvailable = false;
                    break;
                }
            }
            #endregion
            return isDriveLetterAvailable;
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


        private bool NetworkAlreadyCreated()
        {
            return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
        }

        private bool VirtualDriveAlreadyCreated()
        {
            return File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME));
        }

        private void DeleteVirtualDriveFileSpec()
        {
            File.Delete(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME));
        }

        //private bool NetworkAlreadyRunning()
        //{
        //    bool result = false;
        //    var nodeExecutable = Program.NodeExecutableName.ToUpper();
        //    foreach (var node in NetworkSpec.NodeList)
        //    {
        //        try
        //        {
        //            var p = Process.GetProcessById(node.ProcessID);
        //            if( p.ProcessName.ToUpper().IndexOf(nodeExecutable ) == 0  )
        //            {
        //                // Found a process in the NetworkSpec still running...
        //                result = true;
        //                break;
        //            }
        //        }
        //        catch (Exception)
        //        {
        //            // NOP and continue
        //        }
        //    }
        //    return result;
        //}

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
