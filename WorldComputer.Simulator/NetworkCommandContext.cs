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
//using UnoSys.Api.Models;
using System.Security.Cryptography;
using static System.Net.WebRequestMethods;
using System.Collections.Generic;
//using System.Threading;

namespace WorldComputer.Simulator
{

	public class NetworkCommandContext : ICommandContext
	{
        #region Field Members

        Hashtable switchSet = new Hashtable( 20 );
		string[] cmdArgs;
		int totalNodeCount = 1;
        List<int> startNodeCount = new List<int>();
        List<int> stopNodeCount = new List<int>();
        bool animateSimulator = false;
        bool startSimulator = true;
		bool stopSimulator = false;
        bool simulatorHelp = false;
        bool createSimulator = false;
        bool deleteSimulator = false;

        NetworkSpec NetworkSpec = null;

		#endregion

		#region Constructors
		public NetworkCommandContext( string[] commandContextArgs )
		{
			cmdArgs = commandContextArgs;
			// Define valid context switches
			string[] allowedSwitches = new string[6];
            allowedSwitches[0] = "/CREATE:";
            allowedSwitches[1] = "/DELETE";
            allowedSwitches[2] = "/START:";
			allowedSwitches[3] = "/STOP:";
			allowedSwitches[4] = "/ANIMATE";
			allowedSwitches[5] = "/?";
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

                #region Create a new set of nodes
                if (createSimulator)
                {
                    CreateNetwork();
                }
                #endregion


                #region Delete existing Simulator
                if (deleteSimulator)
                {
                    DeleteNetwork();
                }
                #endregion

                #region Start a new set of nodes
                if (startSimulator && !createSimulator)
				{
					StartNetwork();
				}
				#endregion


				#region Stop existing Simulator
				if (stopSimulator && !deleteSimulator)
				{
                    StopNetwork();
				}
				#endregion 
			}
        }

		public bool ValidateContext()
		{
			object switchValue = null;

            #region Create switch processing
            switchValue = switchSet["/CREATE"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    //string[] values = ((string)switchValue).Split(new char[] { ',' }, StringSplitOptions.None); // NOTE: We do NOT throw away empty values in order to check for invalid switches

                    if (string.IsNullOrEmpty((string)switchValue))
                    {
                        Program.InvalidSwitch = true;
                    }
                    else
                    {
                        if (!Int32.TryParse((string)switchValue, out totalNodeCount))
                        {
                            Program.InvalidSwitch = true;
                        }
                        else
                        {
                            if (totalNodeCount < 0 || totalNodeCount > 100)
                            {
                                throw new CommandLineToolInvalidSwitchArgumentException($"/CREATE:{switchValue} - # of nodes must be between 1 and 100 inclusively");
                            }
                        }
                    }
                    if (Program.InvalidSwitch)
                    {
                        throw new CommandLineToolInvalidSwitchException("/CREATE:" + switchValue);
                    }
                    createSimulator = true;
                }
            }
            else
            {
                if (! (switchValue is bool))
                {
                    createSimulator = false;
                }
                else
                {
                    createSimulator = (bool)switchValue;
                }
            }
            #endregion

            #region Delete switch processing
            switchValue = switchSet["/DELETE"];
            if (!(switchValue is Boolean))
            {
                throw new CommandLineToolInvalidSwitchException("/DELETE:" + switchValue);
            }
            if (!(switchValue is bool))
            {
                deleteSimulator = false;
            }
            else
            {
                deleteSimulator = (bool)switchValue;
            }
            #endregion

            #region Start switch processing
            // Examples of possible legal /Start switches:  Exs:  /Start /Start: /Start:4 /Start:4,2,10
            switchValue = switchSet["/START"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    string[] values = ((string)switchValue).Split(new char[] { ',' }, StringSplitOptions.None); // NOTE: We do NOT throw away empty values in order to check for invalid switches
                    foreach( var val in values )
                    {
                        if (!Int32.TryParse(val, out int nodenum))
                        {
                            Program.InvalidSwitch = true;
                        }
                        else
                        {
                            if (nodenum < 0 || nodenum > Simulator.MAX_NETWORK_NODE_SIZE)
                            {
                                throw new CommandLineToolInvalidSwitchArgumentException($"/START:{switchValue} - node #s must be between 1 and {Simulator.MAX_NETWORK_NODE_SIZE}");
                            }
                            if (!startNodeCount.Contains(nodenum))
                            {
                                startNodeCount.Add(nodenum);
                            }
                        }
                    }

                    if (Program.InvalidSwitch)
                    {
                        throw new CommandLineToolInvalidSwitchException("/START:" + switchValue);
                    }
                    startSimulator = true;
                }
            }
            else
            {
                if (!(switchValue is bool))
                {
                    startSimulator = false;
                }
                else
                {
                    startSimulator = (bool)switchValue;
                }
            }
            #endregion

            #region Stop switch processing
            // Examples of possible legal /Stop switches:  Exs:  /Stop /Stop: /Stop:4 /Stop:4,2,10
            switchValue = switchSet["/STOP"];
            if (switchValue is string)
            {
                if (!string.IsNullOrEmpty((string)switchValue))
                {
                    string[] values = ((string)switchValue).Split(new char[] { ',' }, StringSplitOptions.None); // NOTE: We do NOT throw away empty values in order to check for invalid switches
                    foreach (var val in values)
                    {
                        if (!Int32.TryParse(val, out int nodenum))
                        {
                            Program.InvalidSwitch = true;
                        }
                        else
                        {
                            if (nodenum < 0 || nodenum > Simulator.MAX_NETWORK_NODE_SIZE)
                            {
                                throw new CommandLineToolInvalidSwitchArgumentException($"/START:{switchValue} - node #s must be between 1 and {Simulator.MAX_NETWORK_NODE_SIZE}");
                            }
                            if (!stopNodeCount.Contains(nodenum))
                            {
                                stopNodeCount.Add(nodenum);
                            }
                        }
                    }

                    if (Program.InvalidSwitch)
                    {
                        throw new CommandLineToolInvalidSwitchException("/STOP:" + switchValue);
                    }
                    stopSimulator = true;
                }
            }
            else
            {
                if (!(switchValue is bool))
                {
                    stopSimulator = false;
                }
                else
                {
                    stopSimulator = (bool)switchValue;
                }
            }
            #endregion

            #region Animate switch processing
            switchValue = switchSet["/ANIMATE"];
            if (!(switchValue is Boolean))
            {
                throw new CommandLineToolInvalidSwitchException("/ANIMATE:" + switchValue);
            }
            if (!(switchValue is bool))
            {
                animateSimulator = false;
            }
            else
            {
                animateSimulator = (bool)switchValue;
            }
            #endregion


            #region Invalid Switch Combination Check
            if (createSimulator && deleteSimulator)
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/CREATE and /DELETE cannot be used together.");
            }

           
            if (createSimulator && stopSimulator)
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/CREATE and /STOP cannot be used together.");
            }
            if (deleteSimulator && startSimulator)
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/DELETE and /START cannot be used together.");
            }
           
            if ( startSimulator && stopSimulator)
			{
				throw new CommandLineToolInvalidSwitchCombinationException( "/START and /STOP cannot be used together.");
			}

            if( animateSimulator && !startSimulator)
            {
                throw new CommandLineToolInvalidSwitchCombinationException("/ANIMATE cannot be used without /START.");
            }

            #endregion

            bool result= (createSimulator || deleteSimulator || startSimulator|| stopSimulator|| animateSimulator || simulatorHelp);
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
			Console.Error.WriteLine( "WorldComputer NODE command usage:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "      WCSim NODE | N  [<switches>]" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "where <switches> are one or more of:" );
			Console.Error.WriteLine( "" );
            Console.Error.WriteLine( " [/CREATE[:<# of nodes]\t\tCreate a network of WorldComputer nodes (1)");
            Console.Error.WriteLine( " [/DELETE]\t\t\tDelete the network of WorldComputer nodes");
            Console.Error.WriteLine( " [/START[:<# of nodes[,...]]\tStart one or more WorldComputer nodes (all)" );
            Console.Error.WriteLine( " [/STOP[:<# of nodes[,...]]\tStop one or more WorldComputer nodes (all)");
            Console.Error.WriteLine( " [/ANIMATE]\t\t\tAnimate the WorldComputer nodes");
			Console.Error.WriteLine( " [/?]\t\t\t\tUsage information" );
			Console.Error.WriteLine( "" );
            Console.WriteLine("============================================================================");
        }
		#endregion


		#region Helpers
		private NetworkSpec CreateNetworkSpec()
		{
			NetworkSpec spec = new NetworkSpec();
			for ( int i = 1; i <= totalNodeCount; i++ )
			{
				spec.NodeList.Add(new Node(i));
			}
			return spec;
		}


		private bool NetworkSimulationAlreadyCreated()
		{
			return System.IO.File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
		}

        //private bool SimulationProcessorExists()
        //{
        //    return File.Exists(Path.Combine(Program.NodeDirectory, Program.NodeExecutableName + ".EXE"));
        //}


        private void CreateNetwork()
        {
            NetworkSpec = CreateNetworkSpec();

            // Step #1:  Check if a simulation has already been created and if so error
            if (NetworkSimulationAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException("A World Computer node network already exists.  You must explicitly DELETE it before being able to create a new one.");
            }

            // Step #2:  Create new NetworkSpec 
            NetworkSpec = CreateNetworkSpec();

            

            // Step #3:  Launch required copies of the Program.NodeExecutableName, their config files, and their corresponding root storage folder
            if (startSimulator)
            {
                StartNetworkNodes();
            }

            // Step #4:  Serialize NetworkSpec to a file of name SIMULATOR_SETTINGS_FILE_NAME
            // NOTE:  MUST BE DONE _AFTER_ start the network up since need to capture Process Ids in the serialization
            var nspecJson = JsonSerializer.Serialize(NetworkSpec);
            System.IO.File.WriteAllText(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), nspecJson);
        }


        private void StartNetwork()
        {
            // Step #1:  Check if a network exists and if not error
            if (!NetworkAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer node network does not exist yet. Use NODE /CREATE to create a node network.");
            }

            // Step #2: Deserialize the SIMULATOR_SETTINGS_FILE_NAME settings file
            using (FileStream fs = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                NetworkSpec = (NetworkSpec)JsonSerializer.Deserialize<NetworkSpec>(fs);
            }


            // Step #1:  Check if a simulation has already been created and if so error
            //if (NetworkSimulationAlreadyCreated())
            //{
            //    throw new CommandLineToolInvalidOperationException("A World Computer Network Simulation is already created.  You must explicitly DELETE it before being able to create a new one.");
            //}

            //// Step #2:  Create new NetworkSpec 
            //NetworkSpec = CreateNetworkSpec();

            //// Step #3:	Check if a simulation is already running and if so error
            //if (NetworkAlreadyRunning())
            //{
            //    throw new CommandLineToolInvalidOperationException("A World Computer Network Simulation is already running.  You must explicitly STOP and DELETE it before being able to create a new one.");
            //}

            // Step #3:  Create required copies of the Program.NodeExecutableName, their config files, and their corresponding root storage folder
            StartNetworkNodes();

            // Step #4:  Serialize NetworkSpec to a file of name SIMULATOR_SETTINGS_FILE_NAME
            var nspecJson = JsonSerializer.Serialize(NetworkSpec);
            System.IO.File.WriteAllText(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), nspecJson);
        }

        private void DeleteNetwork()
        {
            // Step #1:  Check if a network exists and if not error
            if (!NetworkAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer node network does not exist to be deleted.");
            }

            // Step #2: Deserialize the SIMULATOR_SETTINGS_FILE_NAME settings file
            using (FileStream fs = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                NetworkSpec = (NetworkSpec)JsonSerializer.Deserialize<NetworkSpec>(fs);
            }

            // Step #3: Check if a VDrive defintion exists
            if(VDriveExists())
            {
                throw new CommandLineToolInvalidOperationException("A World Computer VDrive has been defined on top of the node network. Use VDRIVE /DELETE to remove it before deleting the node network.");
            }

            if (ClusterExists())
            {
                throw new CommandLineToolInvalidOperationException("A World Computer Cluster has been defined on top of the node network. Use CLUSTER /DELETE to remove it before deleting the node network.");
            }

            //// Step #3:	Check if an of the nodes are already running and if so error
            if (!stopSimulator && NetworkAlreadyRunning(NetworkSpec))
            {
                throw new CommandLineToolInvalidOperationException("A World Computer node network is running. Use /STOP to stop it before deleting it");
            }

            // Step #4:  Stop all running nodes 
            if (stopSimulator)
            {
                StopNetworkNodes();
            }

            // Step #4: Delete the simulation settings file
            System.IO.File.Delete(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
            System.IO.File.Delete(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME));

            // Stop #5: Cleanup LocalStore
            PurgeLocalStore();
        }

        private void PurgeLocalStore()
        {
            var localStorePath = Path.Combine(Program.NodeDirectory, Program.LocalStoreDirectoryName);
            var files = Directory.GetFiles(localStorePath, "*", SearchOption.AllDirectories);
            foreach (var file in files)
            {
                int retries = 5;
                while (retries-- > 0)
                {
                    try
                    {
                        System.IO.File.Delete(file);
                        break;
                    }
                    catch (Exception)
                    {
                        Task.Delay(250).Wait();
                        continue;
                    }
                }
            }
            int retry = 5;
            bool success = false;
            while (retry-- > 0)
            {
                try
                {
                    Directory.Delete(localStorePath, true);
                    success = true;
                    break;
                }
                catch (Exception)
                {
                    Task.Delay(1000).Wait();
                    continue;
                }
            }
            if( success)
            {
                Directory.CreateDirectory(localStorePath);
            }
            else
            {
                Console.WriteLine($"NOTE:  WCSim Node LocalStore '{localStorePath}' requires manual clean up.");
            }

        }

        private void StopNetwork()
        {
            // Step #1:  Check if a network exists and if not error
            if (!NetworkAlreadyCreated())
            {
                throw new CommandLineToolInvalidOperationException($"A World Computer node network does not exist to be stop.");
            }

            // Step #2: Deserialize the SIMULATOR_SETTINGS_FILE_NAME settings file
            using (FileStream fs = new FileStream(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                NetworkSpec = (NetworkSpec) JsonSerializer.Deserialize<NetworkSpec>(fs);
            }

            //// Step #3:	Check if an of the nodes are already running and if so error
            //if (!NetworkAlreadyRunning())
            //{
            //    throw new CommandLineToolInvalidOperationException("The World Computer Network Simulation is not running.");
            //}

            // Step #3:  Delete all processors, their config files and their root storage folders
            StopNetworkNodes();
        }

        private void StopNetworkNodes()
        {
            var json = @$"{{""V"" : 1,""F"" : 0,""A"" : {/*(int)ApiIdentifier.ShutDownNode*/95},""I"" : [] }}";
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var nodeList = new List<Node>();
            if (stopNodeCount.Count > 0)
            {
                // Stop only the nodes specified
                foreach( var nodeNumb in stopNodeCount)
                {
                    foreach( var node in NetworkSpec.NodeList)
                    {
                        if( node.Number == nodeNumb)
                        {
                            nodeList.Add(node);
                        }
                    }
                }
            }
            else
            {
                nodeList = NetworkSpec.NodeList;  // stop all
            }
            foreach (var node in nodeList)
            {
                using (Program.UnoSysApiConnection = new UnoSysConnection(string.Format(Program.UnoSysApiUrlTemplate, Program.BasePort + ((node.Number - 1) * Program.BasePortSpacing))))
                {
                    var nodeName = $"{Program.NodeExecutableName} #{node.Number}";
                    Console.WriteLine($"STOPPING : {nodeName} ");
                    try
                    {

                        string result = null!;
                        var response = Program.UnoSysApiConnection.PostAsync(content).Result;
                        string responseJson = null!;
                        if (response.StatusCode == HttpStatusCode.OK)
                        {
                            // Retrieve result of call
                            responseJson = response.Content.ReadAsStringAsync().Result;
                            using var doc = JsonDocument.Parse(responseJson);
                            var element = doc.RootElement;
                            result = element.GetProperty("O")[0].GetProperty("V").GetString();
                        }
                        else
                        {
                            // NOP - best effort
                            Console.WriteLine($"ShutDown API Failed - response.StatusCode={response.StatusCode},  responseJson={responseJson}");
                        }
                    }
                    catch (Exception ex)
                    {
                        // NOP..
                        Console.WriteLine($"ShutDown API Failed - {ex}");
                    }
                }
                int delay = 1 + (Convert.ToInt32(BitConverter.ToUInt32(RandomNumberGenerator.GetBytes(sizeof(int))) % 20));
                Task.Delay(delay).Wait();
            }
            
        }

        private bool NetworkAlreadyCreated()
        {
            return System.IO.File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_SETTINGS_FILE_NAME));
        }

        private bool VDriveExists()
        {
            return System.IO.File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_VDRIVE_FILE_NAME));
        }

        private bool ClusterExists()
        {
            return System.IO.File.Exists(Path.Combine(Program.WorkingDir, Simulator.SIMULATOR_CLUSTER_FILE_NAME));
        }

        internal static  bool NetworkAlreadyRunning(NetworkSpec newworkSpec)
        {
            bool result = false;
            var nodeExecutable = Program.NodeExecutableName.ToUpper();
            foreach (var node in newworkSpec.NodeList)
            {
                try
                {
                    var p = Process.GetProcessById(node.ProcessID);
                    if( p.ProcessName.ToUpper().IndexOf(nodeExecutable ) == 0  )
                    {
                        // Found a process in the NetworkSpec still running...
                        result = true;
                        break;
                    }
                }
                catch (Exception)
                {
                    // NOP and continue
                }
            }
            return result;
        }

        private void StartNetworkNodes()
        {
            var nodeList = new List<Node>();
            if (startNodeCount.Count > 0)
            {
                // Start only the nodes specified
                foreach (var nodeNumb in startNodeCount)
                {
                    foreach (var node in NetworkSpec.NodeList)
                    {
                        if (node.Number == nodeNumb)
                        {
                            nodeList.Add(node);
                        }
                    }
                }
            }
            else
            {
                nodeList = NetworkSpec.NodeList;  // start all
            }
            foreach (var node in nodeList)
            {
                var nodeName = $"{Program.NodeExecutableName} #{node.Number}";
                
                try
                {
                    Process proc = new Process();
                    ProcessStartInfo psi = new ProcessStartInfo();
                    psi.WorkingDirectory = Program.NodeDirectory;
                    psi.FileName = Path.Combine(Program.NodeDirectory, Program.NodeExecutableName + ".exe");
                    psi.UseShellExecute = true;
                    psi.WindowStyle = ProcessWindowStyle.Normal;
                    psi.Arguments = $"{Program.MinimumLoggingLevel} {Program.BasePort + ((node.Number-1) * Program.BasePortSpacing)} {node.Number} {(animateSimulator ? "/A" : "")}";  // e.g.:  WCNode.exe 50060 1 /A
                    proc.StartInfo = psi;
                    proc.Start();

                    node.ProcessID = proc.Id;
                    Console.WriteLine($"STARTING : {nodeName} ");
                    //Task.Delay(1000).Wait();
                    //int delay = 50 + (Convert.ToInt32(BitConverter.ToUInt32(RandomNumberGenerator.GetBytes(sizeof(int))) % 20));
                    //Console.WriteLine($"delay={delay}");
                    Task.Delay(5).Wait();
                    //MoveWindow(proc.MainWindowHandle, 0, ((node.Number-1)*100) + 0, 200, 150, true);
                }
                catch (Exception ex)
                {
                    Debug.Print($"*** Error attempting to start {nodeName}, {ex}  ");
                }
            }
        }
        #endregion
    }
}
