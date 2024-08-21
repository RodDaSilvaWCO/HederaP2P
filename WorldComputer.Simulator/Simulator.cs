using System;
//using System.Configuration;
//using DiskBounty.Tools.Common;

namespace WorldComputer.Simulator
{
	internal class Simulator : CommandLineTool
	{
		// Switches
		//private bool cloudContext;
		private bool netContext;
		private bool vdContext;
		private bool clusterContext;
		private bool help;
		private ICommandContext commandContext;
		private string[] commands;
		//static string FILESYSTEM_DRIVE_LETTER = "W:";
		static string UNOSYS_NODE_BASE_DIR;
        //static string UNOSYS_COMPUTER_MANAGER_URL;
        //static string GRID_HOST_ASYMMETRIC_PUBLIC_KEY;
        //static string COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY;
        //static string UNOSYS_NODE_URL_TEMPLATE;

        static internal string SIMULATOR_SETTINGS_FILE_NAME = "WCSim.json";
        static internal string SIMULATOR_PROCESSOR_EXE_NAME = "WCNode";
        static internal string SIMULATOR_VDRIVE_FILE_NAME = "WCSimVDrive.json";
        static internal string SIMULATOR_CLUSTER_FILE_NAME = "WCSimCluster.json";
        static internal int MAX_NETWORK_NODE_SIZE = 100;


        internal Simulator(  )
			: base( "WorldComputer Simulator (WCSim) Utility" )
		{


			// Define valid commands
			commands = new string[4];
			//commands[0] = "CLOUD|C";
			commands[0] = "NODE    | N";
            commands[1] = "CLUSTER | C";
            commands[2] = "VDRIVE  | VD";
			commands[3] = "HELP    | ?";
		}

		internal void ProcessCommand( string[] args )
		{
			
			if (args.Length == 0)
			{
				// Invalid commandline - show usage
				CommandLineTool.DisplayBanner();
				this.DisplayUsage();
			}
			else
			{
				string restOfCommandLine = string.Empty;
				// Determine command to run
				this.ParseCommandLineCommand( args, commands, ref restOfCommandLine );
				netContext = (bool) Commands["NODE"] || (bool)Commands["N"];
                vdContext = (bool)Commands["VDRIVE"] || (bool)Commands["VD"];
                clusterContext = (bool)Commands["CLUSTER"] || (bool)Commands["C"];
                help = (bool) Commands["HELP"] || (bool) Commands["?"];

				// Process commands
				if (help)
				{
					CommandLineTool.DisplayBanner();
					this.DisplayUsage();
				}
				else
				{
					string[] commandContextArgs = restOfCommandLine.Split( new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries );
					if(netContext)
					{
						commandContext = new NetworkCommandContext( commandContextArgs );
					}
					else if (vdContext)
					{
						commandContext = new VirtualDriveCommandContext(commandContextArgs);
					}
					else if(clusterContext)
					{
                        commandContext = new ClusterCommandContext(commandContextArgs);
                    }

					if (commandContext.ValidateContext())
					{
						commandContext.ProcessCommand();
					}

				}
			}
		}

		public override void DisplayUsage()
		{
			Console.Error.WriteLine( "WorldComputer Simulator (WCSim) usage:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "        WCSim <command>" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "where <command> is one of:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "        NODE    | N\t<swithces> - Manage a Node network" );
            Console.Error.WriteLine( "        CLUSTER | C\t<swithces> - Manage a Cluster of nodes");
            Console.Error.WriteLine( "        VDRIVE  | VD\t<swithces> - Manage a Virtual Drive backed by nodes");
            Console.Error.WriteLine( "        HELP    | ?");
			Console.Error.WriteLine( "" );
            Console.WriteLine("============================================================================");
        }


		~Simulator()
		{
			this.Dispose();
		}

		public void Dispose()
		{
		}


		//internal static string GridHostAsymmetricPublicKey
		//{
		//	get
		//	{
		//		if (string.IsNullOrEmpty( GRID_HOST_ASYMMETRIC_PUBLIC_KEY ))
		//		{
		//			GRID_HOST_ASYMMETRIC_PUBLIC_KEY = ConfigurationManager.AppSettings["GRID_HOST_ASYMMETRIC_PUBLIC_KEY"];
		//		}
		//		return GRID_HOST_ASYMMETRIC_PUBLIC_KEY;
		//	}
		//}

		//internal static string ComputerManagerToolAsymmetricPrivateKey
		//{
		//	get
		//	{
		//		if (string.IsNullOrEmpty( COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY ))
		//		{
		//			COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY = ConfigurationManager.AppSettings["COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY"];
		//		}
		//		return COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY;
		//	}
		//}

		//internal static string UnosysComputerManagerUrl
		//{
		//	get
		//	{
		//		if (string.IsNullOrEmpty( UNOSYS_COMPUTER_MANAGER_URL ))
		//		{
		//			UNOSYS_COMPUTER_MANAGER_URL = ConfigurationManager.AppSettings["UNOSYS_COMPUTER_MANAGER_URL"];
		//		}
		//		return UNOSYS_COMPUTER_MANAGER_URL;
		//	}
		//}

		internal static string UnoSysBaseDir
		{
			get
			{
				if (string.IsNullOrEmpty(UNOSYS_NODE_BASE_DIR))
				{
					//UNOSYS_NODE_BASE_DIR = ConfigurationManager.AppSettings["UNOSYS_NODE_BASE_DIR"];
				}
				return UNOSYS_NODE_BASE_DIR;
			}
		}
		//internal static string FileSystemDriveLetter
		//{
		//	get
		//	{
		//		if (string.IsNullOrEmpty(FILESYSTEM_DRIVE_LETTER))
		//		{
		//			//FILESYSTEM_DRIVE_LETTER = ConfigurationManager.AppSettings["FILESYSTEM_DRIVE_LETTER"];
		//		}
		//		return FILESYSTEM_DRIVE_LETTER;
		//	}
		//}

		//internal static string NodeUrlTemplate
		//{
		//	get
		//	{
		//		if (string.IsNullOrEmpty(UNOSYS_NODE_URL_TEMPLATE))
		//		{
		//			UNOSYS_NODE_URL_TEMPLATE = ConfigurationManager.AppSettings["UNOSYS_NODE_URL_TEMPLATE"];
		//		}
		//		return UNOSYS_NODE_URL_TEMPLATE;
		//	}
		//}


	}
}
