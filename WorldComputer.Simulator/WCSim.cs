using System;

namespace WorldComputer.Simulator
{
	//internal class WCSim : CommandLineTool
	//{
	//	// Switches
	//	//private bool cloudContext;
	//	private bool storageContext;
	//	private bool help;
	//	private ICommandContext commandContext;
	//	private string[] commands;

	//	//static string UNOSYS_COMPUTER_MANAGER_URL;
	//	//static string GRID_HOST_ASYMMETRIC_PUBLIC_KEY;
	//	//static string COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY;

	//	internal WCSim(  )
	//		: base( "World Computer Simulator" )
	//	{


	//		// Define valid commands
	//		commands = new string[3];
	//		//commands[0] = "CLOUD|C";
	//		commands[0] = "STORAGE|S";
	//		commands[1] = "HELP|?";
	//	}

	//	internal void ProcessCommand( string[] args )
	//	{
			
	//		if (args.Length == 0)
	//		{
	//			// Invalid commandline - show usage
	//			CommandLineTool.DisplayBanner();
	//			this.DisplayUsage();
	//		}
	//		else
	//		{
	//			string restOfCommandLine = string.Empty;
	//			// Determine command to run
	//			this.ParseCommandLineCommand( args, commands, ref restOfCommandLine );
	//			//cloudContext = (bool) Commands["CLOUD"] || (bool) Commands["C"];
	//			storageContext = (bool) Commands["STORAGE"] || (bool) Commands["S"];
	//			help = (bool) Commands["HELP"] || (bool) Commands["?"];

	//			// Process commands
	//			if (help)
	//			{
	//				CommandLineTool.DisplayBanner();
	//				this.DisplayUsage();
	//			}
	//			else
	//			{
	//				string[] commandContextArgs = restOfCommandLine.Split( new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries );
	//				// Process Commnad
	//				//if (cloudContext)  // Cloud context?
	//				//{
	//				//	commandContext = new CloudCommandContext( commandContextArgs );
	//				//}
	//				//else  // must be Storage context
	//				if(storageContext)
	//				{
	//					commandContext = new NetworkCommandContext( commandContextArgs );
	//				}

	//				if (commandContext.ValidateContext())
	//				{
	//					commandContext.ProcessCommand();
	//				}

	//			}
	//		}
	//	}

	//	public override void DisplayUsage()
	//	{
	//		Console.Error.WriteLine( "WorldComputer Simulator (WCSim) usage:" );
	//		Console.Error.WriteLine( "" );
	//		Console.Error.WriteLine( "        WCSim <command>" );
	//		Console.Error.WriteLine( "" );
	//		Console.Error.WriteLine( "where <command> is one of:" );
	//		Console.Error.WriteLine( "" );
	//		//Console.Error.WriteLine( "        CLOUD   | C  <switches>\t- Cloud context" );
	//		Console.Error.WriteLine( "        NETWORK | N  <swithces>\t- Storage context" );
	//		Console.Error.WriteLine( "        HELP    | ?" );
	//		Console.Error.WriteLine( "" );
 //           Console.WriteLine("============================================================================");
 //       }


	//	~WCSim()
	//	{
	//		this.Dispose();
	//	}

	//	public void Dispose()
	//	{
	//	}


	//	//internal static string GridHostAsymmetricPublicKey
	//	//{
	//	//	get
	//	//	{
	//	//		if (string.IsNullOrEmpty( GRID_HOST_ASYMMETRIC_PUBLIC_KEY ))
	//	//		{
	//	//			GRID_HOST_ASYMMETRIC_PUBLIC_KEY = ConfigurationManager.AppSettings["GRID_HOST_ASYMMETRIC_PUBLIC_KEY"];
	//	//		}
	//	//		return GRID_HOST_ASYMMETRIC_PUBLIC_KEY;
	//	//	}
	//	//}

	//	//internal static string ComputerManagerToolAsymmetricPrivateKey
	//	//{
	//	//	get
	//	//	{
	//	//		if (string.IsNullOrEmpty( COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY ))
	//	//		{
	//	//			COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY = ConfigurationManager.AppSettings["COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY"];
	//	//		}
	//	//		return COMPUTERMANAGER_TOOL_ASYMMETRIC_PRIVATE_KEY;
	//	//	}
	//	//}

	//	//internal static string UnosysComputerManagerUrl
	//	//{
	//	//	get
	//	//	{
	//	//		if (string.IsNullOrEmpty( UNOSYS_COMPUTER_MANAGER_URL ))
	//	//		{
	//	//			UNOSYS_COMPUTER_MANAGER_URL = ConfigurationManager.AppSettings["UNOSYS_COMPUTER_MANAGER_URL"];
	//	//		}
	//	//		return UNOSYS_COMPUTER_MANAGER_URL;
	//	//	}
	//	//}

	//}
}
