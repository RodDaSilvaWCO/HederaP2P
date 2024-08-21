using System;
using System.Collections;

namespace WorldComputer.Simulator
{

	public class CloudCommandContext : ICommandContext
	{
		bool computerHelp = false;
		Hashtable switchSet = new Hashtable( 20 );
		string[] cmdArgs;


		public CloudCommandContext( string[] commandContextArgs )
		{
			cmdArgs = commandContextArgs;
			string[] allowedSwitches = new string[1];
			allowedSwitches[0] = "/?";
			CommandLineTool.ParseCommandContextSwitches( commandContextArgs, allowedSwitches, switchSet, this );
			computerHelp =  (bool) switchSet["/?"];
		}

		public bool ValidateContext()
		{
			return true;  // TODO
		}

		public void ProcessCommand()
		{
			if (computerHelp)
			{
				CommandLineTool.DisplayBanner();
				DisplayUsage();
			}
			else
			{
				if (cmdArgs.Length == 0)
				{
					Console.WriteLine( "Computer command has no switches" );
				}
				else
				{
					Console.WriteLine( "Todo:  Computer command processing goes here...." );
				}
			}
		}

		public void DisplayUsage()
		{
			Console.Error.WriteLine( "WorldComputer CLOUD usage:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "      WCSim CLOUD | C  <switches>" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( "where <switches> are one or more of:" );
			Console.Error.WriteLine( "" );
			Console.Error.WriteLine( " /?\t\t\t- Usage information" );
			Console.Error.WriteLine( "" );

		}
	}
}
