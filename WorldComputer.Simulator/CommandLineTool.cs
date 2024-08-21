using System;
using System.Collections;

namespace WorldComputer.Simulator
{
		abstract public class CommandLineTool
		{
			protected Hashtable Commands;
			protected Hashtable Switches;
			static string UtilityName;
			public CommandLineTool( string sUtilityName )
			{
				UtilityName = sUtilityName;
			}

			#region Public Methods

			//public virtual string ParseCommandArgs( string[] args, 	string[] asSwitches )
			//{


			//	this.ParseCommandArgs( args, asSwitches, true );
			//	if( ! (bool) Switches["/Q"] )
			//		this.DisplayBanner();


			//	if( (bool) Switches["/?"]  || args[0] == "/?" )
			//	{
			//		this.DisplayUsage();
			//		throw new CommandLineToolUsageException("");
			//	}


			//	string FileSpec;
			//	if( args.Length == 0 )
			//	{
			//		this.DisplayUsage();
			//		throw new CommandLineToolFileSpecException();
			//	}
			//	else
			//		FileSpec = args[0];

			//	if( FileSpec.Substring(0,1) == "/" )
			//	{
			//		this.DisplayUsage();
			//		throw new CommandLineToolFileSpecException();
			//	}

			//	return FileSpec;
			//}

			public virtual void ParseCommandLineCommand( string[] commandLineArgs, 	string[] commands, ref string restOfCommandLine )
			{
				restOfCommandLine = "";

				Commands = new Hashtable( 3 );
				for (int i = 0; i < commands.Length; i++)
				{
					string[] parts = commands[i].ToUpper().Trim().Split( new char[] { '|' } );
					foreach (var s in parts)
					{
						Commands.Add( s.Trim(), false );
					}
				}

				// Check which (exactly one) command is passed and override it's default values accordingly
				string cmd = commandLineArgs[0].ToUpper().Trim();
				if (Commands[cmd] == null)
				{
					// Invalid command usage
					DisplayBanner();
					this.DisplayUsage();
					throw new CommandLineToolInvalidCommandException( commandLineArgs[0] );
				}
				else
				{
					Commands[cmd] = true;
				}

				// Compute rest of commnadline
				if( commandLineArgs.Length > 1)
				{
					for(int i=1; i< commandLineArgs.Length; i++)
					{
						restOfCommandLine += commandLineArgs[i].Trim()  + " ";
					}
				}
			}


			static public void ParseCommandContextSwitches( string[] commandContextArgs, string[] allowedSwitches, Hashtable switchset, ICommandContext commandContext )
			{
				for (int i = 0; i < allowedSwitches.Length; i++)
				{
					string[] parts = allowedSwitches[i].ToUpper().Trim().Split( new char[] { '|' } );
					foreach (var s in parts)
					{
						string[] values = s.Split( new char[] { ':' } );
						if (values.Length == 1)
						{
							switchset.Add( s.Trim(), false );
						}
						else
						{
							switchset.Add( values[0].Trim(), false );
						}
					}
				}

				// Check if switches are passed and override their default values accordingly
				for (int i = 0; i < commandContextArgs.Length; i++)
				{
					string sSwitch = commandContextArgs[i].ToUpper().Trim();
					string[] parts = sSwitch.Split( new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries );
					if (parts.Length == 1)
					{
						if (switchset[parts[0].Trim()] == null)
						{
							// Invalid switch - show usage
							DisplayBanner();
							commandContext.DisplayUsage();
							throw new CommandLineToolInvalidSwitchException( sSwitch );
						}
						else
						{
							// If we make it here the switch is allowed...
							//
							// Check to see if switch has already been specified before (i.e.; is either a string or is true boolean)
							if (!(switchset[parts[0].Trim()] is bool) || (bool)(switchset[parts[0].Trim()]))
							{
								throw new CommandLineToolDuplicateSwitchesException( sSwitch );
							}
							switchset[parts[0].Trim()] = true;
						}
					}
					else
					{
						if (!switchset.ContainsKey(parts[0].Trim() ) )
						{
							// Invalid switch - show usage
							DisplayBanner();
							commandContext.DisplayUsage();
							throw new CommandLineToolInvalidSwitchException( sSwitch );
						}
						else
						{
							switchset[parts[0].Trim()] = parts[1].Trim();
						}
					}
				}
			}

			static public void DisplayBanner(  )
			{
					Console.WriteLine("============================================================================");
					Console.WriteLine( UtilityName );
					//Console.WriteLine(DiskBounty.Common.DiskBountySettings.ASSEMBLY_COPYRIGHT);
					Console.WriteLine("");
					Console.WriteLine(Environment.CommandLine);
					Console.WriteLine("");
					
        }

			public  virtual void DisplayUsage()
			{
				// Overrid to display usage of utility
			}
			#endregion

		}

		#region CommandLineTool Exceptions
		public class CommandLineToolUsageException : Exception 
		{
			public CommandLineToolUsageException( string sMsg ) : base( sMsg ) {}
		}
		public class CommandLineToolDuplicateSwitchesException : Exception
		{
			public CommandLineToolDuplicateSwitchesException( string sMsg ) : base( sMsg ) { }
		}
		public class CommandLineToolInvalidSwitchCombinationException : Exception
		{
			public CommandLineToolInvalidSwitchCombinationException( string sMsg ) : base( sMsg ) { }
		}

		public class CommandLineToolInvalidOperationException : Exception
		{
			public CommandLineToolInvalidOperationException( string sMsg ) : base( sMsg ) { }
		}												

		public class CommandLineToolFileSpecException: Exception 
		{
			public CommandLineToolFileSpecException(  ) : base( "ERROR: Required file spec missing or invalid" ) {}
		}												
		public class CommandLineToolInvalidSwitchException: Exception 
		{
			public CommandLineToolInvalidSwitchException( string sMsg ) : base( sMsg ) {}
		}
		public class CommandLineToolInvalidCommandException : Exception
		{
			public CommandLineToolInvalidCommandException( string sMsg ) : base( sMsg ) { }
		}												
		public class CommandLineToolFileNotFoundInGridException : Exception
		{
			public CommandLineToolFileNotFoundInGridException( string sMsg ) : base( sMsg ) { }
		}
		public class CommandLineToolGridNotFoundException : Exception
		{
			public CommandLineToolGridNotFoundException() : base() {}
		}
		public class CommandLineToolFileAlreadyExistsInGridException : Exception
		{
			public CommandLineToolFileAlreadyExistsInGridException( string sMsg ) : base( sMsg ) { }
		}
		public class CommandLineToolAccessDeniedInGridException : Exception
		{
			public CommandLineToolAccessDeniedInGridException( string sMsg ) : base( sMsg ) {}
		}
		public class CommandLineToolUnosysProcessorNotInstalled : Exception
		{
			public CommandLineToolUnosysProcessorNotInstalled( string sMsg ) : base( sMsg ) { }
		}

		public class CommandLineToolInvalidSwitchArgumentException : Exception
		{
			public CommandLineToolInvalidSwitchArgumentException(string sMsg) : base(sMsg) { }
		}
    #endregion
    
        public interface ICommandContext
		{
			void DisplayUsage();
			void ProcessCommand();
			bool ValidateContext();
		}
}