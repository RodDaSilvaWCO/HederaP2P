using System;
using System.Collections.Generic;

namespace WorldComputer.Simulator
{
	[Serializable]
	public sealed class SimulatorClusterSpec
	{
		public List<SimulatorNodeSpec> NodeSet = null;
		//public int Ordinal;

	
		public SimulatorClusterSpec() { 

		//public SimulatorClusterSpec( int ordinal )
		//{
		//	//Ordinal = ordinal;
			NodeSet = new List<SimulatorNodeSpec>();
		}

		public void AddNode( SimulatorNodeSpec node )
		{
			NodeSet.Add( node );
		}
	}
}
