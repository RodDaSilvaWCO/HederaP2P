using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorldComputer.Simulator
{
	[Serializable]
	public class SimulatorStorageGridSpec
	{

        public int BlockSize = 4096;
		public Guid CID;
		public Guid SDID;
		public Guid SCID;
		public List<SimulatorClusterSpec> AddressSpace = null;
        public int SplitNode = 0;
		public bool IsCloudSimulation = false;
        public SimulatorStorageGridSpec()
		{
			AddressSpace = new List<SimulatorClusterSpec>();
		}
	}
}
