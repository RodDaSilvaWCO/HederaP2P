using System;


namespace WorldComputer.Simulator
{
	[Serializable]
	public class SimulatorNodeSpec
	{
		public Guid NodeID;
		public int BasePort;
		public string NodeAsymmetricPrivateKey;
		public string NodeAsymmetricPublicKey;
		public string BlockDeviceStorageRoot;
		public string FileSystemDriveLetter;
	}
}
