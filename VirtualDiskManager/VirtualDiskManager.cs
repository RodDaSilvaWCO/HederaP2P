namespace UnoSysKernel
{
    using Microsoft.Extensions.Logging;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using System.IO;
    using System;
    using System.Text.Json;
    using System.Text;
    using UnoSysCore;
    using System.Diagnostics;
    using UnoSys.Api.Exceptions;
    using Microsoft.CodeAnalysis.Operations;
    using Microsoft.AspNetCore.Identity;

    internal sealed class VirtualDiskManager : SecuredKernelService, IVirtualDiskManager, ICriticalShutdown
    {
        #region Member Fields
        private Dictionary<Guid, VirtualDisk>? _virtualDisks = null!;
        private Stream? _virtualDisksFile = null!;
        private IGlobalPropertiesContext? _globalPropertiesContext = null!;
        private INodeClusterContext? _nodeClusterContext = null!;
        private ILocalNodeContext? _localNodeContext = null!;
        private Dictionary<Guid, VDiskVolume> _volumeTable = null!;
        private IGlobalEventSubscriptionManager? _globalEventSubscriptionManager = null!;
        private Guid onVDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATIONeventTargetID = default(Guid);
        private Guid onVDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATIONeventTargetID = default(Guid);
        private Guid onVDISK_FILECREATEeventTargetID = default(Guid);
        private Guid onVDISK_FILECLOSEeventTargetID = default(Guid);
        private Guid onVDISK_FILEDELETEeventTargetID = default(Guid);
        private Guid onVDISK_FILERENAMEORMOVEeventTargetID = default(Guid);
        private ILoggerFactory? _loggerFactory = null!;
        private ITime? _timeManager = null!;
        private ICacheManager? _cacheManager = null!;    
        private INetworkManager? _networkManager = null!;
        private IKernelConcurrencyManager? _kernelConcurrencyManager = null!;
        private BlockManager _blockManager = null!;
        private uint _volumeBlockSize;
        #endregion

        #region Constructors
        public VirtualDiskManager(IGlobalPropertiesContext gProps, ILoggerFactory loggerFactory, IKernelConcurrencyManager concurrencyManager, ISecurityContext securityContext, 
                                   ILocalNodeContext localNodeContext,  INodeClusterContext nodeclustercontext, IGlobalEventSubscriptionManager gesubmanager,
                                   INetworkManager networkmanager, ITime time, ICacheManager cacheManager) : base(loggerFactory.CreateLogger("VirtualDiskManager"), concurrencyManager)
		{
            _globalPropertiesContext = gProps;
            _timeManager = time;
            _loggerFactory = loggerFactory;
            _kernelConcurrencyManager = concurrencyManager;
            _networkManager = networkmanager;
            _cacheManager = cacheManager;
            _localNodeContext = localNodeContext;
            _nodeClusterContext = nodeclustercontext;
            _globalEventSubscriptionManager = gesubmanager;
            _volumeTable = new Dictionary<Guid, VDiskVolume>();

            // Create a secured object for this kernel service 
            securedObject = securityContext.CreateDefaultKernelServiceSecuredObject();
        }
        #endregion


        #region IDisposable Implementation
        public override void Dispose()
        {
            _globalPropertiesContext = null!;
            _nodeClusterContext = null!;
            _localNodeContext = null!;
            _globalEventSubscriptionManager = null!;
            _loggerFactory = null!;
            _timeManager = null!;
            _cacheManager = null!;
            _networkManager = null!;
            _kernelConcurrencyManager = null!;
            _volumeTable = null!;
            _blockManager?.Dispose();
            _blockManager= null!;
            foreach (var vdisk in _virtualDisks!)
            {
                vdisk.Value?.Dispose();
            }
            _virtualDisks = null!;
            _virtualDisksFile?.Flush();
            _virtualDisksFile?.Close();
            _virtualDisksFile?.Dispose();
            _virtualDisksFile = null!;
            //simulatorConnectionTask = null!;
            //peerSet = null!;
            base.Dispose();
        }

        #endregion 


        #region IKernelService Implementation
        public override async Task StartAsync(CancellationToken cancellationToken)
		{
			using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
			{
                #region Check for and Open/Read Virtual Disks local file to populate _virtualDisks dictionary
                string virtualDisksFileName = null!;
                //byte[] virtualDisksJsonBytes = null!;
                if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0)
                {
                    virtualDisksFileName = HostCryptology.ConvertBytesToHexString(HostCryptology.Encrypt2(Encoding.ASCII.GetBytes(
                                $"VD{HostCryptology.ConvertBytesToHexString(_nodeClusterContext!.WorldComputerSymmetricKey)}" +
                                   _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToString("N").ToUpper()), _nodeClusterContext.NodeSymmetricKey, _nodeClusterContext.NodeSymmetricIV));
                }
                else
                {
                    virtualDisksFileName = HostCryptology.ConvertBytesToHexString(HostCryptology.Encrypt2(Encoding.ASCII.GetBytes(
                                $"VD{HostCryptology.ConvertBytesToHexString(_nodeClusterContext!.WorldComputerSymmetricKey)}" +
                                    _nodeClusterContext.NodeID.ToString("N").ToUpper()), _nodeClusterContext.NodeSymmetricKey, _nodeClusterContext.NodeSymmetricIV));
                }
                var virtualDisksFilePath = Path.Combine(Path.Combine(_globalPropertiesContext.NodeDirectoryPath, _nodeClusterContext.LocalStoreDirectoryName), virtualDisksFileName);
                if (!File.Exists(virtualDisksFilePath))
                {
                    if (_globalPropertiesContext.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0)
                    {
                        // Assume first time Simulation Node is being run so create the missing file with a file wtih no entries
                        _virtualDisksFile = new FileStream(virtualDisksFilePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                        _virtualDisks = new Dictionary<Guid,VirtualDisk>();
                        RewriteVDisksFile();
                        //var json = JsonSerializer.Serialize<Dictionary<Guid, VirtualDisk>>(_virtualDisks);
                        //virtualDisksJsonBytes = Encoding.UTF8.GetBytes(json);
                        //// Encrypt in-place  using Node keys
                        //HostCryptology.EncryptBufferInPlaceWith32ByteKey(virtualDisksJsonBytes, _nodeClusterContext.NodeSymmetricKey);
                        //_virtualDisksFile.Write(virtualDisksJsonBytes, 0, virtualDisksJsonBytes.Length);
                        //_virtualDisksFile.Flush();
                    }
                    else
                    {
                        // For a regular node the file should have been created by spawn, so error
                        throw new System.Exception($"The Node's virtual disks file was not found - shutting down.");
                    }
                }
                else
                {
                    // If we make it here we simply open the existing connectedNodeClustersFilePath file
                    _virtualDisksFile = new FileStream(virtualDisksFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    byte[] virtualDisksJsonBytes = new byte[_virtualDisksFile.Length];
                    _virtualDisksFile.ReadExactly(virtualDisksJsonBytes, 0, virtualDisksJsonBytes.Length);
                    // Decrypt "in-place"
                    HostCryptology.DecryptBufferInPlaceWith32ByteKey(virtualDisksJsonBytes, _nodeClusterContext.NodeSymmetricKey);
                    var json = Encoding.UTF8.GetString(virtualDisksJsonBytes);
                    _virtualDisks = JsonSerializer.Deserialize<Dictionary<Guid,VirtualDisk>>(json)!;
                }
                #endregion

                if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext?.SimulationNodeNumber > 0)
                {
                    #region Connect all fully allocated VDisks
                    foreach (var vdisk in _virtualDisks)
                    {
                        if (vdisk.Value.IsFullyAllocated())
                        {
                            ConnectIfNodeParticipatesInCluster(vdisk.Value, true);
                        }
                    }
                    #endregion
                }

                #region Register to receive global events VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION and VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION and then start event streaming!
                onVDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATIONeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION_Event, GlobalEventType.VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION);
                onVDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATIONeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION_Event, GlobalEventType.VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION);
                onVDISK_FILECREATEeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_FILECREATE_Event, GlobalEventType.VDISK_FILECREATE);
                onVDISK_FILECLOSEeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_FILECLOSE_Event, GlobalEventType.VDISK_FILECLOSE);
                onVDISK_FILEDELETEeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_FILEDELETE_Event, GlobalEventType.VDISK_FILEDELETE);
                onVDISK_FILERENAMEORMOVEeventTargetID = _globalEventSubscriptionManager!.RegisterEventTarget(On_VDISK_FILERENAMEORMOVE_Event, GlobalEventType.VDISK_FILERENAMEORMOVE);
                _globalEventSubscriptionManager!.StartGlobalEventStreaming();  // *** IMPORTANT *** this is the only time that global eventing is started in all of UnoSysKernel!!
                #endregion

                await base.StartAsync(cancellationToken).ConfigureAwait(false);
			}
		}

		public override async Task StopAsync(CancellationToken cancellationToken)
		{
            //Debug.Print($"*** VirtualDiskManager.StopAsync(ENTER...)");
            
            using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
			{
                #region  Stop global event streaming and then unregister to stop receiving global events 
                await _globalEventSubscriptionManager!.StopGlobalEventStreaming().ConfigureAwait(false);  // *** IMPORTANT *** this is the only time that global eventing is stopped in all of UnoSysKernel!!
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_FILERENAMEORMOVEeventTargetID);
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_FILEDELETEeventTargetID);
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_FILECLOSEeventTargetID);
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_FILECREATEeventTargetID);
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATIONeventTargetID);
                _globalEventSubscriptionManager!.UnregisterEventTarget(onVDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATIONeventTargetID);
                #endregion 

                List<Task> disconnectTasks = new List<Task>();
                foreach (var vdisk in _virtualDisks!)
                {
                    disconnectTasks.Add(vdisk.Value.DisconnectAsync(_networkManager!.CancellationToken));
                }
                await Task.WhenAll(disconnectTasks).ConfigureAwait(false);

                #region Flush Connected Node Clusters file contents and close
                _virtualDisksFile?.Flush();
                _virtualDisksFile?.Close();
                _virtualDisksFile?.Dispose();
                _virtualDisksFile = null!;
                #endregion

                await base.StopAsync(cancellationToken).ConfigureAwait(false);
			}
            //Debug.Print($"*** VirtualDiskManager.StopAsync(...LEAVE)");
        }

        #endregion

        #region ICriticalShutdown Implementaiton
        public async Task CriticalStopAsync( CancellationToken cancellationToken )
        {
            await StopAsync(cancellationToken).ConfigureAwait(false);
        }
        #endregion 

        #region IVirtualDiskManager Implementation
        public async Task<string> CreateAsync(int clusterSize, int replicationFactor)
        {
            #region Step #1 - create a new VirtualDisk ID
            Guid provisionVDiskID = Guid.NewGuid();
            #endregion

            #region Step #2 - Send the VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION global event with a payload of providsionVDiskID
            GlobalEvent globalEvent = null!;

            #region Pass ClusterSize and ReplicationFactor in the payload of the event
            #region 1st version of VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION event
            byte[] payload = new byte[ 1+ sizeof(int) + sizeof(int) + 16 * clusterSize ];
            if (payload.Length > GlobalEvent.MaximumPayloadSize)
            {
                throw new ArgumentException("Maximum number of clusters is 63.");
            }
            int pos = 0;
            payload[pos++] = 0;  // Event playload version number - zero is first of 256 possible versions;
            Buffer.BlockCopy( BitConverter.GetBytes(clusterSize), 0, payload, pos, sizeof(int));
            pos+= sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(replicationFactor), 0, payload, pos, sizeof(int));
            pos+= sizeof(int);
            #region Generate a Guid ID for each cluster required
            for (int i = 0; i<clusterSize; i++ )
            {
                Buffer.BlockCopy( Guid.NewGuid().ToByteArray(), 0, payload, pos, 16);
                pos += 16;
            }
            #endregion 
            #endregion
            #endregion

            if ( GlobalPropertiesContext.IsSimulatedNode() )
            { 
                globalEvent = new GlobalEvent(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber], 
                                                GlobalEventType.VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION, provisionVDiskID, payload);
                #region Display Submission to Console
                if (!EntryPoint.AnimateNodesOnScreen)
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Submitted Event: ", $"PROPOSE", ConsoleColor.Yellow);
                }
                #endregion

            }
            else 
            {
                globalEvent = new GlobalEvent(_localNodeContext!.NodeDIDRef, GlobalEventType.VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION, provisionVDiskID, payload);
            }
            await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
            #endregion


            #region Step# 3 - Wait for signal in On_VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION_Event that indicates the above event has been received by this node and VDisk has been created
            IVirtualDisk virtualDisk = null!;
            while (true)
            {
                lock (_virtualDisks!)
                {
                    if (_virtualDisks.ContainsKey(provisionVDiskID))
                    {
                        virtualDisk = _virtualDisks[provisionVDiskID];
                        break;
                    }
                }
                await Task.Delay(5).ConfigureAwait(false);  
            }
            #endregion

            
            return provisionVDiskID.ToString("N").ToUpper();
        }


        public async Task<string> MountAsync(Guid vdiskID, uint blockSize )
        {
            string vDriveDef = null!;
            try
            {
                _volumeBlockSize = blockSize;
                IVirtualDisk vDiskDef = null!;
                lock (_virtualDisks!)
                {
                    if (!_virtualDisks.ContainsKey(vdiskID))
                    {
                        throw new UnoSysResourceNotFoundException();
                    }
                    vDiskDef = _virtualDisks[vdiskID];
                }
                #region Wait until vDisk is fully alloocated
                // NOTE:  There is a natural race condition that occurs when the Mounting a VDrive immediately after calling CreateAsync() above to create a Virtual Disk.
                //        The CreateAsync() call returns as soon as the "first" VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION is received from any node.  HOwever, we
                //        can't actually "mount" a VDrive until we have a fully allocated VDisk.  So we wait here until that is the case.

                // %TODO%  This loop should timeout after a certain amount of time!
                while (!vDiskDef.IsFullyAllocated())
                {
                    await Task.Delay(5).ConfigureAwait(false);
                }
                #endregion 

                #region Create a new Mounted PeerSet that contains all nodes of all clusters of the vDisk 
                using (var _crc32 = new HostCrc32())
                {
                    int thisNodeCluster = -1;
                    int thisNodeReplica = -1;
                    #region Determine 'this' node's ID
                    ulong ThisNodeID = 0;
                    if (GlobalPropertiesContext.IsSimulatedNode())
                    {
                        ThisNodeID = BitConverter.ToUInt32(_crc32.ComputeHash(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToByteArray()), 0);
                    }
                    else
                    {
                        ThisNodeID = BitConverter.ToUInt32(_crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                    }
                    #endregion
                    ConnectionMetaData[,] storageGrid = new ConnectionMetaData[vDiskDef.ClusterSize, vDiskDef.ReplicationFactor];
                    var sbVDriveDef = new StringBuilder();
                    for (int c = 0; c < vDiskDef.ClusterSize; c++)
                    {
                        var nodeset = vDiskDef.ClusterList[c].Nodes;
                        for (int r = 0; r < vDiskDef.ReplicationFactor; r++)
                        {
                            ulong connectionId = BitConverter.ToUInt32(_crc32.ComputeHash(nodeset[r].ID.ToByteArray()), 0);
                            storageGrid[c, r] = new ConnectionMetaData(connectionId, nodeset[r].PublicKey, nodeset[r].IPv4Address, nodeset[r].Port, GlobalPropertiesContext.ComputeNodeNumberFromPort(nodeset[r].Port).ToString());
                            if (connectionId == ThisNodeID)
                            {
                                thisNodeCluster = c;
                                thisNodeReplica = r;
                            }
                            sbVDriveDef.Append($"{nodeset[r].IPv4Address}:{nodeset[r].Port}");
                            if (r < vDiskDef.ReplicationFactor - 1) sbVDriveDef.Append(',');

                        }
                        if (c < vDiskDef.ClusterSize - 1) sbVDriveDef.Append('|');
                    }

                    #region Allocate a new Volume ID for this Mount operation
                    var volID = Guid.NewGuid();
                    var vDiskVolume = new VDiskVolume(volID, vdiskID);
                    #endregion

                    #region Instantiate the BlockManager that represents this vDrive
                    
                    _blockManager = new BlockManager(vdiskID, volID, vDiskDef.ClusterSize, vDiskDef.ReplicationFactor, storageGrid, blockSize,
                                             Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath,_localNodeContext!.LocalStoreDirectoryName),
                                                    $"Node{{0}}_Data_{vdiskID.ToString("N").ToUpper()}") );
                    #endregion 

                    #region Create a Mounted PeerSet for the Volume 
                    //vDiskVolume.MountedPeerSet = StorageArrayPeerSet.Create(
                    //                _globalPropertiesContext!,
                    //                _timeManager!,
                    //                _networkManager!.ConnectionPool!,
                    //                _cacheManager!,
                    //                BitConverter.ToUInt32(_crc32.ComputeHash(vDiskDef.ClusterList[thisNodeCluster].ID.ToByteArray()), 0),
                    //                storageGrid,
                    //                vDiskDef.ClusterSize,
                    //                vDiskDef.ReplicationFactor,
                    //                thisNodeCluster,
                    //                thisNodeReplica,
                    //                false,
                    //                _networkManager);
                    #endregion

                    #region Register the new Volume 
                    lock (_volumeTable)
                    {
                        _volumeTable.Add(volID, vDiskVolume);
                    }
                    #endregion 

                    vDriveDef = volID.ToString("N").ToUpper() + sbVDriveDef.ToString();
                    //#region Connect all the nodes in the Mounted PeerSet
                    //((StorageArrayPeerSet)(vDiskVolume.MountedPeerSet)).StartAsync(_networkManager!.CancellationToken);
                    //#endregion
                    //#region Wait until Mounted PeerSet is reliably connected
                    //// %TODO%  This loop should timeout after a certain amount of time!
                    //while (vDiskVolume.MountedPeerSet.ConnectionReliability <= ConnectionReliability.UnReliablyClusterConnected)
                    //{
                    //    await Task.Delay(5).ConfigureAwait(false);
                    //}
                    //#endregion
                }
                #endregion
            }
            catch (Exception ex)
            {
                Debug.Print($"VirtualDiskManager.MountAsync(ERROR) {ex}");
            }
            return await Task.FromResult(vDriveDef).ConfigureAwait(false);
        }

        public async Task UnmountAsync(Guid volumeID)
        {
            try
            {
                VDiskVolume vDiskVolume = null!;

                #region Remove the Volume with volumeID from _volumeTable
                lock (_volumeTable!)
                {
                    if (!_volumeTable.ContainsKey(volumeID))
                    {
                        throw new UnoSysResourceNotFoundException();
                    }
                    vDiskVolume = _volumeTable[volumeID];
                    // Now remove the volume entry altogether
                    _volumeTable.Remove(volumeID);
                }
                #endregion
                //lock (_virtualDisks!)
                //{
                //    if (!_virtualDisks.ContainsKey(vDiskVolume.VDiskID))
                //    {
                //        throw new UnoSysResourceNotFoundException();
                //    }
                //    vDiskDef = _virtualDisks[vDiskVolume.VDiskID];
                //}

                //#region Disconnect Mounted PeerSet
                //await ((StorageArrayPeerSet)(vDiskVolume.MountedPeerSet!)).StopAsync(_networkManager!.CancellationToken).ConfigureAwait(false);
                //#endregion
            }
            catch (Exception ex)
            {
                Debug.Print($"VirtualDiskManager.UnmountAsync(ERROR) {ex}");
            }
            await Task.Delay(5).ConfigureAwait(false);
        }

        public async Task<string> VolumeDataOperationAsync(string base64Operation)
        {
           string result = "OK";
            try
            {
                var rawOperationbytes = Convert.FromBase64String(base64Operation);
                VolumeDataOperation operation = new VolumeDataOperation(rawOperationbytes);
                switch (operation.OperationType)
                {
                    case VolumeDataOperationType.FILE_WRITE:
                        {
                            var bytesWritten = _blockManager.WriteSetMemberAsync(operation.FileID, operation.FileLength, operation.ByteBuffer , operation.Position, operation.ByteCount).Result;
                            if( operation.FileLength < operation.Position + bytesWritten )
                            {
                                // If we make it here we have a new "high water mark" for the length of the file, so update it in the operation and send it back
                                operation.FileLength = operation.Position + bytesWritten;
                            }
                            operation.ByteCount = bytesWritten;
                            operation.ByteBuffer = null!;  // no need to send the buffer back to the caller
                        }
                        break;

                    case VolumeDataOperationType.FILE_READ:
                        {
                            operation.ByteBuffer = new byte[operation.ByteCount];
                            var bytesRead = _blockManager.ReadSetMemberAsync(operation.FileID, operation.FileLength, operation.ByteBuffer, operation.Position, operation.ByteCount).Result;
                            operation.ByteCount = bytesRead;
                        }
                        break;
                }
                result = JsonSerializer.Serialize<VolumeDataOperation>(operation);
            }
            catch (Exception)
            {

                throw;
            }
            return await Task.FromResult(result);
        }

        public async Task<string> VolumeMetaDataOperationAsync(string base64Operation)
        {
            string result = "OK";
            try
            {
                var rawOperationbytes = Convert.FromBase64String(base64Operation);
                VolumeMetaDataOperation operation = new VolumeMetaDataOperation(rawOperationbytes);
                switch (operation.OperationType)
                {
                    case VolumeMetaDataOperationType.FILE_CREATE:
                        {
                            var fcOperation = new FILECREATE_Operation(rawOperationbytes);
                            bool IsStream = fcOperation.FileName.Contains(":");
                            if (!IsStream)
                            {
                                CreateFileObject(GlobalPropertiesContext.IsSimulatedNode(), fcOperation);

                                #region Send the VDISK_FILE_CREATE global event with a payload of fcOperation
                                GlobalEvent globalEvent = null!;

                                if (GlobalPropertiesContext.IsSimulatedNode())
                                {
                                    globalEvent = new GlobalEvent(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber],
                                                                    GlobalEventType.VDISK_FILECREATE, operation.FileID, rawOperationbytes);
                                }
                                else
                                {
                                    globalEvent = new GlobalEvent(_localNodeContext!.NodeDIDRef, GlobalEventType.VDISK_FILECREATE, operation.FileID, rawOperationbytes);
                                }
                                await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
                                #endregion
                            }

                            //FILECREATE_Operation fcOperation = new FILECREATE_Operation(rawOperationbytes);
                            //#region For Directories we are done now that we have created the directory and sent the global event
                            //if ( fcOperation.IsDirectory)
                            //{
                            //    break;
                            //}
                            //#endregion 

                            //#region For File Creates we must wait for signal in On_VDISK_FILECREATE_Event that indicates the above event has been received by this node and the file has been created
                            //// NOTE:  We do not want to return from this operation unless we know the file was deleted on this Node - the Node that instigated the FILE_DELETE operation
                            //lock (_pendingFileCreateTable)
                            //{
                            //    _pendingFileCreateTable.Add(operation.FileID, false);
                            //}
                            //while (true)
                            //{
                            //    lock (_pendingFileCreateTable!)
                            //    {
                            //        if (_pendingFileCreateTable[operation.FileID])
                            //        {
                            //            _pendingFileCreateTable.Remove(operation.FileID); // Remove the FileID since the Create is no longer pending on this node
                            //            break;
                            //        }
                            //    }
                            //    await Task.Delay(5).ConfigureAwait(false);
                            //}
                            //#endregion
                        }
                        break;

                    case VolumeMetaDataOperationType.FILE_CLOSE:
                        {
                            Debug.Print($"!!!!!!! FILE (CREATE) CLOSE invoked...  ");
                            FILECLOSE_Operation fcOperation = new FILECLOSE_Operation(rawOperationbytes);
                            #region Send the VDISK_FILE_CLOSE global event with a payload of fcOperation
                            GlobalEvent globalEvent = null!;

                            if (GlobalPropertiesContext.IsSimulatedNode())
                            {
                                globalEvent = new GlobalEvent(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber],
                                                                GlobalEventType.VDISK_FILECLOSE, operation.FileID, rawOperationbytes);
                            }
                            else
                            {
                                globalEvent = new GlobalEvent(_localNodeContext!.NodeDIDRef, GlobalEventType.VDISK_FILECLOSE, operation.FileID, rawOperationbytes);
                            }
                            await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
                            #endregion

                        }
                        break;
                    
                    case VolumeMetaDataOperationType.FILE_SETATTRIBUTES:
                        {
                            Debug.Print($"!!!!!!! FILE SET ATTRIBUTES invoked...");
                            FILESETATTRIBUTES_Operation fsaOperation = new FILESETATTRIBUTES_Operation(rawOperationbytes);

                            uint nodeNumber = 0;
                            var thisNodeID = default(Guid);
                            bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                            if (isSimulatedNode)
                            {
                                nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                                thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                            }
                            else
                            {
                                using (var crc32 = new HostCrc32())
                                {
                                    nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                                }
                                thisNodeID = _localNodeContext!.NodeDIDRef;
                            }

                            string vDiskRootFolderName = null!;
                            string vDiskRootDataFolderName = null!;
                            
                            if (isSimulatedNode)
                            {
                                vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{fsaOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{fsaOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            else
                            {
                                vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{fsaOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{fsaOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            try
                            {
                                //var absoluteFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName), fsaOperation.FileName.Substring(1));
                                var absoluteFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        fsaOperation.FileName.Substring(1));
                                var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                                    vDiskRootDataFolderName),
                                                                        fsaOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");
                                #region Open the file (not directory) requested and update the attributes meta data
                                
                                FILECLOSE_Operation? newMetaData = new FILECLOSE_Operation(fsaOperation.VDiskID, fsaOperation.VolumeID, fsaOperation.FileName, fsaOperation.FileID, fsaOperation.IsDirectory);
                                newMetaData.CreateTime = fsaOperation.CreateTime;
                                newMetaData.LastAccessTime = fsaOperation.LastAccessTime;
                                newMetaData.LastWriteTime = fsaOperation.LastWriteTime;
                                newMetaData.Attributes = fsaOperation.Attributes;
                                newMetaData.EventOrigin = fsaOperation.EventOrigin;
                                
                                if (fsaOperation.IsDirectory)
                                {
                                    using (var fs = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                                    {
                                        FILECLOSE_Operation oldMetaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs)!;
                                        newMetaData.FileLength = oldMetaData!.FileLength;
                                        fs.Seek(0, SeekOrigin.Begin);
                                        // %TODO% this should be decrypted
                                        JsonSerializer.Serialize(fs, newMetaData);
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                else 
                                { 
                                    using (var fs = new FileStream(absoluteFileName, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                                    {
                                        FILECLOSE_Operation oldMetaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs)!;
                                        newMetaData.FileLength = oldMetaData!.FileLength;
                                        if (newMetaData.FileLength == 0)
                                        {
                                            Debug.Print($"###################################################################################");
                                            Debug.Print($"FILE SET ATTRIBUTES on Node #{nodeNumber} is writing a 0 len file for file {fsaOperation.FileName}");
                                            Debug.Print($"###################################################################################");
                                        }
                                        fs.Seek(0, SeekOrigin.Begin);   
                                        // %TODO% this should be decrypted
                                        JsonSerializer.Serialize(fs, newMetaData);
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event(ERROR 1) {ex}");
                            }
                        }
                        break;

                    case VolumeMetaDataOperationType.FILE_GETATTRIBUTES:
                        {
                            FILEGETATTRIBUTES_Operation fgaOperation = new FILEGETATTRIBUTES_Operation(rawOperationbytes);
                            Debug.Print($"!!!!!!! FILE GET ATTRIBUTES invoked... fileName={fgaOperation.FileName}");

                            uint nodeNumber = 0;
                            var thisNodeID = default(Guid);
                            bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                            if (isSimulatedNode)
                            {
                                nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                                thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                            }
                            else
                            {
                                using (var crc32 = new HostCrc32())
                                {
                                    nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                                }
                                thisNodeID = _localNodeContext!.NodeDIDRef;
                            }

                            string vDiskRootFolderName = null!;
                            string vDiskRootDataFolderName = null!;
                            FILECLOSE_Operation? metaData = null!;
                            if (isSimulatedNode)
                            {
                                vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{fgaOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{fgaOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            else
                            {
                                vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{fgaOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{fgaOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            try
                            {
                                #region Open the file object requested
                                //var absoluteFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName), fgaOperation.FileName.Substring(1));
                                var absoluteFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        fgaOperation.FileName.Substring(1));
                                var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                                    vDiskRootDataFolderName),
                                                                        fgaOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");
                                if (Directory.Exists(absoluteFileName))    // NOTE:  fgaOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                                {
                                    using (var fs = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                                    {
                                        // %TODO% this should be decrypted
                                        metaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                else if( File.Exists(absoluteFileName))    // NOTE:  fgaOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                                {
                                    using (var fs = new FileStream(absoluteFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                                    {
                                        // %TODO% this should be decrypted
                                        metaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                                        //if (metaData.FileLength == 0)
                                        //{
                                        //    Debug.Print($"###################################################################################");
                                        //    Debug.Print($"FILE GET ATTRIBUTES on Node #{nodeNumber} is reading a 0 len file for file {fgaOperation.FileName}");
                                        //    Debug.Print($"###################################################################################");
                                        //}
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event(ERROR 1) {ex}");
                            }

                            
                            result = JsonSerializer.Serialize(metaData);

                        }
                        break;

                    case VolumeMetaDataOperationType.FILE_DELETE:
                        {

                            DeleteFileObject(new FILEDELETE_Operation(rawOperationbytes), GlobalPropertiesContext.IsSimulatedNode());

                            #region Send the VDISK_FILE_DELETE global event with a payload of fdOperation
                            GlobalEvent globalEvent = null!;

                            if (GlobalPropertiesContext.IsSimulatedNode())
                            {
                                globalEvent = new GlobalEvent(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber],
                                                                GlobalEventType.VDISK_FILEDELETE, operation.FileID, rawOperationbytes);
                            }
                            else
                            {
                                globalEvent = new GlobalEvent(_localNodeContext!.NodeDIDRef, GlobalEventType.VDISK_FILEDELETE, operation.FileID, rawOperationbytes);
                            }
                            await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
                            #endregion

                            //#region Wait for signal in On_VDISK_FILEDELETE_Event that indicates the above event has been received by this node and the file has been deleted
                            //// NOTE:  We do not want to return from this operation unless we know the file was deleted on this Node - the Node that instigated the FILE_DELETE operation
                            //lock (_pendingFileDeleteTable)
                            //{
                            //    _pendingFileDeleteTable.Add(operation.FileID, false);
                            //}
                            //while (true)
                            //{
                            //    lock (_pendingFileDeleteTable!)
                            //    {
                            //        if (_pendingFileDeleteTable[operation.FileID])
                            //        {
                            //            _pendingFileDeleteTable.Remove(operation.FileID); // Remove the FileID since the delete is no longer pending on this node
                            //            break;
                            //        }
                            //    }
                            //    await Task.Delay(5).ConfigureAwait(false);
                            //}
                            //#endregion
                        }
                        break;

                    case VolumeMetaDataOperationType.FILE_RENAMEORMOVE:
                        {
                            RenameOrMoveFileObject(new FILERENAMEORMOVE_Operation(rawOperationbytes), GlobalPropertiesContext.IsSimulatedNode());

                            #region Send the VDISK_FILE_DELETE global event with a payload of fdOperation
                            GlobalEvent globalEvent = null!;

                            if (GlobalPropertiesContext.IsSimulatedNode())
                            {
                                globalEvent = new GlobalEvent(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber],
                                                                GlobalEventType.VDISK_FILERENAMEORMOVE, operation.FileID, rawOperationbytes);
                            }
                            else
                            {
                                globalEvent = new GlobalEvent(_localNodeContext!.NodeDIDRef, GlobalEventType.VDISK_FILERENAMEORMOVE, operation.FileID, rawOperationbytes);
                            }
                            await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
                            #endregion

                            //#region Wait for signal in On_VDISK_FILERENAMEORMOVE_Event that indicates the above event has been received by this node and the file has been renamed or moved
                            //// NOTE:  We do not want to return from this operation unless we know the file was renamed or moved on this Node - the Node that instigated the FILE_RENAMEORMOVE operation
                            //lock (_pendingFileRenameOrMoveTable)
                            //{
                            //    _pendingFileRenameOrMoveTable.Add(operation.FileID, false);
                            //}
                            //while (true)
                            //{
                            //    lock (_pendingFileRenameOrMoveTable!)
                            //    {
                            //        if (_pendingFileRenameOrMoveTable[operation.FileID])
                            //        {
                            //            _pendingFileRenameOrMoveTable.Remove(operation.FileID); // Remove the FileID since the rename or move is no longer pending on this node
                            //            break;
                            //        }
                            //    }
                            //    await Task.Delay(5).ConfigureAwait(false);
                            //}

                            //#endregion
                        }
                        break;

                    case VolumeMetaDataOperationType.FILE_OPEN:
                        {
                            FILEGETATTRIBUTES_Operation foOperation = new FILEGETATTRIBUTES_Operation(rawOperationbytes);
                            Debug.Print($"!!!!!!! FILE OPEN invoked... fileName={foOperation.FileName}");

                            uint nodeNumber = 0;
                            var thisNodeID = default(Guid);
                            bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                            if (isSimulatedNode)
                            {
                                nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                                thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                            }
                            else
                            {
                                using (var crc32 = new HostCrc32())
                                {
                                    nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                                }
                                thisNodeID = _localNodeContext!.NodeDIDRef;
                            }

                            string vDiskRootFolderName = null!;
                            string vDiskRootDataFolderName = null!;
                            FILECLOSE_Operation? metaData = null!;
                            if (isSimulatedNode)
                            {
                                vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{foOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{foOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            else
                            {
                                vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{foOperation.VDiskID.ToString("N").ToUpper()}";
                                vDiskRootDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{foOperation.VDiskID.ToString("N").ToUpper()}";
                            }
                            try
                            {
                                #region Open the file (not directory) requested
                                //var absoluteFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName), foOperation.FileName.Substring(1));
                                var absoluteFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        foOperation.FileName.Substring(1));
                                var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                                    vDiskRootDataFolderName),
                                                                        foOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");
                                if ( foOperation.IsDirectory)
                                {
                                    using (var fs = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                                    {
                                        // %TODO% this should be decrypted
                                        metaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                else 
                                {
                                    using (var fs = new FileStream(absoluteFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                                    {
                                        // %TODO% this should be decrypted
                                        metaData = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                                        fs.Flush();
                                        fs.Close();
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event(ERROR 1) {ex}");
                            }


                            result = JsonSerializer.Serialize(metaData);

                        }
                        break;

                }
            }
            catch (Exception ex)
            {
                Debug.Print($"VirtualDiskManager.VolumeMetaDataOperationAsync(ERROR) {ex}");
                throw;
            }
            return result;
        }
        #endregion

        #region Helpers
        #region Global Event Handlers
        private void On_VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION_Event(Guid senderID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            var runInBackground = EntryPoint.FlashNode( EntryPoint.FlashConsoleColorEvent);
            if( ! EntryPoint.AnimateNodesOnScreen)
            {
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"PROPOSE from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(senderID)}", EntryPoint.FlashConsoleColorEvent);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"PROPOSE from Node:{senderID}", EntryPoint.FlashConsoleColorEvent);
                }
            }
            Guid vDiskID = globalEvent.CorrelationID;
            
            #region Parse payload variables
            #region  1st Version of VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION event
            int pos = 0;
            byte eventPayloadVersion = globalEvent.Payload![pos++];  // 0 is 1st version
            int clusterSize = 0;
            int replicationFactor = 0;
            Guid[] clusterIds = null!;
            clusterSize = BitConverter.ToInt32(globalEvent.Payload!, pos);
            pos += sizeof(int);
            replicationFactor = BitConverter.ToInt32(globalEvent.Payload!, pos);
            pos += sizeof(int);
            clusterIds = new Guid[clusterSize];
            byte[] clusterIdBytes = new byte[16];
            for (int i = 0; i < clusterSize; i++)
            {
                Buffer.BlockCopy(globalEvent.Payload!, pos, clusterIdBytes, 0, 16);
                clusterIds[i] = new Guid(clusterIdBytes);
                pos += 16;
            }
            #endregion 
            #endregion

            #region Add the provisionVDiskID request to the pending provisioning VDisk table
            lock (_virtualDisks!)
            {
                _virtualDisks!.Add(vDiskID, new VirtualDisk(vDiskID, clusterIds, replicationFactor ) );  // NOTE:  Triggers Step #3 wait loop in ProvisionVirtualDisk() if that routine instigated the creation of the VDisk
                RewriteVDisksFile();  // Persist above change to _virtualDisks 

                #region Create "root" VDisk mount folder on Local Store for this Node
                string vDiskRootFolderName = null!;
                string vDiskRootDataFolderPath = null!;
                if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0)
                {
                    vDiskRootFolderName =   $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{vDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderPath = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{vDiskID.ToString("N").ToUpper()}";
                }
                else
                {
                    vDiskRootFolderName =  $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{vDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderPath = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{vDiskID.ToString("N").ToUpper()}";
                }
                Directory.CreateDirectory(Path.Combine(Path.Combine(_globalPropertiesContext.NodeDirectoryPath, _nodeClusterContext!.LocalStoreDirectoryName), vDiskRootFolderName));
                Directory.CreateDirectory(Path.Combine(Path.Combine(_globalPropertiesContext.NodeDirectoryPath, _nodeClusterContext!.LocalStoreDirectoryName), vDiskRootDataFolderPath));
                #endregion 

                #region Confirm whether this node can commit to participation in the VDisk storage being solicited
                if (CanParticipateInVDiskStorage())
                {
                    SendParticipateInVDiskStoargeAffirmativeResponse(vDiskID, globalEvent, clusterSize, replicationFactor).Wait();
                }
                #endregion
            }
            #endregion


            //if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext?.SimulationNodeNumber > 0)
            //{
            //    Debug.Print($"VDisk Event {vDiskID} successfully Delivered to Node# {_globalPropertiesContext.SimulationNodeNumber}:  ({clusterSize}x{replicationFactor}) -> {sequence}) {timeStampSeconds}.{timeStampNanos} -> SenderDIDRef = {globalEvent.SenderDIDRef}, EventType={globalEvent.EventType}, CorrelationID={globalEvent.CorrelationID} ");
            //}
            //else
            //{
            //    Debug.Print($"VDisk Event {vDiskID}  successfully Delivered to Node: {_localNodeContext!.NodeDIDRef}: ({clusterSize}x{replicationFactor}) -> {sequence}) {timeStampSeconds}.{timeStampNanos} -> SenderDIDRef = {globalEvent.SenderDIDRef}, EventType={globalEvent.EventType}, CorrelationID={globalEvent.CorrelationID} ");
            //}

        }


        private void On_VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION_Event(Guid nodeID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            try
            {
                var runInBackground = EntryPoint.FlashNode( EntryPoint.FlashConsoleColorEvent);
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"ACCEPT from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(nodeID)}", EntryPoint.FlashConsoleColorEvent);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ",$"ACCEPT from Node:{nodeID}", EntryPoint.FlashConsoleColorEvent);
                }


                Guid vDiskID = globalEvent.CorrelationID;
                #region Decode V1 payload arguements
                int pos = 0;
                byte version = globalEvent.Payload![pos++];  // Version
                int clusterSize = BitConverter.ToInt32(globalEvent.Payload!, pos);
                pos += sizeof(int);
                int replicationFactor = BitConverter.ToInt32(globalEvent.Payload!, pos);
                pos += sizeof(int);
                int ipAddressAsInt = BitConverter.ToInt32(globalEvent.Payload!, pos);
                string ipAddress = "";
                byte[] ipAddressBytes = BitConverter.GetBytes(ipAddressAsInt);
                for (int b = 0; b < 4; b++)
                {
                    ipAddress += ipAddressBytes[b].ToString() + (b < 3 ? "." : "");
                }
                pos += sizeof(int);
                int port = BitConverter.ToInt32(globalEvent.Payload!, pos);
                pos += sizeof(int);
                byte[] nodePublicKeyBytes = new byte[globalEvent.Payload!.Length - (1 + (4 * sizeof(int)))];
                Buffer.BlockCopy(globalEvent.Payload!, pos, nodePublicKeyBytes, 0, nodePublicKeyBytes.Length);
                pos += nodePublicKeyBytes.Length;
                #endregion

                #region Add the node to the provisionVDiskID to the pending provisioning VDisk table
                // NOTE:  Due to the fact that this event callback is always called serially, we do not have to worry about parralel calls taking place FOR THE SAME vDiskID,
                //        however it is possible that multiple simultaneous callbacks can take place servicing different vDiskIDs, therefore each thread takes a lock on _pendingProvisioningVirtualDisks
                //        while potentially updating the structure
                lock (_virtualDisks!)
                {
                    if (_virtualDisks.ContainsKey(vDiskID))  // We are only interested in VDisk events that we receive AFTER we received an associated VDISK_PROPOSE_STORAGE_REPLICATION_PARTICIPATION event 
                    {
                        IVirtualDisk vDiskDef = _virtualDisks[vDiskID];
                        if (!vDiskDef.IsFullyAllocated())
                        {
                            Node participantNode = new Node(nodeID, Encoding.UTF8.GetString(nodePublicKeyBytes), ipAddress, port);

                            #region Populate first available slot in vDiskDef CxR matrix of nodes, traversing across the replicas of each cluster
                            for (int c = 0; c < vDiskDef.ClusterSize; c++)
                            {
                                if (vDiskDef.ClusterList[c].Size < replicationFactor)
                                {
                                    vDiskDef.ClusterList[c].Nodes.Add(participantNode);

                                    #region Check if Animiating Node on screen                                    
                                    using (HostCrc32 _crc32 = new HostCrc32())
                                    {
                                        var thisNodeId = BitConverter.ToUInt32(_crc32.ComputeHash(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToByteArray()), 0);
                                        var allocatedNodeId = BitConverter.ToUInt32(_crc32.ComputeHash(participantNode.ID.ToByteArray()), 0);
                                        if (thisNodeId == allocatedNodeId)
                                        {
                                            // 'This' node has just been allocated to the cluster so Animate it on screen
                                            //Debug.Print($"VirtualDiskManager -> Adding node {GlobalPropertiesContext.ComputeNodeNumberFromPort(port)} to cluster {c + 1} in position {vDiskDef.ClusterList[c].Nodes.Count}");
                                            runInBackground.GetAwaiter().GetResult();
                                            EntryPoint.CheckForScreenAnimation(c + 1, vDiskDef.ClusterList[c].Nodes.Count, port, EntryPoint.FlashConsoleColorEvent);
                                        }
                                    }
                                    #endregion
                                    break;
                                }
                            }
                            #endregion

                            RewriteVDisksFile();  // Persist above change to _virtualDisks 

                            if (vDiskDef.IsFullyAllocated())
                            {
                                // If we make it here its the first time this node has fully allocated this vDiskDef, so we want to make the vDisk "offical"
                                // by moving the vDisk to the official _virtualDisks dictionary and persist it to disk
                                #region Connect up the new virutal disk
                                if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext?.SimulationNodeNumber > 0)
                                {
                                    #region Display vDiskDef to Console
                                    if (!EntryPoint.AnimateNodesOnScreen)
                                    {
                                        foreach (var cnc in vDiskDef.ClusterList)
                                        {
                                            var vdiskdefDisplay = GlobalPropertiesContext.GenClusterListAsString(cnc);
                                            _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"", $"{vdiskdefDisplay}", ConsoleColor.Yellow);
                                        }
                                    }
                                    #endregion 

                                    ConnectIfNodeParticipatesInCluster(vDiskDef);
                                    //Debug.Print($"*********** VDisk {vDiskDef} Dump -  Node{nodeID}:{_globalPropertiesContext.SimulationNodeNumber} ******************");
                                    //for (int c = 0; c < vDiskDef.ClusterList.Count; c++)
                                    //{
                                    //    Debug.Print($"Cluster {c + 1} of {vDiskDef.ClusterList.Count}:  ID={vDiskDef.ClusterList[c].ID}  (Size={vDiskDef.ClusterList[c].Size})----------------------------");
                                    //    for (int r = 0; r < vDiskDef.ClusterList[c].Nodes.Count; r++)
                                    //    {
                                    //        Debug.Print($"\t\tReplica: {r + 1}: NodeId:{vDiskDef.ClusterList[c].Nodes[r].ID}, IpAddress={vDiskDef.ClusterList[c].Nodes[r].IPv4Address}, Port={vDiskDef.ClusterList[c].Nodes[r].Port}, key={vDiskDef.ClusterList[c].Nodes[r].PublicKey}");
                                    //    }
                                    //}
                                    //Debug.Print($"VDisk Event {vDiskID} type {globalEvent.EventType} from Node:{nodeID} successfully Delivered to Node# {_globalPropertiesContext.SimulationNodeNumber}:  ({clusterSize}x{replicationFactor}) ");
                                }
                                else
                                {
                                    //Debug.Print($"VDisk Event {vDiskID} type {globalEvent.EventType} from Node:{nodeID} successfully Delivered to Node: {_localNodeContext!.NodeDIDRef}: ({clusterSize}x{replicationFactor}) ");
                                }
                                #endregion 
                            }
                        }
                    }
                    else
                    {
                        // In rare cases its possible that the vDisk doesn't exist in the _pendingProvisioningVirtualDisks table,
                        // in which case we simply ignore the VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION since the VDisk has since
                        // been Deleted or was never properly formed.
                        // 
                        // NOP
                    }
                    //Debug.Print($"*** VirtualDiskManager.On_VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION_Event(... LEAVE)");
                }
                #endregion 
            }
            catch (Exception ex)
            {
                Debug.Print($"*** VirtualDiskManager.On_VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION_Event(ERROR) {ex}");
            }
            #endregion



        }


        private void On_VDISK_FILECREATE_Event(Guid nodeID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            try
            {
                uint nodeNumber = 0;
                var thisNodeID = default(Guid);
                bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                if (isSimulatedNode)
                {
                    nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                    thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                }
                else
                {
                    using (var crc32 = new HostCrc32())
                    {
                        nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                    }
                    thisNodeID = _localNodeContext!.NodeDIDRef;
                }

                FILECREATE_Operation fcOperation = new FILECREATE_Operation(globalEvent.Payload!);
                var runInBackground = EntryPoint.FlashNode(EntryPoint.FlashConsoleColorEvent);
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILECREATE from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(nodeID)} for file {fcOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILECREATE from Node:{nodeID} for file {fcOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }
                bool IsStream = fcOperation.FileName.Contains(":");
                if (!IsStream)
                {
                    if (nodeID != thisNodeID)
                    {
                        // We only Create the file object if this node DID NOT instigated (i.e.; received the API call), 
                        // since it would have already been created in the VolumeMetaDataOperationType.FILE_CREATE case
                        // of the VolumeMetaDataOperationAsync() method above, on the instigating node
                        CreateFileObject(isSimulatedNode, fcOperation);
                    }

                }
                //else
                //{
                //    Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event - ignore FILECREATE operation for *Stream*={fcOperation.FileName}");
                //}

                //if (!fcOperation.IsDirectory)
                //{
                //    // If we make it here we need to record the details of the file being created so that we can maintain its values in memory until it is closed
                //    lock (_pendingFileCreateTable)
                //    {
                //        if (_pendingFileCreateTable.ContainsKey(fcOperation.FileID))  // This is only true on the Node that instigated the FILE_CREATE - otherwise ignore
                //        {

                //            #region Trigger the wait loop in VolumeMetaDataOperationAsync() for the VolumeMetaDataOperationType.FILE_CREATE operaton if that routine instigated the operation
                //            _pendingFileCreateTable[fcOperation.FileID] = true;
                //            #endregion
                //        }
                //    }
                //}
            }
            catch (Exception ex)
            {
                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event(ERROR 2) {ex}");
            }
        }

        private void On_VDISK_FILECLOSE_Event(Guid nodeID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            try
            {
                uint nodeNumber = 0;
                var thisNodeID = default(Guid);
                bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                if (isSimulatedNode)
                {
                    nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                    thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                }
                else
                {
                    using (var crc32 = new HostCrc32())
                    {
                        nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                    }
                    thisNodeID = _localNodeContext!.NodeDIDRef;
                }

                FILECLOSE_Operation fcOperation = new FILECLOSE_Operation(globalEvent.Payload!);
                var runInBackground = EntryPoint.FlashNode(EntryPoint.FlashConsoleColorEvent);
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILECLOSE from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(nodeID)} for file {fcOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILECLOSE from Node:{nodeID} for file {fcOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }
                if (nodeID != thisNodeID)
                {
                    // We only Close the file object if this node DID NOT instigated (i.e.; received the API call), 
                    // since it would have already been created in the VolumeMetaDataOperationType.FILE_CLOSE case
                    // of the VolumeMetaDataOperationAsync() method above, on the instigating node
                    CloseFileObject(isSimulatedNode, fcOperation);
                }
                
            }
            catch (Exception ex)
            {
                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILECREATE_Event(ERROR 2) {ex}");
            }
        }


        private void On_VDISK_FILEDELETE_Event(Guid nodeID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            try
            {
                FILEDELETE_Operation fdOperation = new FILEDELETE_Operation(globalEvent.Payload!);
                var runInBackground = EntryPoint.FlashNode(EntryPoint.FlashConsoleColorEvent);
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILEDELETE from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(nodeID)} for file {fdOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILEDELETE from Node:{nodeID} for file {fdOperation.FileName}", EntryPoint.FlashConsoleColorAPI);
                }


                #region Determine Absolute local name of the file or directory to be deleted
                uint nodeNumber = 0;
                var thisNodeID = default(Guid);
                bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                if (isSimulatedNode)
                {
                    nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                    thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                }
                else
                {
                    using (var crc32 = new HostCrc32())
                    {
                        nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                    }
                    thisNodeID = _localNodeContext!.NodeDIDRef;
                }

                if (nodeID != thisNodeID )
                {
                    // We only Delete the file object if this node DID NOT instigated (i.e.; received the API call), 
                    // since it would have already been deleted in the VolumeMetaDataOperationType.FILE_DELETE case
                    // of the VolumeMetaDataOperationAsync() method above, on the instigating node
                    DeleteFileObject(fdOperation, isSimulatedNode);
                }
                #endregion

                //lock (_pendingFileDeleteTable)
                //{
                //    if (_pendingFileDeleteTable.ContainsKey(fdOperation.FileID))  // This is only true on the Node that instigated the FILE_DELETE - otherwise ignore
                //    {

                //        #region Trigger the wait loop in VolumeMetaDataOperationAsync() for the VolumeMetaDataOperationType.FILE_DELETE operaton if that routine instigated the operation
                //        _pendingFileDeleteTable[fdOperation.FileID] = true;
                //        #endregion
                //    }
                //}

            }
            catch (Exception ex)
            {
                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILEDELETE_Event(ERROR 2) {ex}");
                //throw;
            }
        }


        private void On_VDISK_FILERENAMEORMOVE_Event(Guid nodeID, IGlobalEvent globalEvent, ulong sequence, long timeStampSeconds, int timeStampNanos)
        {
            try
            {
                FILERENAMEORMOVE_Operation frmOperation = new FILERENAMEORMOVE_Operation(globalEvent.Payload!);
                var runInBackground = EntryPoint.FlashNode(EntryPoint.FlashConsoleColorEvent);
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDRIVE_FILERENAMEORMOVE from Node #{GlobalPropertiesContext.ComputeNodeNumberFromSimulationNodeID(nodeID)} for old file: {frmOperation.OldFileName}, new file: {frmOperation.NewFileName}", EntryPoint.FlashConsoleColorAPI);
                }
                else
                {
                    _globalPropertiesContext!.WriteToConsole!.DynamicInvoke($"Received ", $"VDISK_FILERENAMEORMOVE from Node:{nodeID} for old file: {frmOperation.OldFileName}, new file: {frmOperation.NewFileName}", EntryPoint.FlashConsoleColorAPI);
                }


                #region Determine Absolute local name of the file or directory to be deleted
                uint nodeNumber = 0;
                var thisNodeID = default(Guid);
                bool isSimulatedNode = GlobalPropertiesContext.IsSimulatedNode();
                if (isSimulatedNode)
                {
                    nodeNumber = uint.Parse(GlobalPropertiesContext.ThisNodeNumber());
                    thisNodeID = _globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                }
                else
                {
                    using (var crc32 = new HostCrc32())
                    {
                        nodeNumber = BitConverter.ToUInt32(crc32.ComputeHash(_localNodeContext!.NodeID.ToByteArray()), 0);
                    }
                    thisNodeID = _localNodeContext!.NodeDIDRef;
                }
                if (nodeID != thisNodeID)
                {
                    // We only Rename or Move the file object if this node DID NOT instigated (i.e.; received the API call), 
                    // since it would have already been renamed or moved in the VolumeMetaDataOperationType.FILE_RENAMEORMOVE case
                    // of the VolumeMetaDataOperationAsync() method above, on the instigating node
                    RenameOrMoveFileObject(frmOperation, isSimulatedNode);
                }
                //lock (_pendingFileRenameOrMoveTable)
                //{
                //    if (_pendingFileRenameOrMoveTable.ContainsKey(frmOperation.FileID))  // This is only true on the Node that instigated the FILE_RENAMEORMOVE - otherwise ignore
                //    {

                //        #region Trigger the wait loop in VolumeMetaDataOperationAsync() for the VolumeMetaDataOperationType.FILE_RENAMEORMOVE operaton if that routine instigated the operation
                //        _pendingFileRenameOrMoveTable[frmOperation.FileID] = true;
                //        #endregion 
                //    }
                //}
            }
            catch (Exception ex)
            {
                Debug.Print($"*** VirtualDiskManager.On_VDISK_FILERENAMEORMOVE_Event(ERROR 2) {ex}");
                //throw;
            }
        }



        
        private async void ConnectIfNodeParticipatesInCluster(IVirtualDisk vDiskDef, bool shouldAnimate = false)
        {
            // This routine loops through every node of every cluster to determine if 'this' node is in a cluster,
            // and if so it creates a peer set from the cluster and connects it.  If 'this' node isn't in any of the clusters
            // there is nothing to connect up
            using (HostCrc32 _crc32 = new HostCrc32())
            {
                var thisNodeID = BitConverter.ToUInt32(_crc32.ComputeHash(_globalPropertiesContext!.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToByteArray()), 0);
                var computer = _localNodeContext!.WorldComputerNodeContext.GetComputer(0);
                var thisNodeCluster = 0;
                var thisNodeReplica = 0;
                var thisNodeName = _globalPropertiesContext.SimulationNodeNumber.ToString();
                var setDomain = computer.GetFatFileSystemSetDomain(0);
                bool isNodeInCluster = false;
                for (int c = 0; c < vDiskDef.ClusterList.Count; c++)
                {
                    List<Node> nodeset = vDiskDef.ClusterList[c].Nodes;
                    for (int r = 0; r < nodeset.Count; r++)
                    {
                        ulong connectionId = BitConverter.ToUInt32(_crc32.ComputeHash(nodeset[r].ID.ToByteArray()), 0);

                        if (connectionId == thisNodeID)
                        {
                            thisNodeCluster = c;
                            thisNodeReplica = r;
                            if( shouldAnimate )
                            {
                                #region Check if Animiating Node on screen                                    
                                // 'This' node has just been allocated to the cluster so Animate it on screen
                                //Debug.Print($"VirtualDiskManager -> Adding node {GlobalPropertiesContext.ComputeNodeNumberFromPort(nodeset[r].Port)} to cluster {c + 1} in position {nodeset.Count}");
                                EntryPoint.CheckForScreenAnimation(c + 1, r+1, nodeset[r].Port, ConsoleColor.White);
                                #endregion
                            }
                            //thisNodeName = $"({(c + 1).ToString("X8")},{(r + 1).ToString("X8")})";
                            isNodeInCluster = true;  // we have found the node in the cluster so we can break out of the double loop and proceed below to create a PeerSet from the nodes in the cluster
                            break;
                        }
                        if( isNodeInCluster )
                        {
                            break;
                        }
                    }
                }
                if ( isNodeInCluster )
                {
                    ConnectionMetaData[,] storageGrid = new ConnectionMetaData[1, vDiskDef.ReplicationFactor];
                    List<Node> nodeset = vDiskDef.ClusterList[thisNodeCluster].Nodes;
                    for (int r = 0; r < vDiskDef.ClusterList[thisNodeCluster].Nodes.Count; r++)
                    {
                        ulong connectionId = BitConverter.ToUInt32(_crc32.ComputeHash(nodeset[r].ID.ToByteArray()), 0);
                        //storageGrid[0, r] = new ConnectionMetaData(connectionId, nodeset[r].PublicKey, nodeset[r].IPv4Address, nodeset[r].Port,
                        //                          string.Format("({0},{1})", (thisNodeCluster + 1).ToString("X8"), (r + 1).ToString("X8")));
                        storageGrid[0, r] = new ConnectionMetaData(connectionId, nodeset[r].PublicKey, nodeset[r].IPv4Address, nodeset[r].Port, GlobalPropertiesContext.ComputeNodeNumberFromPort(nodeset[r].Port).ToString());
                    }
                    
                    #region Create a PeerSet and associate it with the cluster
                    vDiskDef.AddClusterPeerSet(StorageArrayPeerSet.Create(
                                    _globalPropertiesContext,
                                    _timeManager!,
                                    _networkManager!.ConnectionPool!,
                                    _cacheManager!,
                                    BitConverter.ToUInt32(_crc32.ComputeHash(vDiskDef.ClusterList[thisNodeCluster].ID.ToByteArray()), 0),
                                    storageGrid,
                                    1,
                                    vDiskDef.ReplicationFactor,
                                    thisNodeCluster,
                                    thisNodeReplica,
                                    false,
                                    _networkManager));
                    #endregion
                    
                    #region Connect all the nodes in the cluster
                    vDiskDef.Connect(_networkManager!.CancellationToken);
                    #endregion

                    #region Wait until cluster is reliably connected
                    while(vDiskDef.PeerSetList![vDiskDef.PeerSetList.Count - 1].ConnectionReliability <= ConnectionReliability.UnReliablyClusterConnected)
                    {
                        await Task.Delay(5).ConfigureAwait(false);
                    }
                    #endregion 
                    //Debug.Print($"===> Node# {thisNodeName}: Cluster[{thisNodeCluster+1}]={GlobalPropertiesContext.GenClusterListAsString(vDiskDef.ClusterList[thisNodeCluster])} status={vDiskDef.PeerSetList![vDiskDef.PeerSetList.Count - 1].ConnectionReliability}");
                }
                else
                {
                    //Debug.Print($"===> Node# {thisNodeName}: *** NOT IN CLUSTER ***");
                }

            }
        }


        private bool CanParticipateInVDiskStorage()
        {
            return true;  // %TODO% - to be filled in later
        }

        private async Task SendParticipateInVDiskStoargeAffirmativeResponse( Guid vDiskID, IGlobalEvent sourceEvent, int clusterSize, int replicationFactor )
        {
            GlobalEvent globalEvent = null!;

            byte[] nodePublicKey = Encoding.UTF8.GetBytes(_localNodeContext!.Node2048AsymmetricPublicKey);
            byte[] payload = new byte[1+(4* sizeof(int)) + nodePublicKey.Length]; // i.e.; Version + ClusterSize + ReplicaFactor + IPAddress + Port + PublicKey
            Guid senderID = default(Guid);
            int ipAddress = 0;
            int port = 0;
            if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0)
            {
                senderID = _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber];
                ipAddress = BitConverter.ToInt32(new byte[] { 127, 0, 0, 1 }, 0);  // All nodes of the Simulation are assumed to be running on the same local machine as 'this' node
                port = _globalPropertiesContext.DefaultLocalListeningPort + 1;
            }
            else
            {
                senderID = _localNodeContext!.NodeDIDRef;
                ipAddress = BitConverter.ToInt32(new byte[] { 0, 0, 0, 0 }, 0);  // %TODO%  Get actual IP Address of 'this' Node
                port = _globalPropertiesContext.DefaultLocalListeningPort + 1;
                //Buffer.BlockCopy(senderID.ToByteArray(), 0, payload, 0, 16);
            }

            #region Encode V1 Payload parameters for VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION event
            int pos = 0;
            payload[pos++] = 0;  // 0 is first version 
            Buffer.BlockCopy(BitConverter.GetBytes(clusterSize), 0, payload, pos, sizeof(int));
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(replicationFactor), 0, payload, pos, sizeof(int));
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(ipAddress), 0, payload, pos, sizeof(int));
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(port), 0, payload, pos, sizeof(int));
            pos += sizeof(int);
            Buffer.BlockCopy(nodePublicKey, 0, payload, pos, nodePublicKey.Length);
            pos += nodePublicKey.Length;
            #endregion 

            globalEvent = new GlobalEvent(senderID, GlobalEventType.VDISK_ACCEPT_STORAGE_REPLICATION_PARTICIPATION, vDiskID, payload);
            await _globalEventSubscriptionManager!.SubmitGlobalEventAsync(globalEvent).ConfigureAwait(false);
        }

        private void RewriteVDisksFile()
        {
            // *** IMPORTANT *** this routine assumed to be called while _virtualDisks is locked
            //
            // NOTE:    Need to be persisting "parital" unofficial vDiskDefs in the event this node shuts down while still building its offical vDiskDef.
            //          This partial vDiskDef would be loaded as like any other vDiskDef during start up and would continue to build its definition as it receives
            //          undelivered pending events, at which point it would transition to offical vDiskDef.  It could/would then start receiving pending FAT 
            //          events which it would "play out" to bring the node current with the state the of FAT in the rest of the network.  Note, the node would be too
            //          late to actually participate in the storage of the vDisk, however, but that wouldn't stop it from "replicate state machine" the FAT table operations.
            var json = JsonSerializer.Serialize<Dictionary<Guid, VirtualDisk>>(_virtualDisks!);
            byte[] virtualDisksJsonBytes = Encoding.UTF8.GetBytes(json);
            // Encrypt in-place  using Node keys
            HostCryptology.EncryptBufferInPlaceWith32ByteKey(virtualDisksJsonBytes, _nodeClusterContext!.NodeSymmetricKey);
            _virtualDisksFile!.Seek(0, SeekOrigin.Begin);
            _virtualDisksFile!.Write(virtualDisksJsonBytes, 0, virtualDisksJsonBytes.Length);
            _virtualDisksFile.Flush();
        }
        #endregion


        #region File Object Helpers
        private async void CloseFileObject( bool isSumlatedNode, FILECLOSE_Operation fcOperation )
        {
            await Task.Run(() =>
            {
                #region Write out FILECLOSE_Operation as the meta data for the file
                string vDiskRootFolderName = null!;
                string vDiskRootDataFolderName = null!;
                // If we make it here this node has received a FILECREATE operation that did not originate on it and so we must Create the file
                if (GlobalPropertiesContext.IsSimulatedNode())
                {
                    vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                }
                else
                {
                    vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                }
                #endregion

                try
                {
                    #region Open the file and write all metadata that is part of a FILECLOSE_Operation
                    //var absoluteFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName), fcOperation.FileName.Substring(1));
                    var absoluteFileName = Path.Combine(Path.Combine(
                                                        Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                        vDiskRootFolderName),
                                            fcOperation.FileName.Substring(1));
                    var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                        Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                        vDiskRootDataFolderName),
                                                            fcOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");
                    if (fcOperation.IsDirectory)
                    {
                        using (var fs = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Open, FileAccess.Write, FileShare.None))
                        {
                            // %TODO% this should be encrypted
                            JsonSerializer.Serialize(fs, fcOperation);
                            fs.Flush();
                            fs.Close();
                        }
                    }
                    else
                    {
                        Debug.Print($"FILECLOSE Metadata Write: {JsonSerializer.Serialize(fcOperation)}");
                        if (fcOperation.FileLength == 0)
                        {
                            Debug.Print($"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                        }
                        using (var fs = new FileStream(absoluteFileName, FileMode.Open, FileAccess.Write, FileShare.None))
                        {
                            // %TODO% this should be encrypted
                            JsonSerializer.Serialize(fs, fcOperation);
                            fs.Flush();
                            fs.Close();
                        }
                    }
                    #endregion
                }
                catch (Exception ex)
                {
                    Debug.Print($"*** VirtualDiskManager.CloseFileObject(ERROR 1) {ex}");
                }
            });
        }


        private async void CreateFileObject(bool isSimulatedNode, FILECREATE_Operation fcOperation)
        {
            await Task.Run(() =>
            {
                string vDiskRootFolderName = null!;
                string vDiskRootDataFolderName = null!;
                // If we make it here this node has received a FILECREATE operation that did not originate on it and so we must Create the file
                if (isSimulatedNode)
                {
                    vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                }
                else
                {
                    vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskRootDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{fcOperation.VDiskID.ToString("N").ToUpper()}";
                }
                try
                {
                    var absoluteFileName = Path.Combine(Path.Combine(
                                                                Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                vDiskRootFolderName),
                                                    fcOperation.FileName.Substring(1));
                    var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                        Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                        vDiskRootDataFolderName),
                                                            fcOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");
                    #region Check for and create Node's _Root_ and _Data_ local store sub folders, if they do not already exist

                    if (!Directory.Exists(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName)))
                    {
                        Directory.CreateDirectory(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName));
                    }
                    if (!Directory.Exists(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootDataFolderName)))
                    {
                        Directory.CreateDirectory(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootDataFolderName));
                    }
                    #endregion

                    #region Create the file or directory requested
                    if (fcOperation.IsDirectory)
                    {
                        Directory.CreateDirectory(absoluteFileName);
                        // Now write a metadata file for the directory 
                        using (var fcreate = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Create, FileAccess.Write, FileShare.None))
                        {
                            // %TODO% this should be encrypted
                            JsonSerializer.Serialize(fcreate, new FILECLOSE_Operation(fcOperation.VDiskID, fcOperation.VolumeID, fcOperation.FileName, fcOperation.FileID, fcOperation.IsDirectory));
                            fcreate.Flush();
                            fcreate.Close();
                        }
                    }
                    else
                    {
                        using (var fcreate = new FileStream(absoluteFileName, FileMode.Create, FileAccess.Write, FileShare.None))
                        {
                            // %TODO% this should be encrypted
                            JsonSerializer.Serialize(fcreate, new FILECLOSE_Operation(fcOperation.VDiskID, fcOperation.VolumeID, fcOperation.FileName, fcOperation.FileID, fcOperation.IsDirectory));
                            fcreate.Flush();
                            fcreate.Close();
                        }
                    }
                    #endregion
                }
                catch (Exception ex)
                {
                    Debug.Print($"*** VirtualDiskManager.CreateFileObject(ERROR 1) {ex}");
                }
            });
        }

        private async void DeleteFileObject(FILEDELETE_Operation fdOperation, bool isSimulatedNode)
        {
            await Task.Run(() =>
            {
                string vDiskRootFolderName = null!;
                string vDiskDataFolderName = null!;
                if (isSimulatedNode)
                {
                    vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{fdOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{fdOperation.VDiskID.ToString("N").ToUpper()}";
                }
                else
                {
                    vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{fdOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{fdOperation.VDiskID.ToString("N").ToUpper()}";
                }
                var absoluteFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        fdOperation.FileName.Substring(1));
                var absoluteDirectoryMetaDataFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskDataFolderName),
                                                        fdOperation.FileName.Replace("/", "_").Replace("\\", "_") + "0");

                #region Obtain the FileID from the actual file's metadata that is being deleted as the FileID in the operation passed in isn't accurate
                FILECLOSE_Operation? metadata = null!;
                FileSystemInfo info = null!;
                bool isDirectory = false;
                if (Directory.Exists(absoluteFileName))  // NOTE:  fdOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                {
                    isDirectory = true;
                    info = new DirectoryInfo(absoluteFileName);
                    using (var fs = new FileStream(absoluteDirectoryMetaDataFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        metadata = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                        fs.Flush();
                        fs.Close();
                    }
                }
                else if (File.Exists(absoluteFileName))  // NOTE:  fdOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                {
                    isDirectory = false;
                    info = new FileInfo(absoluteFileName);
                    using (var fs = new FileStream(absoluteFileName, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        metadata = JsonSerializer.Deserialize<FILECLOSE_Operation>(fs);
                        fs.Flush();
                        fs.Close();
                    }
                }
                #endregion

                #region Delete the File or Directory
                info.Delete();

                #region Delete any blocks for file (or the metadata block for a directory) stored on Node
                if (metadata != null && metadata.FileID != Guid.Empty)
                {
                    if (isDirectory)
                    {
                        // Delete the Directory's metadata file
                        File.Delete(absoluteDirectoryMetaDataFileName);
                    }
                    else
                    {
                        var blockFileNameSpec = Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskDataFolderName);
                        var files = Directory.GetFiles(blockFileNameSpec, metadata!.FileID.ToString("N").ToUpper() + "*");
                        foreach (var file in files)
                        {
                            File.Delete(file);
                        }
                    }
                }
                #endregion
                #endregion
            });
        }

        private async void RenameOrMoveFileObject(FILERENAMEORMOVE_Operation frmOperation, bool isSimulatedNode)
        {
            await Task.Run(() =>
            {
                string vDiskRootFolderName = null!;
                string vDiskDataFolderName = null!;
                if (isSimulatedNode)
                {
                    vDiskRootFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Root_{frmOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskDataFolderName = $"Node{GlobalPropertiesContext.ThisNodeNumber()}_Data_{frmOperation.VDiskID.ToString("N").ToUpper()}";
                }
                else
                {
                    vDiskRootFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Root_{frmOperation.VDiskID.ToString("N").ToUpper()}";
                    vDiskDataFolderName = $"{_localNodeContext!.NodeDIDRef.ToString("N").ToUpper()}_Data_{frmOperation.VDiskID.ToString("N").ToUpper()}";
                }
                //var absoluteOldFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName),
                //                    frmOperation.OldFileName.Substring(1));
                //var absoluteNewFileName = Path.Combine(Path.Combine(Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName), vDiskRootFolderName),
                //                    frmOperation.NewFileName.Substring(1));
                var absoluteOldFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        frmOperation.OldFileName.Substring(1));
                var absoluteDirectoryMetaDataOldFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskDataFolderName),
                                                        frmOperation.OldFileName.Replace("/", "_").Replace("\\", "_") + "0");
                var absoluteNewFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskRootFolderName),
                                                        frmOperation.NewFileName.Substring(1));
                var absoluteDirectoryMetaDataNewFileName = Path.Combine(Path.Combine(
                                                                    Path.Combine(_globalPropertiesContext!.NodeDirectoryPath, _localNodeContext!.LocalStoreDirectoryName),
                                                                    vDiskDataFolderName),
                                                        frmOperation.NewFileName.Replace("/", "_").Replace("\\", "_") + "0");

                #region Rename or Move the File or Directory
                if (Directory.Exists(absoluteOldFileName))  // NOTE:  frmOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                {
                    // Move the directory
                    DirectoryInfo dirinfo = new DirectoryInfo(absoluteOldFileName);
                    dirinfo.MoveTo(absoluteNewFileName);
                    // Need to also move the meta data file for the Directory
                    FileInfo finfo = new FileInfo(absoluteDirectoryMetaDataOldFileName);
                    FileInfo finfo1 = new FileInfo(absoluteDirectoryMetaDataNewFileName);
                    if (finfo1.Exists)
                    {
                        finfo1.Delete();
                    }
                    finfo.MoveTo(absoluteDirectoryMetaDataNewFileName);
                }
                else if (File.Exists(absoluteOldFileName))   // NOTE:  frmOperation.IsDirectory cannot be trusted to indicate if a file or a directory - must determine on our own 
                {
                    FileInfo finfo = new FileInfo(absoluteOldFileName);
                    FileInfo finfo1 = new FileInfo(absoluteNewFileName);
                    if (finfo1.Exists)
                    {
                        finfo1.Delete();
                    }
                    finfo.MoveTo(absoluteNewFileName);
                }
                #endregion
            });
        }
        #endregion 


        #endregion 
    }
}
