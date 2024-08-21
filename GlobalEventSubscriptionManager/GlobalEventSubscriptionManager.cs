namespace UnoSysKernel
{
    using Microsoft.Extensions.Logging;
    using System.Threading;
	using System.Threading.Tasks;
    using System.Text;
	using UnoSysCore;
    using System.IO;
    using System.Collections.Generic;
    using System.Text.Json;
    using System;
    using System.Diagnostics;
    using Hashgraph;

    internal sealed class GlobalEventSubscriptionManager : SecuredKernelService, IGlobalEventSubscriptionManager, ICriticalShutdown
    {
        #region Member Fields
        private ILocalNodeContext? localNodeContext = null!;
        private IHederaDTLContext? HederaDTLContext = null!;
        private bool done = false;
        private Stream? globalSubscriptionsFile = null!;
        private List<GlobalEventSubscription>? globalEventSubscriptions = null!;
        private IGlobalPropertiesContext? _globalPropertiesContext = null!;
        private Delegate? _writeToConsole = null!;
        private List<Task> backgroundSubscriptionListeners = null!;
        private Task? backgroundMonitorLogsProcess = null!;
        private CancellationTokenSource? _cancellationTokenSource = null!;
        private Dictionary<Guid, Dictionary<GlobalEventType, Dictionary<Guid, Action<Guid, IGlobalEvent, ulong, long, int>>>>? _eventTargetTable = null!;
        private bool _doneMonitoring = false;
        private IWorldComputerCryptologyContext? _worldComputerCryptologyContext = null!;
        private ITime? _timeManager = null!;
        private bool _hasShutDown = false;
        #endregion

        #region Constructors
        public GlobalEventSubscriptionManager(IGlobalPropertiesContext globalProps, IWorldComputerCryptologyContext wcCryptoContext, ILoggerFactory loggerFactory, IKernelConcurrencyManager concurrencyManager, ITime time,
                        ILocalNodeContext localnodecontext, ISecurityContext securityContext, IHederaDTLContext hederaContext) : base(loggerFactory.CreateLogger("GlobalEventSubscriptionManager"), concurrencyManager)
        {
            _globalPropertiesContext = globalProps;
            _writeToConsole = globalProps.WriteToConsole!;
            localNodeContext = localnodecontext;
            HederaDTLContext = hederaContext;
            _timeManager = time;
            _worldComputerCryptologyContext = wcCryptoContext;
            _cancellationTokenSource = new CancellationTokenSource();
            _eventTargetTable = new Dictionary<Guid, Dictionary<GlobalEventType, Dictionary<Guid, Action<Guid, IGlobalEvent, ulong, long, int>>>>();
            // Create a secured object for this kernel service 
            securedObject = securityContext.CreateDefaultKernelServiceSecuredObject();
        }
        #endregion

        #region IDisposable Implementation
        public override void Dispose()
        {
            _globalPropertiesContext = null!;
            _timeManager = null!;
            HederaDTLContext = null!;
            localNodeContext = null!;
            if (globalEventSubscriptions != null)
            {
                foreach (var sub in globalEventSubscriptions)
                {
                    sub?.Dispose();
                }
            }
            globalEventSubscriptions = null!;
            globalSubscriptionsFile?.Flush();
            globalSubscriptionsFile?.Close();
            globalSubscriptionsFile?.Dispose();
            globalSubscriptionsFile = null!;
            _writeToConsole = null!;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null!;
            backgroundSubscriptionListeners = null!;
            backgroundMonitorLogsProcess = null!;
            _worldComputerCryptologyContext = null!;
            _eventTargetTable = null!;
            base.Dispose();
        }
        #endregion

     
        #region IKernelService Implementation
        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
            {
                #region Check for Global Subscriptions local file 
                string globalEventSubscriptionsFileName = null!;
                byte[] globalEventSubscriptionsJsonBytes = null!;
                if (_globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0) 
                {
                    globalEventSubscriptionsFileName = HostCryptology.ConvertBytesToHexString(HostCryptology.Encrypt2(Encoding.ASCII.GetBytes(
                                $"{HederaDTLContext!.GlobalEventTopic.ShardNum}.{HederaDTLContext.GlobalEventTopic.RealmNum}.{HederaDTLContext.GlobalEventTopic.AccountNum}" +
                                   _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToString("N").ToUpper()), localNodeContext!.NodeSymmetricKey, localNodeContext.NodeSymmetricIV));
                }
                else
                {
                    globalEventSubscriptionsFileName = HostCryptology.ConvertBytesToHexString(HostCryptology.Encrypt2(Encoding.ASCII.GetBytes(
                        $"{HederaDTLContext!.GlobalEventTopic.ShardNum}.{HederaDTLContext.GlobalEventTopic.RealmNum}.{HederaDTLContext.GlobalEventTopic.AccountNum}" +
                                    localNodeContext!.NodeID.ToString("N").ToUpper()), localNodeContext.NodeSymmetricKey, localNodeContext.NodeSymmetricIV));
                }
                var globalSubscriptionsFilePath = Path.Combine(Path.Combine(_globalPropertiesContext.NodeDirectoryPath, localNodeContext.LocalStoreDirectoryName), globalEventSubscriptionsFileName);
                if (!File.Exists(globalSubscriptionsFilePath))
                {
                    if (_globalPropertiesContext.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0)
                    {
                        // Assume first time Simulation Node is being run so create the missing file
                        globalSubscriptionsFile = new FileStream(globalSubscriptionsFilePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                        globalEventSubscriptions = new List<GlobalEventSubscription>();
                        // NOTE:    For simulations, we assume if we are creating this file because it doesn't it exist, it is in fact simulating creating a new node
                        //          in a manner of speaking, so we set the intialialStartSeconds/Nanos to now.  This way the new node won't receive events from prior simulation VDisk Create operations.
                        DateTime EPOCH = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        long NanosPerTick = 1_000_000_000L / TimeSpan.TicksPerSecond;
                        TimeSpan timespan = _timeManager!.ProcessorUtcTime - EPOCH;  // %TODO%  - When implementing an /ADD node feature to the NETWORK simulator, will need to persist this value so new node starts from this time as well (in order to get relevant history)
                        long seconds = (long)timespan.TotalSeconds;
                        globalEventSubscriptions.Add(new GlobalEventSubscription(localNodeContext.NodeDIDRef, HederaDTLContext.GlobalEventTopic.ShardNum,
                                                            HederaDTLContext.GlobalEventTopic.RealmNum, HederaDTLContext.GlobalEventTopic.AccountNum,
                                                            seconds, (int)((timespan.Ticks - (seconds * TimeSpan.TicksPerSecond)) * NanosPerTick)));
                        var json = JsonSerializer.Serialize<List<GlobalEventSubscription>>(globalEventSubscriptions);
                        globalEventSubscriptionsJsonBytes = Encoding.UTF8.GetBytes(json);
                        // Encrypt Bytes using Node keys
                        HostCryptology.EncryptBufferInPlaceWith32ByteKey(globalEventSubscriptionsJsonBytes, localNodeContext.NodeSymmetricKey);
                        globalSubscriptionsFile.Write(globalEventSubscriptionsJsonBytes, 0, globalEventSubscriptionsJsonBytes.Length);
                        globalSubscriptionsFile.Flush();
                    }
                    else
                    {
                        // For a regular node the file should have been created by spawn, so error
                        throw new System.Exception($"The Node's global subscriptions file was not found - shutting down.");
                    }
                }
                else
                {
                    // If we make it here we simply open the existing globalSubscriptionsFilePath file
                    globalSubscriptionsFile = new FileStream(globalSubscriptionsFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    globalEventSubscriptionsJsonBytes = new byte[globalSubscriptionsFile.Length];
                    globalSubscriptionsFile.ReadExactly(globalEventSubscriptionsJsonBytes, 0, globalEventSubscriptionsJsonBytes.Length);

                    //globalEventSubscriptionsJsonBytes = HostCryptology.AsymmetricDecryptionWithoutCertificate(globalEventSubscriptionsJsonBytes, localNodeContext.Node2048AsymmetricPrivateKey);
                    HostCryptology.DecryptBufferInPlaceWith32ByteKey(globalEventSubscriptionsJsonBytes, localNodeContext.NodeSymmetricKey);
                    var json = Encoding.UTF8.GetString(globalEventSubscriptionsJsonBytes);
                    //_globalPropertiesContext.WriteToConsole!.DynamicInvoke($"GEL.StartAsync:", $"I - json={json}");
                    globalEventSubscriptions = JsonSerializer.Deserialize<List<GlobalEventSubscription>>(json)!;
                }
                #endregion

                #region Check for log files for each global subscription and open if they exist, or if a simulation, create and open if they don't already exist
                foreach( var sub in globalEventSubscriptions)
                {
                    await sub.OpenLogFileAsync(_globalPropertiesContext, localNodeContext).ConfigureAwait(false); 
                }
                #endregion

                await base.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        


        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
            {
                if (!_hasShutDown)
                {
                    await StopGlobalEventStreaming().ConfigureAwait(false);
                }

                await base.StopAsync(cancellationToken).ConfigureAwait(false);
            }
        }


        #endregion

        #region ICriticalShutdown Implementaiton
        public async Task CriticalStopAsync(CancellationToken cancellationToken)
        {
            await StopAsync(cancellationToken).ConfigureAwait(false);
        }
        #endregion 

        #region GlobalEventLogManager Implementation
        public void StartGlobalEventStreaming()
        {
            // *** IMPORTANT ***  This should not be called until all subscribers have registered for their events and are ready to receive delivery
            #region Start Listeners for each global subscription
            backgroundSubscriptionListeners = new List<Task>();
            foreach (var sub in globalEventSubscriptions!)
            {
                backgroundSubscriptionListeners.Add(sub.SubscriptionListenerAsync(_cancellationTokenSource!.Token, HederaDTLContext!));
            }
            #endregion

            #region Start MonitorLogProcess in background task
            backgroundMonitorLogsProcess = MonitorLogFilesProcess();
            #endregion

        }

        public async Task StopGlobalEventStreaming()
        {
            // *** IMPORTANT ***  This should be called BEFORE all subscribers have unregistered for their events to ensure no more events are delivered
            if (!_hasShutDown)
            {
                #region Shutdown the Global stream of events from Hedera
                _cancellationTokenSource!.Cancel();
                _doneMonitoring = true;
                foreach (var sub in globalEventSubscriptions!)
                {
                    sub.StopListening();
                }
                await Task.WhenAll(backgroundSubscriptionListeners).ConfigureAwait(false);
                #endregion

                #region Stop delivering events to event target processesd
                await backgroundMonitorLogsProcess!.ConfigureAwait(false);
                #endregion

                #region Flush global subscriptions file contents and close
                globalSubscriptionsFile?.Flush();
                globalSubscriptionsFile?.Close();
                globalSubscriptionsFile?.Dispose();
                globalSubscriptionsFile = null!;
                #endregion

                _hasShutDown = true;
            }
        }


        public Guid AddGlobalEventSubscription(long shard, long realm, long account)
        {
            Guid subscriptionID = Guid.NewGuid();
            // %TODO%
            return subscriptionID;
        }

        public void RemoveGlobalEventSubscription(Guid subscriptionID)
        {
            // %TODO
        }

        public Guid RegisterEventTarget(Action<Guid, IGlobalEvent, ulong, long, int> eventCallBack, GlobalEventType globalEventType = GlobalEventType.ALL, Guid subscriptionID = default(Guid))
        {
            Guid eventTargetID = Guid.NewGuid();
            if (subscriptionID == default(Guid))
            {
                subscriptionID = localNodeContext.NodeDIDRef;  // Default is the Global Event Subscription ID
            }
            lock (_eventTargetTable)
            {
                if (!_eventTargetTable.ContainsKey(subscriptionID))
                {
                    _eventTargetTable.Add(subscriptionID, new Dictionary<GlobalEventType, Dictionary<Guid, Action<Guid, IGlobalEvent, ulong, long, int>>>());
                }

                if (!_eventTargetTable[subscriptionID].ContainsKey(globalEventType))
                {
                    _eventTargetTable[subscriptionID].Add(globalEventType, new Dictionary<Guid, Action<Guid, IGlobalEvent, ulong, long, int>>());
                }
                _eventTargetTable[subscriptionID][globalEventType].Add(eventTargetID, eventCallBack);
            }
            return eventTargetID;

        }

        public void UnregisterEventTarget(Guid eventTargetID, GlobalEventType globalEventType = GlobalEventType.ALL, Guid subscriptionID = default(Guid) )
        {
            if (subscriptionID == default(Guid))
            {
                subscriptionID = localNodeContext!.NodeDIDRef;  // Default is the Global Event Subscription ID
            }
            lock (_eventTargetTable!)
            {
                _eventTargetTable[subscriptionID][globalEventType].Remove(eventTargetID);
            }
        }

        public async Task SubmitGlobalEventAsync(IGlobalEvent globalEvent)
        {
            try
            {
                await using (Client client = HederaDTLContext!.NewClient())
                {
                    byte[] contents = globalEvent.AsBytes;
                    // Encrypt contents "in-place"  with World Computer Symmetric key
                    HostCryptology.EncryptBufferInPlaceWith32ByteKey(contents, _worldComputerCryptologyContext!.WorldComputerSymmetricKey);
                    var receipt = await client.SubmitMessageAsync(HederaDTLContext.GlobalEventTopic, contents);
                    //Debug.Print($"Submitted Global Event returned: sequence#={receipt.SequenceNumber}, receipt={receipt.Id},  hash={HostCryptology.ConvertBytesToHexString(receipt.RunningHash.ToArray())}, CostUSD={receipt.CurrentExchangeRate!.USDCentEquivalent}, CostHBar={receipt.CurrentExchangeRate.HBarEquivalent}, CostExpiration={receipt.CurrentExchangeRate.Expiration}");
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.ToString());
            }
        }


        #endregion

        #region Helpers
        private async Task MonitorLogFilesProcess()
        {
            bool isSimulationNode = _globalPropertiesContext!.IsIntendedForSimulation && _globalPropertiesContext.SimulationNodeNumber > 0;
            var thisNodeDidRef = (isSimulationNode ? _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber] : localNodeContext!.NodeDIDRef);
            while (! _doneMonitoring )
            {
                foreach (var sub in globalEventSubscriptions!)
                {
                    List<byte[]> logRecords = null!;
                    //using (var exclusiveLock = await sub.GetExclusiveLockAsync().ConfigureAwait(false))
                    using (var exclusiveLock = await sub.GetExclusiveLockAsync().ConfigureAwait(false))
                    {
                        //Debug.Print("########### Calling sub.ReadAndRemovePendingLogRecordsAsync()...");
                        //logRecords = await sub.ReadAndRemovePendingLogRecordsAsync().ConfigureAwait(false);
                        logRecords = await sub.ReadAndRemovePendingLogRecordsAsync().ConfigureAwait(false);
                        //Debug.Print("########### ...Returned from Calling sub.ReadAndRemovePendingLogRecordsAsync()");
                    }
                    foreach (var logRecordbytes in logRecords)
                    {
                        try
                        {
                            // Decrypt logBytes in-place
                            HostCryptology.DecryptBufferInPlaceWith32ByteKey(logRecordbytes, localNodeContext!.NodeSymmetricKey);
                            var logRecord = new GlobalEventLogFileRecord(logRecordbytes);
                            // Decrypt in-place logRecord Payload
                            byte[] rawPayLoad = logRecord.Payload!;
                            HostCryptology.DecryptBufferInPlaceWith32ByteKey(rawPayLoad!, _worldComputerCryptologyContext!.WorldComputerSymmetricKey);
                            GlobalEvent ge = new GlobalEvent(rawPayLoad);

                            #region Filter out messages that are not intended for this 
                            #region  Exclude events not explicitly targeting this Node
                            if (ge.NodeTargetList != null && ge.NodeTargetList.Length > 0)
                            {
                                var eventTargetsThisNode = false;
                                foreach( var targetNodeDIDRef in ge.NodeTargetList)
                                {
                                    if (targetNodeDIDRef.Equals(thisNodeDidRef))
                                    {
                                        Debug.Print("...Event explicitly targeted....");
                                        eventTargetsThisNode = true;
                                        break;
                                    }
                                }
                                if(!eventTargetsThisNode)
                                {
                                    // If we make it here the Event wasn't intended for this Node, so don't deliver it (i.e., ignore it)  and continue to next event
                                    Debug.Print($"Event on Node{(isSimulationNode ? "# "+ _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToString() : "")} wasn't targeted for this Event");
                                    continue; 
                                }
                            }
                            #endregion

                            // NOTE:  If this Node is in both the Targeted and Excluded list, Exclusion takes precedence and the event will be excluded
                            #region Exclude events if this Node has been explicitly excluded
                            if (ge.NodeExclusionList != null && ge.NodeExclusionList.Length > 0)
                            {
                                var eventExcludesThisNode = false;
                                foreach( var exludedNodeDIDRef in ge.NodeExclusionList)
                                {
                                    if( exludedNodeDIDRef.Equals(thisNodeDidRef))
                                    {
                                        Debug.Print("...Event explicitly excluded....");
                                        eventExcludesThisNode = true;
                                        break;
                                    }
                                }
                                if(eventExcludesThisNode)
                                {
                                    // If we make it here the Event explicitly excludes this Node, so don't deliver it (i.e., ignore it)  and continue to next event
                                    Debug.Print($"Event on Node{(isSimulationNode ? "# " + _globalPropertiesContext.SimulationPool![_globalPropertiesContext.SimulationNodeNumber].ToString() : "")} was explicitly excluded from receiving this event.");
                                    continue;
                                }
                            }
                            #endregion 
                            #endregion
                            #region Route Event to interested subscribers
                            lock (_eventTargetTable!)
                            {
                                if (_eventTargetTable.ContainsKey(sub.ID))
                                {
                                    foreach (var eventType in _eventTargetTable[sub.ID].Keys)
                                    {
                                        if (eventType == GlobalEventType.ALL || eventType == ge.EventType)
                                        {
                                            foreach (var deliveryTarget in _eventTargetTable[sub.ID][eventType])
                                            {
                                                // !!IMPORTANT!!  To retain "in order" event delivery semantics the deliveryTarget callback must NOT be async!
                                                // If a set of events can truly be processed in parallel (i.e.; have no casual relationship with each other - or are mutually exclusive)
                                                // then the deliveryTarget callback can decided this and process the message asynchronously.  However, the call site here must be syncrhonous
                                                // to ensure that each event is in fact "delivered in order" to the target callback.
                                                deliveryTarget.Value(ge.SenderDIDRef, ge, logRecord.ConsensusSequence, logRecord.ConsensusTimeSeconds, logRecord.ConsensusTimeNanos);
                                            }
                                        }
                                        //else
                                        //{
                                        //    Debug.Print($"Event IGNORED!! :  {logRecord.ConsensusSequence}) {logRecord.ConsensusTimeSeconds}.{logRecord.ConsensusTimeNanos} -> SenderDIDRef = {ge.SenderDIDRef}, EventType={ge.EventType}, CorrelationID={ge.CorrelationID} ");
                                        //}
                                    }
                                }
                            }
                            #endregion 
                        }
                        catch (Exception ex)
                        {
                            Debug.Print($"Event failed to be Delivered - Error: {ex} ");
                        }
                    }
                    await Task.Delay(50);
                    //await Task.Yield();
                }
            }
        }
        #endregion 
    }
}
