using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using System.Threading;

namespace WorldComputer.Simulator
{
    internal class BlockDevice :  IBlockDevice //: KernelService,
    {
       
        #region Members
        private bool stopProcessing = false;
        private int writeRetry;
        private int writeRetryDelayinMilliseconds;
        private int readRetry;
        private int readRetryDelayInMilliseconds;
        private uint storageBlockSize;
        //private int blockDeviceInstance = -1;
        private int storageArrayWidth = 0;  // Replication Factor of Grid
        private int instance = 0;
        private List<DriveStorageDistribution> storageDistributionList = null!;
        private SetManager.SimpleCrc32 _crc32 = null!;
        private BufferBlock<string> blockDeletionQueue = null!;
        //private AsyncManualResetEvent shutDownEvent = null!;
        private string nodeHashID = null!;
        private Task blockDeletionQueueConsumer = null!;
        private bool performBlockContentValidation = false;
        private bool performBlockNameValidation = false;
        private bool performZeroBlockOnDelete = false;
        private Microsoft.Extensions.Logging.ILogger<BlockDevice> _logger = null!;
        #endregion

        #region Constructors
        public BlockDevice(ILoggerFactory loggerFactory, ILocalNodeContext localNodeContext, IKernelConcurrencyManager concurrencyManager, INodeInstallationContext niContext,
                              IStorageGridContext sgContext, int blockInstance, HostCrc32 crc32) : base(loggerFactory.CreateLogger("BlockDevice"), concurrencyManager)
        {
            //_logger = loggerFactory.CreateLogger<BlockDevice>();  
            instance = blockInstance;
            storageDistributionList = niContext.DriveStorageDistribution;
            _crc32 = crc32;
            nodeHashID = localNodeContext.NodeHashID;
            storageBlockSize = niContext.DefaultStorageBlockSize;
            storageArrayWidth = sgContext.StorageGridReplicaCount; // DefaultAppDomainManager.StorageArrayWidth;
            performBlockContentValidation = sgContext.PerformBlockContentValidation;
            performBlockNameValidation = sgContext.PerformBlockNameValidation;
            performZeroBlockOnDelete = sgContext.PerformZeroBlockOnDelete;
            //if (instance == 0 && sgContext.RequiresLocalJournalStorage ) // Is this the IBlockDevice associated with the Journal?
            //{
            //    // Adjust mount path for each drive in the storageDistributionList to use the raw the disk rather than a mounted VHD
            //    foreach (var sd in storageDistributionList)
            //    {
            //        sd.DriveStorageMountPath = sd.DriveStorageMountPath.Substring(0, sd.DriveStorageMountPath.Length - 11);  // Strip off suffixing "StoreMount\"
            //                                                                                                                 // PAL.Log($"BlockDevice.RegisterDefaultAppDomainManager() - DriveStorageMountPath={sd.DriveStorageMountPath}");
            //    }
            //}
        }
        #endregion


        #region IKernelService Implementation
        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
            {
                //shutDownEvent = new AsyncManualResetEvent();
                stopProcessing = false;
                blockDeletionQueue = new BufferBlock<string>();
                //var runInBackGround = Task.Run(async () => await BlockDeletionQueueConsumer(CancellationToken.None).ConfigureAwait(false));
                blockDeletionQueueConsumer = BlockDeletionQueueConsumer(CancellationToken.None);
                await base.StartAsync(cancellationToken);
            }
        }

        public override async Task StopAsync(CancellationToken canceelationToken)
        {
            using (var exclusiveLock = await _concurrencyManager.KernelLock.WriterLockAsync().ConfigureAwait(false))
            {
                stopProcessing = true;
                if (blockDeletionQueue != null!)
                {
                    blockDeletionQueue.Complete();
                    blockDeletionQueue.Completion.Wait();  // Need to wait until queue completes
                    //shutDownEvent.WaitAsync().Wait();
                }
                // *** NOTE:  We MUST NOT await the line below.  Instead we use the GetAwaiter().GetResult() to wait
                //            for the task to complete.   The reason is that using await causes the caller of this assembly
                //            to hold some reference to this type in memory which later prevents the UnoSysKernel.dll assembly
                //            (which contains this type) to unload.  This is likely due to the compiler generated "state machine"
                //            that gets inserted into the code when await is used.  This problem does not occur when using the pure
                //            api approach to waiting on the Task to complete (i.e.; GetAwaiter()).
                blockDeletionQueueConsumer.GetAwaiter().GetResult();
                await base.StopAsync(canceelationToken);
            }
        }

        public override void Dispose()
        {
            nodeHashID = null!;
            storageDistributionList = null!;
            blockDeletionQueue = null!;
            //shutDownEvent = null!;
            blockDeletionQueueConsumer?.Dispose();
            blockDeletionQueueConsumer = null!;
            _logger = null!;
            base.Dispose();
        }
        #endregion

        #region IBlockDevice Implementation
        public void InittializeBlockDevice(int numberOfReadRetries, int readRetryDelayInMilliseconds, int numberOfWriteRetries, int writeRetryDelayinMilliseconds)
        {
            this.writeRetry = numberOfWriteRetries;
            this.writeRetryDelayinMilliseconds = writeRetryDelayinMilliseconds;
            this.readRetry = numberOfReadRetries;
            this.readRetryDelayInMilliseconds = readRetryDelayInMilliseconds;
        }


        public async Task<SetManager.BlockIOResponse> ReadBlockAsync(SetManager.BlockFileInfo readBlockInfoFileMask, byte[] buffer, int bufferoffset, int bytesToRead, int blockoffset, uint crcNamePlaceHolder)
        {
            int bytesRead = 0;
            int tries = 1;
            bool done = false;
            string crcBlockContent = null!;
            SetManager.BlockFileInfo foundBlockFileInfo = null!;
            // PAL.Log($"READ ({(blockDeviceInstance==0? "JOURNAL":"ACTUAL")}) blockid ->  {readBlockInfoFileMask.BlockID}");
            while (!done && tries <= readRetry)
            {
                //Debug.Print("BlockDevice.ReadBlockAsync() - A");
                try
                {
                    foundBlockFileInfo = await LookUpExactlyOneBlockFileInfo(readBlockInfoFileMask).ConfigureAwait(false);
                    if (foundBlockFileInfo == null!)
                    {
                        //Debug.Print("BlockDevice.ReadBlockAsync() - B");
                        //Debug.Print("[UNX] SERVER:  BlockDevice.ReadBlockAsync() for -  blockid={0}, files found={1}, files[0]={2}", blockid, files.Length, (files.Length > 0? files[0]:"None") );
                        // We can get here legally when we are attempting to write a partial block to the end of a file - where the block doesn't exist yet.
                        return new SetManager.BlockIOResponse(bytesRead.ToString("X8"), false);
                    }
                    else
                    {

                        #region Compute crc of found Block Name and Validate against original
                        if (performBlockNameValidation)
                        {
                            crcBlockContent = foundBlockFileInfo.CrcContent;
                            //if (bytesToRead != storageBlockSize)
                            //{
                            //    Debug.Print("BlockDevice.ReadBlockAsync(1) - foundBLockFileInfo.CrcConent={0}, bytesToRead={1} - {2}", crcBlockContent, bytesToRead, Environment.StackTrace);
                            //}
                            // Validate the block's file name to ensure it hasn't been tampored with
                            if (!foundBlockFileInfo.ValidateCrcName(crcNamePlaceHolder.ToString("X8")))
                            {
                                Debug.Print("[UNX] SERVER:  BlockDevice.ReadBlockAsync() Crc of block name failed!   crcBlockContent={0},  crcNamePlaceHolder={1}, foundBlockFileInfo.BlockID={2} - stackTrace={3}",
                                                                              crcBlockContent, crcNamePlaceHolder, foundBlockFileInfo.BlockID, Environment.StackTrace);
                                return new SetManager.BlockIOResponse(bytesRead.ToString("X8"), false, true);    // could not read block due to CRC check failure
                            }
                        }
                        #endregion
                    }
                    using (FileStream stream = new FileStream(
                            foundBlockFileInfo.AbsoluteBlockFileSpec,
                            FileMode.Open,
                            FileAccess.Read,
                            FileShare.None,
                            (int)storageBlockSize, //DEFAULT_BLOCK_SIZE
                            true // useAsync
                            ))
                    {
                        //PAL.Log($"BlockDevice.ReadBlockAsync() - C - {foundBlockFileInfo.AbsoluteBlockFileSpec}");
                        //if (buffer.Length >= storageBlockSize)
                        if (bytesToRead == 0)
                        {
                            Debug.Print("BlockDevice.ReadBlockAsync(SPECIAL CASE - bytesToRead=0!!) - {0} ", Environment.StackTrace);
                        }
                        //if (buffer.Length >= storageBlockSize)
                        if (bytesToRead >= storageBlockSize)
                        {
                            //Debug.Print("BlockDevice.ReadBlockAsync(2) - buffer.Length={0}, blockoffset={1} - bytesToRead={2}", buffer.Length, blockoffset, bytesToRead);
                            //bytesRead = await stream.ReadAsync(buffer, blockoffset, (bytesToRead == 0 ? Convert.ToInt32(stream.Length) : bytesToRead));  // NOTE:  a bytesToRead value of zero means read all bytes in block file
                            bytesRead = await stream.ReadAsync(buffer, bufferoffset, bytesToRead).ConfigureAwait(false);
                            //PAL.Log($"BlockDevice.ReadBlockAsync() - D - {buffer[0 + blockoffset]}, {buffer[1 + blockoffset]}, {buffer[2 + blockoffset]}, {buffer[3 + blockoffset]}, {buffer[4 + blockoffset]})");
                            //Debug.Print( "^^^^^+^^^^^ BlockDevice.ReadBlockAsync AFTER block read - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { buffer[0 + blockoffset], buffer[1 + blockoffset], buffer[2 + blockoffset], buffer[3 + blockoffset], buffer[4 + blockoffset], buffer[5 + blockoffset], buffer[6 + blockoffset] } );
                            #region Compute crc of Block Content and Verify
                            if (performBlockContentValidation)
                            {
                                string computedCrcBlockContent = BitConverter.ToUInt32(
                                                    _crc32.ComputeHash(buffer, bufferoffset, bytesRead), 0).ToString("X8");
                                if (crcBlockContent != computedCrcBlockContent)
                                {
                                    Debug.Print("[UNX] SERVER:  BlockDevice.ReadBlockAsync(1) Crc of block content failed!   crcBlockContent={0},  computedCrcContent={1}, foundBlockFileInfo.BlockID={2},  - stackTrace={3}",
                                                        crcBlockContent, computedCrcBlockContent, foundBlockFileInfo.BlockID, Environment.StackTrace);
                                    return new BlockIOResponse(bytesRead.ToString("X8"), false, false, true);    // could not read block due to CRC check failure
                                }
                            }
                            #endregion
                        }
                        else  // Request < storageBlockSize bytes, so must adjust to read a full block in order to properly compute crcContent value
                        {
                            //Debug.Print("BlockDevice.ReadBlockAsync() - E");
                            byte[] fullBuffer = new byte[storageBlockSize];
                            var fullBufferBytesRead = await stream.ReadAsync(fullBuffer, 0, (int)storageBlockSize).ConfigureAwait(false);  // Read a full block
                                                                                                                                           //Debug.Print( "^^^^^+^^^^^ BlockDevice.ReadBlockAsync AFTER block read - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { buffer[0 + blockoffset], buffer[1 + blockoffset], buffer[2 + blockoffset], buffer[3 + blockoffset], buffer[4 + blockoffset], buffer[5 + blockoffset], buffer[6 + blockoffset] } );
                            #region Compute crc of Block Content and Verify
                            if (performBlockContentValidation)
                            {
                                string computedCrcBlockContent = BitConverter.ToUInt32(
                                                    _crc32.ComputeHash(fullBuffer, 0, fullBufferBytesRead), 0).ToString("X8");
                                if (crcBlockContent != computedCrcBlockContent)
                                {
                                    Debug.Print("[UNX] SERVER:  BlockDevice.ReadBlockAsync(2) Crc of block content failed!   crcBlockContent={0},  computedCrcContent={1}, foundBlockFileInfo.BlockID={2},  - stackTrace={3}",
                                                        crcBlockContent, computedCrcBlockContent, foundBlockFileInfo.BlockID, Environment.StackTrace);
                                    return new BlockIOResponse(bytesRead.ToString("X8"), false, false, true);    // could not read block due to CRC check failure
                                }
                            }
                            #endregion
                            // Copy back into buffer the relevant data
                            //if( blockoffset + bytesToRead > fullBuffer.Length || bufferoffset + bytesToRead > buffer.Length )
                            //{
                            //    PAL.Log($"PROBLEM! blockoffset={blockoffset}, bytesToRead={bytesToRead}, fullBuffer.Length={fullBuffer.Length}, buffer.Length={buffer.Length}",
                            //                blockoffset, bytesToRead, fullBuffer.Length, buffer.Length);
                            //}
                            Buffer.BlockCopy(fullBuffer, blockoffset, buffer, bufferoffset, (bytesToRead == 0 ? Convert.ToInt32(stream.Length) : bytesToRead));
                            bytesRead = bytesToRead;
                        }
                        //await stream.FlushAsync().ConfigureAwait(false);
                        stream.Close();
                    }
                    done = true;
                }
                catch (System.IO.IOException)
                {
                    // This error happens occassionally (even frequently) when a block is already opened exclusively by another thread when the above new FileStream(...) call is made.
                    // We can safely ignore it with a retry
                    tries++;
                    Task.Delay(readRetryDelayInMilliseconds).Wait();
                }
                catch (Exception ex)
                {
                    // If we make it here it is unexpected and we want to know why...but we will also ignore and retry
                    Debug.Print("BlockDevice.ReadBlockAsync - try={2} -  blockid={1} - {0}  ", ex, (string.IsNullOrEmpty(readBlockInfoFileMask.BlockID) ? "null" : readBlockInfoFileMask.BlockID), tries);
                    tries++;
                    Task.Delay(readRetryDelayInMilliseconds).Wait();
                }
            }
            if (tries > readRetry)
            {
                bytesRead = 0;
                Debug.WriteLine(string.Format("[UNX] SERVER:  BlockDevice.ReadBlockAsync() ERROR in OnDiskRead() - tries > {1} ....  offset: {0}", bufferoffset, readRetry));
            }
            return new BlockIOResponse(foundBlockFileInfo.BlockName + bytesRead.ToString("X8"), false);
        }


        public async Task<BlockIOResponse> WriteBlockAsync(BlockFileInfo blockFileInfo, BlockFileInfo deleteblockFileInfoMask, bool overwrite, byte[] buffer, int bufferOffset, int bytesToWrite, int blockoffset, bool isPartialBlock, uint crc, bool queueDelete = true)
        {
            int tries = 1;
            bool done = false;
            //PAL.Log($"WRITE ({(blockDeviceInstance == 0 ? "JOURNAL" : "ACTUAL")}) blockid ->  {blockFileInfo.BlockID}");
            //Debug.Print("WRITE blockid -> {0}, {1}, {2}", bytesToWrite, blockFileInfo.BlockID, deleteblockFileInfoMask.BlockID);
            //var storageroot = DetermineStorageRootForRelativeBlockId(blockid);
            //string fileName = Path.Combine(storageroot, blockid);
            //string relativeBlockId = new FileInfo(fileName).Name;
            //Debug.Print( "A deleteblockid={0}", deleteblockid == null ? "null" : (deleteblockid == string.Empty ? "empty" : deleteblockid) );
            //if (!string.IsNullOrEmpty(deleteblockid))
            //Debug.Print("BlockDevice.WriteBlockAsync() - A");
            if (deleteblockFileInfoMask != null!)
            {
                //PAL.Log($"BlockDevice.WriteBlockAsync() - B ");
                //Debug.Print("WRITE deleteblockid -> {0}", DetermineStorageRootForRelativeBlockId(deleteblockid));
                // If we make it here an actual deleteblockid has been provided signalling that we want to delete any matching deleteblockid block(s)
                // before writing out blockid.  This only happens for "actual" blocks (i.e.; NOT for "journal" blocks)
                //DeleteBlock(deleteblockFileInfoMask, "WriteBlockAsync1", null);
                var runinbackground = QueueBlockForDeletion(deleteblockFileInfoMask, blockFileInfo);
            }
            else
            {
                if (!overwrite)
                {
                    // A deleteblockid of null and an overwrite flag of false signals we do NOT want to overwrite any block that has a more recent LastModifiedTimeStamp,
                    // Therefore, must check if there is such a block and if so we must return from this call WITHOUT overwritting it
                    BlockFileInfo existingBlock = blockFileInfo.DetermineExistingBlockFileInfo();
                    //Debug.Print( "BlockDevice.WriteBlockAsync() - readBlockId={0}", readBlockId );
                    BlockFileInfo[] blockNames = GetBlockNames(existingBlock);
                    if (blockNames != null && blockNames.Length == 1)
                    {
                        // If we make it here we have found our expected block, so ensure that the LastModifiedTimeStamp is not more current than that of the block being written
                        //string currentBlock = Path.GetFileName(blockNames[0]);
                        //string currentBlockModifiedUpdateTimeStampToCheck = currentBlock.Substring(currentBlock.Length - CacheBlockValue.SchemaVersionPlaceHolderLength - CacheBlockValue.LastModifiedTimeStampPlaceHolderLength, CacheBlockValue.LastModifiedTimeStampPlaceHolderLength);
                        //string blockLastModifiedTimeStampToCheck = blockid.Substring(blockid.Length - CacheBlockValue.SchemaVersionPlaceHolderLength - CacheBlockValue.LastModifiedTimeStampPlaceHolderLength, CacheBlockValue.LastModifiedTimeStampPlaceHolderLength);
                        // if (currentBlockModifiedUpdateTimeStampToCheck.CompareTo(blockLastModifiedTimeStampToCheck) > 0)
                        if (blockNames[0].LastModifiedTimeStamp.CompareTo(blockFileInfo.LastModifiedTimeStamp) > 0)
                        {
                            Debug.Print("^^^ BlockDevice.WriteBlockAsync() - Block Overwrite check FAILED as there is a more recent block to overwrite - no block write occurred");
                            // If we make it here we are attempting to overwrite a more recent block, so return indicating "no block write" took place
                            return new BlockIOResponse(blockFileInfo.BlockName + 0.ToString("X8"), false);
                        }
                        else  // CompareTo() result is <= 0 - both signal the all clear to overwrite
                        {
                            //Debug.Print( "BlockDevice.WriteBlockAsync() - Block Overwrite check occurred and the block is cleared to be overwritten" );
                            // Delete the block(s) we are overwriting
                            if (!DeleteBlock(existingBlock))//, null))
                            {
                                Debug.Print("^^^ BlockDevice.WriteBlockAsync() - Block Overwrite DeleteBlock() failed");
                            }
                        }
                    }
                    else
                    {
                        if (blockNames == null || blockNames.Length == 0)
                        {
                            //Debug.Print( "BlockDevice.WriteBlockAsync() - Block Overwrite check occurred and no existing block was found...carrying on with block write." );
                        }
                        else
                        {
                            //Debug.Print( "^^^ BlockDevice.WriteBlockAsync() - Block Overwrite check occurred and more than one block was found - SOMETHING WRONG - no block write took place." );
                            // If we make it here then something is wrong since we should only ever find exatctly one block, so return indicating "no block write" took place
                            return new BlockIOResponse(blockFileInfo.BlockName + 0.ToString("X8"), false);
                        }
                    }
                }
            }
            //bool isFileSetBlock = (blockid.Substring(blockid.Length - 2) == "00");
            //bool isFileSetBlock = (blockid.SchemaVersion == "00");
            // NOTE:  We loop here since the OpenFileForWrite() will fail on the attempt to write the first block because the directory path
            //		  all the way up to the file does not yet exist.  However, the call to OpenFileForWrite() creates all necessary directories
            //		  as a side effect, so looping corrects the problem
            while (!done && tries <= writeRetry)
            {
                try
                {
                    var storageroot = DetermineStorageRoot(blockFileInfo);
                    string fileName = Path.Combine(storageroot, blockFileInfo.BlockID);  // Here we write the EXACT fileName (i.e.; fully specified by the complete BlockID)
                    using (FileStream stream = OpenFileForWrite(fileName))
                    {
                        //PAL.Log($"BlockDevice.WriteBlockAsync() - G - {fileName}");
                        //PAL.Log($"BlockDevice.WriteBlockAsync() - H - {buffer[0 + blockoffset]}, {buffer[1 + blockoffset]}, {buffer[2 + blockoffset]}, {buffer[3 + blockoffset]}, {buffer[4 + blockoffset]})");
                        await stream.WriteAsync(buffer, bufferOffset, bytesToWrite).ConfigureAwait(false);

                        //Debug.Print( "^^^^^^^^^^^ BlockDevice.WriteBlockAsync BEFORE block write - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { buffer[0 + blockOffset], buffer[1 + blockOffset], buffer[2 + blockOffset], buffer[3 + blockOffset], buffer[4 + blockOffset], buffer[5 + blockOffset], buffer[6 + blockOffset] } );
                        //                  if (isFileSetBlock)
                        //{
                        //	//Debug.Print( "BlockDevice.WriteBlockAsync() buffer.Length={0}, blockoffset={1}, bytesToWrite={2}, CacheBlockValue.BytesPerSector={3}", buffer.Length, blockOffset, bytesToWrite, CacheBlockValue.BytesPerSector );
                        //	if (bytesToWrite <= UnosysSettings.MAX_RESIDENT_FILE_SIZE)		// NOTE:  MAX_RESIDENT_FILE_SIZE is the max size of a file that Windows will store directly in the Master File Table (as opposed to on disk)
                        //	{
                        //		// If we make it here we wish to pad the small block to the max size where it will still fit in the MFT
                        //		// This aids in hiding the "real size" of the block for added "security through obscurity"
                        //		byte[] fullBlock = HostUtilities.GenRandomBlock( UnosysSettings.MAX_RESIDENT_FILE_SIZE );
                        //		Buffer.BlockCopy( buffer, bufferOffset, fullBlock, 0, bytesToWrite );
                        //		await stream.WriteAsync( fullBlock, 0, UnosysSettings.MAX_RESIDENT_FILE_SIZE ).ConfigureAwait( false );
                        //	}
                        //	else
                        //	{
                        //		if (bytesToWrite == CacheBlockValue.BytesPerSector)
                        //		{
                        //			// Just write the completely full block
                        //			await stream.WriteAsync( buffer, bufferOffset, bytesToWrite ).ConfigureAwait( false );
                        //		}
                        //		else
                        //		{
                        //			// block > MAX_RESIDENT_FILE_SIZE and < CacheBlockValue.BytesPerSector, so pad it to CacheBlockValue.BytesPerSector in size before writing
                        //			byte[] fullBlock = HostUtilities.GenRandomBlock( CacheBlockValue.BytesPerSector );
                        //			Buffer.BlockCopy( buffer, bufferOffset, fullBlock, 0, bytesToWrite );
                        //			await stream.WriteAsync( fullBlock, 0, CacheBlockValue.BytesPerSector ).ConfigureAwait( false );
                        //		}
                        //	}
                        //	//Debug.Print( "BlockDevice.WriteBlockAsync() blockid={0}, bytesToWrite={1}, bytesActuallyWritten={2}", fileName, bytesToWrite, (bytesToWrite <= UnosysSettings.MAX_RESIDENT_FILE_SIZE ? UnosysSettings.MAX_RESIDENT_FILE_SIZE : (bytesToWrite == CacheBlockValue.BytesPerSector ? bytesToWrite : CacheBlockValue.BytesPerSector)) );
                        //}
                        //else // Table block
                        //{
                        //	//Debug.Print( "BlockDevice.WriteBlockAsync() - should not get here in File Testing....fileName={0}", fileName );
                        //	// Use actual size 
                        //	await stream.WriteAsync( buffer, bufferOffset, bytesToWrite ).ConfigureAwait( false );
                        //}
                        await stream.FlushAsync().ConfigureAwait(false);
                        //stream.Flush(true);
                        stream.Close();
                    }
                    //Debug.Print( "FILENAME EXISTS={0}", File.Exists(fileName) );
                    done = true;
                }
                catch (Exception)
                {
                    //Debug.Print(ex.ToString());
                    tries++;
                    Task.Delay(writeRetryDelayinMilliseconds).Wait();// NOTE:  can't await in the body of catch clause
                }
            }  // while (!done && tries <= writeRetry)
            if (tries > writeRetry)
            {
                Debug.WriteLine(string.Format("[UNX] SERVER:  BlockDevice.WriteBlockAsync() [ {3} ] - try {1} - ERROR in WriteBlock  tries > {2}: offset {0}", new object[] { bufferOffset, tries, writeRetry, System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() }));
                return new BlockIOResponse(blockFileInfo.BlockName + 0.ToString("X8"), false);
            }
            return new BlockIOResponse(blockFileInfo.BlockName + bytesToWrite.ToString("X8"), false);
        }


        public async Task<BlockFileInfo> LookUpExactlyOneBlockFileInfo(BlockFileInfo readBlockInfoFileMask)
        {
            // This routine looks for and returns the block file found for the passed in readBlockIdMask,
            // or else returns null - i.e.; if there is no such block file found or if there is more than one such block file found
            // NOTE:  There is a race condition that can occur when for example a block is written and then immediately read.
            //        In such a scneario the write will attempt to first delete any previous version of the block it is writing.
            //        However, this delete is done in the background (see the call to  QueueBlockForDeletion(deleteblockFileInfoMask, blockFileInfo)
            //        in above WriteBlockAsync() call) so as not to delay the write of the new block which will have a unique file name.
            //        This potentially "delayed" delete of any previous block can affect this routine, since its possible an immediate subsequent
            //        read operation (which calls this routine) will in fact find 2 (or more) files matching the passed in readBlockInfoFileMask.
            //        Therefore, to handle this potential edge case we actually loop up to 3 times to check for exactly one file before giving up.
            //        This has the result of slowing down the read operation under the circumstances of this rare edge case, rather than slowing 
            //        down EVERY write operation by making the "delete previous block" synchronous before the write of the new block
            BlockFileInfo blockFileInfo = null!;
            try
            {
                var storageroot = DetermineStorageRoot(readBlockInfoFileMask);
                var mask = readBlockInfoFileMask.ThruOrdinal + "*";
                var edgeCaseRetry = 10; // see NOTE above
                while (edgeCaseRetry != 0)
                {
                    // Read the file using readBlockIdMask which contains wildcards
                    string[] files = Directory.GetFiles(storageroot, mask);
                    if (files.Length == 1)
                    {
                        blockFileInfo = new BlockFileInfo(files[0].Substring(storageroot.Length), storageroot); // strip off storageroot from file name 
                        break;
                    }
                    else
                    {
                        if (files.Length == 0)
                        {
                            break;  // If we did not find a block file then we can return immediately
                        }
                        else
                        {
                            // If we found more than one block then we are likely in the above mentioned race condition (see NOTE above)
                            edgeCaseRetry--;
                            PAL.Log($"BlockDevice.LookUpExactlyOneBlockFileInfo() - Edgecase {edgeCaseRetry}, {files.Length}");
                            //Task.Yield().GetAwaiter().GetResult();  // Delay a small amount
                            Task.Delay(1).Wait();  // Delay a very small amount
                        }
                    }
                }
            }
            catch (DirectoryNotFoundException)
            {
                // NOP  Can/will get here when looking for a block that hasn't been written yet.
                //		In this case we just ignore causing a null to be returned
            }
            catch (Exception ex)
            {
                // NOP  If we make it here it is unexpected and so we want to know about it.
                //		However still just ignore causing a null to be returned
                Debug.Print("BlockDevice.LookUpExactlyOneBlockFileInfo - error - {0}", ex);
            }
            return await Task.FromResult<BlockFileInfo>(blockFileInfo);
        }


        public List<IEnumerable<string>> EnumerateBlockContainerList(string container, out int storageRootLength)
        {
            List<IEnumerable<string>> result = new List<IEnumerable<string>>();
            string storageroot = "";
            foreach (var s in storageDistributionList)
            {
                storageroot = s.DriveStorageMountPath + Path.DirectorySeparatorChar;
                result.Add(Directory.EnumerateDirectories(Path.Combine(storageroot, container)));
            }
            //Debug.Print("BlockDevice.EnumerateBLockContainerList() - storageroot={0}, storageRootLength={1}", storageroot, storageroot.Length);
            storageRootLength = storageroot.Length;
            return result;
            // Debug.Print("BlockDevice.EnumerateBlockContainers storageroot={0}, container={1} ", storageroot, container);
            //  storageRootLength = storageroot.Length;
            // return Directory.EnumerateDirectories(Path.Combine(storageroot, container));
        }


        public IEnumerable<string> EnumerateBlocks(string container, BlockFileInfo blockFileInfoMask = null!)
        {
            if (blockFileInfoMask == null!)
            {
                return Directory.EnumerateFiles(container);
            }
            else
            {
                return Directory.EnumerateFiles(container, blockFileInfoMask.BlockName);
            }
        }


        public bool DeleteBlock(BlockFileInfo deleteBlockFileInfo)
        {
            bool result = true;
            int tries = 1;
            bool done = false;
            string deleteblockid = deleteBlockFileInfo.ThruOrdinal + "*";
            //string deleteblockid = deleteBlockFileInfo.BlockID;
            var storageroot = DetermineStorageRoot(deleteBlockFileInfo.SetIdPlusOrdinal);
            // Need to delete blocks that currently exist matching blockid pattern
            while (!done && tries < 3)
            {
                try
                {

                    string[] files = Directory.GetFiles(storageroot, deleteblockid);
                    if (files.Length == 0)
                    {
                        result = false;  // No block to delete
                    }
                    else
                    {
                        //if (files.Length > 1)
                        //{
                        //    Debug.Print("BlockDevice.DeleteBlock deleteblockid found {0} matching blocks - {1} ", files.Length, deleteblockid);
                        //    foreach (string fileName in files)
                        //    {
                        //        Debug.Print("BlockDevice.DeleteBLock found filename={0}, context={1}", fileName, context);
                        //    }
                        //}
                        foreach (string fileName in files)
                        {
                            #region Zero Block On Delete
                            if (performZeroBlockOnDelete)
                            {
                                //Debug.Print( "----------------------- Deleting file: {0}", fileName );
                                // As a security measure we want to open the file we are about to delete
                                // and "zero it" by writing zeros to it, before we delete it.  This way,
                                // no one can use low-level disk scanning tools to retrieve the data later.
                                // This is being extra careful since the file is already doublly encrypted anyway
                                try
                                {
                                    using (FileStream block = OpenFileForWrite(fileName))
                                    {
                                        if (block != null!)
                                        {
                                            long blockFileSize = block.Length;
                                            //byte[] buffer = new byte[Math.Min(blockFileSize, DEFAULT_BLOCK_SIZE)];  // We will only write at most DEFAULT_BLOCK_SIZE zeros
                                            byte[] buffer = new byte[Math.Min(blockFileSize, storageBlockSize)];  // We will only write at most DEFAULT_BLOCK_SIZE zeros
                                                                                                                  // Write the zero buffer to disk and close.
                                            block.Write(buffer, 0, buffer.Length);
                                            block.Close();
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    // NOP - zeroing the block is on a "best effort" basis - so igonre any error we might get (such as failed to get an exclusive "lock")
                                    //Debug.Print("BlockDevice.DeleteBLock() - zeroing failed!!");
                                }
                            }
                            #endregion

                            // Regardless of whether or not the above zeroing worked, we still want to delete the file as intended
                            //if (peerSet != null && peerSet.ProcessorReplica == 0)
                            //{
                            //	Debug.Print( "BlockDevice.DeleteBLock deleted ({2}) block={0}, deleteblockid={1}", fileName, deleteblockid, fileName.Length );
                            //}
                            File.Delete(fileName);
                            result = !File.Exists(fileName);
                            //if (peerSet != null && peerSet.ProcessorReplica == 0)
                            //{
                            //	if (!result)
                            //	{
                            //		Debug.Print( "*** BlockDevice.DeleteBLock failed to delete block={0}, deleteblockid={1}", fileName, deleteblockid );
                            //	}
                            //}
                        }
                    }
                    done = true;
                }
                catch (Exception)
                {
                    // Ignore any errors but signal with a false result
                    //if (context != "WriteBlockAsync")
                    //{
                    //	Debug.Print( "BlockDevice.DeleteBlock() - {0} - deleteblockid={1}, context={2}, error={3}", ex.Message, deleteblockid, context, ex.GetType().ToString() );
                    //}
                    result = false;
                    tries++;
                    Task.Delay(5).Wait();
                }
            }//  retry loop
            return result;
        }

        public async Task<bool> DeleteContainerContainingBlockAsync(BlockFileInfo blockFileInfo, bool isFirstBlock)
        {
            await Task.CompletedTask;
            throw new NotImplementedException();
        }

        public bool RenameBlock(BlockFileInfo oldBlockFileInfo, BlockFileInfo newBlockFileInfo)
        {
            bool result = true;
            try
            {
                string oldBlockId = oldBlockFileInfo.BlockID;
                string newBlockId = newBlockFileInfo.BlockID;
                if (oldBlockId.ToUpper().Equals(newBlockId.ToUpper()))
                {
                    _logger.LogWarning("^^^ BlockDevice.RenameBlock - oldBlockId == newBlockId - nothing to do");
                }
                else
                {
                    var storageroot = DetermineStorageRoot(oldBlockFileInfo.SetIdPlusOrdinal);  // NOTE:  It doesn't matter which block used here since both have same SetIdPlusOrdinal
                    // Ensure that the target does not exist.
                    DeleteBlock(newBlockFileInfo);//, null);
                    oldBlockId = Path.Combine(storageroot, oldBlockId);
                    newBlockId = Path.Combine(storageroot, newBlockId);
                    // Move (i.e.; rename) the block.
                    File.Move(oldBlockId, newBlockId);
                    // See if the original exists now.
                    if (File.Exists(oldBlockId))
                    {
                        _logger.LogWarning("^^^ BlockDevice.RenameBlock - The original file still exists, which is unexpected.");
                        DeleteBlock(newBlockFileInfo);//,  null);  // Delete any newBlockId that was created and we are back to where we started.
                        result = false;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "");
                result = false;
            }
            return result;
        }

        public BlockFileInfo[] GetBlockNames(BlockFileInfo blockFileInfoMask)
        {
            BlockFileInfo[] results = null!;
            string[] files = null!;
            string blockFileNameMask = blockFileInfoMask.ThruOrdinal + "*";
            string storageroot = null!;
            try
            {
                //var storageroot = DetermineStorageRootForRelativeBlockId(blockFileInfoMask);
                storageroot = DetermineStorageRoot(blockFileInfoMask.SetIdPlusOrdinal);
                files = Directory.GetFiles(storageroot, blockFileNameMask);
            }
            catch (Exception)
            {
                // NOP:  Ignore error				
            }
            if (files != null!)
            {
                results = new BlockFileInfo[files.Length];
                for (int i = 0; i < files.Length; i++)
                {
                    results[i] = new BlockFileInfo(files[i].Substring(storageroot.Length));
                }
            }
            return results;
        }


        public void BlockPurge(BlockFileInfo blockFileMask)
        {
            // Loop through all local storage drives on this Node and remove all blocks in the provided path
            foreach (var sd in storageDistributionList)
            {
                string dirToDelete = null!;
                int retries = 3;
                while (retries != 0)
                {
                    try
                    {
                        dirToDelete = Path.Combine(sd.DriveStorageMountPath, blockFileMask.BlockPath);
                        _logger.LogInformation($"   *** BlockDevice.BlockPurge - removing directory: {dirToDelete} on Node {nodeHashID}");
                        Directory.Delete(dirToDelete, true);
                        break;  // We are done so exit loop.
                    }
                    catch (DirectoryNotFoundException)
                    {
                        // Will often be the case where there is no directory found (especially in the case of Journal Block)
                        // So just ignore and quit as there is nothing to do
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Error attempting to Delete Directory {dirToDelete} on Node {nodeHashID} - {ex.ToString()}");
                        retries--;
                    }
                }
            }
        }
        #endregion


        #region Helpers
        private FileStream OpenFileForWrite(string fileName) //, int blocksize )
        {
            FileStream fileStream = null!;
            try
            {
                fileStream = new FileStream(
                                fileName,
                                FileMode.OpenOrCreate,
                                FileAccess.Write,
                                FileShare.None,
                                (int)storageBlockSize, //DEFAULT_BLOCK_SIZE,
                                true  // userAsync 
                                );
            }
            catch (DirectoryNotFoundException)
            {
                try
                {
                    string dirName = Path.GetDirectoryName(fileName);
                    string[] adirs = dirName.Split(new char[] { Path.DirectorySeparatorChar });
                    // Ensure directory chain exists
                    string dir = adirs[0] + Path.DirectorySeparatorChar;
                    for (int i = 1; i < adirs.Length; i++)
                    {
                        dir = Path.Combine(dir, adirs[i]);
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Debug.Print("OpenFileForWrite Exception (INNER) - SHOULD NOT HAPPEN!: {0} ", ex);
                }
            }
            catch (UnauthorizedAccessException)
            {
                //Debug.Print("OpenFileForWrite UnauthorizedAccessException ");
                // NOP - have seen this exception occassionaly but can safely ignore since we will retry
                fileStream = null!;
            }
            catch (IOException)
            {
                //Debug.Print("OpenFileForWrite IOException ");
                // NOP - we get this exception if the file is in use by another thread - can safely ignore since we will retry
                fileStream = null!;
            }
            catch (Exception ex)
            {
                // NOP - if we get here it is unexpected so we want to know why - but safely ignore anyway since we will retry
                Debug.Print("OpenFileForWrite Exception: {0} ", ex);
                fileStream = null!;
            }
            if (fileStream == null!)
                throw new DirectoryNotFoundException(fileName);
            return fileStream;
        }


        private string DetermineStorageRoot(string setIdPlusOrdinal)
        {
            string storePath = "";
            byte[] setIdPlusOrdinalBytes = Encoding.ASCII.GetBytes(setIdPlusOrdinal);
            // NOTE:  We seed the random number generator with the block hash to ensure it is deterministic
            //        in always generating the same random value for the same block.  This is critical or
            //        else we won't be able to find blocks once we write them.  We also need a brand
            //        new Random object each time for this to work (i.e.; can't reuse an existing one)
            var r = new Random(BitConverter.ToInt32(_crc32.ComputeHash(setIdPlusOrdinalBytes), 0));
            double ordhash = r.NextDouble();
            int index = 0;
            double checkPoint = 0;
            foreach (var sd in storageDistributionList)
            {
                checkPoint += storageDistributionList[index].DriveContributionPercentageToNode;
                if (ordhash <= checkPoint)
                {
                    storePath = sd.DriveStorageMountPath;
                    break;
                }
                index++;
            }
            return storePath;
        }



        private string DetermineStorageRoot(BlockFileInfo blockFileInfo)
        {
            return DetermineStorageRoot(blockFileInfo.SetIdPlusOrdinal);
            //string storePath = "";
            //byte[] setIdPlusOrdinalBytes = Encoding.ASCII.GetBytes(blockid.SetIdPlusOrdinal);
            //// NOTE:  We seed the random number generator with the block hash to ensure it is deterministic
            ////        in always generating the same random value for the same block.  This is critical or
            ////        else we won't be able to find blocks once we write them.  We also need a brand
            ////        new Random object each time for this to work (i.e.; can't reuse an existing one)
            //var r = new Random(BitConverter.ToInt32(crc32.ComputeHash(setIdPlusOrdinalBytes), 0));
            //double ordhash = r.NextDouble();
            //int index = 0;
            //double checkPoint = 0;
            //foreach (var sd in storageDistributionList)
            //{
            //    checkPoint += storageDistributionList[index].DriveContributionPercentageToNode;
            //    if (ordhash <= checkPoint)
            //    {
            //        storePath = sd.DriveStorageMountPath;
            //        break;
            //    }
            //    index++;
            //}
            //return storePath;
        }


        private async Task<bool> QueueBlockForDeletion(BlockFileInfo deleteBlockFileInfo, BlockFileInfo ignoredBlockFileInfo)
        {
            bool result = false;
            try
            {
                // The ignoredBlockFileInfo represents the block file that is about to be written to the cloud (i.e.; the new most recent).
                // However, because this is all happening asynchronously, it is possible it gets written before we make the check for blocks
                // below, so we want to actively check for it below and ignore it so we don't accidently delete it 
                long ignoredLastModifiedUtcTimeStampInTicks = long.Parse(ignoredBlockFileInfo.LastModifiedTimeStamp, System.Globalization.NumberStyles.HexNumber);
                string[] files = null!;
                string blockFileNameMask = deleteBlockFileInfo.ThruOrdinal + "*";
                string storageroot = null!;
                try
                {
                    //var storageroot = DetermineStorageRootForRelativeBlockId(blockFileInfoMask);
                    storageroot = DetermineStorageRoot(deleteBlockFileInfo.SetIdPlusOrdinal);
                    files = Directory.GetFiles(storageroot, blockFileNameMask);
                }
                catch (Exception)
                {
                    // NOP:  Ignore error				
                }
                // Loop through all block files found, looking for the one with the greates LastModifiedUtcTimeStamp
                if (files != null!)
                {
                    foreach (var fileName in files)
                    {
                        BlockFileInfo deletionCandidateFileInfo = new BlockFileInfo(fileName.Substring(storageroot.Length));
                        long deletionBlockTimeStampInTicks = long.Parse(deletionCandidateFileInfo.LastModifiedTimeStamp, System.Globalization.NumberStyles.HexNumber);
                        // Only consider blocks that are not as recent as the ignore block passed in as it is supposed to be written right after this call, 
                        // but due to race conditions caused by asynchronous thread operation, it (as well as other more recent blocks) may have already been written
                        if (deletionBlockTimeStampInTicks < ignoredLastModifiedUtcTimeStampInTicks)
                        {
                            // Queue for deletion all blocks that are NOT most recent
                            await blockDeletionQueue.SendAsync<string>(fileName).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // NOP
                Debug.Print("BlockDevice.QueueBLockForDeletion() - error {0}", ex.ToString());
            }
            return result;
        }


        private async Task BlockDeletionQueueConsumer(CancellationToken cancellationToken)
        {
            //shutDownEvent.Reset();
            string blockFileToDelete = null!;
            try
            {
                while (!stopProcessing)
                {
                    try
                    {
                        while (!stopProcessing && await blockDeletionQueue.OutputAvailableAsync(cancellationToken).ConfigureAwait(false))
                        {
                            // Loop to process all available queued messages at this moment
                            while (!stopProcessing && blockDeletionQueue.TryReceive<string>(out blockFileToDelete))
                            {
                                //PAL.Log($"Processing Delete Block: {blockFileToDelete}");
                                // Delete the block in the background if it exists
                                var runInBackGround = DeleteBlockFile(blockFileToDelete);
                            } // (!stopProcessing && blockDeletionQueue.TryReceive<CloudBlockBlob>(out blockToDelete))
                        } // while (await inboundMessageQueue.OutputAvailableAsync( cancellationToken ).ConfigureAwait( false ))
                        await Task.Yield();
                    }
                    catch (Exception ex)
                    {
                        Debug.Print("Error in: BlockDevice.BlockDeletionQueueConsumer - {0}", ex);
                    }
                } // while (!stopProcessing)
            }
            finally
            {
                //shutDownEvent.Set();  // Signal this method is completing
            }
        }


        private async Task<bool> DeleteBlockFile(string deleteBlockFileName, bool zeroDeletionBlocks = true)
        {
            bool result = false;
            bool done = false;
            int tries = 0;
            // Need to delete blocks that currently exist matching blockid pattern
            while (!done && tries < 3)
            {
                try
                {
                    //Debug.Print( "----------------------- Deleting file: {0}", fileName );
                    if (zeroDeletionBlocks)
                    {
                        // As a security measure we want to open the file we are about to delete
                        // and "zero it" by writing zeros to it, before we delete it.  This way,
                        // no one can use low-level disk scanning tools to retrieve the data later.
                        // This is being extra careful since the file is already doublly encrypted anyway
                        try
                        {
                            using (FileStream block = OpenFileForWrite(deleteBlockFileName))
                            {
                                if (block != null!)
                                {
                                    long blockFileSize = block.Length;
                                    //byte[] buffer = new byte[Math.Min(blockFileSize, DEFAULT_BLOCK_SIZE)];  // We will only write at most DEFAULT_BLOCK_SIZE zeros
                                    byte[] buffer = new byte[Math.Min(blockFileSize, storageBlockSize)];  // We will only write at most DEFAULT_BLOCK_SIZE zeros
                                                                                                          // Write the zero buffer to disk and close.
                                    block.Write(buffer, 0, buffer.Length);
                                    block.Close();
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // NOP - zeroing the block is on a "best effort" basis - so igonre any error we might get (such as failed to get an exclusive "lock")
                            //Debug.Print("BlockDevice.DeleteBLock() - zeroing failed!!");
                        }
                    }
                    // Regardless of whether or not the above zeroing worked, we still want to delete the file as intended
                    File.Delete(deleteBlockFileName);
                    result = !File.Exists(deleteBlockFileName);
                    done = true;
                }
                catch (Exception)
                {
                    result = false;
                    tries++;
                    await Task.Delay(5).ConfigureAwait(false);  // Short delay before trying again
                }
            }//  retry loop
            return result;
        }
        #endregion
    }


    internal interface IBlockDevice //: IKernelBlockDeviceExtension
    {
        void InittializeBlockDevice(int numberOfReadRetries, int readRetryDelayInMilliseconds, int numberOfWriteRetries, int writeRetryDelayinMilliseconds);
        Task<SetManager.BlockIOResponse> ReadBlockAsync(SetManager.BlockFileInfo blockFileInfo, byte[] buffer, int bufferoffset, int bytesToRead, int blockoffset, uint crcNamePlaceHolder);
        Task<SetManager.BlockIOResponse> WriteBlockAsync(SetManager.BlockFileInfo blockFileInfo, SetManager.BlockFileInfo deleteBlockFileInfo, bool overwrite, byte[] buffer, int bufferoffset, int bytesToWrite, int blockoffset, bool isPartialBlock);
        bool DeleteBlock(SetManager.BlockFileInfo blockFileInfo, string context, IPeerSet peerSet);
        SetManager.BlockFileInfo LookUpExactlyOneBlockFileInfo(SetManager.BlockFileInfo readBlockFileInfo);
        bool RenameBlock(SetManager.BlockFileInfo oldBlockFileInfo, SetManager.BlockFileInfo newBlockFileInfo);
        SetManager.BlockFileInfo[] GetBlockNames(SetManager.BlockFileInfo blockFileInfoMask);
        List<IEnumerable<string>> EnumerateBlockContainerList(string container, out int storageRootLength);
        IEnumerable<string> EnumerateBlocks(string container, SetManager.BlockFileInfo blockFileInfoMask = null);
    }
}
