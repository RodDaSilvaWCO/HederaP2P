//#define NO_CACHE
namespace WorldComputer.Simulator
{
    using Microsoft.Extensions.DependencyInjection;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class CacheManager : IDisposable
    {
        private static SetManager.SimpleCrc32 crc32 = new SetManager.SimpleCrc32();
        internal static List<IBlockDevice> blockdevices = null;

        internal CacheManager()
        {

        }

        public void Dispose()
        {
            crc32?.Dispose();
        }

        public async Task<SetManager.BlockIOResponse> GetBlockAsync(IPeerSet peerSet, byte[] bufferToFill, SetManager.BlockMetaData blockMetaData)
        {
//#if NO_CACHE
            SetManager.BlockIOResponse result = await blockCache.GetBlockNoCache(peerSet, bufferToFill, blockMetaData).ConfigureAwait(false);
//#else
//            SetManager.BlockIOResponse result = await blockCache.GetBlock(peerSet, bufferToFill, blockMetaData).ConfigureAwait(false);
//#endif
            // NOTE:  blockMetaData.OperationByteCount < 0 means its a TableSet - blockMetaData.OperationByteCount is MAX_RECORD_SIZE * -1 in this case
            if ((blockMetaData.OperationByteCount < 0 && result.ByteCount <= blockMetaData.OperationByteCount * -1) || (result.ByteCount == blockMetaData.OperationByteCount))  // NOTE:  First case for TableSet members, and second case for FileSet memebers
            {
                // %TODO - Potentially use "vDisk Key" to decrypt all blocks
                //if (storageLocation != StorageLocation.Cloud)  // %TODO - How to handle Hybrid scenario!!!
                //{
                //    // NOTE:  We only want to decrypt the block with this NodeID if we are not storing the data in the cloud.  This is because
                //    //        data stored in the cloud is shared by EVERY Cloud Access node, and there fore we can't use any one particular Node's ID
                //    //        to encrypt it.  Note, however, the data is still encrypted by the Key associated with the File so that the cloud provider 
                //    //        object storage can not understand the block contents.  In addition, the cloud storage provider also itself encrypts the
                //    //        block at rest, the block is obstensibly "doubly-encrypted" and completely supports end-to-end encryption at all times.
                //    #region  Decrypt the block
                //    //Debug.Print( "^^^^^^^^^^^ CacheManager.GetBlockAsync BEFORE block decryption - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { bufferToFill[0 + blockMetaData.BufferOffset], bufferToFill[1 + blockMetaData.BufferOffset], bufferToFill[2 + blockMetaData.BufferOffset], bufferToFill[3 + blockMetaData.BufferOffset], bufferToFill[4 + blockMetaData.BufferOffset], bufferToFill[5 + blockMetaData.BufferOffset], bufferToFill[6 + blockMetaData.BufferOffset] } );
                //    byte[] key = defaultAppDomainManager.NodeID.ToByteArray();
                //    unsafe
                //    {
                //        fixed (byte* p = bufferToFill, pwd = key)
                //        {
                //            if (!defaultAppDomainManager.DecryptBuffer((IntPtr)(p + blockMetaData.BufferOffset), result.ByteCount, (IntPtr)pwd, 16))
                //            {
                //                Debug.Print("CacheManager.GetBlockAsync()  call to DecryptBuffer() failed");
                //            }
                //        }
                //    }
                //    //Debug.Print( "^^^^^%^^^^^^ CacheManager.GetBlockAsync AFTER block decryption  - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { bufferToFill[0 + blockMetaData.BufferOffset], bufferToFill[1 + blockMetaData.BufferOffset], bufferToFill[2 + blockMetaData.BufferOffset], bufferToFill[3 + blockMetaData.BufferOffset], bufferToFill[4 + blockMetaData.BufferOffset], bufferToFill[5 + blockMetaData.BufferOffset], bufferToFill[6 + blockMetaData.BufferOffset] } );
                //    #endregion
                //}
            }
            return result;
        }


        public async Task<SetManager.BlockIOResponse> PutBlockAsync(IPeerSet peerSet, byte[] bufferToEmpty, SetManager.BlockMetaData blockMetaData)
        {
            //try
            //{
            //    // %TODO - Potentially use "vDisk Key" to encrypt all blocks
            //    if (storageLocation != StorageLocation.Cloud)  // %TODO - How to handle Hybrid scenario!!!
            //    {
            //        // NOTE:  We only want to encrypt the block with this NodeID if we are not storing the data in the cloud.  This is because
            //        //        data stored in the cloud is shared by EVERY Cloud Access node, and there fore we can't use any one particular Node's ID
            //        //        to encrypt it.  Note, however, the data is still encrypted by the Key associated with the File so that the cloud provider 
            //        //        object storage can not understand the block contents.  In addition, the cloud storage provider also itself encrypts the
            //        //        block at rest, the block is obstensibly "doubly-encrypted" and completely supports end-to-end encryption at all times.
            //        #region Encrypt the block
            //        //Debug.Print( "^^^^^^^^^^^ CacheManager.PutBlockAsync BEFORE block encryption - {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { bufferToEmpty[0 + blockMetaData.BufferOffset], bufferToEmpty[1 + blockMetaData.BufferOffset], bufferToEmpty[2 + blockMetaData.BufferOffset], bufferToEmpty[3 + blockMetaData.BufferOffset], bufferToEmpty[4 + blockMetaData.BufferOffset], bufferToEmpty[5 + blockMetaData.BufferOffset], bufferToEmpty[6 + blockMetaData.BufferOffset] } );
            //        byte[] key = defaultAppDomainManager.NodeID.ToByteArray();
            //        unsafe
            //        {
            //            fixed (byte* p = bufferToEmpty, pwd = key)
            //            {
            //                if (!defaultAppDomainManager.EncryptBuffer((IntPtr)(p + blockMetaData.BufferOffset), blockMetaData.OperationByteCount, (IntPtr)pwd, 16))
            //                {
            //                    Debug.Print("CacheManager.PubBlockAsync() call to EncryptBuffer() failed");
            //                }
            //            }
            //        }
            //        //Debug.Print( "^^^^^^^^^^^ CacheManager.PutBlockAsync AFTER block encryption -  {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { bufferToEmpty[0 + blockMetaData.BufferOffset], bufferToEmpty[1 + blockMetaData.BufferOffset], bufferToEmpty[2 + blockMetaData.BufferOffset], bufferToEmpty[3 + blockMetaData.BufferOffset], bufferToEmpty[4 + blockMetaData.BufferOffset], bufferToEmpty[5 + blockMetaData.BufferOffset], bufferToEmpty[6 + blockMetaData.BufferOffset] } );
            //        #endregion
            //    }
            //}
            //catch (Exception ex)
            //{
            //    Debug.Print("Error in CacheManager.PutBlockAsync() {0}", ex);
            //    throw;
            //}
            // Process the IO operation locally on this node
            if (blockMetaData.BlockOrdinal == 0UL)  // If this is a recovery block then skip cache and write it directly
            {
                // Block Reovery write
                // Compute the blockid (i.e.; name) of this block - note that recovery blocks always have block ordinal 0
                SetManager.BlockFileInfo recoveryblockfileinfo = new SetManager.BlockFileInfo(blockMetaData, true);
                SetManager.BlockFileInfo deleteblockfileinfo = new SetManager.BlockFileInfo(blockMetaData, false);
                recoveryblockfileinfo.SetCRC(bufferToEmpty, blockMetaData);
                blockMetaData.OperationByteCount = bufferToEmpty.Length;
                return await CommitBlockToDeviceAsync(peerSet, recoveryblockfileinfo, deleteblockfileinfo, bufferToEmpty, blockMetaData).ConfigureAwait(false);
            }
            else // Normal (i.e.; non-recovery) block write - so go through cache
            {
#if NO_CACHE
                return await blockCache.PutBlockNoCache(peerSet, bufferToEmpty, blockMetaData).ConfigureAwait(false);
#else
                return await blockCache.PutBlock(peerSet, bufferToEmpty, blockMetaData).ConfigureAwait(false);
#endif
            }
        }


        #region Helpers
        //internal async Task<BlockIOResponse> CommitBlockToDeviceAsync( IPeerSet peerSet, string writeblockid, string deleteblockid, byte[] blockBuffer, /*int bufferOffset, int bytesToWrite,*/ BlockMetaData blockMetaData )
        internal async Task<SetManager.BlockIOResponse> CommitBlockToDeviceAsync(IPeerSet peerSet, SetManager.BlockFileInfo writeblockfileinfo, SetManager.BlockFileInfo deleteblockfileinfo, byte[] blockBuffer, BlockMetaData blockMetaData)
        {
            // This routine implements SEC (strong eventual consistency) semantics and in so doing needs to log the commit block request to the jouranl
            //
            // This is the single routine responsible for commiting a block to the device.  It needs to deal with the commitment of the block
            // not only to this device but the journal as well if and only if the PeerSet Matrix width is > 1.  
            // If PeerSet Matrix equals 1 then no need for journaling since there are no replicas - journaling exists entirely to manage replicas in a peer set)
            // 
            SetManager.BlockIOResponse blockIoResponse = null;
            if (storageLocation != StorageLocation.Cloud && peerSet.MatrixWidth > 1)  // If we have at least one OTHER replica (i.e 2 or more replica nodes) we call JournalManager.CreateJournalEntryAsync() to manage journal & block persistence for us
            {
    RDS:// blockIoResponse = await blockDevices[ACTUAL_BLOCK_STORE].WriteBlockAsync( blockfileinfo, deleteblockfileinfo, false, buffer, bufferOffset, bytesToWrite, blockOffset, isPartialBlock).ConfigureAwait(false);
                blockIoResponse = await journalmanager.CreateJournalEntryAsync(peerSet, writeblockfileinfo, "CacheManager.CommitBlockToDeviceAsync", blockMetaData.EncryptionSalt,
                                deleteblockfileinfo, blockBuffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.IsPartialBlock).ConfigureAwait(false);
            }
            else
            {
                //if (blockdevices.Count == 1)
                if (storageLocation != StorageLocation.Cloud && peerSet.MatrixWidth == 1)
                {
                    // On-Prem Node with no replicas so we simply write the block out using the ACTUAL block store BlockDevice (i.e.; index 1)
                    // NOTE:  index 0 (the JOURNAL block store) is not used in this case)
                    // Debug.Print("^^^^^$^^^^^ CacheManager.CommitBlockToDeviceAsync block encryption -  {0}, {1}, {2}, {3}, {4}, {5}, {6}", new object[] { blockBuffer[0 + bufferOffset], blockBuffer[1 + bufferOffset], blockBuffer[2 + bufferOffset], blockBuffer[3 + bufferOffset], blockBuffer[4 + bufferOffset], blockBuffer[5 + bufferOffset], blockBuffer[6 + bufferOffset] });
                    blockIoResponse = await blockdevices[1].WriteBlockAsync(writeblockfileinfo, deleteblockfileinfo, false, blockBuffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.IsPartialBlock).ConfigureAwait(false);
                }
                else  // Hybrid or Cloud scenario
                {
                    // If we make it here we are dealing with potentially "Multi-Cloud" scenario and need to write the block to all blockdevices in parallel with an all-or-nothing "atomic" semantic.
                    // That is, we need to write to all N blockdevices completely or not at all.  It is not allowed to have one blockdevice updated and another not.  So if one or more fails
                    // we have to be sure to "undo" updates to the other blockdevices in order that they may remain in sync.
                    if (storageLocation == StorageLocation.Cloud)
                    {
                        if (blockdevices.Count == 1)  // Single Cloud scenario
                        {
                            blockIoResponse = await blockdevices[0].WriteBlockAsync(writeblockfileinfo, deleteblockfileinfo, false, blockBuffer, blockMetaData.BufferOffset,
                                        blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.IsPartialBlock).ConfigureAwait(false);
                        }
                        else
                        {
                            // *** NOTE: IMPORTANT  ***
                            // For the cloud scenario the storageArray is a two dimensional array of ConnectionMetaData objects (i.e.; ConnectionMetaData[,] )
                            // where the first subscript determines the cloud provider account (replica), and the 2nd the cloud provider (the cluster).  This is 
                            // transposed from the case of an on-prem storage grid where the first subscript is the cluster and the 2nd is the replica
                            if (storageArrayWidth > 1) // width tell us how many storage providers we will write to in parallel
                            {
                                // multi-cloud
                                // %% TODO
                                throw new NotImplementedException("Multi cloud scenario not yet implemented");
                            }
                            else
                            {
                                // If we make it here we have a single cluster with multiple cloud provider storage accounts
                                // E.g.;  Azure East US data center as the single cluster, with multiple storage accounts within it for striping purposes
                                blockIoResponse = await blockdevices[DetermineBlockDevice(writeblockfileinfo)].WriteBlockAsync(writeblockfileinfo, deleteblockfileinfo, false, blockBuffer, blockMetaData.BufferOffset,
                                        blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.IsPartialBlock).ConfigureAwait(false);
                            }
                        }

                        //else  // Multi-Cloud scenario
                        //{
                        //    // For now we are just going to deal with the "happy" path and assume that both updates will happen
                        //    // %TODO% - DEAL WITH UNHAPPY PATH
                        //    List<Task<BlockIOResponse>> atomicTasks = new List<Task<BlockIOResponse>>();
                        //    for (int bd = 0; bd < blockdevices.Count; bd++)
                        //    {
                        //        atomicTasks.Add(blockdevices[bd].WriteBlockAsync(writeblockfileinfo, deleteblockfileinfo, false, blockBuffer, blockMetaData.BufferOffset, 
                        //                blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.IsPartialBlock));
                        //        //Debug.Print( $"CacheManager.CommitBlockToDeviceAsync() blockdevice instance={bd}");

                        //    }
                        //    try
                        //    {
                        //        var taskResults = await Task.WhenAll(atomicTasks).ConfigureAwait(false);
                        //        bool wasError = false;
                        //        foreach (var tr in taskResults)
                        //        {
                        //            if (tr.ByteCount != blockMetaData.OperationByteCount)
                        //            {
                        //                Debug.Print("****************** ERROR Writing one of the cloud providers ***************** ");
                        //                wasError = true;
                        //            }
                        //        }
                        //        if (!wasError)
                        //        {
                        //            blockIoResponse = taskResults[0];  // Any result will do to indicate success, so pick first one
                        //        }
                        //    }
                        //    catch (Exception)
                        //    {

                        //        throw;
                        //    }
                        //}

                    }
                    else // Hybrid scenario.... %TODO%
                    {

                    }
                }
            }
            return blockIoResponse;
        }

        internal static int DetermineBlockDevice(SetManager.BlockFileInfo blockFile, int cluster = 0 /* Assume default of single cloud */)
        {
            int bd = 0;
            var hashkey = blockFile.SetidHash + blockFile.Ordinal;
            int replica = (int)(BitConverter.ToUInt32(crc32.ComputeHash(System.Text.Encoding.ASCII.GetBytes(hashkey)), 0) % storageArrayLength);  // NOTE:  storageArrayLength determines number of replicas
            if (storageArrayWidth != 1 || cluster != 0)  // NOTE:  storageArrayWidth determines number of clusters
            {
                throw new NotImplementedException("Multi cloud scenarios not yet supported!");
            }
            bd = (cluster * (storageArrayWidth - 1)) + replica;
            //Debug.Print($"CacheManager.DetermineBlockDevice bd={bd}, hashkey={hashkey}, cluster={cluster}, replica={replica}, l={storageArrayLength}, w={storageArrayWidth}" );
            return bd;
        }
        #endregion 
    }

    internal class BlockCache
    {
        #region Field Members
        private const int NOT_ALLOCATED = -1;
        //private SortedDictionary<string, CacheBlockValue> dictionary = null;
        private SortedDictionary<ulong, string> lrudictionary = null;
        private ReaderWriterLockSlim rwlock;
        //private int maxentries = NOT_ALLOCATED;
        //	private int numentries = NOT_ALLOCATED;
        //private ulong cachedblockcount = 0;
        private CacheManager cacheManager = null;
        private Random rand = new Random();
        #endregion


        #region Constructors
        internal BlockCache(CacheManager cachemanager)
        {
            cacheManager = cachemanager;
        }
        #endregion


        internal void InitializeCache()
        {
            //maxentries = maxEntries;
            //	numentries = 0;
            rwlock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            //dictionary = new SortedDictionary<string, CacheBlockValue>();
            lrudictionary = new SortedDictionary<ulong, string>();
        }


        internal async Task<SetManager.BlockIOResponse> GetBlockNoCache(IPeerSet peerSet, byte[] bufferToFill, SetManager.BlockMetaData blockMetaData)
        {
            SetManager.BlockIOResponse blkResponse = new SetManager.BlockIOResponse(0.ToString("X8"), false);
            try
            {
                //       blkResponse.BlockId = blockMetaData.FullKey;
                //Debug.Print( "READ Cache Miss! = {0}", readcacheblockvalue.FullKey );
                if (blockMetaData.OperationByteCount < 0)  // NOTE:  For TableSet members we don't know how big the record is so we pass MAX_RECORD_SIZE * -1 as the bytesToGet 
                    blockMetaData.OperationByteCount = blockMetaData.OperationByteCount * -1;
                //Debug.Print( "CacheManager.BlockCache.GetBlock - C bytesToGet={0}, buferToFill.Length={1}", bytesToGet, bufferToFill.Length );
                blkResponse = await CacheBlockNoCache(peerSet, bufferToFill, blockMetaData, false);
            }
            catch (Exception ex)
            {
                Debug.Print("[ERR] SERVER:  CacheManager.BlockCache.GetBlockNoCache()  Error:{0}", ex);
                throw;
            }
            return blkResponse;
        }


        internal async Task<SetManager.BlockIOResponse> PutBlockNoCache(IPeerSet peerSet, byte[] bufferToEmpty, SetManager.BlockMetaData blockMetaData)
        {
            SetManager.BlockIOResponse blkResponse = new SetManager.BlockIOResponse(blockMetaData.OperationByteCount.ToString("X8"), false);
            try
            {
                //    blkResponse.BlockId = blockMetaData.FullKey;
                blkResponse = await CacheBlockNoCache(peerSet, bufferToEmpty, blockMetaData, true);
            }
            catch (Exception ex)
            {
                Debug.Print("[ERR] SERVER:  CacheManager.BlockCache.PutBlockNoCache()  Error:{0}", ex);
                throw;
            }
            return blkResponse;
        }

        #region Helpers

        private async Task<SetManager.BlockIOResponse> CacheBlockNoCache(IPeerSet peerset, byte[] buffer, SetManager.BlockMetaData blockMetaData, bool writeOperation)
        {
            SetManager.BlockIOResponse blkResponse = null;
            try
            {
                SetManager.BlockFileInfo writeblockidwithcrc = null;
                if (writeOperation) // Write operation:  so must compute the writeblockid & set the CRCs on the cbv
                {
                    writeblockidwithcrc = new SetManager.BlockFileInfo(blockMetaData, true).SetCRC(buffer, blockMetaData);
                }
                else // Read operation:  so must do the read in order to fill the buffer to add to the cache below
                {
                    int numberOfProviders = CacheManager.blockdevices.Count;
                    if (CacheManager.storageLocation != StorageLocation.Cloud)
                    {
                        // Wait for the read io operation to complete....
                        blkResponse = await CacheManager.blockdevices[1].ReadBlockAsync(new SetManager.BlockFileInfo(blockMetaData, false),
                                                buffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.EncryptionSalt).ConfigureAwait(false);
                    }
                    else  // Must be a Cloud scenario
                    {
                        if (numberOfProviders == 1)  // Optimize for the single could scenario
                        {
                            blkResponse = await CacheManager.blockdevices[0].ReadBlockAsync(new SetManager.BlockFileInfo(blockMetaData, false),
                               buffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.EncryptionSalt).ConfigureAwait(false);
                        }
                        else
                        {
                            // *** NOTE: IMPORTANT  ***
                            // For the cloud scenario the storageArray is a two dimensional array of ConnectionMetaData objects (i.e.; ConnectionMetaData[,] )
                            // where the first subscript determines the cloud provider account (replica), and the 2nd the cloud provider (the cluster).  This is 
                            // transposed from the case of an on-prem storage grid where the first subscript is the cluster and the 2nd is the replica
                            if (CacheManager.storageArrayWidth > 1) // width tell us how many storage providers we will write to in parallel
                            {
                                // multi-cloud
                                // %% TODO
                                throw new NotImplementedException("Multi cloud scenario not yet implemented");
                            }
                            else
                            {
                                // If we make it here we have a single cluster with multiple cloud provider storage accounts
                                // E.g.;  Azure East US data center as the single cluster, with multiple storage accounts within it for striping purposes
                                SetManager.BlockFileInfo readblock = new SetManager.BlockFileInfo(blockMetaData, false);
                                blkResponse = await CacheManager.blockdevices[CacheManager.DetermineBlockDevice(readblock)].ReadBlockAsync(readblock,
                                   buffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.EncryptionSalt).ConfigureAwait(false);
                            }
                        }
                        //else  // Multi-cloud scenario
                        //{
                        //    // If we make it here we have a  multi-cloud scenario, in which case we want to pick a random cloud provider and fetch the block from it. 
                        //    // If it should happen to fail then we want to try each of the other cloud providers in succession and see if we can fetch the block from it.
                        //    //
                        //    //
                        //    // Choose the starting cloud provider randomly
                        //    int startPoint = rand.Next() % numberOfProviders;
                        //    // Loop through all possible cloud providrs, starting at a random startPoint, until successfully retrieve the block
                        //    for (int i = 0; i < CacheManager.blockdevices.Count; i++)
                        //    {
                        //        blkResponse = await CacheManager.blockdevices[(i + startPoint) % numberOfProviders].ReadBlockAsync(new BlockFileInfo(blockMetaData, false),
                        //                            buffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount, blockMetaData.BlockOffset, blockMetaData.EncryptionSalt).ConfigureAwait(false);
                        //        if (blkResponse.ByteCount != 0)
                        //        {
                        //            //Debug.Print($"CacheManager.BlockCache.CacheBlockNoCache() successfully read block from blockdevice {(i + startPoint) % numberOfProviders}");
                        //            break;  // Success!
                        //        }
                        //        else
                        //        {
                        //            Debug.Print($"CacheManager.BlockCache.CacheBlockNoCache() **** FAILED TO READ BLOCK *** from blockdevice {(i + startPoint) % numberOfProviders}");
                        //        }
                        //    }
                        //}
                    }
                    blockMetaData.OperationByteCount = blkResponse.ByteCount;
                    // Set the CRCs on the cbv 
                    //                    cbv.SetCRC(cbv.GenBlockID(blockMetaData, true), buffer, blockMetaData.BufferOffset, blockMetaData.OperationByteCount);
                    //Debug.Print( " CacheBlock(READ) blockusedspace={0}, blockoffset={1}", blockusedspace, blockoffset );
                }
                if (writeOperation)
                {
                    SetManager.BlockFileInfo deleteblockid = new SetManager.BlockFileInfo(blockMetaData, false);
                    blkResponse = await cacheManager.CommitBlockToDeviceAsync(peerset, writeblockidwithcrc, deleteblockid, buffer, blockMetaData).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.Print("[ERR] SERVER:  CacheManager.BlockCache.CacheBlockNoCache()  Error:{0}", ex);
                throw;
            }
            //Debug.Print( "numbytes = {0}", numBytes );
            return blkResponse;
        }
 
        #endregion
    }
}
