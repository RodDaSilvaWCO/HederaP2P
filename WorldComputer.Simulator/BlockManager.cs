using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace WorldComputer.Simulator
{
    internal class BlockManager : IDisposable
    {
        private const int MAX_RESIDENT_FILE_SIZE = 312;
        private uint storageBlockSize = 0;
        private Guid vDiskID;
        private Guid VolumeID;
        private SimpleCrc32 crc32 = null!;
        private byte[] setdomidBytes = null!;
        private string clientDataPathTemplate = null!;
        private int vDiskClusterSize;
        private int vDiskReplicationFactor;
        private int[,] vDiskStorageArray;

        internal BlockManager( Guid vdiskid, Guid volumeid, int vdiskclustersize, int vdiskreplicationfactor, int[,] vdiskstoragearray, uint blockSize, string clientdatapathtemplate ) 
        {
            vDiskID = vdiskid;
            VolumeID = volumeid;
            vDiskClusterSize = vdiskclustersize;
            vDiskReplicationFactor = vdiskreplicationfactor;
            storageBlockSize = blockSize;
            crc32 = new SimpleCrc32();
            setdomidBytes = vdiskid.ToByteArray();
            clientDataPathTemplate = clientdatapathtemplate;
            vDiskStorageArray = vdiskstoragearray;
        }

        public void Dispose()
        {
            crc32?.Dispose();
        }

        internal async Task<int> WriteSetMemberAsync(Guid fileid, long filelength, byte[] bufferToEmpty, ulong deviceOffsetToWriteTo, int numberBytesToWrite)
        {
            int BytesWritten = 0;
            if (numberBytesToWrite >= storageBlockSize)
            {
                // Usual case...
                BytesWritten = await SetMemberIOOperationAsync(fileid, filelength, true,  bufferToEmpty, deviceOffsetToWriteTo, numberBytesToWrite).ConfigureAwait(false);
            }
            else
            {
                // Special handling of parital blocks (i.e.; blocks that are < storageBlockSize in size
                if (numberBytesToWrite <= MAX_RESIDENT_FILE_SIZE)      // NOTE:  MAX_RESIDENT_FILE_SIZE is the max size of a file that Windows will store directly in the Master File Table (as opposed to on disk)
                {
                    // If we make it here we wish to pad the small block to the max size where it will still fit in the MFT
                    // This aids in hiding the "real size" of the block for added "security through obscurity"
                    byte[] fullBlock = GenRandomBlock(MAX_RESIDENT_FILE_SIZE);
                    Buffer.BlockCopy(bufferToEmpty, 0, fullBlock, 0, numberBytesToWrite);
                    numberBytesToWrite = await SetMemberIOOperationAsync(fileid, filelength, true, fullBlock, deviceOffsetToWriteTo, MAX_RESIDENT_FILE_SIZE).ConfigureAwait(false);
                    if (numberBytesToWrite == MAX_RESIDENT_FILE_SIZE)
                    {
                        BytesWritten = numberBytesToWrite;  // Upon successful write of fullBlock report numberBytesToWrite written back to caller
                    }
                }
                else  // numberBytesToWrite > UnosysSettings.MAX_RESIDENT_FILE_SIZE && numberBytesToWrite < storageBlockSize
                {
                    // Pad buffer to storageBlockSize in size before writing
                    byte[] fullBlock = GenRandomBlock((int)storageBlockSize);
                    Buffer.BlockCopy(bufferToEmpty, 0, fullBlock, 0, numberBytesToWrite);
                    BytesWritten = await SetMemberIOOperationAsync(fileid, filelength, true, fullBlock, deviceOffsetToWriteTo, numberBytesToWrite).ConfigureAwait(false);
                    if (BytesWritten == (int)storageBlockSize)
                    {
                        BytesWritten = numberBytesToWrite;  // Upon successful write of fullBlock report numberBytesToWrite written back to caller
                    }
                }
            }
            return BytesWritten;
        }

        public async Task<int> ReadSetMemberAsync(Guid fileid, long filelength, byte[] bufferToFill, ulong deviceOffsetToReadFrom, int numberBytesToRead)
        {
            return await SetMemberIOOperationAsync(fileid, filelength, false, bufferToFill, deviceOffsetToReadFrom, numberBytesToRead).ConfigureAwait(false);
        }


        internal async Task<int> SetMemberIOOperationAsync(Guid fileid, long filelength, bool isWriteOperation,  byte[] bufferToOperateOn, ulong deviceOffsetToOperateAt, int numberBytesInRequest)
        {
            // if( numberBytesToWrite > 4096 )
            //Debug.Print( "SetManager.WriteSetMemberAsync:: ENTER -  deviceOffsetToWriteTo={0}, numberBytesToWrite={1}", deviceOffsetToWriteTo, numberBytesToWrite );
            // NOTE:	WriteSetMemberAsync() differs from  WriteTableSetMemberAsync() in that a write to a Set can span multiple blocks whereas a 
            //			a write to a TableSet is always just for one complete record.  Also a TableSet has a schema that must be considered whereas
            //			a regular Set has no schema

            // If we are writing to the start of the file we assume its our first write and therefore write a recovery block.
            // NOTE: scenarios where the first block is written to after the file already exists just causes additional recovery block updates to be written,
            //       but is not otherwise harmful
            //  %TODO% - skip writing the recovery block if the file is opened in readonly mode!?!
            try
            {
                //if (isWriteOperation && deviceOffsetToOperateAt == 0)
                //{
                //    if (!await _WriteSetRecoveryBlock(pSetInstanceInfo))
                //        return 0;
                //}

                int BytesOperatedOn = 0;
                int blockIndex = 0;
                long startblock = -1;
                uint maxBlockSize = storageBlockSize;
                int cid = 10;       // %TODO% - for now
                int sdid = 10;      // %TODO% - for now
                Guid scid = VolumeID;
                Guid setid = fileid;
                IntPtr pwdPtr = IntPtr.Zero;
                ulong endofset = Convert.ToUInt64(filelength);
                //Key32 password = SetInstanceInfo.GetSetContextExtended(pSetInstanceInfo, out cid, out sdid, out scid, out setid, out maxBlockSize, out endofset);
                Key32 password = new Key32();  // %TODO% - for now
                startblock = (long)(deviceOffsetToOperateAt / (uint)maxBlockSize);
                var isoffsetaligned = ((long)(deviceOffsetToOperateAt % (uint)maxBlockSize) == 0);
                var isbytestowritealigned = ((uint)numberBytesInRequest % maxBlockSize == 0);
                long numberofcompleteblocks = ((uint)numberBytesInRequest / maxBlockSize);
                long numberofpartialblocks = (isbytestowritealigned ? 0L : 1L);
                bool needToPreReadFirstBlock = !isoffsetaligned || ((numberBytesInRequest < maxBlockSize) && ((ulong)numberBytesInRequest != endofset));
                bool needToPreReadLastBlock = false;
                byte[] preReadFirstBlockBuffer = null;
                byte[] preReadLastBlockBuffer = null;
                var numblocks = 0L;
                #region Determine numblocks
                if (isoffsetaligned)
                {
                    numblocks = (numberofcompleteblocks + numberofpartialblocks);
                }
                else// Special handling when offset not aligned on BLOCK_SIZE boundary since can result in needing to consider an extra block
                {
                    // Determine if we cross an alignment boundary
                    long crossboundary = 0L;
                    if ((uint)numberBytesInRequest % maxBlockSize == 0)
                    {
                        crossboundary += (((deviceOffsetToOperateAt % (ulong)maxBlockSize) + (uint)numberBytesInRequest) > (ulong)maxBlockSize ? 1L : 0L);
                    }
                    else
                    {
                        crossboundary += (((deviceOffsetToOperateAt % (ulong)maxBlockSize) + (ulong)((uint)numberBytesInRequest % maxBlockSize)) > (ulong)maxBlockSize ? 1L : 0L);
                    }
                    numblocks = numberofcompleteblocks + numberofpartialblocks + crossboundary;
                    needToPreReadLastBlock = (crossboundary == 1);
                }
                #endregion
                Task<int>[] OperationTasks = new Task<int>[numblocks];
                int[] BlockOperationOffsets = new int[numblocks];
                int[] OperationByteCounts = new int[numblocks];
                long startblockOffset = ((long)deviceOffsetToOperateAt % maxBlockSize);
                long bufferoffset = 0L;
                int TotalBytesToBeOperatedOn = 0;

                #region Compute the BlockOperationOffsets and OperationByteCounts for the first block
                if (isoffsetaligned && ((uint)numberBytesInRequest) >= maxBlockSize)
                {
                    BlockOperationOffsets[0] = 0;
                    OperationByteCounts[0] = (int)maxBlockSize;
                }
                else
                {
                    if (isoffsetaligned)  // means ((uint)numberBytesToWrite)< maxBlockSize
                    {
                        BlockOperationOffsets[0] = 0;
                        if (isWriteOperation)
                        {
                            OperationByteCounts[0] = Math.Min(numberBytesInRequest, (int)maxBlockSize);  // Write in full blocks
                        }
                        else
                        {
                            OperationByteCounts[0] = numberBytesInRequest;  // Read only what we need
                        }
                    }
                    else // means !isoffsetaligned
                    {
                        BlockOperationOffsets[0] = (int)startblockOffset;
                        //if (deviceOffsetToOperateAt + (uint)numberBytesInRequest >= (ulong)maxBlockSize)
                        if ((deviceOffsetToOperateAt % (ulong)maxBlockSize) + (uint)numberBytesInRequest >= (ulong)maxBlockSize)
                        {
                            if (isWriteOperation)
                            {
                                OperationByteCounts[0] = (int)((ulong)maxBlockSize - (deviceOffsetToOperateAt % (ulong)maxBlockSize));
                            }
                            else
                            {
                                if (numblocks > 1)
                                {
                                    OperationByteCounts[0] = (int)((ulong)maxBlockSize - (deviceOffsetToOperateAt % (ulong)maxBlockSize));
                                }
                                else
                                {
                                    OperationByteCounts[0] = numberBytesInRequest;
                                }

                            }
                        }
                        else // means ((uint)numberBytesToWrite)< maxBlockSize
                        {
                            if (isWriteOperation)
                            {
                                OperationByteCounts[0] = Math.Min((int)maxBlockSize, numberBytesInRequest);
                            }
                            else
                            {
                                OperationByteCounts[0] = numberBytesInRequest;
                            }
                        }
                    }
                }
                #endregion
                long bytesRemaining = numberBytesInRequest - OperationByteCounts[0];
                #region  Compute the BlockOperationOffsets and OperationByteCounts for 2nd and subsequent blocks
                for (int i = 1; i < numblocks; i++)
                {
                    #region OperationByteCounts
                    if (bytesRemaining >= maxBlockSize)
                    {
                        OperationByteCounts[i] = (int)maxBlockSize;
                        bytesRemaining -= maxBlockSize;
                    }
                    else
                    {
                        OperationByteCounts[i] = (int)bytesRemaining;
                        bytesRemaining = 0L;
                    }
                    #endregion

                    #region BlockOperationOffsets
                    if (isWriteOperation)
                    {
                        // %TODOO - This looks wrong - it should always be Zero for 2nd and subsequent blocks
                        //BlockOperationOffsets[i] = BlockOperationOffsets[i - 1] + (!isoffsetaligned && ((uint)numberBytesInRequest) < maxBlockSize ? numberBytesInRequest : OperationByteCounts[i - 1]);
                        BlockOperationOffsets[i] = 0;
                    }
                    else
                    {
                        BlockOperationOffsets[i] = 0;
                    }
                    #endregion
                }
                #endregion

                #region Pre-Read for Write Operations
                if (isWriteOperation)
                {
                    if (needToPreReadFirstBlock)
                    {
                        preReadFirstBlockBuffer = new byte[maxBlockSize];
                        var bytesRead = await PerformReadTaskAsync(preReadFirstBlockBuffer, 0, (ulong)(startblock + 1), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password).ConfigureAwait(false);

                    }
                    if (needToPreReadLastBlock)
                    {
                        preReadLastBlockBuffer = new byte[maxBlockSize];
                        var bytesRead = await PerformReadTaskAsync(preReadLastBlockBuffer, 0, (ulong)(startblock + numblocks), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password).ConfigureAwait(false);
                    }
                }
                else
                {
                    if (needToPreReadFirstBlock)
                    {
                        preReadFirstBlockBuffer = new byte[maxBlockSize];
                        var bytesRead = await PerformReadTaskAsync(preReadFirstBlockBuffer, 0, (ulong)(startblock + 1), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password).ConfigureAwait(false);
                    }
                    if (needToPreReadLastBlock)
                    {
                        preReadLastBlockBuffer = new byte[maxBlockSize];
                        var bytesRead = await PerformReadTaskAsync(preReadLastBlockBuffer, 0, (ulong)(startblock + numblocks), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password).ConfigureAwait(false);
                    }
                }
                #endregion
                for (long block = startblock; block < startblock + numblocks; block++)
                {
                    bool blockProcessed = false;
                    #region Special case First block
                    if (block == startblock)
                    {
                        if (isWriteOperation)
                        {
                            if (needToPreReadFirstBlock)
                            {
                                //Debug.Print("C");
                                //Debug.Print($"bufferToEmpty.Length={bufferToEmpty.Length}, readBufferFirstBlock.Length={readBufferFirstBlock.Length}, BlockWriteOffsets[blockIndex]={BlockWriteOffsets[blockIndex]}");
                                // Copy data to write to the read first buffer
                                if (OperationByteCounts[blockIndex] > bufferToOperateOn.Length || BlockOperationOffsets[blockIndex] + OperationByteCounts[blockIndex] > preReadFirstBlockBuffer.Length)
                                {
                                    Debug.Print($"PROBLEM(1), BlockOperationOffsets[blockIndex]={BlockOperationOffsets[blockIndex]}, OperationByteCounts[blockIndex]={OperationByteCounts[blockIndex]}, " +
                                        $"preReadLastBlockBuffer.Length={preReadFirstBlockBuffer.Length}, bufferoffset={bufferoffset}, bufferToOperateOn.Length={bufferToOperateOn.Length}, isWriteOperation={isWriteOperation}, deviceOffsetToOperateAt={deviceOffsetToOperateAt}, numberBytesInRequest ={numberBytesInRequest}");
                                }
                                //Buffer.BlockCopy(bufferToOperateOn, 0, preReadFirstBlockBuffer, BlockOperationOffsets[blockIndex], numberBytesInRequest);
                                Buffer.BlockCopy(bufferToOperateOn, 0, preReadFirstBlockBuffer, BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex]);
                                //Debug.Print($"D1a -  block={block + 1}, startblockOffset={0}, bytesToWrite={readBufferFirstBlock.Length},  bufferoffset={bufferoffset} ");
                                OperationTasks[blockIndex] = PerformWriteTaskAsync(preReadFirstBlockBuffer, 0, (ulong)(block + 1), 0, preReadFirstBlockBuffer.Length, false, cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += preReadFirstBlockBuffer.Length;
                            }
                            else
                            {
                                //Debug.Print($"D1b -  block={block + 1}, startblockOffset={BlockWriteOffsets[blockIndex]}, bytesToWrite={WriteCounts[blockIndex]},   bufferoffset={bufferoffset}, needToReadLastBlock={needToReadLastBlock} ");
                                OperationTasks[blockIndex] = PerformWriteTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], false, cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                            }
                        }
                        else  // Read Operation
                        {
                            if (needToPreReadFirstBlock)
                            {
                                //preReadFirstBlockBuffer = new byte[maxBlockSize];
                                ////Debug.Print($"START needToReadFirstBlock - block={startblock + 1} ");
                                OperationTasks[blockIndex] = Task.FromResult(OperationByteCounts[blockIndex]);
                                ////OperationTasks[blockIndex] = PerformReadTaskAsync(preReadFirstBlockBuffer, 0, (ulong)(startblock + 1), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password);
                                //OperationTasks[blockIndex] = PerformReadTaskAsync(preReadFirstBlockBuffer, 0, (ulong)(block + 1), 0, (int)maxBlockSize, cid, sdid, scid, setid, 0, password);
                                // Now copy bytesRead into operation buffer
                                if (BlockOperationOffsets[blockIndex] + OperationByteCounts[blockIndex] > preReadFirstBlockBuffer.Length || bufferoffset + OperationByteCounts[blockIndex] > bufferToOperateOn.Length)
                                {
                                    Debug.Print($"PROBLEM(2), BlockOperationOffsets[blockIndex]={BlockOperationOffsets[blockIndex]}, OperationByteCounts[blockIndex]={OperationByteCounts[blockIndex]}, " +
                                        $"preReadLastBlockBuffer.Length={preReadFirstBlockBuffer.Length}, bufferoffset={bufferoffset}, bufferToOperateOn.Length={bufferToOperateOn.Length}, isWriteOperation={isWriteOperation}, deviceOffsetToOperateAt={deviceOffsetToOperateAt},  numberBytesInRequest={numberBytesInRequest}");
                                }
                                Buffer.BlockCopy(preReadFirstBlockBuffer, BlockOperationOffsets[blockIndex], bufferToOperateOn, (int)bufferoffset, OperationByteCounts[blockIndex]);
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                                // Debug.Print($"END needToReadFirstBlock - block={startblock + 1}, bytesRead={bytesRead}");
                            }
                            else
                            {
                                //if (bufferoffset + OperationByteCounts[blockIndex] > bufferToOperateOn.Length )
                                //{
                                //    Debug.Print("PROBLEM(2)");
                                //}
                                OperationTasks[blockIndex] = PerformReadTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                            }
                        }
                        blockProcessed = true;
                    }
                    #endregion

                    #region Zero or more Middle blocks
                    if (!blockProcessed && (block > startblock) && (block < (startblock + numblocks - 1)))
                    {
                        if (isWriteOperation)
                        {
                            OperationTasks[blockIndex] = PerformWriteTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], false, cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                        }
                        else
                        {
                            OperationTasks[blockIndex] = PerformReadTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                        }
                        TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                        blockProcessed = true;
                    }
                    #endregion

                    #region Special case Last block
                    if (!blockProcessed && block == (startblock + numblocks - 1))
                    {
                        if (isWriteOperation)
                        {
                            if (needToPreReadLastBlock)
                            {
                                // Copy data to write to the read last buffer
                                if (bufferoffset + OperationByteCounts[blockIndex] > bufferToOperateOn.Length || OperationByteCounts[blockIndex] > preReadLastBlockBuffer.Length)
                                {
                                    Debug.Print($"PROBLEM(3), BlockOperationOffsets[blockIndex]={BlockOperationOffsets[blockIndex]}, OperationByteCounts[blockIndex]={OperationByteCounts[blockIndex]}, " +
                                        $"preReadLastBlockBuffer.Length={preReadLastBlockBuffer.Length}, bufferoffset={bufferoffset}, bufferToOperateOn.Length={bufferToOperateOn.Length}, isWriteOperation={isWriteOperation}, deviceOffsetToOperateAt={deviceOffsetToOperateAt}, numberBytesInRequest={numberBytesInRequest}");
                                }
                                Buffer.BlockCopy(bufferToOperateOn, (int)bufferoffset, preReadLastBlockBuffer, 0, OperationByteCounts[blockIndex]);
                                //Debug.Print($"DLa -  block={block + 1}, startblockOffset={0}, bytesToWrite={readBufferLastBlock.Length},  bufferoffset={bufferoffset} ");
                                OperationTasks[blockIndex] = PerformWriteTaskAsync(preReadLastBlockBuffer, 0, (ulong)(block + 1), 0, preReadLastBlockBuffer.Length, false, cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += preReadLastBlockBuffer.Length;
                            }
                            else
                            {
                                // Debug.Print($"DLb -  block={block + 1}, startblockOffset={BlockWriteOffsets[blockIndex]}, bytesToWrite={WriteCounts[blockIndex]},  bufferoffset={bufferoffset} ");
                                OperationTasks[blockIndex] = PerformWriteTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], false, cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                            }
                        }
                        else
                        {
                            if (needToPreReadLastBlock)
                            {
                                // Now copy bytesRead into operation buffer
                                OperationTasks[blockIndex] = Task.FromResult(OperationByteCounts[blockIndex]);
                                //if (BlockOperationOffsets[blockIndex] + OperationByteCounts[blockIndex] > preReadLastBlockBuffer.Length || bufferoffset + OperationByteCounts[blockIndex] > bufferToOperateOn.Length)
                                //{
                                //    Debug.Print($"PROBLEM(4), BlockOperationOffsets[blockIndex]={BlockOperationOffsets[blockIndex]}, OperationByteCounts[blockIndex]={OperationByteCounts[blockIndex]}, " +
                                //        $"preReadLastBlockBuffer.Length={preReadLastBlockBuffer.Length}, bufferoffset={bufferoffset}, bufferToOperateOn.Length={bufferToOperateOn.Length}, isWriteOperation={isWriteOperation}, deviceOffsetToOperateAt={deviceOffsetToOperateAt}, numberBytesInRequest={numberBytesInRequest}");
                                //}
                                Buffer.BlockCopy(preReadLastBlockBuffer, BlockOperationOffsets[blockIndex], bufferToOperateOn, (int)bufferoffset, OperationByteCounts[blockIndex]);
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                            }
                            else
                            {
                                // Debug.Print($"DLb -  block={block + 1}, startblockOffset={BlockWriteOffsets[blockIndex]}, bytesToWrite={WriteCounts[blockIndex]},  bufferoffset={bufferoffset} ");
                                OperationTasks[blockIndex] = PerformReadTaskAsync(bufferToOperateOn, (int)bufferoffset, (ulong)(block + 1), BlockOperationOffsets[blockIndex], OperationByteCounts[blockIndex], cid, sdid, scid, setid, 0, password); // NOTE:  (block+1) so that block ordinals are 1s based - leaving block 0 for the recovery block, last zero since File Sets have no schemaversion
                                TotalBytesToBeOperatedOn += OperationByteCounts[blockIndex];
                            }
                        }
                    }
                    #endregion

                    bufferoffset += OperationByteCounts[blockIndex];
                    blockIndex++;
                } // for (long block = startblock; block < startblock + numblocks; block++)
                // Wait until all operational tasks complete
                int[] results = await Task.WhenAll(OperationTasks).ConfigureAwait(false);
                foreach (var r in results)
                {
                    BytesOperatedOn += r;
                }
                // We check if BytesOperatedOn == TotalBytesToBeOperatedOn and if so return numberBytesInRequest because when partial blocks are requested to be operated on
                // (i.e.; blocks that don't align on maxBlockSize boundaries) we can actually end up reading/writing MORE than the numberBytesInRequest in order to correctly write 
                // the requested number of bytes.  However, we can't return that number back to the caller since they would see it as an error.  By checking
                // if BytesOperatedOn == TotalBytesToBeOperatedOn we know that we have written the correct (if not more than requested) amount of data and can therefore
                // report back to the caller that numberBytesInRequest were in fact written (in addition to other data necessary to maintain system correctness).
                //if (isWriteOperation)
                //{
                if (isWriteOperation)
                {
                    if (BytesOperatedOn == TotalBytesToBeOperatedOn)
                    {
                        return numberBytesInRequest;
                    }
                    return BytesOperatedOn;
                }
                else
                {
                    return TotalBytesToBeOperatedOn;
                }
            }
            catch (Exception ex)
            {
                Debug.Print($"^^^BlockManager.SetMemberIOOperationAsync() - Error {ex.Message}, {ex.StackTrace}");
                throw;
            }
        }

        private async Task<int> PerformReplicaReadAsync(BlockMetaData bmd, int replicaNodeNumber, byte[] bufferToFill, int bufferoffset, int bytesToRead)
        {
            var blockFileName = Path.Combine(string.Format(clientDataPathTemplate, replicaNodeNumber), $"{bmd.SETID.ToString("N").ToUpper()}{bmd.BlockOrdinal.ToString()}");
            var bytesRead = 0;
            using (var fs = new FileStream(blockFileName, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                bytesRead = await fs.ReadAsync(bufferToFill, bufferoffset, bytesToRead).ConfigureAwait(false);
                await fs.FlushAsync().ConfigureAwait(false);
                fs.Close();
            }
            return bytesRead;
        }

        private async Task<int> PerformClusterReadAsync(BlockMetaData bmd, int cluster, byte[] bufferToFill, int bufferoffset, int bytesToRead)
        {
            Task<int>[] replicaReadTask = new Task<int>[vDiskReplicationFactor];
            for (int replica = 0; replica < vDiskReplicationFactor; replica++)
            {
                //Debug.Print($"SetManager.PerformClusterReadAsync() creating async parallel Task for block ordinal {bmd.BlockOrdinal}, ({cluster + 1},{replica + 1}) node: {vDiskStorageArray[cluster, replica]}");
                replicaReadTask[replica] = PerformReplicaReadAsync(bmd, vDiskStorageArray[cluster, replica], bufferToFill, bufferoffset, bytesToRead);
            }
            var taskResults = await Task.WhenAny(replicaReadTask).ConfigureAwait(false);
            return bytesToRead;
        }

        private async Task<int> PerformReadTaskAsync(byte[] bufferToFill, int bufferoffset, ulong block, int blockoffset, int bytesToRead, int cid, int sdid, Guid scid, Guid setid, byte schemaVersion, Key32 password)
        {
            byte[] setidBytes = setid.ToByteArray();
            string ord = Constants.baseHexGuid + block.ToString("X");
            ord = ord.Substring(ord.Length - Constants.baseHexGuidLength);
            byte[] ordBytes = ConvertHexStringToBytes(ord);
            byte[] byteCountBytes = BitConverter.GetBytes(bytesToRead);
            //unsafe
            //{
            //    fixed (byte* pSetid = setidBytes, pOrdBytes = ordBytes, pByteCountBytes = byteCountBytes, pSetDomId = setdomidBytes)
            //    {
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)(pSetid), 16, (IntPtr)pSetDomId, 16);
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)pOrdBytes, 16, (IntPtr)pSetDomId, 16);
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)pByteCountBytes, sizeof(int), (IntPtr)pSetDomId, 16);
            //    }
            //}

            BlockMetaData bmd = new BlockMetaData(storageBlockSize, bufferoffset, block, ordBytes, bytesToRead, BitConverter.ToUInt32(byteCountBytes, 0), cid, sdid,
                     BitConverter.ToInt32(crc32.ComputeHash(scid.ToByteArray()), 0), setid, schemaVersion, BitConverter.ToUInt32(crc32.ComputeHash(setidBytes), 0));
            await PerformClusterReadAsync(bmd, ComputeCluster(bmd), bufferToFill, bufferoffset, bytesToRead).ConfigureAwait(false);
            return bytesToRead;
            //return bytesToRead;// $TODO%  For now
            //bmd.BlockOffset = blockoffset;
            //BlockIOResponse blkResponse = await IOManager.ReadSetMemberAsync(bufferToFill, bmd).ConfigureAwait(false);
            ////#region  Decrypt the block
            ////unsafe
            ////{
            ////    fixed (byte* p = bufferToFill, pSetDomId = setdomidBytes)
            ////    {
            ////        defaultAppDomainManager.DecryptBuffer((IntPtr)(p + bufferoffset), bytesToRead, (IntPtr)(&password), 32);
            ////    }
            ////}
            ////#endregion
            //return blkResponse.ByteCount;
        }

        private async Task<int> PerformReplicaWriteAsync(BlockMetaData bmd, int replicaNodeNumber, byte[] bufferToEmpty, int bufferoffset, int bytesToWrite )
        {
            var blockFileName = Path.Combine(string.Format(clientDataPathTemplate, replicaNodeNumber), $"{bmd.SETID.ToString("N").ToUpper()}{(bmd.BlockOrdinal).ToString()}");
            using (var fs = new FileStream(blockFileName, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await fs.WriteAsync(bufferToEmpty, bufferoffset, bytesToWrite).ConfigureAwait(false);
                await fs.FlushAsync().ConfigureAwait(false);
                fs.Close();
            }
            return bytesToWrite;
        }

        private async Task<int> PerformClusterWriteAsync(BlockMetaData bmd, int cluster, byte[] bufferToEmpty, int bufferoffset, int bytesToWrite)
        {
            Task<int>[] replicaWriteTask = new Task<int>[vDiskReplicationFactor];
            for(int replica = 0; replica < vDiskReplicationFactor; replica++)
            {
                //Debug.Print($"SetManager.PerformClusterWriteAsync() creating async parallel Task for block ordinal {bmd.BlockOrdinal}, ({cluster+1},{replica+1}) node: {vDiskStorageArray[cluster, replica]}");
                replicaWriteTask[replica] = PerformReplicaWriteAsync(bmd, vDiskStorageArray[cluster,replica], bufferToEmpty, bufferoffset,bytesToWrite);
            }
            var taskResults = await Task.WhenAll(replicaWriteTask).ConfigureAwait(false);
            return bytesToWrite;
        }

        private async Task<int> PerformWriteTaskAsync(byte[] bufferToEmpty, int bufferoffset, ulong block, int blockoffset, int bytesToWrite, bool isPartialBlock, int cid, int sdid, Guid scid, Guid setid, byte schemaVersion, Key32 password)
        {
            byte[] setidBytes = setid.ToByteArray();
            string ord = Constants.baseHexGuid + block.ToString("X");
            ord = ord.Substring(ord.Length - Constants.baseHexGuidLength);
            byte[] ordBytes = ConvertHexStringToBytes(ord);
            byte[] byteCountBytes = BitConverter.GetBytes(bytesToWrite);
            #region Encrypt the block
            //unsafe
            //{
            //    fixed (byte* p = bufferToEmpty, pSetid = setidBytes, pOrdBytes = ordBytes, pByteCountBytes = byteCountBytes, pSetDomId = setdomidBytes)
            //    {
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)(p + bufferoffset), bytesToWrite, (IntPtr)(&password), 32);
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)pSetid, 16, (IntPtr)pSetDomId, 16);
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)pOrdBytes, 16, (IntPtr)pSetDomId, 16);
            //        defaultAppDomainManager.EncryptBuffer((IntPtr)pByteCountBytes, sizeof(int), (IntPtr)pSetDomId, 16);
            //    }
            //}
            #endregion
            BlockMetaData bmd = new BlockMetaData(storageBlockSize, bufferoffset, block, ordBytes, bytesToWrite, BitConverter.ToUInt32(byteCountBytes, 0), cid, sdid,
                 BitConverter.ToInt32(crc32.ComputeHash(scid.ToByteArray()), 0), setid, schemaVersion, BitConverter.ToUInt32(crc32.ComputeHash(setidBytes), 0),
                 DateTime.UtcNow.Ticks);
            //Debug.Print($"SetManager.PerformWriteTaskAsync() for block ordinal {bmd.BlockOrdinal}, getting sent to cluster {ComputeCluster(bmd) + 1}");
            bmd.BlockOffset = blockoffset;
            bmd.IsPartialBlock = isPartialBlock;
            await PerformClusterWriteAsync( bmd, ComputeCluster(bmd), bufferToEmpty, bufferoffset, bytesToWrite ).ConfigureAwait(false);
            return bytesToWrite;  // %TODO For Now
            //BlockIOResponse blkResponse = await IOManager.WriteSetMemberAsync(bufferToEmpty, bmd).ConfigureAwait(false);
            //return blkResponse.ByteCount;
        }


        private int ComputeCluster(BlockMetaData blockMetaData)
        {
            int cluster = -1;
            byte[] id = new byte[16 + sizeof(long)];
            Buffer.BlockCopy(blockMetaData.SETID.ToByteArray(), 0, id, 0, 16);
            Buffer.BlockCopy(BitConverter.GetBytes(blockMetaData.BlockOrdinal), 0, id, 16, sizeof(long));
            uint hash = BitConverter.ToUInt32(crc32.ComputeHash(id), 0);
            cluster = Convert.ToInt32(hash % vDiskClusterSize);
            return cluster; 
        }

        private static string ConvertBytesToHexString(byte[] bytes)
        {
            string sbytes = BitConverter.ToString(bytes);       // Convert to hyphen delimited string of hex characters
            return sbytes.Replace("-", "");
        }

        private static string ConvertBytesToHexString(byte[] buffer, int offset, int byteCount)
        {
            byte[] bytes = new byte[byteCount];
            Buffer.BlockCopy(buffer, offset, bytes, 0, byteCount);
            string sbytes = BitConverter.ToString(bytes);       // Convert to hyphen delimited string of hex characters
            return sbytes.Replace("-", "");
        }

        private static byte[] ConvertHexStringToBytes(string hexString)
        {
            // Convert Hex string to byte[]
            byte[] HexAsBytes = new byte[hexString.Length / 2];
            for (int index = 0; index < HexAsBytes.Length; index++)
            {
                string byteValue = hexString.Substring(index * 2, 2);
                HexAsBytes[index] = byte.Parse(byteValue, System.Globalization.NumberStyles.HexNumber, System.Globalization.CultureInfo.InvariantCulture);
            }
            return HexAsBytes;
        }

        private static byte[] ConvertStringToBytes(string String)
        {
            // Convert string to byte[]
            byte[] AsBytes = new byte[String.Length];
            for (int index = 0; index < AsBytes.Length; index++)
            {
                AsBytes[index] = (byte)String[index];
            }
            return AsBytes;
        }

        private static byte[] GenRandomBlock(int blockSize)
        {
            byte[] block = new byte[blockSize];
            for (int i = 0; i < blockSize; i++)
            {
                block[i] = (byte)(RandomNumberGenerator.GetInt32(256));    // Fill with a random byte
            }
            return block;
        }


        [Serializable]
        [StructLayout(LayoutKind.Explicit, Pack = 4)]
        internal struct Key32
        {
            [FieldOffset(0)] public Guid bBytesA;
            [FieldOffset(16)] public Guid bBytesB;
        }


        public class BlockFileInfo
        {
            #region Static field members
            static private int RelativeSetContainerIdOffset = (9 * 2);                                                     // 00000000\00000000\
            static private int RelativeSetIdOffset = (9 * 3);                                                              // 00000000\00000000\00000000\
            static private int RelativeBlockNameOffset = (9 * 4);                                                          // 00000000\00000000\00000000\00000000\

            static private string ScidReadPlaceHolder = Constants.baseHexInt32;
            static private string ScidWritePlaceHolder = Constants.baseHexInt32;
            static private int ScidPlaceHolderLength = ScidWritePlaceHolder.Length;
            static private int ScidPlaceHolderOffset = 0;                                                                  // (0 - first byte of ScidHash)

            static private string BlockTypeReadPlaceHolder = Constants.baseHexInt4;
            static private string BlockTypeWritePlaceHolder = Constants.baseHexInt4;
            static private int BlockTypePlaceHolderLength = BlockTypeWritePlaceHolder.Length;
            static private int BlockTypePlaceHolderOffset = ScidPlaceHolderOffset + ScidPlaceHolderLength;                // 00000000 (8 - first byte of BlockType)

            static private int OrdinalPlaceHolderLength = Constants.baseHexGuidLength;
            static private int OrdinalPlaceHolderOffset = BlockTypePlaceHolderOffset + BlockTypePlaceHolderLength;        // 000000000 (9 - first byte of BlockType)

            static private int CrcContentPlaceHolderLength = Constants.CrcContentWritePlaceHolder.Length;
            static private int CrcContentPlaceHolderOffset = OrdinalPlaceHolderOffset + OrdinalPlaceHolderLength;          // 00000000000000000000000000000000000000000  (41 - first byte of CrcCntnt)
            static private int CrcNamePlaceHolderLength = Constants.CrcNameWritePlaceHolder.Length;
            static private int CrcNamePlaceHolderOffset = CrcContentPlaceHolderOffset + CrcContentPlaceHolderLength;       // 0000000000000000000000000000000000000000000000000  (49 - first byte of CrcName)

            static private string FlagsReadPlaceHolder = Constants.baseHexInt4;
            static private string FlagsWritePlaceHolder = Constants.baseHexInt4;
            static private int FlagsPlaceHolderLength = FlagsWritePlaceHolder.Length;
            static private int FlagsPlaceHolderOffset = CrcNamePlaceHolderOffset + CrcNamePlaceHolderLength;               // 000000000000000000000000000000000000000000000000000000000  (57 - first byte of Flags)

            static private string UnusedReadPlaceHolder = Constants.baseHexInt8;
            static private string UnusedWritePlaceHolder = Constants.baseHexInt8;
            static private int UnusedPlaceHolderLength = UnusedWritePlaceHolder.Length;
            static private int UnusedPlaceHolderOffset = FlagsPlaceHolderOffset + FlagsPlaceHolderLength;                  // 00000000000000000000000000000000000000000000000000000000000  (59 - first byte of Unused)



            static private int BytesUsedPlaceHolderLength = Constants.BytesUsedReadPlaceHolder.Length;
            static private int BytesUsedPlaceHolderOffset = UnusedPlaceHolderOffset + UnusedPlaceHolderLength;             // 000000000000000000000000000000000000000000000000000000000000  (60 - first byte of BytesUsed)


            static private string NextPtrWritePlaceHolder = Constants.baseHexGuid;
            static private int NextPtrPlaceHolderLength = NextPtrWritePlaceHolder.Length;
            static private int NextPtrPlaceHolderOffset = BytesUsedPlaceHolderOffset + BytesUsedPlaceHolderLength;

            static private string PreviousPtrReadPlaceHolder = Constants.baseHexGuid;
            static private string PreviousPtrWritePlaceHolder = Constants.baseHexGuid;
            static private int PreviousPtrPlaceHolderLength = PreviousPtrWritePlaceHolder.Length;
            static private int PreviousPtrPlaceHolderOffset = NextPtrPlaceHolderOffset + NextPtrPlaceHolderLength;

            static private string IsDeletedReadPlaceHolder = Constants.baseHexInt4;
            static private string IsDeletedWritePlaceHolder = Constants.baseHexInt4;
            static private int IsDeletedPlaceHolderLength = IsDeletedWritePlaceHolder.Length;
            static private int IsDeletedPlaceHolderOffset = PreviousPtrPlaceHolderOffset + PreviousPtrPlaceHolderLength;

            static private string KeyLengthReadPlaceHolder = Constants.baseHexInt16;
            static private string KeyLengthdWritePlaceHolder = Constants.baseHexInt16;
            static private int KeyLengthPlaceHolderLength = KeyLengthdWritePlaceHolder.Length;
            static private int KeyLengthPlaceHolderOffset = IsDeletedPlaceHolderOffset + IsDeletedPlaceHolderLength;

            static private string MaxKeyCountWritePlaceHolder = Constants.baseHexInt32;
            static private int MaxKeyCountPlaceHolderLength = MaxKeyCountWritePlaceHolder.Length;
            static private int MaxKeyCountPlaceHolderOffset = KeyLengthPlaceHolderOffset + KeyLengthPlaceHolderLength;

            static public int LastModifiedTimeStampPlaceHolderLength = Constants.LastModifiedTimeStampReadPlaceHolder.Length;
            static public int LastModifiedTimeStampPlaceHolderOffset = MaxKeyCountPlaceHolderOffset + MaxKeyCountPlaceHolderLength;

            static public int SchemaVersionPlaceHolderLength = Constants.SchemaVersionReadPlaceHolder.Length;
            static private int SchemaVersionPlaceHolderOffset = LastModifiedTimeStampPlaceHolderOffset + LastModifiedTimeStampPlaceHolderLength;
            static public int AbsoluteBlockIdLength = RelativeBlockNameOffset + SchemaVersionPlaceHolderOffset + SchemaVersionPlaceHolderLength;
            static public int JournalEntryKeyLength = AbsoluteBlockIdLength - CrcContentPlaceHolderLength - CrcNamePlaceHolderLength + 9 + 8; // 9 = PeerSetId.ToString("X8") + pathSeparator, 8 = suffixing crcPlaceHolder
            static public int JournalEntryMessageSize = JournalEntryKeyLength + sizeof(ulong);
            static public int RelativeBlockIdLength = AbsoluteBlockIdLength - RelativeBlockNameOffset;
            //static private int BytesPerSector = 0;  // Kernel will set this value on startup
            #endregion

            #region Instance field members
            protected string blockId = null;
            private string storageRoot = null;
            private string cidHash = null;
            private string sdidHash = null;
            private string scidHash = null;
            private string setidHash = null;
            private string ordinal = null;
            private string crccontent = null;
            private string crcname = null;
            private string flags = null;
            private string unused = null;
            private string bytesUsed = null;
            private string nextPtr = null;
            private string previousPtr = null;
            private string blockType = null;
            private string isDeleted = null;
            private string keyLength = null;
            private string maxKeyCount = null;
            private string lastmodifiedtimestamp = null;
            private string schemaversion = null;
            private string directoryPortion = null;
            #endregion

            #region Constructors
            public BlockFileInfo()
            {
                // This constructor creates a block filled with '?'
                directoryPortion = @"00000000\00000000\00000000\00000000\";
                blockId = directoryPortion + new String('?', RelativeBlockIdLength);
            }
            public BlockFileInfo(string blockid)
            {
                blockId = blockid;
                if (blockId.Length != 199)
                {
                    Debug.Print("BlockFileInfo.ctor(1) - blockId.Length = {0} - {1} -> {2}", blockId.Length, blockId, Environment.StackTrace);
                }
            }


            public BlockFileInfo(string blockid, string storageroot) : this(blockid)
            {
                storageRoot = storageroot;
            }

            public BlockFileInfo(string blockid, bool isWriteBlock) : this(blockid)
            {
                CrcContent = (isWriteBlock ? Constants.CrcContentWritePlaceHolder : Constants.CrcContentReadPlaceHolder);                                // Stubbed for now
                CrcName = (isWriteBlock ? Constants.CrcNameWritePlaceHolder : Constants.CrcNameReadPlaceHolder);                                         // Stubbed for now
                                                                                                                                                         //flags = (isWriteBlock ? FlagsWritePlaceHolder : FlagsReadPlaceHolder);
                                                                                                                                                         //unused = (isWriteBlock ? UnusedWritePlaceHolder : UnusedReadPlaceHolder);
                                                                                                                                                         //bytesUsed = string.Empty;
                                                                                                                                                         //if (isWriteBlock)
                                                                                                                                                         //{
                                                                                                                                                         //    if (blockMetaData.SchemaVersion == 0) // Is a file block?
                                                                                                                                                         //    {
                                                                                                                                                         //        if (blockMetaData.OperationByteCount == CacheBlockValue.BytesPerSector) // is file block full?
                                                                                                                                                         //        {
                                                                                                                                                         //            // NOTE:  Since most blocks (in filese > BytesPerSector in size) will be full, we signal this with uint.MaxValue.ToString("X8") so as 
                                                                                                                                                         //            //        not to give hints to hackers about what the ByteCountId is - for added "security through obscurity"
                                                                                                                                                         //            bytesUsed = uint.MaxValue.ToString("X8");
                                                                                                                                                         //        }
                                                                                                                                                         //        else  // not full so used ByteCountId (encoded bytesUsed)
                                                                                                                                                         //        {
                                                                                                                                                         //            bytesUsed = blockMetaData.ByteCountId.ToString("X8");
                                                                                                                                                         //        }
                                                                                                                                                         //    }
                                                                                                                                                         //    else  // Table block
                                                                                                                                                         //    {
                                                                                                                                                         //        bytesUsed = blockMetaData.ByteCountId.ToString("X8");
                                                                                                                                                         //    }
                                                                                                                                                         //}
                                                                                                                                                         //else
                                                                                                                                                         //{
                                                                                                                                                         //    bytesUsed = CacheBlockValue.BytesUsedReadPlaceHolder;
                                                                                                                                                         //}
                                                                                                                                                         //string lastmodifiedtimestamp = (isWriteBlock ? LastModifiedTimeStamp.ToString("X16") : CacheBlockValue.LastModifiedTimeStampReadPlaceHolder);
                                                                                                                                                         //lastmodifiedtimestamp = (isWriteBlock ? blockMetaData.TimeStampInTicks.ToString("X16") : CacheBlockValue.LastModifiedTimeStampReadPlaceHolder);
                                                                                                                                                         //schemaversion = (isWriteBlock ? blockMetaData.SchemaVersion.ToString("X2") : CacheBlockValue.SchemaVersionReadPlaceHolder);
            }

            public BlockFileInfo(BlockMetaData blockMetaData, bool isWriteBlock)
            {
                SimpleCrc32 crc32 = new SimpleCrc32();  // NOTE:  CANNOT make this an instance variable.  Must use a new unique version for each object
                cidHash = Constants.baseHexInt32 + blockMetaData.CID.ToString("X8");
                cidHash = cidHash.Substring(cidHash.Length - Constants.baseHexInt32Length);
                sdidHash = Constants.baseHexInt32 + blockMetaData.SDID.ToString("X8");
                sdidHash = sdidHash.Substring(sdidHash.Length - Constants.baseHexInt32Length);
                ordinal = ConvertBytesToHexString(blockMetaData.BlockId.ToByteArray());
                // We reverse the bytes of the Ordinal in order to make a more B-Tree friendly key for NTFS file lookups
                // i.e.; we want filenames to be differentiated earlier rather than later so instead of '00000...001' & '00000...002', etc.,
                // we will have '01000...000' and '02000...000'
                byte[] ordinalBytes = ConvertHexStringToBytes(ordinal);
                // Loop to "inplace reverse" a 16 byte structure
                for (int i = 0; i < 8; i++)
                {
                    byte b = ordinalBytes[i];
                    ordinalBytes[i] = ordinalBytes[15 - i];
                    ordinalBytes[15 - i] = b;
                }
                ordinal = ConvertBytesToHexString(ordinalBytes);
                crccontent = (isWriteBlock ? Constants.CrcContentWritePlaceHolder : Constants.CrcContentReadPlaceHolder);                                // Stubbed for now
                crcname = (isWriteBlock ? Constants.CrcNameWritePlaceHolder : Constants.CrcNameReadPlaceHolder);                                         // Stubbed for now
                flags = (isWriteBlock ? FlagsWritePlaceHolder : FlagsReadPlaceHolder);
                unused = (isWriteBlock ? UnusedWritePlaceHolder : UnusedReadPlaceHolder);
                bytesUsed = string.Empty;
                if (isWriteBlock)
                {
                    if (blockMetaData.SchemaVersion == 0) // Is a file block?
                    {
                        //if (blockMetaData.OperationByteCount == CacheBlockValue.BytesPerSector) // is file block full?
                        if (blockMetaData.OperationByteCount == blockMetaData.StorageBlockSize) // is file block full?
                        {
                            // NOTE:  Since most blocks (in filese > BytesPerSector in size) will be full, we signal this with uint.MaxValue.ToString("X8") so as 
                            //        not to give hints to hackers about what the ByteCountId is - for added "security through obscurity"
                            bytesUsed = uint.MaxValue.ToString("X8");
                        }
                        else  // not full so used ByteCountId (encoded bytesUsed)
                        {
                            bytesUsed = blockMetaData.ByteCountId.ToString("X8");
                        }
                    }
                    else  // Table block
                    {
                        bytesUsed = blockMetaData.ByteCountId.ToString("X8");
                    }
                }
                else
                {
                    bytesUsed = Constants.BytesUsedReadPlaceHolder;
                }
                //string lastmodifiedtimestamp = (isWriteBlock ? LastModifiedTimeStamp.ToString("X16") : CacheBlockValue.LastModifiedTimeStampReadPlaceHolder);
                lastmodifiedtimestamp = (isWriteBlock ? blockMetaData.TimeStampInTicks.ToString("X16") : Constants.LastModifiedTimeStampReadPlaceHolder);
                schemaversion = (isWriteBlock ? blockMetaData.SchemaVersion.ToString("X2") : Constants.SchemaVersionReadPlaceHolder);
                //string scidHash = BitConverter.ToUInt32( crc32.ComputeHash( blockMetaData.SCID.ToByteArray() ), 0 ).ToString( "X8" );
                scidHash = blockMetaData.SCID.ToString("X8");
                setidHash = BitConverter.ToUInt32(crc32.ComputeHash(blockMetaData.SETID.ToByteArray()), 0).ToString("X8");
                directoryPortion = Path.Combine(new string[]                                                            // TOTAL:  36 
                           {
                        cidHash,																					// Computer (9 - including \)
						sdidHash,																					// SetDomain (9 - including \)
						scidHash,																					// SetContainer (9 - including \)
						setidHash });                                                                               // setid   (9 - including \)

            }

            public BlockFileInfo(BlockFileInfo blockToBePatched, string crcNamePlaceHolder) : this(blockToBePatched.ToCrcName + crcNamePlaceHolder + blockToBePatched.FromFlags)
            {
                // This constructor creates a crcName patched BlockFileInfo from a supplied BlockFileInfo object, using the crcNamePlaceHolder supplied
                storageRoot = blockToBePatched.storageRoot;
            }

            public BlockFileInfo(string remoteblockid, byte[] blockBuffer, uint crcNamePlaceHolder, uint storageBlockSize) : this(remoteblockid)
            {
                // This constructor creates a BlockFileInfo from a remote BlockFileInfo
                blockId = SetCRC(blockBuffer, crcNamePlaceHolder, storageBlockSize).BlockID;
            }

            //public BlockFileInfo(JournalKey journalKey)
            //{
            //    // This constructor creates a BlockFileInfo from a JournalKey
            //    // To convert a JournalEntryKey to a JournalBlockId we:
            //    //	i) remove the prefixing PeerSetId and Seperator ('\') - a total of 9 bytes
            //    // ii) insert the crcContent and crcName with their read placeholders
            //    //iii) replace the LastModifiedTimeStamp with a read place holder
            //    // iv) strip off suffixing crcNamePlaceHolder
            //    blockId = journalKey.Key.Substring(9); // i)
            //    blockId = blockId.Substring(0, RelativeBlockNameOffset + CrcContentPlaceHolderOffset) + Constants.CrcContentReadPlaceHolder + Constants.CrcNameReadPlaceHolder
            //                 + journalKey.Key.Substring(9 + RelativeBlockNameOffset + CrcContentPlaceHolderOffset);  // ii)
            //    blockId = blockId.Substring(0, RelativeBlockNameOffset + LastModifiedTimeStampPlaceHolderOffset) + journalKey.LastModifiedTimeStamp +
            //                blockId.Substring(RelativeBlockNameOffset + SchemaVersionPlaceHolderOffset, SchemaVersionPlaceHolderLength); // iii) & iv)
            //    if (blockId.Length != 199)
            //    {
            //        Debug.Print("BlockFileInfo.ctor(2) - blockId.Length = {0} - {1} -> {2}", blockId.Length, blockId, Environment.StackTrace);
            //    }
            //}
            #endregion


            #region Public Properties
            #region blockId reliant properties
            public string BlockID
            {
                get
                {
                    if (!string.IsNullOrEmpty(blockId))
                    {
                        return blockId;
                    }
                    else
                    {
                        // Example:
                        //                                                                                                           U                                                                                                      S                                 
                        //                                                         B                                                Fn                                                                         I                            c
                        //                                                         T                                                lu                                                                         s                            h 
                        //                                                         y                                                as                                                                         D                            e
                        //                                                         p                                                ge                                                                         e                            m
                        // store      \cidHash \sdidHash\scidHash\setidhsh\ScidHasheOrdinal                         Crccnnt Crcname sd ByteusedNextPtr                         PreviousPtr                     lKLenMaxKeyCtLastModifiedTS  a
                        // <storeRoot>\00000000\00000000\00000000\00000000\0000000010000000000000000000000000000000011111111000000001001111111100000000000000000000000000000000111111111111111111111111111111110111100000000111111111111111100 

                        // NOTE:  Can't use Path.Combine call here since there can be "placeholders" such as * and | in
                        //		  the portions below that make up the filename.  Therefore must manually add the filename to the
                        //        above computed directoryPortion to create the fullpath.
                        blockId = BlockPath + Constants.platformIndependentPathSeparator +
                                    // block filename portion from here down...                                                // TOTAL:  163
                                    ScidHash +                                                                                 // ScidHash (8)
                                    Constants.baseHexInt4 +                                                                    // BlockType (1)
                                    Ordinal +                                                                                  // ordinal of block   (32)
                                    CrcContent +                                                                               // crc of file (block) content (8)
                                    CrcName +                                                                                  // crc of file (block) name  (8)
                                    Flags +                                                                                    // flags (reserved for future use) (1)
                                    Unused +                                                                                   // unused (2)
                                    BytesUsed +                                                                                // BytesUsed (8)
                                    Constants.baseHexGuid +                                                                    // NextPtr (32)
                                    Constants.baseHexGuid +                                                                    // PreviousPtr (32)
                                    Constants.baseHexInt4 +                                                                    // IsDeleted (1)
                                    Constants.baseHexInt16 +                                                                   // KeyLength (4)
                                    Constants.baseHexInt32 +                                                                   // MaxKeyLength (8)
                                    LastModifiedTimeStamp +                                                                    // LastModifiedTimeStamp (16)
                                    SchemaVersion                                                                              // ShcemaVersion (2)
                                    ;
                        if (blockId.Length != 199)
                        {
                            Debug.Print("BlockFileInfo.BlockID - blockId.Length = {0}, {1} -> {2}", blockId.Length, blockId, Environment.StackTrace);
                        }
                        return blockId;
                    }
                }
            }

            public string BlockPath
            {
                get
                {
                    if (!string.IsNullOrEmpty(directoryPortion))
                    {
                        return directoryPortion;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            directoryPortion = blockId.Substring(0, RelativeBlockNameOffset);
                            return directoryPortion;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }


            public string CidHash
            {
                get
                {
                    if (!string.IsNullOrEmpty(cidHash))
                    {
                        return cidHash;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            cidHash = blockId.Substring(0, Constants.baseHexInt32Length);
                            return cidHash;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string SdidHash
            {
                get
                {
                    if (!string.IsNullOrEmpty(sdidHash))
                    {
                        return sdidHash;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            sdidHash = blockId.Substring(9, Constants.baseHexInt32Length);
                            return sdidHash;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string SetidHash
            {
                get
                {
                    if (!string.IsNullOrEmpty(setidHash))
                    {
                        return setidHash;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            setidHash = blockId.Substring(27, Constants.baseHexInt32Length);
                            return setidHash;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }



            public string ScidHash
            {
                get
                {
                    if (!string.IsNullOrEmpty(scidHash))
                    {
                        return scidHash;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            scidHash = blockId.Substring(RelativeBlockNameOffset + ScidPlaceHolderOffset, ScidPlaceHolderLength);
                            return scidHash;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }


            public string BlockType
            {
                get
                {
                    if (!string.IsNullOrEmpty(blockType))
                    {
                        return blockType;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            blockType = blockId.Substring(RelativeBlockNameOffset + BlockTypePlaceHolderOffset, BlockTypePlaceHolderLength);
                            return blockType;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string Ordinal
            {
                get
                {
                    if (!string.IsNullOrEmpty(ordinal))
                    {
                        return ordinal;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            ordinal = blockId.Substring(RelativeBlockNameOffset + OrdinalPlaceHolderOffset, OrdinalPlaceHolderLength);
                            return ordinal;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }


            public string CrcContent
            {
                get
                {
                    if (!string.IsNullOrEmpty(crccontent))
                    {
                        return crccontent;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            crccontent = blockId.Substring(RelativeBlockNameOffset + CrcContentPlaceHolderOffset, CrcContentPlaceHolderLength);
                            return crccontent;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
                set
                {
                    crccontent = value;
                    blockId = ToCrcContent + crccontent + FromCrcName;
                }
            }


            public string CrcName
            {
                get
                {
                    if (!string.IsNullOrEmpty(crcname))
                    {
                        return crcname;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            crcname = blockId.Substring(RelativeBlockNameOffset + CrcNamePlaceHolderOffset, CrcNamePlaceHolderLength);
                            return crcname;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
                set
                {
                    crcname = value;
                    blockId = ToCrcName + crcname + FromFlags;
                }
            }

            public string Flags
            {
                get
                {
                    if (!string.IsNullOrEmpty(flags))
                    {
                        return flags;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            flags = blockId.Substring(RelativeBlockNameOffset + FlagsPlaceHolderOffset, FlagsPlaceHolderLength);
                            return flags;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }



            public string Unused
            {
                get
                {
                    if (!string.IsNullOrEmpty(unused))
                    {
                        return unused;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            unused = blockId.Substring(RelativeBlockNameOffset + UnusedPlaceHolderOffset, UnusedPlaceHolderLength);
                            return unused;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string BytesUsed
            {
                get
                {
                    if (!string.IsNullOrEmpty(bytesUsed))
                    {
                        return bytesUsed;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            bytesUsed = blockId.Substring(RelativeBlockNameOffset + BytesUsedPlaceHolderOffset, BytesUsedPlaceHolderLength);
                            return bytesUsed;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string NextPtr
            {
                get
                {
                    if (!string.IsNullOrEmpty(nextPtr))
                    {
                        return nextPtr;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            nextPtr = blockId.Substring(RelativeBlockNameOffset + NextPtrPlaceHolderOffset, NextPtrPlaceHolderLength);
                            return nextPtr;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string PreviousPtr
            {
                get
                {
                    if (!string.IsNullOrEmpty(previousPtr))
                    {
                        return previousPtr;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            previousPtr = blockId.Substring(RelativeBlockNameOffset + PreviousPtrPlaceHolderOffset, PreviousPtrPlaceHolderLength);
                            return previousPtr;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }



            public string IsDeleted
            {
                get
                {
                    if (!string.IsNullOrEmpty(isDeleted))
                    {
                        return isDeleted;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            isDeleted = blockId.Substring(RelativeBlockNameOffset + IsDeletedPlaceHolderOffset, IsDeletedPlaceHolderLength);
                            return isDeleted;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string KeyLength
            {
                get
                {
                    if (!string.IsNullOrEmpty(keyLength))
                    {
                        return keyLength;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            keyLength = blockId.Substring(RelativeBlockNameOffset + KeyLengthPlaceHolderOffset, KeyLengthPlaceHolderLength);
                            return keyLength;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string MaxKeyCount
            {
                get
                {
                    if (!string.IsNullOrEmpty(maxKeyCount))
                    {
                        return maxKeyCount;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            maxKeyCount = blockId.Substring(RelativeBlockNameOffset + MaxKeyCountPlaceHolderOffset, MaxKeyCountPlaceHolderLength);
                            return maxKeyCount;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string LastModifiedTimeStamp
            {
                get
                {
                    if (!string.IsNullOrEmpty(lastmodifiedtimestamp))
                    {
                        return lastmodifiedtimestamp;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            lastmodifiedtimestamp = blockId.Substring(RelativeBlockNameOffset + LastModifiedTimeStampPlaceHolderOffset, LastModifiedTimeStampPlaceHolderLength);
                            return lastmodifiedtimestamp;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }

            public string SchemaVersion
            {
                get
                {
                    if (!string.IsNullOrEmpty(schemaversion))
                    {
                        return schemaversion;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(blockId))
                        {
                            schemaversion = blockId.Substring(RelativeBlockNameOffset + SchemaVersionPlaceHolderOffset, SchemaVersionPlaceHolderLength);
                            return schemaversion;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }
            #endregion

            public string StorageRoot
            {
                get
                {
                    return storageRoot;
                }
            }

            public string AbsoluteBlockFileSpec
            {
                get
                {
                    return Path.Combine(storageRoot, BlockID);
                }
            }

            public string SetIdPlusOrdinal
            {
                get
                {
                    // static public int RelativeSetIdOffset = (9 * 3);          // 00000000\00000000\00000000\
                    // static public int RelativeBlockNameOffset = (9 * 4);      // 00000000\00000000\00000000\00000000\

                    return SetidHash + Ordinal;
                }
            }

            public string BlockName
            {
                get
                {
                    // Just the name of the block (i.e.; no path)
                    return BlockID.Substring(RelativeBlockNameOffset);
                }
            }


            #endregion


            #region Public Methods
            public bool ValidateCrcName(string crcNamePlaceHolder)
            {
                using (SimpleCrc32 crc32 = new SimpleCrc32())  // NOTE:  MUST create a new instance here each time this method is called!!!!!  DO NOT MOVE to instance variables for reuse
                                                               // Note that the crc of the original blockid on write, included a placeholder (passed in) for the crcName portion, so we need to put back in
                                                               // the placeholder in its correct place in order to re-compute the original crcName and then compare it to 
                                                               // the actual computed one that was written into the blockid
                                                               //string adjustedBlockName = BlockName.Substring(0, CacheBlockValue.CrcNamePlaceHolderOffset) + crcNamePlaceHolder + FlagsOnwards ;
                {
                    BlockFileInfo patchedBlockFileInfo = new BlockFileInfo(this, crcNamePlaceHolder);
                    //string computedCrcBlockName = BitConverter.ToUInt32(crc32.ComputeHash(Encoding.ASCII.GetBytes(relativeFileName)), 0).ToString("X8");
                    return CrcName == BitConverter.ToUInt32(crc32.ComputeHash(Encoding.ASCII.GetBytes(patchedBlockFileInfo.BlockName)), 0).ToString("X8");
                    //{
                    //    result = false;
                    //}
                    //return result;
                }
            }

            public BlockFileInfo SetCRC(byte[] blockBuffer, uint encryptionSalt, uint storageBlockSize)
            {
                BlockMetaData blockmetadata = new BlockMetaData();
                blockmetadata.BufferOffset = 0;
                blockmetadata.OperationByteCount = blockBuffer.Length;
                blockmetadata.EncryptionSalt = encryptionSalt;
                blockmetadata.StorageBlockSize = storageBlockSize;
                return SetCRC(blockBuffer, blockmetadata);
            }

            public BlockFileInfo SetCRC(byte[] buffer, BlockMetaData blockmetadata)// int bufferStartOffset = 0, int byteCount = -1)
            {
                // NOTE:  The CRC is computed twice further down below - once with the passed in blockid and again with blockIdWithOutTimeStamp
                //		  This is necessary since for cache purposes we wish to avoid the timestamp since every write has a unique timestamp which would 
                //		  result in never having a cache hit.  Therefore we update the this.crcName below with the CRC without the timestamp but use
                //		  the crcName with the timestamp in the final result passed out
                //Debug.Print( "DiskBounty.Common.CacheBlockValue.SetSRC - blockid.Length={0}, blockid={1}", blockid.Length, blockid );
                //string blockIdWithOutTimeStamp = BlockIDWithoutTimestamp;
                //Compute CRC for block (name and content) and enter them into the block name
                using (SimpleCrc32 crc32 = new SimpleCrc32()) // NOTE:  MUST create a new instance here each time this method is called!!!!!  DO NOT MOVE to instance variables for reuse
                                                              // Compute CRC of block content
                {
                    if (blockmetadata.OperationByteCount == -1)
                    {
                        //Debug.Print( "DiskBounty.Common.SetCRC(1) buffer={0}", HostCryptology.ConvertBytesToHexString( buffer ) );
                        CrcContent = BitConverter.ToUInt32(crc32.ComputeHash(buffer), 0).ToString("X8");
                    }
                    else
                    {
                        //Debug.Print( "DiskBounty.Common.SetCRC(2) buffer={0}", HostCryptology.ConvertBytesToHexString( buffer, bufferStartOffset, byteCount ) );
                        CrcContent = BitConverter.ToUInt32(crc32.ComputeHash(buffer, blockmetadata.BufferOffset, blockmetadata.OperationByteCount), 0).ToString("X8");
                    }
                    //if (blockmetadata.OperationByteCount != blockmetadata.StorageBlockSize)
                    //{
                    //    Debug.Print($"BlockFileInfo.SetCRC() - bufferLength={buffer.Length}, bufferOffset={blockmetadata.BufferOffset},  ByteCount={blockmetadata.OperationByteCount}, CrcContent={CrcContent}, {Environment.StackTrace}");
                    //}
                    // Place content CRC into both blockId and blockIdWithOutTimeStamp
                    //
                    //	Next we want to compute the the CrcName value to be embedded into the blockid using a placeholder for the CrcNameWritePlaceHolder itself.
                    //  However that placeholder must be carefully computed so that it is not possible for a nefarious agent to change some aspect of the blockid and then 
                    //  re-compute the CrcName using the same placeholder and re-embed it into blockid.  Such an approach would succeed in tamporing with
                    //  blockid without UnoSys knowning since the CrcName would match that of the tampered with blockid.
                    //
                    // The placeholder has to be choosen carefully such that it is:
                    //
                    //	i)		efficiently computed at the time of writing the block
                    //  ii)		efficiently re-computed at the time of reading the block
                    //  iii)	psudeo-unique to each block
                    //  iv)		not knowable by inspecting any aspect of the block (e.g.; the block file name, or its contents)
                    //  v)		not knowable by inspecting the DiskBounty source code
                    //  vi)		computes the same on all replicas
                    //
                    //  Therefore we use a CrcNameAWritePlaceHolder placeholder that is NOT a constant (which could be determined by examining the DiskBounty source code),
                    //  but rather is computed at runtime ONLY based on the SetDomainID the Set (i.e.; file or table) belongs to and that is part of it's identity.
                    //  The SetDomainId is then used to encrypt the setid of the block.  The result is formatted as an "X8" 8-byte hex value and used as the CrcNameWritePlaceHolder.
                    //
                    //string patchedBlockId = BlockID;
                    //patchedBlockId = patchedBlockId.Replace(Constants.CrcNameWritePlaceHolder, blockmetadata.EncryptionSalt.ToString("X8"));
                    BlockFileInfo patchedBlockFileInfo = new BlockFileInfo(this, blockmetadata.EncryptionSalt.ToString("X8"));
                    //Debug.Print( "DiskBounty.Common.SetCRC(4) Dynamic crcNamePlaceHolder = {0}, patchedBlockId={1}", CrcNamePlaceHolder, patchedBlockId );
                    // Now compute the Crc of the name for each of the above 2 blocks, but only use the proper filename of the block (i.e.; ignore the path portion)
                    //CrcName = BitConverter.ToUInt32(crc32.ComputeHash(Encoding.ASCII.GetBytes(patchedBlockId.Substring(RelativeBlockNameOffset))), 0).ToString("X8");
                    CrcName = BitConverter.ToUInt32(crc32.ComputeHash(Encoding.ASCII.GetBytes(patchedBlockFileInfo.BlockName)), 0).ToString("X8");
                }
                return this;
            }


            public BlockFileInfo DetermineExistingBlockFileInfo()
            {
                return new BlockFileInfo(BlockPath +
                                    // block filename portion from here down...                                                // TOTAL:  163
                                    ScidHash +                                                                                 // ScidHash (8)
                                    Constants.baseHexInt4 +                                                                    // BlockType (1)
                                    Ordinal +                                                                                  // ordinal of block   (32)
                                    Constants.CrcContentReadPlaceHolder +                                                      // crc of file (block) content (8)
                                    Constants.CrcNameReadPlaceHolder +                                                         // crc of file (block) name  (8)
                                    Flags +                                                                                    // flags (reserved for future use) (1)
                                    Unused +                                                                                   // unused (2)
                                    Constants.BytesUsedReadPlaceHolder +                                                       // BytesUsed (8)
                                    Constants.baseHexGuid +                                                                    // NextPtr (32)
                                    Constants.baseHexGuid +                                                                    // PreviousPtr (32)
                                    Constants.baseHexInt4 +                                                                    // IsDeleted (1)
                                    Constants.baseHexInt16 +                                                                   // KeyLength (4)
                                    Constants.baseHexInt32 +                                                                   // MaxKeyLength (8)
                                    Constants.LastModifiedTimeStampReadPlaceHolder +                                           // LastModifiedTimeStamp (16)
                                    Constants.SchemaVersionReadPlaceHolder                                                     // ShcemaVersion (2)
                                    );
            }
            #endregion


            //#region Static Methods
            //public static JournalBlockFileInfo DetermineReadJournalBlockIdFromJournalEntryKey(JournalKey journalkey)
            //{
            //    // To convert a JournalEntryKey to a JournalBlockId we:
            //    //	i) remove the prefixing PeerSetId and Seperator ('\') - a total of 9 bytes
            //    // ii) insert the crcContent and crcName with their read placeholders
            //    //iii) replace NextPtr and MaxKeyCount with read place holders
            //    // iv) Swap out SCID for PeerSetId
            //    //  v) Strip off suffixing crcPlaceHolder (8 bytes)
            //    BlockFileInfo blockFileInfo = new BlockFileInfo(journalkey); // i), ii)
            //    string journalBlockId = blockFileInfo.ToScid + journalkey.PeerSetPrefix + blockFileInfo.FromSetIdToCrcContent +
            //            Constants.CrcContentReadPlaceHolder + Constants.CrcNameReadPlaceHolder + blockFileInfo.FromFlagsToNextPtr +
            //            Constants.NextPtrReadPlaceHolder + blockFileInfo.FromPreviousPtrToMaxKeyCount + Constants.MaxKeyCountReadPlaceHolder +
            //            blockFileInfo.FromLastModifiedTimeStamp;  // iii), iv) & v)
            //    return new JournalBlockFileInfo(journalBlockId);
            //}

            //static public JournalBlockFileInfo ReadExistingJournalBlockIdFromBlockId(IPeerSet peerSet, BlockFileInfo blockFileInfo, int confirmMaskBitToSet, uint crcNamePlaceHolder, byte[] crcNamePlaceHolderBytes)
            //{
            //    // To convert a blockId to an existing ReadJournalBlockId we:
            //    // i)   replace the SCID with the PeerSetID
            //    // ii)  replace the crcContent and crcName with their read placeholders
            //    // iii) replace NextPtr and MaxKeyCount with read place holders
            //    string journalBlockId = blockFileInfo.ToScid +
            //                     peerSet.PeerSetId.ToString("X8") + Constants.platformIndependentPathSeparator +  // i)
            //                     blockFileInfo.FromSetIdToCrcContent +
            //                     Constants.CrcContentReadPlaceHolder + Constants.CrcNameReadPlaceHolder + // ii)
            //                     blockFileInfo.FromFlagsToNextPtr +
            //                     Constants.NextPtrReadPlaceHolder +  // iii)
            //                     blockFileInfo.FromPreviousPtrToMaxKeyCount +
            //                     //blockId.Substring(CacheBlockValue.RelativeBlockNameOffset + CacheBlockValue.PreviousPtrPlaceHolderOffset, CacheBlockValue.PreviousPtrPlaceHolderLength + CacheBlockValue.BlockTypePlaceHolderLength + CacheBlockValue.IsDeletedPlaceHolderLength + CacheBlockValue.KeyLengthPlaceHolderLength) +
            //                     Constants.MaxKeyCountReadPlaceHolder + // iii)
            //                     blockFileInfo.FromLastModifiedTimeStamp;
            //    //blockId.Substring(CacheBlockValue.RelativeBlockNameOffset + CacheBlockValue.LastModifiedTimeStampPlaceHolderOffset);
            //    return new JournalBlockFileInfo(journalBlockId);
            //}


            //static public BlockFileInfo DetermineReadBlockIdFromJournalBlockId(JournalBlockFileInfo journalBlockFileInfo)
            //{
            //    // To convert a JournalBlockId to a Read BlockId we: 
            //    //	 i) substitue the PeerSetId with the ScidHash embedded in journalBlockId
            //    //  ii) insert the crcContent and crcName read placeholders back into the blockid name as read place holders
            //    // iii) replace the NextPtr with read placeholder
            //    //  iv) replace the MaxKeyCount and LastModifiedTimeStamp with a read place holders
            //    string blockId = journalBlockFileInfo.ToScid +
            //                               journalBlockFileInfo.ScidHash + Constants.platformIndependentPathSeparator +  // i)
            //                               journalBlockFileInfo.FromSetIdToCrcContent +
            //                               Constants.CrcContentReadPlaceHolder + Constants.CrcNameReadPlaceHolder + // ii)
            //                               journalBlockFileInfo.FromFlagsToNextPtr +
            //                               Constants.NextPtrReadPlaceHolder +  // iii)
            //                               journalBlockFileInfo.FromPreviousPtrToMaxKeyCount +
            //                               Constants.MaxKeyCountReadPlaceHolder + Constants.LastModifiedTimeStampReadPlaceHolder + // iv)
            //                               journalBlockFileInfo.SchemaVersion;
            //    return new BlockFileInfo(blockId);
            //}
            //#endregion

            #region Helpers
            public string ToScid
            {
                get
                {
                    return BlockID.Substring(0, RelativeSetContainerIdOffset);
                }
            }

            private string FromSetId
            {
                get
                {
                    return BlockID.Substring(RelativeSetIdOffset);
                }
            }

            private string FromSetIdToCrcContent
            {
                get
                {
                    return BlockID.Substring(RelativeSetIdOffset, 9 + CrcContentPlaceHolderOffset);
                }
            }

            public string FromSetidToNextPtr
            {
                get
                {
                    return BlockID.Substring(RelativeSetIdOffset, 9 + NextPtrPlaceHolderOffset);
                }
            }

            public string ToNextPtr
            {
                get
                {
                    return blockId.Substring(0, RelativeBlockNameOffset + NextPtrPlaceHolderOffset);
                }
            }

            public string FromPreviousPtrToMaxKeyCount
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + PreviousPtrPlaceHolderOffset, PreviousPtrPlaceHolderLength + IsDeletedPlaceHolderLength + KeyLengthPlaceHolderLength);
                }
            }

            public string FromPreviousPtr
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + PreviousPtrPlaceHolderOffset);
                }
            }

            public string FromLastModifiedTimeStamp
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + LastModifiedTimeStampPlaceHolderOffset);
                }
            }

            public string ThruOrdinal
            {
                get
                {
                    return BlockID.Substring(0, RelativeBlockNameOffset + OrdinalPlaceHolderOffset + OrdinalPlaceHolderLength);
                }
            }

            protected string ToCrcContent
            {
                get
                {
                    return BlockID.Substring(0, RelativeBlockNameOffset + CrcContentPlaceHolderOffset);
                }
            }

            protected string ToCrcName
            {
                get
                {
                    return BlockID.Substring(0, RelativeBlockNameOffset + CrcNamePlaceHolderOffset);
                }
            }

            public string FromCrcName
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + CrcNamePlaceHolderOffset);
                }
            }

            public string FromFlags
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + FlagsPlaceHolderOffset);
                }
            }

            public string FromFlagsToNextPtr
            {
                get
                {
                    return BlockID.Substring(RelativeBlockNameOffset + FlagsPlaceHolderOffset, FlagsPlaceHolderLength + UnusedPlaceHolderLength + BytesUsedPlaceHolderLength);
                }
            }
            #endregion
        }

        public class BlockIOResponse
        {
            public int ByteCount = 0;
            public string BlockId = string.Empty;
            public bool IsDeleted;
            public bool CacheHit = false;
            public long UtcTimeInTicks = 0;
            public bool CrcNameCheckFailed = false;
            public bool CrcContentCheckFailed = false;

            public BlockIOResponse()
            { }
            public BlockIOResponse(string blockId, bool isDeleted, bool crcNameCheckFailed = false, bool crcContentCheckFailed = false)
            {
                BlockId = blockId;
                ByteCount = int.Parse(blockId.Substring(blockId.Length - 8), System.Globalization.NumberStyles.HexNumber);
                if (/* ! string.IsNullOrEmpty(blockId) &&*/ blockId.Length > 8)
                {
                    BlockId = BlockId.Substring(0, blockId.Length - 8);
                    if (BlockId.Length != 163)
                    {
                        Debug.Print("BlockIOResponse.ctor() ({0},{1}) {2} -> {3}", blockId.Length, BlockId.Length, blockId, Environment.StackTrace);
                    }

                    string utctimeticks = blockId.Substring(BlockFileInfo.LastModifiedTimeStampPlaceHolderOffset, BlockFileInfo.LastModifiedTimeStampPlaceHolderLength);
                    if (utctimeticks != Constants.LastModifiedTimeStampReadPlaceHolder)
                    {
                        UtcTimeInTicks = BitConverter.ToInt64(ConvertHexStringToBytes(utctimeticks), 0);
                    }
                }
                IsDeleted = isDeleted;
                CrcNameCheckFailed = crcNameCheckFailed;
                CrcContentCheckFailed = crcContentCheckFailed;
            }
        }


        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal struct BlockMetaData
        {
            [FieldOffset(0)] public int BufferOffset;
            [FieldOffset(4)] public ulong BlockOrdinal;
            [FieldOffset(12)] public int OperationByteCount;
            [FieldOffset(16)] public int CID;
            [FieldOffset(20)] public int SDID;
            [FieldOffset(24)] public int SCID;
            [FieldOffset(28)] public Guid SETID;
            [FieldOffset(44)] public byte SchemaVersion;
            [FieldOffset(45)] public uint EncryptionSalt;
            [FieldOffset(49)] public long TimeStampInTicks;
            [FieldOffset(57)] public bool IsJournal;
            [FieldOffset(58)] public Guid BlockId;
            [FieldOffset(74)] public uint ByteCountId;
            [FieldOffset(78)] public int BlockOffset;
            [FieldOffset(82)] public bool IsPartialBlock;
            [FieldOffset(86)] public uint StorageBlockSize;

            internal BlockMetaData(uint storageBlockSize, int bufferOffset, ulong blockOrdinal, byte[] blockId, int operationByteCount, uint byteCountId, int cid, int sdid, int scid, Guid setid, byte schemaVersion, uint encryptionSalt, long timeStampInTicks = 0L, bool isJournal = false)
            {
                StorageBlockSize = storageBlockSize;
                BufferOffset = bufferOffset;
                BlockOrdinal = blockOrdinal;
                OperationByteCount = operationByteCount;
                CID = cid;
                SDID = sdid;
                SCID = scid;
                SETID = setid;
                SchemaVersion = schemaVersion;
                EncryptionSalt = encryptionSalt;
                TimeStampInTicks = timeStampInTicks;
                IsJournal = isJournal;
                BlockId = new Guid(blockId);
                //Debug.Print( "UnoSys.Common.BlockMetaqData.ctor() byteCountId = {0}", byteCountId );
                ByteCountId = byteCountId;
                BlockOffset = 0;
                IsPartialBlock = false;
            }

            internal string FullKey
            {
                get
                {
                    string ord = Constants.baseHexGuid + BlockOrdinal.ToString("X");
                    ord = ord.Substring(ord.Length - Constants.baseHexGuidLength);
                    return SETID.ToString("N") + new Guid(ConvertHexStringToBytes(ord)).ToString("N");
                }
            }
        }


        internal class Constants
        {
            static public string platformIndependentPathSeparator = @"\";
            static public string baseHexInt4 = "0";
            static public string baseHexInt8 = baseHexInt4 + baseHexInt4;
            static public string baseHex3 = baseHexInt8 + baseHexInt4;
            static public string baseHex5 = baseHex3 + baseHexInt8;
            static public string baseHexInt16 = baseHexInt8 + baseHexInt8;
            static public string baseHexInt32 = baseHexInt16 + baseHexInt16;
            static public string baseHexInt64 = baseHexInt32 + baseHexInt32;
            static public string baseHexGuid = baseHexInt64 + baseHexInt64;
            static public int baseHexGuidLength = baseHexGuid.Length;
            static public int baseHexInt64Length = baseHexInt64.Length;
            static public int baseHexInt32Length = baseHexInt32.Length;
            static public int baseHexInt8Length = baseHexInt8.Length;
            //static public string CrcReadPlaceHolder = "????????";
            static public string CrcContentReadPlaceHolder = "????????";
            static public string CrcNameReadPlaceHolder = "????????";
            static public string CrcContentWritePlaceHolder = "********";
            static public string CrcNameWritePlaceHolder = "||||||||";
            static public string BytesUsedReadPlaceHolder = "????????";
            static public string LastModifiedTimeStampReadPlaceHolder = "????????????????";
            static public string SchemaVersionReadPlaceHolder = "??";
            static public string MaxKeyCountReadPlaceHolder = "????????";
            static public string NextPtrReadPlaceHolder = "????????????????????????????????";
            //static public int AbsoluteBlockIdLength = 163;
            //static public int JournalEntryKeyLength = AbsoluteBlockIdLength + 1; //= AbsoluteBlockIdLength - CrcContentPlaceHolderLength - CrcNamePlaceHolderLength + 9 + 8; // 9 = PeerSetId.ToString("X8") + pathSeparator, 8 = suffixing crcPlaceHolder
            //static public int JournalEntryMessageSize = JournalEntryKeyLength + sizeof( ulong );
            //static public int RelativeBlockIdLength = AbsoluteBlockIdLength - 36;


        }


        internal sealed class SimpleCrc32 : HashAlgorithm
        {
            public const UInt32 DefaultPolynomial = 0xedb88320u;
            public const UInt32 DefaultSeed = 0xffffffffu;

            static UInt32[] defaultTable;

            readonly UInt32 seed;
            readonly UInt32[] table;
            UInt32 hash;

            public SimpleCrc32()
                : this(DefaultPolynomial, DefaultSeed)
            {
            }

            public SimpleCrc32(UInt32 polynomial, UInt32 seed)
            {
                table = InitializeTable(polynomial);
                this.seed = hash = seed;
            }

            public override void Initialize()
            {
                hash = seed;
            }

            protected override void HashCore(byte[] array, int ibStart, int cbSize)
            {
                hash = CalculateHash(table, hash, array, ibStart, cbSize);
            }

            protected override byte[] HashFinal()
            {
                var hashBuffer = UInt32ToBigEndianBytes(~hash);
                HashValue = hashBuffer;
                return hashBuffer;
            }

            public override int HashSize { get { return 32; } }

            public static UInt32 Compute(byte[] buffer)
            {
                return Compute(DefaultSeed, buffer);
            }

            public static UInt32 Compute(UInt32 seed, byte[] buffer)
            {
                return Compute(DefaultPolynomial, seed, buffer);
            }

            public static UInt32 Compute(UInt32 polynomial, UInt32 seed, byte[] buffer)
            {
                return ~CalculateHash(InitializeTable(polynomial), seed, buffer, 0, buffer.Length);
            }

            static UInt32[] InitializeTable(UInt32 polynomial)
            {
                if (polynomial == DefaultPolynomial && defaultTable != null)
                    return defaultTable;

                var createTable = new UInt32[256];
                for (var i = 0; i < 256; i++)
                {
                    var entry = (UInt32)i;
                    for (var j = 0; j < 8; j++)
                        if ((entry & 1) == 1)
                            entry = (entry >> 1) ^ polynomial;
                        else
                            entry = entry >> 1;
                    createTable[i] = entry;
                }

                if (polynomial == DefaultPolynomial)
                    defaultTable = createTable;

                return createTable;
            }

            static UInt32 CalculateHash(UInt32[] table, UInt32 seed, IList<byte> buffer, int start, int size)
            {
                var hash = seed;
                for (var i = start; i < start + size; i++)
                    hash = (hash >> 8) ^ table[buffer[i] ^ hash & 0xff];
                return hash;
            }

            static byte[] UInt32ToBigEndianBytes(UInt32 uint32)
            {
                var result = BitConverter.GetBytes(uint32);

                if (BitConverter.IsLittleEndian)
                    Array.Reverse(result);

                return result;
            }
        }

        internal enum IPCOperation
        {
            // NOTE:  All two-way IPC Operations (i.e.; Request/Response) have an even numbered IPC Operation command and the response is the negative of that number
            //        Any one-way IPC Operations (i.e.; Events) have an odd numbered IPC Operation command with no response

            //FIRST = -14,
            // IO Related Repsonses
            WRITE_SYNC_BLOCK_RESPONSE = -16,
            WRITE_IS_CONFIRMED_RESPONSE = -14,
            WRITE_RESPONSE = -12,
            READ_RESPONSE = -10,
            // Connection Related Responses
            REKEY_SESSION_RESPONSE = -4,
            CONNECT_RESPONSE = -2,

            UNKNOWN = 0,
            // Connection Related
            CONNECT = 2,
            REKEY_SESSION = 4,
            // IO Related
            READ = 10,
            WRITE = 12,
            WRITE_IS_CONFIRMED = 14,
            WRITE_SYNC_BLOCK = 16

            //		LAST = 14
        }

    }

   
}
