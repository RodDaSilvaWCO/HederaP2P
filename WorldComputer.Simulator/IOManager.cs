using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorldComputer.Simulator
{

    internal class IOManager : IDisposable
    {
        private SetManager.SimpleCrc32 crc32 = new SetManager.SimpleCrc32();
        private int storageArrayLength;

        internal IOManager( int storagearraylength)
        {
            storageArrayLength = storagearraylength;    
        }

        public void Dispose()
        {
            crc32?.Dispose();
        }

        public async Task<SetManager.BlockIOResponse> ReadSetMemberAsync(byte[] bufferToFill, SetManager.BlockMetaData blockMetaData)
        {
            int cluster = ComputeCluster(blockMetaData);
            //if (cluster == processorCluster)  // Is the computed cluster 'this' nodeCluster?
            //{
                // Process the IO operation on this locally on tthis node
                return await cachemanager.GetBlockAsync(storagePeerSet, bufferToFill, blockMetaData).ConfigureAwait(false);
            //}
            //else
            //{
            //    // Process the IO operation on a remote node using the StorageArrayPeerSet
            //    return await storagePeerSet.SendTwoWayAsync(SetManager.IPCOperation.READ, cluster, bufferToFill, blockMetaData).ConfigureAwait(false);
            //}
        }


        //public async Task<BlockIOResponse> WriteSetMemberAsync( byte[] bufferToEmpty, int bufferOffset, ulong block, int bytesToWrite, int cid, int sdid, Guid scid, Guid setid, byte schemaVersion, uint crcNamePlaceHolder  )
        public async Task<SetManager.BlockIOResponse> WriteSetMemberAsync(byte[] bufferToEmpty, SetManager.BlockMetaData blockMetaData)
        {
            // NOTE:  By the time we get to this point bufferToEmpy has already been encrypted with the symmetric "secret" key that is associated with the set handle
            int cluster = ComputeCluster(blockMetaData);
            //if (cluster == processorCluster)  // Is the computed cluster 'this' nodeCluster?
            //{
                // Process the IO operation on this local computer
                return await cachemanager.PutBlockAsync(storagePeerSet, bufferToEmpty, blockMetaData).ConfigureAwait(false);
            //}
            //else
            //{
            //    // Process the IO operation on a remote node using the StorageArrayPeerSet
            //    return await storagePeerSet.SendTwoWayAsync(SetManager.IPCOperation.WRITE, cluster, bufferToEmpty, blockMetaData).ConfigureAwait(false);
            //}
        }

        private int ComputeCluster(SetManager.BlockMetaData blockMetaData)
        {
            int cluster = -1;
            byte[] id = new byte[16 + sizeof(long)];
            Buffer.BlockCopy(blockMetaData.SETID.ToByteArray(), 0, id, 0, 16);
            Buffer.BlockCopy(BitConverter.GetBytes(blockMetaData.BlockOrdinal), 0, id, 16, sizeof(long));
            uint hash = BitConverter.ToUInt32(crc32.ComputeHash(id), 0);
            cluster = Convert.ToInt32(hash % storageArrayLength);
            return cluster;
        }
    }
}
