using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json.Serialization;
using System.Xml.Linq;
using static WorldComputer.Simulator.FILECLOSE_Operation;
using static WorldComputer.Simulator.VDrive;

namespace WorldComputer.Simulator
{
    public class FILECREATE_Operation : VolumeMetaDataOperation
    {
        private byte charSet;  // Reserved for future use
        private int ntDesiredAccess;
        private int ntCreateDisposition;
        private int desiredAccess;
        private int attributes;
        private int shareMode;
        private string absoluteFileName;


        public FILECREATE_Operation(Guid vdiskid, Guid volumeid, Guid fileid, string absolutefilename, int desiredaccess, int attribs,
                        int sharemode, int ntcreatedisposition, int ntdesiredaccess) : base(vdiskid, volumeid, fileid, (attribs & (uint)0x10 /*CBFileAttributes.Directory*/) != 0, VolumeMetaDataOperationType.FILE_CREATE)
        {
            ntDesiredAccess = ntdesiredaccess;
            ntCreateDisposition = ntcreatedisposition;
            desiredAccess = desiredaccess;
            attributes = attribs;
            shareMode = sharemode;
            absoluteFileName = absolutefilename;
        }

        public FILECREATE_Operation(byte[] operationBytes) : base(operationBytes)
        {

            int pos = ReservedGlobalEventPayloadHeaderSize;
            ntCreateDisposition = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            ntDesiredAccess = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            desiredAccess = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            attributes = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            shareMode = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            short fileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeFileNameBytes = new byte[fileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeFileNameBytes, 0, fileNameLength);
            pos += fileNameLength;
            absoluteFileName = Encoding.Unicode.GetString(unicodeFileNameBytes);
        }

        public string AsBase64String()
        {
            byte[] operationBytes = new byte[MAX_MESSAGE_SIZE - ReservedGlobalEventPayloadHeaderSize];
            int pos = 0;
            SerializeOperationHeader(operationBytes, ref pos);
            Buffer.BlockCopy(BitConverter.GetBytes(ntCreateDisposition), 0, operationBytes, pos, sizeof(int));                          // NTCreateDisposition
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(ntDesiredAccess), 0, operationBytes, pos, sizeof(int));                              // NTDesiredAccess
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(desiredAccess), 0, operationBytes, pos, sizeof(int));                                // desoredAccess
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(attributes), 0, operationBytes, pos, sizeof(int));                                   // attributes
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(shareMode), 0, operationBytes, pos, sizeof(int));                                    // shareMode
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(absoluteFileName.Length * 2)), 0, operationBytes, pos, sizeof(short)); // length of unicode encoded FileName
            pos += sizeof(short);
            if (absoluteFileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("Filename too long.");
            }
            byte[] unicodeFileNameBytes = Encoding.Unicode.GetBytes(absoluteFileName);                                                     // unicode encoded FileName
            Buffer.BlockCopy(unicodeFileNameBytes, 0, operationBytes, pos, unicodeFileNameBytes.Length);
            pos += unicodeFileNameBytes.Length;
            return Convert.ToBase64String(operationBytes);
        }

        public string FileName { get { return absoluteFileName; } }

        public int NTDesiredAccess { get { return ntDesiredAccess; } }

        public int NTCreateDisposition { get { return ntCreateDisposition; } }
        public int DesiredAccess { get { return desiredAccess; } }
        public int Attributes { get { return attributes; } }
        public int ShareMode { get { return shareMode; } }
    }


    public class FILESETATTRIBUTES_Operation : VolumeMetaDataOperation
    {
        private string fileName = null!;
        private DateTime createTime;
        private DateTime lastAccessTime;
        private DateTime lastWriteTime;
        private DateTime changeTime;
        private int attributes;
        private int eventOrigin;


        public FILESETATTRIBUTES_Operation(Guid vdiskid, Guid volumeid, string filename, Guid fileid, DateTime createtime, DateTime lastaccesstime, DateTime lastwritetime, DateTime changetime,
                        int attribs, int eventorigin) : base(vdiskid, volumeid, fileid, (attribs & (uint)0x10 /*CBFileAttributes.Directory*/) != 0, VolumeMetaDataOperationType.FILE_SETATTRIBUTES)
        {
            fileName = filename;
            createTime = createtime;
            lastAccessTime = lastaccesstime;
            lastWriteTime = lastwritetime;
            changeTime = changetime;
            attributes = attribs;
            eventOrigin = eventorigin;
        }

        public FILESETATTRIBUTES_Operation(byte[] operationBytes) : base(operationBytes)
        {

            int pos = ReservedGlobalEventPayloadHeaderSize;
            createTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            lastAccessTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            lastWriteTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            changeTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            attributes = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            eventOrigin = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            short fileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeFileNameBytes = new byte[fileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeFileNameBytes, 0, fileNameLength);
            pos += fileNameLength;
            fileName = Encoding.Unicode.GetString(unicodeFileNameBytes);
        }

        public virtual string AsBase64String()
        {
            int pos = 0;
            return Convert.ToBase64String(AsBytes(ref pos));
        }

        public byte[] AsBytes(ref int pos)
        {
            byte[] operationBytes = new byte[MAX_MESSAGE_SIZE - ReservedGlobalEventPayloadHeaderSize];
            SerializeOperationHeader(operationBytes, ref pos);
            Buffer.BlockCopy(BitConverter.GetBytes(createTime.Ticks), 0, operationBytes, pos, sizeof(long));                          // CreateTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(lastAccessTime.Ticks), 0, operationBytes, pos, sizeof(long));                      // LastAccessTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(lastWriteTime.Ticks), 0, operationBytes, pos, sizeof(long));                       // LastWriteTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(changeTime.Ticks), 0, operationBytes, pos, sizeof(long));                          // ChangeTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(attributes), 0, operationBytes, pos, sizeof(int));                                 // Attributes
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(eventOrigin), 0, operationBytes, pos, sizeof(int));                                // EventOrigin
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(fileName.Length * 2)), 0, operationBytes, pos, sizeof(short));     // length of unicode encoded FileName
            pos += sizeof(short);
            Debug.Print($"&&&&&&&&&&&&&&&&&& FILESETATTRIBUTES_Operation.AsBytes() - ({fileName.Length * 2}) FileName={fileName}");
            if (fileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("Filename too long.");
            }
            byte[] unicodeFileNameBytes = Encoding.Unicode.GetBytes(fileName);                                                        // unicode encoded FileName
            Buffer.BlockCopy(unicodeFileNameBytes, 0, operationBytes, pos, unicodeFileNameBytes.Length);
            pos += unicodeFileNameBytes.Length;
            return operationBytes;
        }

        public string FileName { get { return fileName; } }
        public DateTime CreateTime { get { return createTime; } }
        public DateTime LastAccessTime { get { return lastAccessTime; } }
        public DateTime LastWriteTime { get { return lastWriteTime; } }
        public DateTime ChangeTime { get { return changeTime; } }

        public int Attributes { get { return attributes; } }
        public int EventOrigin { get { return eventOrigin; } }

    }

    public class FILEGETATTRIBUTES_Operation : VolumeMetaDataOperation
    {
        private string fileName = null!;
        public FILEGETATTRIBUTES_Operation(Guid vdiskid, Guid volumeid, string filename, bool isdirectory) : base(vdiskid, volumeid, Guid.Empty, isdirectory, VolumeMetaDataOperationType.FILE_GETATTRIBUTES)
        {
            fileName = filename;
        }

        public FILEGETATTRIBUTES_Operation(byte[] operationBytes) : base(operationBytes)
        {
            int pos = ReservedGlobalEventPayloadHeaderSize;
            short fileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeFileNameBytes = new byte[fileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeFileNameBytes, 0, fileNameLength);
            fileName = Encoding.Unicode.GetString(unicodeFileNameBytes);
            pos += fileNameLength;
        }

        public virtual string AsBase64String()
        {
            int pos = 0;
            return Convert.ToBase64String(AsBytes(ref pos));
        }

        public byte[] AsBytes(ref int pos)
        {
            byte[] operationBytes = new byte[MAX_MESSAGE_SIZE - ReservedGlobalEventPayloadHeaderSize];
            SerializeOperationHeader(operationBytes, ref pos);            
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(fileName.Length * 2)), 0, operationBytes, pos, sizeof(short));     // length of unicode encoded FileName
            pos += sizeof(short);
            if (fileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("Filename too long.");
            }
            byte[] unicodeFileNameBytes = Encoding.Unicode.GetBytes(fileName);                                                        // unicode encoded FileName
            Buffer.BlockCopy(unicodeFileNameBytes, 0, operationBytes, pos, unicodeFileNameBytes.Length);
            pos += unicodeFileNameBytes.Length;
            return operationBytes;
        }

        public string FileName { get { return fileName; } set { fileName = value; } }
        


    }


    public class FILEDELETE_Operation : FILEGETATTRIBUTES_Operation
    {
        public FILEDELETE_Operation(Guid vdiskid, Guid volumeid, string filename, bool isDirectory) : base(vdiskid, volumeid, filename, isDirectory)
        {
            operationType = VolumeMetaDataOperationType.FILE_DELETE;
        }

        public FILEDELETE_Operation(byte[] operatoinBytes) : base(operatoinBytes)
        {
        }
    }

    public class FILEOPEN_Operation : FILEGETATTRIBUTES_Operation
    {
        public FILEOPEN_Operation(Guid vdiskid, Guid volumeid, string filename, bool isDirectory) : base(vdiskid, volumeid, filename, isDirectory)
        {
            operationType = VolumeMetaDataOperationType.FILE_OPEN;
        }

        public FILEOPEN_Operation(byte[] operatoinBytes) : base(operatoinBytes)
        {
        }
    }


    public class FILERENAMEORMOVE_Operation : VolumeMetaDataOperation
    {
        private string newFileName = null!;
        private string oldFileName = null!;
        public FILERENAMEORMOVE_Operation(Guid vdiskid, Guid volumeid, bool isDirectory, string oldfilename, string newfilename) : base(vdiskid, volumeid, Guid.Empty, isDirectory, VolumeMetaDataOperationType.FILE_RENAMEORMOVE)
        {
            oldFileName = oldfilename;
            newFileName = newfilename;
        }

        public FILERENAMEORMOVE_Operation(byte[] operationBytes) : base(operationBytes)
        {
            int pos = ReservedGlobalEventPayloadHeaderSize;
            short oldFileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeOldFileNameBytes = new byte[oldFileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeOldFileNameBytes, 0, oldFileNameLength);
            pos += oldFileNameLength;
            oldFileName = Encoding.Unicode.GetString(unicodeOldFileNameBytes);
            short newFileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeNewFileNameBytes = new byte[newFileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeNewFileNameBytes, 0, newFileNameLength);
            pos += oldFileNameLength;
            newFileName = Encoding.Unicode.GetString(unicodeNewFileNameBytes);
        }


        public byte[] AsBytes(ref int pos)
        {
            byte[] operationBytes = new byte[MAX_MESSAGE_SIZE - ReservedGlobalEventPayloadHeaderSize];
            SerializeOperationHeader(operationBytes, ref pos);
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(oldFileName.Length * 2)), 0, operationBytes, pos, sizeof(short));     // length of unicode encoded OldFileName
            pos += sizeof(short);
            if (oldFileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("Old Filename too long.");
            }
            byte[] unicodeOldFileNameBytes = Encoding.Unicode.GetBytes(oldFileName);                                                     // unicode encoded OldFileName
            Buffer.BlockCopy(unicodeOldFileNameBytes, 0, operationBytes, pos, unicodeOldFileNameBytes.Length);
            pos += unicodeOldFileNameBytes.Length;
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(newFileName.Length * 2)), 0, operationBytes, pos, sizeof(short));     // length of unicode encoded NewFileName
            pos += sizeof(short);
            if (newFileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("New Filename too long.");
            }
            byte[] unicodeNewFileNameBytes = Encoding.Unicode.GetBytes(newFileName);                                                     // unicode encoded NewFileName
            Buffer.BlockCopy(unicodeNewFileNameBytes, 0, operationBytes, pos, unicodeNewFileNameBytes.Length);
            pos += unicodeNewFileNameBytes.Length;
            return operationBytes;
        }

        public virtual string AsBase64String()
        {
            int pos = 0;
            return Convert.ToBase64String(AsBytes(ref pos));
        }

        public string OldFileName { get { return oldFileName; } }

        public string NewFileName { get { return newFileName; } }
    }

    public class FILECLOSE_Operation : VolumeMetaDataOperation
    {
        private string fileName = null!;
        private DateTime createTime;
        private DateTime lastAccessTime;
        private DateTime lastWriteTime;
        private DateTime changeTime;
        private int attributes;
        private int eventOrigin;
        private long fileLength;

        public FILECLOSE_Operation() : base() { }  // Parameterless constructor required for deserialization

        public FILECLOSE_Operation(Guid vdiskid, Guid volumeid, string filename, Guid fileid, bool isDirectory) : base(vdiskid, volumeid, fileid, isDirectory, VolumeMetaDataOperationType.FILE_CLOSE)
        {
        }
        public FILECLOSE_Operation(Guid vdiskid, Guid volumeid, string filename, Guid fileid, DateTime createtime, DateTime lastaccesstime, DateTime lastwritetime, DateTime changetime,
                        int attribs, int eventorigin, long filelength) : base(vdiskid, volumeid, fileid, (attribs & (uint)0x10 /*CBFileAttributes.Directory*/) != 0, VolumeMetaDataOperationType.FILE_CLOSE)
        {
            fileName = filename;
            createTime = createtime;
            lastAccessTime = lastaccesstime;
            lastWriteTime = lastwritetime;
            changeTime = changetime;
            attributes = attribs;
            eventOrigin = eventorigin;
            fileLength = filelength;
        }

        public FILECLOSE_Operation(byte[] operationBytes) : base(operationBytes)
        {

            int pos = ReservedGlobalEventPayloadHeaderSize;
            createTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            lastAccessTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            lastWriteTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            changeTime = new DateTime(BitConverter.ToInt64(operationBytes, pos));
            pos += sizeof(long);
            attributes = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            eventOrigin = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(int);
            fileLength = BitConverter.ToInt32(operationBytes, pos);
            pos += sizeof(long);
            short fileNameLength = BitConverter.ToInt16(operationBytes, pos);
            pos += sizeof(short);
            byte[] unicodeFileNameBytes = new byte[fileNameLength];
            Buffer.BlockCopy(operationBytes, pos, unicodeFileNameBytes, 0, fileNameLength);
            pos += fileNameLength;
            fileName = Encoding.Unicode.GetString(unicodeFileNameBytes);
        }

        public virtual string AsBase64String()
        {
            int pos = 0;
            return Convert.ToBase64String(AsBytes(ref pos));
        }

        public byte[] AsBytes(ref int pos)
        {
            byte[] operationBytes = new byte[MAX_MESSAGE_SIZE - ReservedGlobalEventPayloadHeaderSize];
            SerializeOperationHeader(operationBytes, ref pos);
            Buffer.BlockCopy(BitConverter.GetBytes(createTime.Ticks), 0, operationBytes, pos, sizeof(long));                          // CreateTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(lastAccessTime.Ticks), 0, operationBytes, pos, sizeof(long));                      // LastAccessTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(lastWriteTime.Ticks), 0, operationBytes, pos, sizeof(long));                       // LastWriteTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(changeTime.Ticks), 0, operationBytes, pos, sizeof(long));                          // ChangeTime
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(attributes), 0, operationBytes, pos, sizeof(int));                                 // Attributes
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(eventOrigin), 0, operationBytes, pos, sizeof(int));                                // EventOrigin
            pos += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(fileLength), 0, operationBytes, pos, sizeof(long));                                // FileLength
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(Convert.ToInt16(fileName.Length * 2)), 0, operationBytes, pos, sizeof(short));     // length of unicode encoded FileName
            pos += sizeof(short);
            if (fileName.Length * 2 > operationBytes.Length - pos)
            {
                throw new ArgumentException("Filename too long.");
            }
            byte[] unicodeFileNameBytes = Encoding.Unicode.GetBytes(fileName);                                                        // unicode encoded FileName
            Buffer.BlockCopy(unicodeFileNameBytes, 0, operationBytes, pos, unicodeFileNameBytes.Length);
            pos += unicodeFileNameBytes.Length;
            return operationBytes;
        }

        [JsonPropertyName("FN")]
        public string FileName { get { return fileName; } set { fileName = value; } }
        [JsonPropertyName("CT")]
        public DateTime CreateTime { get { return createTime; } set { createTime = value; } }
        [JsonPropertyName("LAT")]
        public DateTime LastAccessTime { get { return lastAccessTime; } set { lastAccessTime = value; } }
        [JsonPropertyName("LWT")]
        public DateTime LastWriteTime { get { return lastWriteTime; } set { lastWriteTime = value; } }
        [JsonPropertyName("CHGT")]
        public DateTime ChangeTime { get { return changeTime; } set { changeTime = value; } }
        [JsonPropertyName("ATT")]
        public int Attributes { get { return attributes; } set { attributes = value; } }
        [JsonPropertyName("EO")]
        public int EventOrigin { get { return eventOrigin; } set { eventOrigin = value; } }
        [JsonPropertyName("FL")]
        public long FileLength { get { return fileLength; } set { fileLength = value; } }


    }

    public class VolumeMetaDataOperation
    {
        protected int MAX_MESSAGE_SIZE = 1024;
        protected VolumeMetaDataOperationType operationType;
        protected Guid volumeID;
        protected Guid vDiskID;
        protected Guid fileID;
        protected bool isDirectory;

        public VolumeMetaDataOperation() { }  // Parameterless constructor required for Deserialization

        public VolumeMetaDataOperation(Guid vdiskid, Guid volumeid, Guid fileid, bool isdirectory, VolumeMetaDataOperationType operationtype) 
        {
            vDiskID = vdiskid;
            volumeID = volumeid;
            operationType = operationtype;
            fileID = fileid;
            isDirectory = isdirectory;
        }

        public VolumeMetaDataOperation(byte[] operationBytes) : base()
        {
            if (operationBytes == null)
            {
                throw new Exception("operationBytes cannot be null.");
            }
            if (operationBytes.Length > MAX_MESSAGE_SIZE)
            {
                throw new Exception("operationBytes exceeds maxium size.");
            }
            DeserializeOperationHeader(operationBytes, out int pos);
        }

        protected void SerializeOperationHeader(byte[] operationBytes, ref int pos)
        {
            operationBytes[pos++] = 0;                                                  // Operation Version# (0 is 1st version)
            operationBytes[pos++] = (byte)(isDirectory ? 1 : 0);                        // isDirectory
            Buffer.BlockCopy(vDiskID.ToByteArray(), 0, operationBytes, pos, 16);        // VDiskID
            pos += 16;
            Buffer.BlockCopy(volumeID.ToByteArray(), 0, operationBytes, pos, 16);       // VDiskID
            pos += 16;
            Buffer.BlockCopy(fileID.ToByteArray(), 0, operationBytes, pos, 16);         // FileID
            pos += 16;
            
            operationBytes[pos++] = Convert.ToByte((int)(operationType));               // OperationType
        }

        protected void DeserializeOperationHeader(byte[] operationBytes, out int pos)
        {
            pos = 0;
            byte version = operationBytes[pos++];
            isDirectory = (operationBytes[pos++] == 1 ? true : false);
            byte[] tempBytes = new byte[16];
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            vDiskID = new Guid(tempBytes);
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            volumeID = new Guid(tempBytes);
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            fileID = new Guid(tempBytes);
            operationType = (VolumeMetaDataOperationType)(Convert.ToInt32(operationBytes[pos++]));
        }

        [JsonPropertyName("VDID")]
        public Guid VDiskID { get { return vDiskID; } set { vDiskID = value; } }
        [JsonPropertyName("VID")]
        public Guid VolumeID { get { return volumeID; } set { volumeID = value; } }
        [JsonPropertyName("FID")]
        public Guid FileID { get { return fileID; } set { fileID = value; } }
        [JsonPropertyName("ISDIR")]
        public bool IsDirectory { get { return isDirectory; } set { isDirectory = value; } }
        [JsonPropertyName("OT")]
        public VolumeMetaDataOperationType OperationType { get { return operationType; } set { operationType = value; } }


        #region Helper
        protected virtual int ReservedGlobalEventPayloadHeaderSize
        {
            get
            {
                return 1 + (16 * 3) + 1 + 1;
            }
        }
        #endregion 

    }


    public class VolumeDataOperation
    {
        protected VolumeDataOperationType operationType;
        protected Guid volumeID;
        protected Guid vDiskID;
        protected Guid fileID;
        protected ulong _position;
        protected uint byteCount;
        protected ulong fileLength;
        protected byte[] _buffer = null!;

        public VolumeDataOperation() { }  // Parameterless constructor required for Deserialization

        public VolumeDataOperation(Guid vdiskid, Guid volumeid, Guid fileid, VolumeDataOperationType operationtype, ulong position, uint bytecount, ulong filelength, byte[] buffer = null!)
        {
            vDiskID = vdiskid;
            volumeID = volumeid;
            operationType = operationtype;
            fileID = fileid;
            _position = position;
            fileLength = filelength;
            byteCount = bytecount;
            _buffer = buffer;
        }

        public VolumeDataOperation(byte[] operationBytes) : base()
        {
            if (operationBytes == null)
            {
                throw new Exception("operationBytes cannot be null.");
            }
            DeserializeOperationHeader(operationBytes, out int pos);
        }

     
        public byte[] AsBytes(ref int pos)
        {
            byte[] operationBytes = new byte[1 + (16 * 3) + 1 + (2*sizeof(ulong)) + sizeof(uint) + (_buffer == null ? 0 : _buffer.Length)];
            SerializeOperationHeader(operationBytes, ref pos);
            return operationBytes;
        }

        public string AsBase64String()
        {
            int pos = 0;
            return Convert.ToBase64String(AsBytes(ref pos));
        }

        [JsonPropertyName("VDID")]
        public Guid VDiskID { get { return vDiskID; } set { vDiskID = value; } }
        [JsonPropertyName("VID")]
        public Guid VolumeID { get { return volumeID; } set { volumeID = value; } }
        [JsonPropertyName("FID")]
        public Guid FileID { get { return fileID; } set { fileID = value; } }
        [JsonPropertyName("P")]
        public ulong Position { get { return _position; } set { _position = value; } }
        [JsonPropertyName("BC")]
        public uint ByteCount { get { return byteCount; } set { byteCount = value; } }
        [JsonPropertyName("B")]
        public byte[] ByteBuffer { get { return _buffer; } set { _buffer = value; } }
        [JsonPropertyName("FL")]
        public ulong FileLength { get { return fileLength; } set { fileLength = value; } }
        [JsonPropertyName("OT")]
        public VolumeDataOperationType OperationType { get { return operationType; } set { operationType = value; } }


        #region Helper
        private void SerializeOperationHeader(byte[] operationBytes, ref int pos)
        {
            operationBytes[pos++] = 0;                                                                  // Operation Version# (0 is 1st version)
            Buffer.BlockCopy(vDiskID.ToByteArray(), 0, operationBytes, pos, 16);                        // VDiskID
            pos += 16;
            Buffer.BlockCopy(volumeID.ToByteArray(), 0, operationBytes, pos, 16);                       // VDiskID
            pos += 16;
            Buffer.BlockCopy(fileID.ToByteArray(), 0, operationBytes, pos, 16);                         // FileID
            pos += 16;
            operationBytes[pos++] = Convert.ToByte((int)(operationType));                               // OperationType
            Buffer.BlockCopy(BitConverter.GetBytes(_position), 0, operationBytes, pos, sizeof(ulong));  // Position
            pos += sizeof(ulong);
            Buffer.BlockCopy(BitConverter.GetBytes(fileLength), 0, operationBytes, pos, sizeof(ulong)); // FileLength
            pos += sizeof(ulong);
            Buffer.BlockCopy(BitConverter.GetBytes(byteCount), 0, operationBytes, pos, sizeof(uint));   // ByteCount
            pos += sizeof(uint);
            if (operationType == VolumeDataOperationType.FILE_WRITE)
            {
                Buffer.BlockCopy(_buffer, 0, operationBytes, pos, (int)byteCount);                      // Buffer (for write operations only)
                pos += (int)byteCount;
            }
        }

        private void DeserializeOperationHeader(byte[] operationBytes, out int pos)
        {
            pos = 0;
            byte version = operationBytes[pos++];
            byte[] tempBytes = new byte[16];
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            vDiskID = new Guid(tempBytes);
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            volumeID = new Guid(tempBytes);
            Buffer.BlockCopy(operationBytes, pos, tempBytes, 0, 16);
            pos += 16;
            fileID = new Guid(tempBytes);
            operationType = (VolumeDataOperationType)(Convert.ToInt32(operationBytes[pos++]));
            _position = BitConverter.ToUInt64(operationBytes, pos);
            pos += sizeof(ulong);
            fileLength = BitConverter.ToUInt64(operationBytes, pos);
            pos += sizeof(ulong);
            byteCount = BitConverter.ToUInt32(operationBytes, pos);
            pos += sizeof(uint);
            if (operationType == VolumeDataOperationType.FILE_WRITE)
            {
                _buffer = new byte[byteCount];
                Buffer.BlockCopy(operationBytes, pos, _buffer, 0, (int)byteCount);
                pos += (int)byteCount;
            }
        }
        #endregion 

    }


    public enum VolumeDataOperationType
    {
        NONE = 0,
        FILE_READ,
        FILE_WRITE
    }


    public enum VolumeMetaDataOperationType
    {
        NONE = 0,
        FILE_CREATE,
        FILE_OPEN,
        FILE_CLOSE,
        FILE_DELETE,
        FILE_RENAMEORMOVE,
        FILE_SETSIZE,
        FILE_SETATTRIBUTES,
        FILE_GETATTRIBUTES
    }

}
