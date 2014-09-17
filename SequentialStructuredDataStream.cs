using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * SequentialStructuredDataStream (java and c#)
 * ------------------------------
 * author: chitta.pardeshi@gmail.com 
 * May 2014
 */
    public class SequentialStructuredDataStream : IDisposable {
	    private static  string CURRENT_VERSION = "ssds0";
	    private const int RAW_TYPE_VARINT = 0;
        private const int RAW_TYPE_FIXED64 = 1;
        private const int RAW_TYPE_LENGTH_DELIMITED = 2;
        private const int RAW_TYPE_START_GROUP = 3;
        private const int RAW_TYPE_END_GROUP = 4;
        private const int RAW_TYPE_FIXED32 = 5;
        private const int RAW_TYPE_SCHEMA = 6;

	    private static  int TAG_TYPE_BITS = 3;
	    private static  int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

	    private const byte ITM_TYPE_BOOLEAN = (int)'b';    // 'b' - bool
        private const byte ITM_TYPE_ENUM = (int)'e';       // 'e' - enumeration
        private const byte ITM_TYPE_UINT32 = (int)'i';     // 'i' - uint32
        private const byte ITM_TYPE_UINT64 = (int)'j';     // 'j' - uint64
        private const byte ITM_TYPE_SINT32 = (int)'u';     // 'u' - sint32
        private const byte ITM_TYPE_SINT64 = (int)'v';     // 'v' - sint64
        private const byte ITM_TYPE_FIXED32 = (int)'q';    // 'q' - fixed32
        private const byte ITM_TYPE_FIXED64 = (int)'r';    // 'r' - fixed64
        private const byte ITM_TYPE_SINGLE = (int)'f';     // 'f' - single
        private const byte ITM_TYPE_DOUBLE = (int)'d';     // 'd' - double
        private const byte ITM_TYPE_STRING = (int)'s';     // 's' - string
        private const byte ITM_TYPE_BYTES = (int)'a';      // 'a' - bytes
        private const byte ITM_TYPE_STRUCT = (int)'m';    // 'm' - structure

	    private class Itm{
		    public byte type;
		    public string name;
		    public int id;
		    public Grp isa = null;
	    }
	    private class Grp{
		    public byte type;
		    public string name;
		    public int count = 0;
            public Dictionary<string, Itm> namedItems = new Dictionary<string, Itm>();
            public Dictionary<int, Itm> indexedItems = new Dictionary<int, Itm>();
	    }

        private Dictionary<string, Grp> namedGroups = new Dictionary<string, Grp>(); 
	    private Stack<Itm> stack = new Stack<Itm>();
	    private readonly Stream inputStream ;
	    private readonly Stream outputStream;
	    private bool eos = false;
	    private Grp version = null;

	    public static SequentialStructuredDataStream createReader(Stream stream)
	    {
		    return new SequentialStructuredDataStream (null, stream) ;
	    }

	    public static SequentialStructuredDataStream createWriter(Stream stream)
	    {
		    return new SequentialStructuredDataStream (stream, null) ;
	    }

	    private SequentialStructuredDataStream(Stream outputStream, Stream inputStream)
	    {
		    this.inputStream = inputStream;
		    this.outputStream = outputStream;
	    }

        private Grp ensureGroup( bool write,  string groupName,  byte groupType)
        {
            Grp g = null;
            if (namedGroups.ContainsKey(groupName))
            {
                g = namedGroups[groupName];
                if (g.type != groupType) {
            	    throw new Exception ("bad group type") ;
                }
            }
            else
            {
                g = new Grp();
                write_raw_varint32((1 << TAG_TYPE_BITS) | RAW_TYPE_SCHEMA);
                write_rawbyte(groupType);
                write_rawstring(groupName) ;
                g.type = groupType;
                g.name = groupName;
                namedGroups.Add(g.name, g);
            }
            if (version == null)
            {
                version = g;
            }
            return g;
        }

        private Grp peekIsa( bool write)
        {
            if (stack.Count > 0)
            {
                return stack.Peek().isa;
            }
            else if (version == null && write == true)
            {
                return ensureGroup(write, CURRENT_VERSION, ITM_TYPE_STRUCT);
            }
            else
            {
                return version;
            }
        }

        private Itm ensureItem( bool write,  string parentName,  byte parentType,  string itemName,  byte itemType,  string isaName,  byte isaType)
        {
            Grp parent = ensureGroup(write, parentName, parentType);
            return ensureItem(write, parent, itemName, itemType, isaName, isaType);
        }

        private Itm ensureItem( bool write,  Grp parent,  string itemName,  byte itemType,  string isaName,  byte isaType)
        {
            Grp isa = null;

            if (!string.IsNullOrEmpty(isaName))
            {
                isa = ensureGroup(write, isaName, isaType);
            }
            return ensureItem(write, parent, itemName, itemType, isa);
        }

        private Itm ensureItem( bool write,  Grp parent,  string itemName,  byte itemType,  Grp isa)
        {
            Itm item = null;

            if (parent.namedItems.ContainsKey(itemName))
            {
                item = parent.namedItems[itemName];
                if (itemType != item.type)
                {
                    throw new Exception("itemtypemismatch");
                }
            }
            else
            {
                item = new Itm();
                item.id = ++parent.count;
                item.name = itemName;
                item.type = itemType;
                item.isa = isa;
                parent.namedItems.Add(item.name, item);
                parent.indexedItems.Add(item.id, item);
                {
                    if (item.isa == null) {
                        write_raw_varint32((2 << TAG_TYPE_BITS) | RAW_TYPE_SCHEMA);
                    }
                    else {
                        write_raw_varint32((3 << TAG_TYPE_BITS) | RAW_TYPE_SCHEMA);
                    }
                    write_rawbyte(item.type);
                    write_rawstring(item.name); 
                    write_rawstring(parent.name); 
                    if (item.isa != null) {
                	    write_rawstring(item.isa.name); 
                    }
                }
            }
            return item;
        }

        public void writeStart( string itemName)
        {
            writeStart(itemName, itemName);
        }

        public void writeStart( string itemName,  string isaName)
        {
            stack.Push(ensureItem(true, peekIsa(true), itemName, ITM_TYPE_STRUCT, isaName, ITM_TYPE_STRUCT));
            write_raw_varint32((stack.Peek().id << TAG_TYPE_BITS) | RAW_TYPE_START_GROUP);
        }

        public void writeString( string itemName,  string value)
        {
            if (value == null) return;

            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_STRING, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_LENGTH_DELIMITED);
        
            write_rawstring(value) ;
        }

        public void writeUInt32( string itemName,  int value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT32, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_raw_varint32(value);

        }

        public void writeUInt64( string itemName,  long value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT64, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_raw_varint64(value);

        }

        public void writeSInt32( string itemName,  int value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINT32, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_raw_varint32((value << 1) ^ (value >> 31));

        }

        public void writeSInt64( string itemName,  long value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINT64, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_raw_varint64((value << 1) ^ (value >> 63));

        }

        public void writeFixed32( string itemName,  int value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_FIXED32, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED32);
            write_raw_littleendian32(value);

        }

        public void writeFixed64( string itemName,  long value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT64, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED64);
            write_raw_littleendian64(value);

        }

        public void writeDouble( string itemName,  double value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_DOUBLE, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED64);
            //write_raw_littleendian64(Double.doubleToRawLongBits(value));
            write_raw_littleendian64(BitConverter.DoubleToInt64Bits(value));
        }

        public void writeSingle( string itemName,  float value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINGLE, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED32);
            //write_raw_littleendian32(Float.floatToRawIntBits(value));
            write_raw_littleendian32(BitConverter.ToInt32(BitConverter.GetBytes(value), 0));
        }

        public void writeBool( string itemName,  bool value)
        {
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_BOOLEAN, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_rawbyte(value ? 1 : 0);

        }

        public void writeBytes( string itemName,  byte[] value)
        {
            if (value == null) return;
            writeBytes(itemName, value, 0, value.Length);
        }

        public void writeBytes( string itemName,  byte[] value,  int start,  int count)
        {
            if (value == null) return;

            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_BOOLEAN, null);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_LENGTH_DELIMITED);
            write_raw_varint32(count);
            write_rawbytes(value, start, count);
        }

        public void writeEnum( string itemName,  string enumValue)
        {
            writeEnum(itemName, itemName, enumValue);
        }

        public void writeEnum( string itemName,  string isaName,  string enumValue)
        {
            if (enumValue == null) return;
            if (version == null) ensureGroup(true, CURRENT_VERSION, ITM_TYPE_STRUCT);
            Itm enm = ensureItem(true, isaName, ITM_TYPE_ENUM, enumValue, ITM_TYPE_STRING, isaName, ITM_TYPE_ENUM);
            Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_ENUM, enm.isa);

            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
            write_raw_varint32(enm.id);
        }

        public void writeEnd()
        {
            Itm item = stack.Pop();
            write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_END_GROUP);
        }


        private void write_raw_varint32(int value)
        {
            while (true)
            {
                if ((value & ~0x7F) == 0)
                {
                    write_rawbyte(value);
                    return;
                }
                else
                {
                    write_rawbyte((value & 0x7F) | 0x80);
                    value = (int)((uint)value >> 7);
                }
            }
        }

        private void write_raw_littleendian64( long value)
        {
            write_rawbyte((int)(value) & 0xFF);
            write_rawbyte((int)(value >> 8) & 0xFF);
            write_rawbyte((int)(value >> 16) & 0xFF);
            write_rawbyte((int)(value >> 24) & 0xFF);
            write_rawbyte((int)(value >> 32) & 0xFF);
            write_rawbyte((int)(value >> 40) & 0xFF);
            write_rawbyte((int)(value >> 48) & 0xFF);
            write_rawbyte((int)(value >> 56) & 0xFF);
        }


        private void write_raw_varint64(long value)
        {
            while (true)
            {
                if ((value & ~0x7FL) == 0)
                {
                    write_rawbyte((int)value);
                    return;
                }
                else
                {
                    write_rawbyte(((int)value & 0x7F) | 0x80);
                    value = (long)((ulong)value >> 7);
                }
            }
        }


        private void write_raw_littleendian32( int value)
        {
            write_rawbyte((value) & 0xFF);
            write_rawbyte((value >> 8) & 0xFF);
            write_rawbyte((value >> 16) & 0xFF);
            write_rawbyte((value >> 24) & 0xFF);
        }

        private void write_rawbyte( int value)
        {
            if (outputStream != null)
            {
        	    try {
				    outputStream.WriteByte((byte)value);
			    } catch (IOException e) {
				    throw new Exception ("cannot write") ;
			    }
            }
        }

        private void write_rawbytes( byte[] value,  int offset,  int length)
        {
            if (outputStream != null)
            {
        	    try {
				    outputStream.Write(value, offset, length);
			    } catch (IOException e) {
				    throw new Exception ("cannot write") ;
			    }
            }
        }
    
        private void write_rawstring ( string value) {
    	    try {
	            byte[] bytes = Encoding.UTF8.GetBytes(value);
	            write_raw_varint32(bytes.Length);
	            write_rawbytes(bytes, 0, bytes.Length);
    	    } catch (Exception e) {
    		    throw new Exception ("utf-8 not supported") ;
    	    }
        }

        private class Fld
        {
            public bool is_start;
            public bool is_end;
            public string name;
            public int index;
            public int level;
            public string type_name;
            public string isa_name;
            public Object value;

            public Fld()
            {
                this.clear();
            }

            public void clear()
            {
                this.is_start = false;
                this.is_end = false;
                this.name = "";
                this.index = 0;
                this.level = 0;
                this.type_name = "";
                this.isa_name = "";
                this.value = null;
            }
        }

        private Fld scannedField = null;
    
 
        public bool readItem()
        {
            int wireTag;
            int wireType;
            int wireFieldNumber;
            Grp group = null;
            Itm item = null;

            if (scannedField == null)
            {
                scannedField = new Fld();
            }

            do
            {
                scannedField.clear();

                if (this.eos)
                {
                    return false;
                }

                wireTag = read_rawvarint32();
                if (wireTag == 0)
                {
                    return false;
                }
                wireType = wireTag & TAG_TYPE_MASK;
                //wireFieldNumber = wireTag >>> TAG_TYPE_BITS;
                wireFieldNumber = (int)((uint)wireTag >> TAG_TYPE_BITS);

                item = null;

                switch (wireType)
                {
                    case RAW_TYPE_END_GROUP:
                        {
                            item = stack.Pop();
                            scannedField.name = item.name;
                            scannedField.level = stack.Count;
                            scannedField.index = item.id;
                            scannedField.isa_name = item.isa.name;
                            scannedField.type_name = "end_group";
                            scannedField.is_end = true;
                        }
                        break;
                    case RAW_TYPE_FIXED32:
                        {
                            item = peekIsa(false).indexedItems[wireFieldNumber];
                            scannedField.name = item.name;
                            scannedField.level = stack.Count;
                            scannedField.index = item.id;
                            switch (item.type)
                            {
                                case ITM_TYPE_FIXED32:
                                    scannedField.type_name = "fixed32";
                                    scannedField.value = read_rawlittleendian32();
                                    break;
                                case ITM_TYPE_SINGLE:
                                    scannedField.type_name = "single";
                                    //scannedField.value = Float.intBitsToFloat(read_rawlittleendian32());
                                    scannedField.value = BitConverter.ToSingle(BitConverter.GetBytes(read_rawlittleendian32()), 0);
                                    break;
                                default:
                                    throw new Exception("bad type for fixed32");
                            }
                        }
                        break;
                    case RAW_TYPE_FIXED64:
                        {
                            item = peekIsa(false).indexedItems[wireFieldNumber];
                            scannedField.name = item.name;
                            scannedField.level = stack.Count;
                            scannedField.index = item.id;
                            switch (item.type)
                            {
                                case ITM_TYPE_FIXED64:
                                    scannedField.type_name = "fixed64";
                                    scannedField.value = read_rawlittleendian64();
                                    break;
                                case ITM_TYPE_DOUBLE:
                                    scannedField.type_name = "double";
                                    //scannedField.value = Double.longBitsToDouble(read_rawlittleendian64());
                                    scannedField.value = BitConverter.Int64BitsToDouble(read_rawlittleendian64());
                                    break;
                                default:
                                    throw new Exception("bad type for fixed64");
                            }
                        }
                        break;
                    case RAW_TYPE_LENGTH_DELIMITED:
                        {
                            item = peekIsa(false).indexedItems[wireFieldNumber];
                            scannedField.name = item.name;
                            scannedField.level = stack.Count;
                            scannedField.index = item.id;
                            switch (item.type)
                            {
                                case ITM_TYPE_STRING:
                                {
                                    scannedField.type_name = "string";
                                    scannedField.value = read_rawstring() ;
                                }
                                    break;
                                case ITM_TYPE_BYTES:
                                {
                                    scannedField.type_name = "bytes";
                                    int size = read_rawvarint32();
                                    byte[] bytes = read_rawbytes(size);
                                    scannedField.value = bytes;
                                }
                                    break;
                                default:
                                    throw new Exception("bad type for length delimited");
                            }
                        }
                        break;
                    case RAW_TYPE_SCHEMA:
                        switch (wireFieldNumber)
                        {
                            case 1:
                                {
                                    group = new Grp();
                                    group.type = (byte)read_rawbyte();
                                    group.name = read_rawstring() ;
                                    namedGroups.Add(group.name, group);
                                    if (version == null)
                                    {
                                        version = group;
                                    }
                                }
                                break;
                            case 2:
                                {
                                    item = new Itm();
                                    item.type = (byte)read_rawbyte();
                                    item.name = read_rawstring() ;
                                    group = namedGroups[read_rawstring()] ;
                                    item.id = ++group.count;
                                    group.namedItems.Add(item.name, item);
                                    group.indexedItems.Add(item.id, item);
                                }
                                break;
                            case 3:
                                {
                                    item = new Itm();
                                    item.type = (byte)read_rawbyte();
                                    item.name = read_rawstring() ;
                                    group = namedGroups[read_rawstring()] ;
                                    item.id = ++group.count;
                                    item.isa = namedGroups[read_rawstring()];
                                    group.namedItems.Add(item.name, item);
                                    group.indexedItems.Add(item.id, item);
                                }
                                break;
                            default:
                                throw new Exception("bad field number for reserved");

                        }
                        break;
                    case RAW_TYPE_START_GROUP:
                        {
                            item = peekIsa(false).indexedItems[wireFieldNumber];
                            scannedField.name = item.name;
                            scannedField.index = item.id;
                            scannedField.level = stack.Count;
                            scannedField.type_name = "start_group";
                            scannedField.is_start = true;
                            stack.Push(item);
                        }
                        break;
                    case RAW_TYPE_VARINT:
                        {
                            item = peekIsa(false).indexedItems[wireFieldNumber];
                            scannedField.name = item.name;
                            scannedField.level = stack.Count;
                            scannedField.index = item.id;
                            switch (item.type)
                            {
                                case ITM_TYPE_BOOLEAN:
                                    scannedField.type_name = "bool";
                                    scannedField.value = read_rawvarint32() != 0;
                                    break;
                                case ITM_TYPE_ENUM:
                                    scannedField.type_name = "enum";
                                    scannedField.isa_name = item.isa.name;
                                    scannedField.value = item.isa.indexedItems[read_rawvarint32()].name;
                                    break;
                                case ITM_TYPE_UINT32:
                                    scannedField.type_name = "uint32";
                                    scannedField.value = read_rawvarint32();
                                    break;
                                case ITM_TYPE_UINT64:
                                    scannedField.type_name = "uint64";
                                    scannedField.value = read_rawvarint64();
                                    break;
                                case ITM_TYPE_SINT32:
                                    scannedField.type_name = "sint32";
                                    {
                                        int n = read_rawvarint32();
                                        scannedField.value = ((int)((uint)n >> 1)) ^ -(n & 1);
                                    }
                                    break;
                                case ITM_TYPE_SINT64:
                                    scannedField.type_name = "sint64";
                                    {
                                        long n = read_rawvarint64();
                                        scannedField.value = ((long)((ulong)n >> 1)) ^ -(n & 1); 
                                    }
                                    break;
                                default:
                                    throw new Exception("bad type for readVarint");
                            }
                        }
                        break;
                    default:
                        throw new Exception("invalidwiretype");
                }
            } while (wireType == RAW_TYPE_SCHEMA);

            return true;
        }

        public bool isStartItem()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.is_start;
        }

        public bool isEndItem()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.is_end;
        }

        public string itemTypeName()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.type_name;
        }

        public string itemTypeIsaName()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.isa_name;
        }

        public string itemName()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.name;
        }

        public int itemIndex()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.index;
        }

        public int itemLevel()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.level;
        }

        public Object itemValue()
        {
            if (scannedField == null) throw new Exception("no item read");
            return scannedField.value;
        }

        private int read_rawvarint32()
        {
            byte tmp = read_rawbyte();
            if ((tmp & 0x80) == 0)
            {
                return tmp;
            }
            int result = tmp & 0x7f;
            if (((tmp = read_rawbyte()) & 0x80) == 0)
            {
                result |= tmp << 7;
            }
            else
            {
                result |= (tmp & 0x7f) << 7;
                if (((tmp = read_rawbyte()) & 0x80) == 0)
                {
                    result |= tmp << 14;
                }
                else
                {
                    result |= (tmp & 0x7f) << 14;
                    if (((tmp = read_rawbyte()) & 0x80) == 0)
                    {
                        result |= tmp << 21;
                    }
                    else
                    {
                        result |= (tmp & 0x7f) << 21;
                        result |= (tmp = read_rawbyte()) << 28;
                        if ((tmp & 0x80) != 0)
                        {
                            for (int i = 0; i < 5; i++)
                            {
                                if ((((tmp = read_rawbyte()) & 0x80) == 0))
                                {
                                    return result;
                                }
                            }
                            throw new Exception("malformd varint32");
                        }
                    }
                }
            }
            return result;
        }

        private long read_rawvarint64()
        {
            int shift = 0;
            long result = 0;
            while (shift < 64)
            {
                byte b = read_rawbyte();
                result |= (long)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return result;
                }
                shift += 7;
            }
            throw new Exception("malformed varint64");
        }

        private int read_rawlittleendian32()
        {
            byte b1 = read_rawbyte();
            byte b2 = read_rawbyte();
            byte b3 = read_rawbyte();
            byte b4 = read_rawbyte();
            return (((int)b1 & 0xff)) |
                    (((int)b2 & 0xff) << 8) |
                    (((int)b3 & 0xff) << 16) |
                    (((int)b4 & 0xff) << 24);
        }

        private long read_rawlittleendian64()
        {
            byte b1 = read_rawbyte();
            byte b2 = read_rawbyte();
            byte b3 = read_rawbyte();
            byte b4 = read_rawbyte();
            byte b5 = read_rawbyte();
            byte b6 = read_rawbyte();
            byte b7 = read_rawbyte();
            byte b8 = read_rawbyte();

            return (((long)b1 & 0xff)) |
                    (((long)b2 & 0xff) << 8) |
                    (((long)b3 & 0xff) << 16) |
                    (((long)b4 & 0xff) << 24) |
                    (((long)b5 & 0xff) << 32) |
                    (((long)b6 & 0xff) << 40) |
                    (((long)b7 & 0xff) << 48) |
                    (((long)b8 & 0xff) << 56);
        }

        private byte read_rawbyte()
        {
            int b;
		    try {
			    b = inputStream.ReadByte();
		    } catch (IOException e) {
			    b = -1 ;
		    }
            if (b < 0)
            {
                this.eos = true;
                return 0;
            }
            return (byte)b;
        }

        private byte[] read_rawbytes( int size)
        {
            byte[] a = new byte[size];
            int c;
		    try {
			    c = inputStream.Read(a, 0, size);
		    } catch (IOException e) {
			    c = -1 ;
		    }
            if (c != size)
            {
                this.eos = true;
                throw new Exception("not enough bytes");
            }
            return a;
        }
    
        private string read_rawstring() {
            int size = read_rawvarint32();
            byte[] bytes = read_rawbytes(size);
    	    try {
    		    return Encoding.UTF8.GetString(bytes) ;
    	    } catch (Exception e) {
    		    throw new Exception ("utf-8 not supported") ;
    	    }
        }

        public void clear()
        {
            try
            {
                if (namedGroups != null)
                {
                    foreach (Grp g in namedGroups.Values)
                    {
                        g.indexedItems.Clear();
                        g.namedItems.Clear();
                        g.indexedItems = null;
                        g.namedItems = null;
                    }
                    namedGroups.Clear();
                }
                if (stack != null)
                {
                    stack.Clear();
                    stack = null;
                }
                if (scannedField != null)
                {
                    scannedField.clear();
                    scannedField = null;
                }
            }
            catch (Exception e)
            {
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        ~SequentialStructuredDataStream() 
        {
            Dispose(false);
        }
        protected virtual void Dispose(bool disposing)
        {
            this.clear();
        }
     }
