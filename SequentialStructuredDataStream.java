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
 
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Stack;

/*
  * SequentialStructuredDataStream (java and c#)
 * ------------------------------
 * author: chitta.pardeshi@gmail.com 
 * May 2014
 */

public class SequentialStructuredDataStream {
	private static final String CURRENT_VERSION = "ssds0";
	private static final int RAW_TYPE_VARINT = 0;
	private static final int RAW_TYPE_FIXED64 = 1;
	private static final int RAW_TYPE_LENGTH_DELIMITED = 2;
	private static final int RAW_TYPE_START_GROUP = 3;
	private static final int RAW_TYPE_END_GROUP = 4;
	private static final int RAW_TYPE_FIXED32 = 5;
	private static final int RAW_TYPE_SCHEMA = 6;

	private static final int TAG_TYPE_BITS = 3;
	private static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

	private static final byte ITM_TYPE_BOOLEAN = (int)'b';    // 'b' - boolean
	private static final byte ITM_TYPE_ENUM = (int)'e';       // 'e' - enumeration
	private static final byte ITM_TYPE_UINT32 = (int)'i';     // 'i' - uint32
	private static final byte ITM_TYPE_UINT64 = (int)'j';     // 'j' - uint64
	private static final byte ITM_TYPE_SINT32 = (int)'u';     // 'u' - sint32
	private static final byte ITM_TYPE_SINT64 = (int)'v';     // 'v' - sint64
	private static final byte ITM_TYPE_FIXED32 = (int)'q';    // 'q' - fixed32
	private static final byte ITM_TYPE_FIXED64 = (int)'r';    // 'r' - fixed64
	private static final byte ITM_TYPE_SINGLE = (int)'f';     // 'f' - single
	private static final byte ITM_TYPE_DOUBLE = (int)'d';     // 'd' - double
	private static final byte ITM_TYPE_STRING = (int)'s';     // 's' - string
	private static final byte ITM_TYPE_BYTES = (int)'a';      // 'a' - bytes
	private static final byte ITM_TYPE_STRUCT = (int)'m';    // 'm' - structure

	private class Itm{
		public byte type;
		public String name;
		public int id;
		public Grp isa = null;
	}
	private class Grp{
		public byte type;
		public String name;
		public int count = 0;
		public HashMap<String, Itm> namedItems = new HashMap<String, Itm>();
		public HashMap<Integer, Itm> indexedItems = new HashMap<Integer, Itm>();
	}

	private final HashMap<String, Grp> namedGroups = new HashMap<String, Grp>(); 
	private Stack<Itm> stack = new Stack<Itm>();
	private final InputStream inputStream  ;
	private final OutputStream outputStream  ;
	private boolean eos = false;
	private Grp version = null;
	
	public static SequentialStructuredDataStream createReader(final InputStream stream)
	{
		return new SequentialStructuredDataStream (null, stream) ;
	}

	public static SequentialStructuredDataStream createWriter(final OutputStream stream)
	{
		return new SequentialStructuredDataStream (stream, null) ;
	}

	private SequentialStructuredDataStream(final OutputStream outputStream, final InputStream inputStream)
	{
		this.inputStream = inputStream;
		this.outputStream = outputStream;
	}

    private Grp ensureGroup(final boolean write, final String groupName, final byte groupType)
    {
        Grp g = null;
        if (namedGroups.containsKey(groupName))
        {
            g = namedGroups.get(groupName);
            if (g.type != groupType) {
            	throw new RuntimeException ("bad group type") ;
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
            namedGroups.put(g.name, g);
        }
        if (version == null)
        {
            version = g;
        }
        return g;
    }

    private Grp peekIsa(final boolean write)
    {
        if (stack.size() > 0)
        {
            return stack.peek().isa;
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

    private Itm ensureItem(final boolean write, final String parentName, final byte parentType, final String itemName, final byte itemType, final String isaName, final byte isaType)
    {
        Grp parent = ensureGroup(write, parentName, parentType);
        return ensureItem(write, parent, itemName, itemType, isaName, isaType);
    }

    private Itm ensureItem(final boolean write, final Grp parent, final String itemName, final byte itemType, final String isaName, final byte isaType)
    {
        Grp isa = null;

        if (isaName != null && !isaName.isEmpty())
        {
            isa = ensureGroup(write, isaName, isaType);
        }
        return ensureItem(write, parent, itemName, itemType, isa);
    }

    private Itm ensureItem(final boolean write, final Grp parent, final String itemName, final byte itemType, final Grp isa)
    {
        Itm item = null;

        if (parent.namedItems.containsKey(itemName))
        {
            item = parent.namedItems.get(itemName);
            if (itemType != item.type)
            {
                throw new RuntimeException("itemtypemismatch");
            }
        }
        else
        {
            item = new Itm();
            item.id = ++parent.count;
            item.name = itemName;
            item.type = itemType;
            item.isa = isa;
            parent.namedItems.put(item.name, item);
            parent.indexedItems.put(item.id, item);
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

    public void writeStart(final String itemName)
    {
        writeStart(itemName, itemName);
    }

    public void writeStart(final String itemName, final String isaName)
    {
        stack.push(ensureItem(true, peekIsa(true), itemName, ITM_TYPE_STRUCT, isaName, ITM_TYPE_STRUCT));
        write_raw_varint32((stack.peek().id << TAG_TYPE_BITS) | RAW_TYPE_START_GROUP);
    }

    public void writeString(final String itemName, final String value)
    {
        if (value == null) return;

        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_STRING, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_LENGTH_DELIMITED);
        
        write_rawstring(value) ;
    }

    public void writeUInt32(final String itemName, final int value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT32, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
        write_raw_varint32(value);

    }

    public void writeUInt64(final String itemName, final long value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT64, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
        write_raw_varint64(value);

    }

    public void writeSInt32(final String itemName, final int value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINT32, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
        write_raw_varint32((value << 1) ^ (value >> 31));

    }

    public void writeSInt64(final String itemName, final long value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINT64, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
        write_raw_varint64((value << 1) ^ (value >> 63));

    }

    public void writeFixed32(final String itemName, final int value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_FIXED32, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED32);
        write_raw_littleendian32(value);

    }

    public void writeFixed64(final String itemName, final long value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_UINT64, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED64);
        write_raw_littleendian64(value);

    }

    public void writeDouble(final String itemName, final double value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_DOUBLE, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED64);
        write_raw_littleendian64(Double.doubleToRawLongBits(value));
    }

    public void writeSingle(final String itemName, final float value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_SINGLE, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_FIXED32);
        write_raw_littleendian32(Float.floatToRawIntBits(value));
    }

    public void writeBool(final String itemName, final boolean value)
    {
        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_BOOLEAN, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_VARINT);
        write_rawbyte(value ? 1 : 0);

    }

    public void writeBytes(final String itemName, final byte[] value)
    {
        if (value == null) return;
        writeBytes(itemName, value, 0, value.length);
    }

    public void writeBytes(final String itemName, final byte[] value, final int start, final int count)
    {
        if (value == null) return;

        Itm item = ensureItem(true, peekIsa(true), itemName, ITM_TYPE_BOOLEAN, null);

        write_raw_varint32((item.id << TAG_TYPE_BITS) | RAW_TYPE_LENGTH_DELIMITED);
        write_raw_varint32(count);
        write_rawbytes(value, start, count);
    }

    public void writeEnum(final String itemName, final String enumValue)
    {
        writeEnum(itemName, itemName, enumValue);
    }

    public void writeEnum(final String itemName, final String isaName, final String enumValue)
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
        Itm item = stack.pop();
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
                value >>>= 7;
            }
        }
    }

    private void write_raw_littleendian64(final long value)
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
                value >>>= 7;
            }
        }
    }


    private void write_raw_littleendian32(final int value)
    {
        write_rawbyte((value) & 0xFF);
        write_rawbyte((value >> 8) & 0xFF);
        write_rawbyte((value >> 16) & 0xFF);
        write_rawbyte((value >> 24) & 0xFF);
    }

    private void write_rawbyte(final int value)
    {
        if (outputStream != null)
        {
        	try {
				outputStream.write((byte)value);
			} catch (IOException e) {
				throw new RuntimeException ("cannot write") ;
			}
        }
    }

    private void write_rawbytes(final byte[] value, final int offset, final int length)
    {
        if (outputStream != null)
        {
        	try {
				outputStream.write(value, offset, length);
			} catch (IOException e) {
				throw new RuntimeException ("cannot write") ;
			}
        }
    }
    
    private void write_rawstring (final String value) {
    	try {
	        byte[] bytes = value.getBytes("utf-8");
	        write_raw_varint32(bytes.length);
	        write_rawbytes(bytes, 0, bytes.length);
    	} catch (Exception e) {
    		throw new RuntimeException ("utf-8 not supported") ;
    	}
    }

    private class Fld
    {
        public boolean is_start;
        public boolean is_end;
        public String name;
        public int index;
        public int level;
        public String type_name;
        public String isa_name;
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
    
 
    public boolean readItem()
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
            wireFieldNumber = wireTag >>> TAG_TYPE_BITS;

            item = null;

            switch (wireType)
            {
                case RAW_TYPE_END_GROUP:
                    {
                        item = stack.pop();
                        scannedField.name = item.name;
                        scannedField.level = stack.size();
                        scannedField.index = item.id;
                        scannedField.isa_name = item.isa.name;
                        scannedField.type_name = "end_group";
                        scannedField.is_end = true;
                    }
                    break;
                case RAW_TYPE_FIXED32:
                    {
                        item = peekIsa(false).indexedItems.get(wireFieldNumber);
                        scannedField.name = item.name;
                        scannedField.level = stack.size();
                        scannedField.index = item.id;
                        switch (item.type)
                        {
                            case ITM_TYPE_FIXED32:
                                scannedField.type_name = "fixed32";
                                scannedField.value = read_rawlittleendian32();
                                break;
                            case ITM_TYPE_SINGLE:
                                scannedField.type_name = "single";
                                scannedField.value = Float.intBitsToFloat(read_rawlittleendian32());
                                break;
                            default:
                                throw new RuntimeException("bad type for fixed32");
                        }
                    }
                    break;
                case RAW_TYPE_FIXED64:
                    {
                        item = peekIsa(false).indexedItems.get(wireFieldNumber);
                        scannedField.name = item.name;
                        scannedField.level = stack.size();
                        scannedField.index = item.id;
                        switch (item.type)
                        {
                            case ITM_TYPE_FIXED64:
                                scannedField.type_name = "fixed64";
                                scannedField.value = read_rawlittleendian64();
                                break;
                            case ITM_TYPE_DOUBLE:
                                scannedField.type_name = "double";
                                scannedField.value = Double.longBitsToDouble(read_rawlittleendian64());
                                break;
                            default:
                                throw new RuntimeException("bad type for fixed64");
                        }
                    }
                    break;
                case RAW_TYPE_LENGTH_DELIMITED:
                    {
                        item = peekIsa(false).indexedItems.get(wireFieldNumber);
                        scannedField.name = item.name;
                        scannedField.level = stack.size();
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
                                throw new RuntimeException("bad type for length delimited");
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
                                namedGroups.put(group.name, group);
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
                                group = namedGroups.get(read_rawstring()) ;
                                item.id = ++group.count;
                                group.namedItems.put(item.name, item);
                                group.indexedItems.put(item.id, item);
                            }
                            break;
                        case 3:
                            {
                                item = new Itm();
                                item.type = (byte)read_rawbyte();
                                item.name = read_rawstring() ;
                                group = namedGroups.get(read_rawstring()) ;
                                item.id = ++group.count;
                                item.isa = namedGroups.get(read_rawstring());
                                group.namedItems.put(item.name, item);
                                group.indexedItems.put(item.id, item);
                            }
                            break;
                        default:
                            throw new RuntimeException("bad field number for reserved");

                    }
                    break;
                case RAW_TYPE_START_GROUP:
                    {
                        item = peekIsa(false).indexedItems.get(wireFieldNumber);
                        scannedField.name = item.name;
                        scannedField.index = item.id;
                        scannedField.level = stack.size();
                        scannedField.type_name = "start_group";
                        scannedField.is_start = true;
                        stack.push(item);
                    }
                    break;
                case RAW_TYPE_VARINT:
                    {
                        item = peekIsa(false).indexedItems.get(wireFieldNumber);
                        scannedField.name = item.name;
                        scannedField.level = stack.size();
                        scannedField.index = item.id;
                        switch (item.type)
                        {
                            case ITM_TYPE_BOOLEAN:
                                scannedField.type_name = "boolean";
                                scannedField.value = read_rawvarint32() != 0;
                                break;
                            case ITM_TYPE_ENUM:
                                scannedField.type_name = "enum";
                                scannedField.isa_name = item.isa.name;
                                scannedField.value = item.isa.indexedItems.get(read_rawvarint32()).name;
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
                                	scannedField.value = (n >>> 1) ^ -(n & 1);
                                }
                                break;
                            case ITM_TYPE_SINT64:
                                scannedField.type_name = "sint64";
                                {
                                	long n = read_rawvarint64();
                                	scannedField.value = (n >>> 1) ^ -(n & 1);
                                }
                                break;
                            default:
                                throw new RuntimeException("bad type for readVarint");
                        }
                    }
                    break;
                default:
                    throw new RuntimeException("invalidwiretype");
            }
        } while (wireType == RAW_TYPE_SCHEMA);

        return true;
    }

    public boolean isStartItem()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.is_start;
    }

    public boolean isEndItem()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.is_end;
    }

    public String itemTypeName()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.type_name;
    }

    public String itemTypeIsaName()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.isa_name;
    }

    public String itemName()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.name;
    }

    public int itemIndex()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.index;
    }

    public int itemLevel()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
        return scannedField.level;
    }

    public Object itemValue()
    {
        if (scannedField == null) throw new RuntimeException("no item read");
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
                        throw new RuntimeException("malformd varint32");
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
        throw new RuntimeException("malformed varint64");
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
			b = inputStream.read();
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

    private byte[] read_rawbytes(final int size)
    {
        byte[] a = new byte[size];
        int c;
		try {
			c = inputStream.read(a, 0, size);
		} catch (IOException e) {
			c = -1 ;
		}
        if (c != size)
        {
            this.eos = true;
            throw new RuntimeException("not enough bytes");
        }
        return a;
    }
    
    private String read_rawstring() {
        int size = read_rawvarint32();
        byte[] bytes = read_rawbytes(size);
    	try {
    		return new String(bytes, "utf-8");
    	} catch (Exception e) {
    		throw new RuntimeException ("utf-8 not supported") ;
    	}
    }

    public void clear()
    {
        try
        {
            if (namedGroups != null)
            {
                for (Grp g : namedGroups.values())
                {
                    g.indexedItems.clear();
                    g.namedItems.clear();
                    g.indexedItems = null;
                    g.namedItems = null;
                }
                namedGroups.clear();
            }
            if (stack != null)
            {
                stack.clear();
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
 }
