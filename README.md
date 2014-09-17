SequentialStructuredDataStream
==============================

Sequential Structured Data Stream

Like XML and JSON, SequentialStructuredDataStream provides language-neutral, platform-neutral
way of serializing any structured data for use in communications protocols, data storage, and more.

Unlike XML and JSON it stores primitive data types in binary format and removes overhead 
of repeating name with each value.

The stream can only be read sequentially and reader needs no prior knowledge of structures present
in the stream.

It provides a simple API for writing and reading.

writing:

 	following API provides nesting of structures:
 
 		void writeStart(string name);
 		void writeStart(string name, string typeName);
 		void writeEnd();

 	following primitives API is supported:
 		void writeString (string name, string value);		// stored in UTF8 bytes
 		void writeUInt32(string name, UInt32 value);		// stored as variable length integer
 		void writeUInt64(string name, UInt64 value);		// stored as variable length integer
 		void writeSInt32(string name, SInt32 value);		// stored as variable length integer with zigzag encoding
 		void writeSInt64(string name, SInt64 value);		// stored as variable length integer with zigzag encoding
 		void writeFixed32(string name, Fixed32 value);		// stored as 4 bytes
 		void writeFixed64(string name, Fixed64 value);		// stored as 8 bytes
 		void writeDouble(string name, double value);		// stored as double
 		void writeSingle(string name, float value);			// stored as float
 		void writeBool(string name, boolean value);			// stored as bit
 		void writeBytes(string name, byte[] value);			// stored as bytes
 		void writeBytes(string name, byte[] value, integer start, integer count) ;
 		void writeEnum(string name, string enumValue);		// distinct values are stored as part of schema
 		void writeEnum(string name, string typeName, string enumValue) ;
 
 reading:
 
 	The reader can traverse the data with simple while loop as follows:
 
 		while (readItem()) {
 			if (isStartItem()) {
 				itemName() ;
 			}
 			else if (isEndItem()) {
 				itemName() ;
 			}
 			else {
 				itemName() ;
 				itemValue();
 			}
 		}
 
 	following API supports reading:
 
 		boolean	readItem();				// reads the next item or returns false for end of stream.
			string	itemName();				// name of the item
 		object	itemValue();			// value of the item
 		boolean	isStartItem();			// indicates item read is start of structure
 		boolean	isEndItem();			// indicates item read is end of structure
 		string	itemTypeName();			// returns optional type name specified while writing
 		integer	itemIndex();			// returns internal index number assigned for this field.
 		integer	itemLevel();			// returns nested level of the item.
 
 The simple and powerful API allows generic tools to be written to
 visualize, combine, transform, query multiple disparate self-describing
 data-sets.
