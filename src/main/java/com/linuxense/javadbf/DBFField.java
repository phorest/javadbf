/*
  DBFField
	Class represents a "field" (or column) definition of a DBF data structure.

  This file is part of JavaDBF packege.

  author: anil@linuxense.com
  license: LGPL (http://www.gnu.org/copyleft/lesser.html)

  $Id: DBFField.java,v 1.7 2004-03-31 10:50:11 anil Exp $
*/

package com.linuxense.javadbf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
	DBFField represents a field specification in an dbf file.

	DBFField objects are either created and added to a DBFWriter object or obtained
	from DBFReader object through getField( int) query.

*/
public class DBFField {

	public enum DataType {
		CHARACTER('C'), LOGICAL('L'), NUMBER('N'), FLOAT('F'), DATE('D'), MEMO('M'), INTEGER('I');

		private final char code;
		
	    private DataType(char code) {
	        this.code = code;
	    }

	    @Override
	    public String toString() {
	        return String.valueOf((char)code);
	    }

	    public char code() {
	        return code;
	    }

	    public static DataType fromCode(char code) {
	        for (DataType t : DataType.values()) {
	            if (t.code == code) {
	                return t;
	            }
	        }
	        throw new IllegalArgumentException(String.valueOf(code));
	    }
	}


	/* Field struct variables start here */
	byte[] fieldName = new byte[11]; /* 0-10*/
	char dataType;                    /* 11 */
	int reserv1;                      /* 12-15 */
	int fieldLength;                 /* 16 */
	byte decimalCount;                /* 17 */
	short reserv2;                    /* 18-19 */
	byte workAreaId;                  /* 20 */
	short reserv3;                    /* 21-22 */
	byte setFieldsFlag;               /* 23 */
	byte[] reserv4 = new byte[ 7];    /* 24-30 */
	byte indexFieldFlag;              /* 31 */
	/* Field struct variables end here */

	/* other class variables */
	int nameNullIndex = 0;

	/**
	Creates a DBFField object from the data read from the given DataInputStream.

	The data in the DataInputStream object is supposed to be organised correctly
	and the stream "pointer" is supposed to be positioned properly.

	@param channel DataInputStream
	@return Returns the created DBFField object.
	@throws IOException If any stream reading problems occures.
	*/
	protected static DBFField createField( ReadableByteChannel channel) 
	throws IOException {

		ByteBuffer buff = ByteBuffer.allocate(32);	
		buff.limit(1);
		
		channel.read(buff);
		
		buff.flip();
		
		if( buff.get() == (byte)0x0d) { /* 0 */
			return null;
		}
		
		buff.limit(32);
		channel.read(buff);
		buff.flip();
		buff.order(ByteOrder.LITTLE_ENDIAN);
				
		DBFField field = new DBFField();
		buff.get(field.fieldName);	/* 0-10 */

		for( int i=0; i<field.fieldName.length; i++) {
			if( field.fieldName[ i] == (byte)0) {
				field.nameNullIndex = i;
				break;
			}
		}

		field.dataType = (char)buff.get(); /* 11 */
		field.reserv1 = buff.getInt(); /* 12-15 */
		field.fieldLength = buff.get() & 0xff;  /* 16 */
		field.decimalCount = buff.get(); /* 17 */
		field.reserv2 = buff.getShort(); /* 18-19 */
		field.workAreaId = buff.get(); /* 20 */
		field.reserv2 = buff.getShort(); /* 21-22 */
		field.setFieldsFlag = buff.get(); /* 23 */
		buff.get( field.reserv4); /* 24-30 */
		field.indexFieldFlag = buff.get(); /* 31 */

		return field;
	}

	/**
		Writes the content of DBFField object into the stream as per
		DBF format specifications.

		@param os OutputStream
		@throws IOException if any stream related issues occur.
	*/
	protected void write( WritableByteChannel byteChannel)
	throws IOException {
		ByteBuffer buff = ByteBuffer.allocate(32);	

		// Field Name
		buff.put(fieldName);        /* 0-10 */

		// data type
		buff.put((byte)dataType); /* 11 */
		buff.putInt( 0x00);   /* 12-15 */
		buff.put( (byte)fieldLength); /* 16 */
		buff.put( decimalCount); /* 17 */
		buff.putShort( (short)0x00); /* 18-19 */
		buff.put( (byte)0x00); /* 20 */
		buff.putShort( (short)0x00); /* 21-22 */
		buff.put( (byte)0x00); /* 23 */
		buff.put( new byte[7]); /* 24-30*/
		buff.put( (byte)0x00); /* 31 */
		
		buff.flip();
		
		byteChannel.write(buff);
	}

	/**
		Returns the name of the field.

		@return Name of the field as String.
	*/
	public String getName() {

		return new String( this.fieldName, 0, nameNullIndex);
	}

	/**
		Returns the data type of the field.

		@return Data type as byte.
	*/
	public DataType getDataType() {
		return DataType.fromCode(dataType);
	}

	/**
		Returns field length.

		@return field length as int.
	*/
	public int getFieldLength() {

		return fieldLength;
	}

	/**
		Returns the decimal part. This is applicable
		only if the field type if of numeric in nature.

		If the field is specified to hold integral values
		the value returned by this method will be zero.

		@return decimal field size as int.
	*/
	public int getDecimalCount() {

		return decimalCount;
	}

	// Setter methods

	// byte[] fieldName = new byte[ 11]; /* 0-10*/
  // byte dataType;                    /* 11 */
  // int reserv1;                      /* 12-15 */
  // byte fieldLength;                 /* 16 */
  // byte decimalCount;                /* 17 */
  // short reserv2;                    /* 18-19 */
  // byte workAreaId;                  /* 20 */
  // short reserv3;                    /* 21-22 */
  // byte setFieldsFlag;               /* 23 */
  // byte[] reserv4 = new byte[ 7];    /* 24-30 */
  // byte indexFieldFlag;              /* 31 */

	/**
	 * @deprecated This method is depricated as of version 0.3.3.1 and is replaced by {@link #setName( String)}.
	 */
	public void setFieldName( String value) {

		setName( value);
	}

	/**
		Sets the name of the field.

		@param name of the field as String.
		@since 0.3.3.1
	*/
	public void setName( String value) {

		if( value == null) {

			throw new IllegalArgumentException( "Field name cannot be null");
		}

		byte[] bytes = value.getBytes();
		
		if( bytes.length == 0 || bytes.length > 10) {

			throw new IllegalArgumentException( "Field name should be of length 0-10");
		}

		fieldName = new byte[11];		
		System.arraycopy(bytes, 0, fieldName, 0, bytes.length);
		this.nameNullIndex = bytes.length;
	}

	/**
		Sets the data type of the field.

		@param type of the field. One of the following:<br>
		C, L, N, F, D, M
	*/
	public void setDataType( byte value) {

		switch( value) {

			case 'D':
				this.fieldLength = 8; /* fall through */
			case 'C':
			case 'L':
			case 'N':
			case 'F':
			case 'M':

				this.dataType = (char)value;
				break;

			default:
				throw new IllegalArgumentException( "Unknown data type");
		}
	}

	/**
		Length of the field.
		This method should be called before calling setDecimalCount().

		@param Length of the field as int.
	*/
	public void setFieldLength( int value) {

		if( value <= 0) {

			throw new IllegalArgumentException( "Field length should be a positive number");
		}

		if( this.dataType == DataType.DATE.code) {
			throw new UnsupportedOperationException( "Cannot do this on a Date field");
		}

		fieldLength = value;
	}

	/**
		Sets the decimal place size of the field.
		Before calling this method the size of the field
		should be set by calling setFieldLength().

		@param Size of the decimal field.
	*/
	public void setDecimalCount( int value) {

		if( value < 0) {

			throw new IllegalArgumentException( "Decimal length should be a positive number");
		}

		if( value > fieldLength) {

			throw new IllegalArgumentException( "Decimal length should be less than field length");
		}

		decimalCount = (byte)value;
	}

}
