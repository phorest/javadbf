/*
	DBFWriter
	Class for defining a DBF structure and addin data to that structure and 
	finally writing it to an OutputStream.

	This file is part of JavaDBF packege.

	author: anil@linuxense.com
	license: LGPL (http://www.gnu.org/copyleft/lesser.html)

	$Id: DBFWriter.java,v 1.11 2004-07-19 08:57:31 anil Exp $
*/
package com.linuxense.javadbf;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Vector;

import org.joda.time.LocalDate;

/**
	An object of this class can create a DBF file.

	Create an object, <br>
	then define fields by creating DBFField objects and<br>
	add them to the DBFWriter object<br>
	add records using the addRecord() method and then<br>
	call write() method.
*/
public class DBFWriter extends DBFBase {

	/* other class variables */
	Vector<Object[]> v_records = new Vector<Object[]>();
	int recordCount = 0;
	FileChannel dataChannel = null; /* Open and append records to an existing DBF */
	boolean appendMode = false;

	/**
		Creates an empty Object.
	*/
	public DBFWriter() {
		header = new DBFHeader();
	}

	/**
	 	Creates a DBFWriter which can append to records to an existing DBF file.
		@param dbfFile. The file passed in shouls be a valid DBF file.
		@exception Throws DBFException if the passed in file does exist but not a valid DBF file, or if an IO error occurs.
	 */
	public DBFWriter( File dbfFile) 
	throws DBFException {

		try {
			dataChannel = new RandomAccessFile( dbfFile, "rw").getChannel();

			/* before proceeding check whether the passed in File object 
			 is an empty/non-existent file or not.
			 */
			if( !dbfFile.exists() || dbfFile.length() == 0) {

				header = new DBFHeader();
				return;
			}

			header = new DBFHeader();
			header.read( dataChannel);

			/* position file pointer at the end of the raf */
			dataChannel.position(dataChannel.size()-1 /* to ignore the END_OF_DATA byte at EoF */);
		}
		catch( FileNotFoundException e) {

			throw new DBFException( "Specified file is not found. " + e.getMessage(), e);
		}
		catch( IOException e) {

			throw new DBFException( e.getMessage() + " while reading header", e);
		}

		this.recordCount = header.getNumberOfRecords();
	}

	/**
		Sets fields.
	*/
	public void setFields( List<DBFField> fields)
	throws DBFException {

		if( header.getFieldList() != null) {

			throw new DBFException( "Fields has already been set");
		}

		if( fields == null || fields.size() == 0) {

			throw new DBFException( "Should have at least one field");
		}

		for( int i=0; i<fields.size(); i++) {

			if( fields.get(i) == null) {

				throw new DBFException( "Field " + (i+1) + " is null");
			}
		}

		header.setFieldList(fields);

		try {

			if( dataChannel != null && dataChannel.size() == 0) {

				/* 
			  	this is a new/non-existent file. So write header before proceeding
		 		*/
				header.write( dataChannel);
			}
		}
		catch( IOException e) {

			throw new DBFException( "Error accesing file", e);
		}
	}

	/**
		Add a record.
	*/
	public void addRecord( Object[] values)
	throws DBFException {

		if( header.getFieldList() == null) {

			throw new DBFException( "Fields should be set before adding records");
		}

		if( values == null) {

			throw new DBFException( "Null cannot be added as row");
		}

		if( values.length != header.getFieldList().size()) {

			throw new DBFException( "Invalid record. Invalid number of fields in row");
		}

		for( int i=0; i<header.getFieldList().size(); i++) {

			
			if( values[i] == null) {

				continue;
			}

			DBFField field = header.getFieldList().get(i);
			
			switch( field.getDataType()) {

				case CHARACTER:
					if( !(values[i] instanceof String)) {
						throw new DBFException( "Invalid value for field " + i);
					}
					break;

				case LOGICAL:
					if( !( values[i] instanceof Boolean)) {
					  throw new DBFException( "Invalid value for field " + i);
					}
					break;

				case NUMBER:
					if( !( values[i] instanceof BigDecimal)) {
						throw new DBFException( "Invalid value for field " + i);
					}
					break;

				case INTEGER:
					if( !( values[i] instanceof Integer)) {
						throw new DBFException( "Invalid value for field " + i);
					}
					break;

				case DATE:
					if( !( values[i] instanceof LocalDate)) {
						throw new DBFException( "Invalid value for field " + i);
					}
					break;

				case FLOAT:
					if( !(values[i] instanceof Double)) {

						throw new DBFException( "Invalid value for field " + i);
					}
					break;
			}
		}

		if( dataChannel == null) {

			v_records.addElement( values);
		}
		else {

			try {
			
				writeRecord( dataChannel, values);
				this.recordCount++;
			}
			catch( IOException e) {

				throw new DBFException( "Error occured while writing record. " + e.getMessage(), e);
			}
		}
	}

	/**
		Writes the set data to the OutputStream.
	*/
	public void write( OutputStream out)
	throws DBFException {

		try {

			if( dataChannel == null) {
				WritableByteChannel dataChannel = Channels.newChannel(out);
							
				header.setNumberOfRecords(v_records.size());
				header.write( dataChannel);

				/* Now write all the records */
				int t_recCount = v_records.size();
				for( int i=0; i<t_recCount; i++) { /* iterate through records */

					Object[] t_values = (Object[])v_records.elementAt( i);

					writeRecord( dataChannel, t_values);
				}

				ByteBuffer buff = ByteBuffer.allocate(1);
				buff.put(END_OF_DATA).flip();
				dataChannel.write(buff);
				// flush
//				dataChannel.;
			}
			else {

				/* everything is written already. just update the header for record count and the END_OF_DATA mark */
				header.setNumberOfRecords(this.recordCount);
				dataChannel.position(0);
				header.write( dataChannel);
				dataChannel.position(dataChannel.size());
				
				ByteBuffer buff = ByteBuffer.allocate(1);
				buff.put(END_OF_DATA).flip();
				dataChannel.write(buff);
				
				dataChannel.close();
			}
		}
		catch( IOException e) {

			throw new DBFException( e.getMessage(), e);
		}
	}

	public void write()
	throws DBFException {

		this.write( null);
	}

	private void writeRecord( WritableByteChannel dataOutput, Object []objectArray) 
	throws IOException {

		ByteBuffer buff = ByteBuffer.allocate(header.getRecordLength());
		buff.order(ByteOrder.LITTLE_ENDIAN);
		
		buff.put( (byte)' ');
		for( int j=0; j<header.getFieldList().size(); j++) { /* iterate throught fields */

			DBFField field = header.getFieldList().get(j);
			
			switch( field.getDataType()) {

				case CHARACTER:
					if( objectArray[j] != null) {

						String str_value = objectArray[j].toString();	
						buff.put( Utils.textPadding( str_value, characterSet, field.getFieldLength()));
					}
					else {

						buff.put( Utils.textPadding( "", characterSet, field.getFieldLength()));
					}

					break;

				case DATE:
					if( objectArray[j] != null) {
												
						buff.put(((LocalDate)objectArray[j]).toString("YYYYMMdd").getBytes());
					}
					else {
						buff.put( "        ".getBytes());
					}

					break;

				case FLOAT:

					if( objectArray[j] != null) {

						buff.put( Utils.doubleFormating( (Double)objectArray[j], characterSet, field.getFieldLength(), field.getDecimalCount()));
					}
					else {

						buff.put( Utils.textPadding( " ", characterSet, field.getFieldLength(), Utils.ALIGN_RIGHT));
					}

					break;

				case NUMBER:

					if( objectArray[j] != null) {

						buff.put(
							Utils.decimalFormating( (BigDecimal)objectArray[j], characterSet, field.getFieldLength(), field.getDecimalCount()));
					}
					else {

						buff.put( 
							Utils.textPadding( " ", characterSet, field.getFieldLength(), Utils.ALIGN_RIGHT));
					}

					break;
					
				case INTEGER:

					if( objectArray[j] != null) {
						buff.putInt(((Integer)objectArray[j]).intValue());
					}
					else {
						buff.putInt(0);
					}

					break;
				case LOGICAL:

					if( objectArray[j] != null) {

						if( (Boolean)objectArray[j] == Boolean.TRUE) {

							buff.put( (byte)'T');
						}
						else {

							buff.put((byte)'F');
						}
					}
					else {

						buff.put( (byte)'?');
					}

					break;

				case MEMO:

					break;

				default:	
					throw new DBFException( "Unknown field type " + field.getDataType());
			}
		}	/* iterating through the fields */
	}

	public void close() throws IOException {
		if (dataChannel != null) {
			dataChannel.close();
		}
    }
}
