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
import java.io.*;
import java.math.BigDecimal;
import java.util.*;

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
	DBFHeader header;
	Vector v_records = new Vector();
	int recordCount = 0;
	RandomAccessFile raf = null; /* Open and append records to an existing DBF */
	boolean appendMode = false;

	/**
		Creates an empty Object.
	*/
	public DBFWriter() {

		this.header = new DBFHeader();
	}

	/**
	 	Creates a DBFWriter which can append to records to an existing DBF file.
		@param dbfFile. The file passed in shouls be a valid DBF file.
		@exception Throws DBFException if the passed in file does exist but not a valid DBF file, or if an IO error occurs.
	 */
	public DBFWriter( File dbfFile) 
	throws DBFException {

		try {

			this.raf = new RandomAccessFile( dbfFile, "rw");

			/* before proceeding check whether the passed in File object 
			 is an empty/non-existent file or not.
			 */
			if( !dbfFile.exists() || dbfFile.length() == 0) {

				this.header = new DBFHeader();
				return;
			}

			header = new DBFHeader();
			this.header.read( raf);

			/* position file pointer at the end of the raf */
			this.raf.seek( this.raf.length()-1 /* to ignore the END_OF_DATA byte at EoF */);
		}
		catch( FileNotFoundException e) {

			throw new DBFException( "Specified file is not found. " + e.getMessage(), e);
		}
		catch( IOException e) {

			throw new DBFException( e.getMessage() + " while reading header", e);
		}

		this.recordCount = this.header.numberOfRecords;
	}

	/**
		Sets fields.
	*/
	public void setFields( DBFField[] fields)
	throws DBFException {

		if( this.header.fieldArray != null) {

			throw new DBFException( "Fields has already been set");
		}

		if( fields == null || fields.length == 0) {

			throw new DBFException( "Should have at least one field");
		}

		for( int i=0; i<fields.length; i++) {

			if( fields[i] == null) {

				throw new DBFException( "Field " + (i+1) + " is null");
			}
		}

		this.header.fieldArray = fields;

		try {

			if( this.raf != null && this.raf.length() == 0) {

				/* 
			  	this is a new/non-existent file. So write header before proceeding
		 		*/
				this.header.write( this.raf);
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

		if( this.header.fieldArray == null) {

			throw new DBFException( "Fields should be set before adding records");
		}

		if( values == null) {

			throw new DBFException( "Null cannot be added as row");
		}

		if( values.length != this.header.fieldArray.length) {

			throw new DBFException( "Invalid record. Invalid number of fields in row");
		}

		for( int i=0; i<this.header.fieldArray.length; i++) {

			if( values[i] == null) {

				continue;
			}

			switch( this.header.fieldArray[i].getDataType()) {

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

		if( this.raf == null) {

			v_records.addElement( values);
		}
		else {

			try {
			
				writeRecord( this.raf, values);
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

			if( this.raf == null) {

				DataOutputStream outStream = new DataOutputStream( out);

				this.header.numberOfRecords = v_records.size();
				this.header.write( outStream);

				/* Now write all the records */
				int t_recCount = v_records.size();
				for( int i=0; i<t_recCount; i++) { /* iterate through records */

					Object[] t_values = (Object[])v_records.elementAt( i);

					writeRecord( outStream, t_values);
				}

				outStream.write( END_OF_DATA);
				outStream.flush();
			}
			else {

				/* everything is written already. just update the header for record count and the END_OF_DATA mark */
				this.header.numberOfRecords = this.recordCount;
				this.raf.seek( 0);
				this.header.write( this.raf);
				this.raf.seek( raf.length());
				this.raf.writeByte( END_OF_DATA);
				this.raf.close();
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

	private void writeRecord( DataOutput dataOutput, Object []objectArray) 
	throws IOException {

		dataOutput.write( (byte)' ');
		for( int j=0; j<this.header.fieldArray.length; j++) { /* iterate throught fields */

			switch( this.header.fieldArray[j].getDataType()) {

				case CHARACTER:
					if( objectArray[j] != null) {

						String str_value = objectArray[j].toString();	
						dataOutput.write( Utils.textPadding( str_value, characterSetName, this.header.fieldArray[j].getFieldLength()));
					}
					else {

						dataOutput.write( Utils.textPadding( "", this.characterSetName, this.header.fieldArray[j].getFieldLength()));
					}

					break;

				case DATE:
					if( objectArray[j] != null) {
												
						dataOutput.write(((LocalDate)objectArray[j]).toString("YYYYMMdd").getBytes());
					}
					else {
						dataOutput.write( "        ".getBytes());
					}

					break;

				case FLOAT:

					if( objectArray[j] != null) {

						dataOutput.write( Utils.doubleFormating( (Double)objectArray[j], this.characterSetName, this.header.fieldArray[j].getFieldLength(), this.header.fieldArray[j].getDecimalCount()));
					}
					else {

						dataOutput.write( Utils.textPadding( " ", this.characterSetName, this.header.fieldArray[j].getFieldLength(), Utils.ALIGN_RIGHT));
					}

					break;

				case NUMBER:

					if( objectArray[j] != null) {

						dataOutput.write(
							Utils.decimalFormating( (BigDecimal)objectArray[j], this.characterSetName, this.header.fieldArray[j].getFieldLength(), this.header.fieldArray[j].getDecimalCount()));
					}
					else {

						dataOutput.write( 
							Utils.textPadding( " ", this.characterSetName, this.header.fieldArray[j].getFieldLength(), Utils.ALIGN_RIGHT));
					}

					break;
					
				case INTEGER:

					if( objectArray[j] != null) {
						dataOutput.write(Utils.littleEndian(((Integer)objectArray[j]).intValue()));
					}
					else {
						dataOutput.write( new byte[4]);
					}

					break;
				case LOGICAL:

					if( objectArray[j] != null) {

						if( (Boolean)objectArray[j] == Boolean.TRUE) {

							dataOutput.write( (byte)'T');
						}
						else {

							dataOutput.write((byte)'F');
						}
					}
					else {

						dataOutput.write( (byte)'?');
					}

					break;

				case MEMO:

					break;

				default:	
					throw new DBFException( "Unknown field type " + this.header.fieldArray[j].getDataType());
			}
		}	/* iterating through the fields */
	}
}
