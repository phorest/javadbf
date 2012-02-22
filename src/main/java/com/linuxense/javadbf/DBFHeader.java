/*	
 DBFHeader
 Class for reading the metadata assuming that the given
 InputStream carries DBF data.

 This file is part of JavaDBF packege.

 Author: anil@linuxense.com
 License: LGPL (http://www.gnu.org/copyleft/lesser.html)

 $Id: DBFHeader.java,v 1.1 2004-08-14 13:09:13 anil Exp $
 */

package com.linuxense.javadbf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

class DBFHeader {

	static final byte SIG_DBASE_III = (byte) 0x03;
	static final byte SIG_VISUAL_FOX_PRO = (byte) 0x30;

	/* DBF structure start here */

	private byte signature; /* 0 */
	private byte year; /* 1 */
	private byte month; /* 2 */
	private byte day; /* 3 */
	private int numberOfRecords; /* 4-7 */
	private short headerLength; /* 8-9 */
	private short recordLength; /* 10-11 */
	private short reserv1; /* 12-13 */
	private byte incompleteTransaction; /* 14 */
	private byte encryptionFlag; /* 15 */
	private int freeRecordThread; /* 16-19 */
	private int reserv2; /* 20-23 */
	private int reserv3; /* 24-27 */
	private byte mdxFlag; /* 28 */
	private LanguageDriver languageDriver; /* 29 */
	private short reserv4; /* 30-31 */
	private List<DBFField> fieldList; /* each 32 bytes */
	private byte terminator1; /* n+1 */

	// byte[] databaseContainer; /* 263 bytes */
	/* DBF structure ends here */

	DBFHeader() {
		this.signature = SIG_DBASE_III;
		this.terminator1 = 0x0D;
		this.languageDriver = new LanguageDriver(LanguageDriver.CodePage.WINDOWS_ANSI.getCode());
	}

	void read(ReadableByteChannel byteChannel) throws IOException {
		ByteBuffer buff = ByteBuffer.allocate(32);
		
		byteChannel.read(buff);
		
		buff.flip();
		buff.order(ByteOrder.LITTLE_ENDIAN);
		
		signature = buff.get(); /* 0 */
		year = buff.get(); /* 1 */
		month = buff.get(); /* 2 */
		day = buff.get(); /* 3 */
		numberOfRecords = buff.getInt(); /* 4-7 */

		headerLength = buff.getShort(); /* 8-9 */
		recordLength = buff.getShort(); /* 10-11 */

		reserv1 = buff.getShort(); /* 12-13 */
		incompleteTransaction = buff.get(); /* 14 */
		encryptionFlag = buff.get(); /* 15 */
		freeRecordThread = buff.getInt(); /* 16-19 */
		reserv2 = buff.getInt(); /* 20-23 */
		reserv3 = buff.getInt(); /* 24-27 */
		mdxFlag = buff.get(); /* 28 */
		languageDriver = new LanguageDriver(buff.get()); /* 29 */
		reserv4 = buff.getShort(); /* 30-31 */

		fieldList = new ArrayList<DBFField>();

		DBFField field = DBFField.createField(byteChannel); /* 32 each */
		while (field != null) {

			fieldList.add(field);
			field = DBFField.createField(byteChannel);
		}
	}

	void write(WritableByteChannel byteChannel) throws IOException {
		ByteBuffer buff = ByteBuffer.allocate(32);
		buff.order(ByteOrder.LITTLE_ENDIAN);
		
		buff.put(signature); /* 0 */

		GregorianCalendar calendar = new GregorianCalendar();
		year = (byte) (calendar.get(Calendar.YEAR) - 1900);
		month = (byte) (calendar.get(Calendar.MONTH) + 1);
		day = (byte) (calendar.get(Calendar.DAY_OF_MONTH));

		buff.put(year); /* 1 */
		buff.put(month); /* 2 */
		buff.put(day); /* 3 */

		buff.putInt(numberOfRecords); /* 4-7 */
		
		headerLength = findHeaderLength();
		buff.putShort(headerLength); /* 8-9 */

		recordLength = findRecordLength();
		buff.putShort(recordLength); /* 10-11 */

		buff.putShort(reserv1); /* 12-13 */
		buff.put(incompleteTransaction); /* 14 */
		buff.put(encryptionFlag); /* 15 */
		buff.putInt(freeRecordThread);/* 16-19 */
		buff.putInt(reserv2); /* 20-23 */
		buff.putInt(reserv3); /* 24-27 */

		buff.put(mdxFlag); /* 28 */
		buff.put(languageDriver == null ? 0 : languageDriver.getCode()); /* 29 */
		buff.putShort(reserv4); /* 30-31 */

		buff.flip();
		
		byteChannel.write(buff);
		
		for (DBFField field : fieldList) {
			field.write(byteChannel);
		}

		buff.reset();
		buff.put(terminator1).flip();
		
		byteChannel.write(buff); /* n+1 */ 
	}

	private short findHeaderLength() {

		return (short) (1 + 3 + 4 + 2 + 2 + 2 + 1 + 1 + 4 + 4 + 4 + 1 + 1 + 2 + (32 * fieldList.size()) + 1);
	}

	private short findRecordLength() {

		int t_recordLength = 0;
		for (DBFField field : fieldList) {

			t_recordLength += field.getFieldLength();
		}

		return (short) (t_recordLength + 1);
	}

	public byte getSignature() {
		return signature;
	}

	public void setSignature(byte signature) {
		this.signature = signature;
	}

	public byte getYear() {
		return year;
	}

	public void setYear(byte year) {
		this.year = year;
	}

	public byte getMonth() {
		return month;
	}

	public void setMonth(byte month) {
		this.month = month;
	}

	public byte getDay() {
		return day;
	}

	public void setDay(byte day) {
		this.day = day;
	}

	public int getNumberOfRecords() {
		return numberOfRecords;
	}

	public void setNumberOfRecords(int numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}

	public short getHeaderLength() {
		return headerLength;
	}

	public void setHeaderLength(short headerLength) {
		this.headerLength = headerLength;
	}

	public short getRecordLength() {
		return recordLength;
	}

	public void setRecordLength(short recordLength) {
		this.recordLength = recordLength;
	}

	public short getReserv1() {
		return reserv1;
	}

	public void setReserv1(short reserv1) {
		this.reserv1 = reserv1;
	}

	public byte getIncompleteTransaction() {
		return incompleteTransaction;
	}

	public void setIncompleteTransaction(byte incompleteTransaction) {
		this.incompleteTransaction = incompleteTransaction;
	}

	public byte getEncryptionFlag() {
		return encryptionFlag;
	}

	public void setEncryptionFlag(byte encryptionFlag) {
		this.encryptionFlag = encryptionFlag;
	}

	public int getFreeRecordThread() {
		return freeRecordThread;
	}

	public void setFreeRecordThread(int freeRecordThread) {
		this.freeRecordThread = freeRecordThread;
	}

	public int getReserv2() {
		return reserv2;
	}

	public void setReserv2(int reserv2) {
		this.reserv2 = reserv2;
	}

	public int getReserv3() {
		return reserv3;
	}

	public void setReserv3(int reserv3) {
		this.reserv3 = reserv3;
	}

	public byte getMdxFlag() {
		return mdxFlag;
	}

	public void setMdxFlag(byte mdxFlag) {
		this.mdxFlag = mdxFlag;
	}

	public LanguageDriver getLanguageDriver() {
		return languageDriver;
	}

	public void setLanguageDriver(LanguageDriver languageDriver) {
		this.languageDriver = languageDriver;
	}

	public short getReserv4() {
		return reserv4;
	}

	public void setReserv4(short reserv4) {
		this.reserv4 = reserv4;
	}

	public List<DBFField> getFieldList() {
		return fieldList;
	}

	public void setFieldList(List<DBFField> fieldList) {
		this.fieldList = fieldList;
	}

	public byte getTerminator1() {
		return terminator1;
	}

	public void setTerminator1(byte terminator1) {
		this.terminator1 = terminator1;
	}

}
