/*
  DBFReader
  Class for reading the records assuming that the given
	InputStream comtains DBF data.

  This file is part of JavaDBF packege.

  Author: anil@linuxense.com
  License: LGPL (http://www.gnu.org/copyleft/lesser.html)

  $Id: DBFReader.java,v 1.9 2004-07-19 08:56:23 anil Exp $
*/

package com.linuxense.javadbf;

import org.joda.time.LocalDate;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * DBFReader class can creates objects to represent DBF data.
 * <p/>
 * This Class is used to read data from a DBF file. Meta data and
 * records can be queried against this document.
 * <p/>
 * <p/>
 * DBFReader cannot write anythng to a DBF file. For creating DBF files
 * use DBFWriter.
 * <p/>
 * <p/>
 * Fetching rocord is possible only in the forward direction and
 * cannot be re-wound. In such situations, a suggested approach is to reconstruct the object.
 * <p/>
 * <p/>
 * The nextRecord() method returns an array of Objects and the types of these
 * Object are as follows:
 * <p/>
 * <table>
 * <tr>
 * <th>xBase Type</th><th>Java Type</th>
 * </tr>
 * <p/>
 * <tr>
 * <td>C</td><td>String</td>
 * </tr>
 * <tr>
 * <td>N</td><td>Integer</td>
 * </tr>
 * <tr>
 * <td>F</td><td>Double</td>
 * </tr>
 * <tr>
 * <td>L</td><td>Boolean</td>
 * </tr>
 * <tr>
 * <td>D</td><td>java.util.Date</td>
 * </tr>
 * </table>
 */
public class DBFReader extends DBFBase {

    FileChannel dataChannel;
    MemoFile memoFile;

    /* Class specific variables */
    boolean isClosed = true;

    /**
     * Initializes a DBFReader object.
     * <p/>
     * When this constructor returns the object
     * will have completed reading the hader (meta date) and
     * header information can be quried there on. And it will
     * be ready to return the first row.
     *
     * @param InputStream where the data is read from.
     */
    public DBFReader(File file) throws DBFException {
        try {
            dataChannel = new FileInputStream(file).getChannel();
            isClosed = false;
            header = new DBFHeader();
            header.read(dataChannel);
            characterSet = header.getLanguageDriver().getCharset();

            if (header.getSignature() == DBFHeader.SIG_VISUAL_FOX_PRO) {
                if ((header.getMdxFlag() & 0x2) > 0) {
                    final String fptFileName = file.getName().replaceAll("\\.[^.]+$", "") + ".fpt";

                    String[] list = file.getParentFile().list(new FilenameFilter() {

                        public boolean accept(File dir, String name) {
                            return fptFileName.equalsIgnoreCase(name);
                        }
                    });

                    if (list.length > 0) {
                        memoFile = new FPTMemoFile(new File(file.getParentFile(), list[0]), "r", characterSet);
                    }
                }
            }

			/* it might be required to leap to the start of records at times */
            if (header.getHeaderLength() > 0) {
                dataChannel.position(header.getHeaderLength());
            }
        } catch (IOException e) {
            throw new DBFException(e.getMessage(), e);
        }
    }


    public String toString() {
        StringBuffer sb = new StringBuffer().append(header.getYear()).append("/").append(header.getMonth()).append("/").append(header.getDay()).append("\n").append("Total records: ")
                .append(header.getNumberOfRecords()).append("\nHEader length: ").append(header.getHeaderLength());

        for (DBFField field : header.getFieldList()) {
            sb.append(field.getName());
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Returns the number of records in the DBF.
     */
    public int getRecordCount() {

        return header.getNumberOfRecords();
    }

    /**
     * Returns the asked Field. In case of an invalid index,
     * it returns a ArrayIndexOutofboundsException.
     *
     * @param index. Index of the field. Index of the first field is zero.
     */
    public DBFField getField(int index)
            throws DBFException {

        checkIfClosed();

        return header.getFieldList().get(index);
    }


    /**
     * Returns the number of field in the DBF.
     */
    public int getFieldCount()
            throws DBFException {

        checkIfClosed();

        if (header.getFieldList() != null) {

            return header.getFieldList().size();
        }

        return -1;
    }

    /**
     * Reads the returns the next row in the DBF stream.
     *
     * @returns The next row as an Object array. Types of the elements
     * these arrays follow the convention mentioned in the class description.
     */
    public Object[] nextRecord()
            throws DBFException {

        checkIfClosed();

        ByteBuffer buff = ByteBuffer.allocate(header.getRecordLength());
        buff.order(ByteOrder.LITTLE_ENDIAN);

        Object recordObjects[] = new Object[header.getFieldList().size()];

        try {
            boolean isDeleted = false;
            do {
                buff.clear();

                dataChannel.read(buff);
                buff.flip();

                if (buff.limit() < 1) {
                    return null;
                }
                int t_byte = buff.get();

                if (t_byte == END_OF_DATA) {
                    return null;
                }

                isDeleted = (t_byte == '*');
            } while (isDeleted);

            for (int i = 0; i < header.getFieldList().size(); i++) {

                DBFField field = header.getFieldList().get(i);

                switch (field.getDataType()) {

                    case CHARACTER:

                        byte b_array[] = new byte[field.getFieldLength()];
                        buff.get(b_array);
                        recordObjects[i] = new String(b_array, characterSet);
                        break;

                    case DATE:

                        StringBuilder sb = new StringBuilder();

                        sb.append((char) buff.get());
                        sb.append((char) buff.get());
                        sb.append((char) buff.get());
                        sb.append((char) buff.get());
                        sb.append("-");
                        sb.append((char) buff.get());
                        sb.append((char) buff.get());
                        sb.append("-");
                        sb.append((char) buff.get());
                        sb.append((char) buff.get());

                        try {
                            recordObjects[i] = new LocalDate(sb.toString());
                        } catch (IllegalArgumentException e) {
                            /* this field may be empty or may have improper value set */
                            recordObjects[i] = null;
                        }

                        break;

                    case FLOAT:

                        try {

                            byte t_float[] = new byte[field.getFieldLength()];
                            buff.get(t_float);
                            t_float = Utils.trimLeftSpaces(t_float);
                            if (t_float.length > 0 && !Utils.contains(t_float, (byte) '?')) {
                                recordObjects[i] = new Double(new String(t_float));
                            } else {
                                recordObjects[i] = null;
                            }
                        } catch (NumberFormatException e) {
                            throw new DBFException("Failed to parse Float: " + e.getMessage(), e);
                        }

                        break;

                    case NUMBER:
                        recordObjects[i] = readNumber(buff, field);
                        break;

                    case INTEGER:

                        recordObjects[i] = Integer.valueOf(buff.getInt());
                        break;

                    case LOGICAL:

                        byte t_logical = buff.get();
                        if (t_logical == 'Y' || t_logical == 't' || t_logical == 'T' || t_logical == 't') {

                            recordObjects[i] = Boolean.TRUE;
                        } else {

                            recordObjects[i] = Boolean.FALSE;
                        }
                        break;

                    case MEMO:
                        if (header.getSignature() == DBFHeader.SIG_VISUAL_FOX_PRO && memoFile != null) {
                            int address = buff.getInt();

                            recordObjects[i] = address > 0 ? memoFile.getMemo(address) : null;
                        } else {
                            byte t_numeric[] = new byte[field.getFieldLength()];
                            buff.get(t_numeric);

                            //
                            recordObjects[i] = null;
                        }

                        break;

                    default:
                        byte bytes[] = new byte[field.getFieldLength()];
                        buff.get(bytes);

                        recordObjects[i] = null;
                }
            }
        } catch (EOFException e) {

            return null;
        } catch (IOException e) {
            throw new DBFException(e.getMessage(), e);
        }

        return recordObjects;
    }

    private Object readNumber(ByteBuffer buffer, DBFField field) throws DBFException {
        byte t_numeric[] = new byte[field.getFieldLength()];
        try {
            buffer.get(t_numeric);
            t_numeric = Utils.trimLeftSpaces(t_numeric);
            t_numeric = Utils.trimNulls(t_numeric);

            if (t_numeric.length > 0 && !(Utils.contains(t_numeric, (byte) '?') || Utils.contains(t_numeric, (byte) '*'))) {
                return new BigDecimal(new String(t_numeric));
            }

            return null;
        } catch (NumberFormatException e) {
            throw new DBFException(String.format("Failed to parse Number: buffer=\"%s\" - %s", Arrays.toString(t_numeric), e.getMessage()), e);
        }
    }

    public void close() throws IOException {
        isClosed = true;

        if (dataChannel != null) {
            dataChannel.close();
        }
    }

    private void checkIfClosed() throws DBFException {
        if (isClosed) {
            throw new DBFException("Source is not open");
        }
    }
}
