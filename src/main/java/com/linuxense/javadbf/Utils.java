/*
 	Utils
  Class for contining utility functions.

  This file is part of JavaDBF packege.

  author: anil@linuxense.com
  license: LGPL (http://www.gnu.org/copyleft/lesser.html)

  $Id: Utils.java,v 1.8 2004-07-19 08:58:11 anil Exp $
 */
package com.linuxense.javadbf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Arrays;

/**
 * Miscelaneous functions required by the JavaDBF package.
 */
public final class Utils {

	public static final int ALIGN_LEFT = 10;
	public static final int ALIGN_RIGHT = 12;

	public static byte[] trimLeftSpaces(byte[] arr) {

		StringBuffer t_sb = new StringBuffer(arr.length);

		for (int i = 0; i < arr.length; i++) {

			if (arr[i] != ' ') {

				t_sb.append((char) arr[i]);
			}
		}

		return t_sb.toString().getBytes();
	}


	public static byte[] textPadding(String text, Charset characterSet, int length) {

		return textPadding(text, characterSet, length, Utils.ALIGN_LEFT);
	}

	public static byte[] textPadding(String text, Charset characterSet, int length, int alignment) {

		return textPadding(text, characterSet, length, alignment, (byte) ' ');
	}

	public static byte[] textPadding(String text, Charset characterSet, int length, int alignment, byte paddingByte) {

		if (text.length() >= length) {

			return text.substring(0, length).getBytes(characterSet);
		}

		byte byte_array[] = new byte[length];
		Arrays.fill(byte_array, paddingByte);

		switch (alignment) {

			case ALIGN_LEFT:
				System.arraycopy(text.getBytes(characterSet), 0, byte_array, 0, text.length());
				break;

			case ALIGN_RIGHT:
				int t_offset = length - text.length();
				System.arraycopy(text.getBytes(characterSet), 0, byte_array, t_offset, text.length());
				break;
		}

		return byte_array;
	}

	public static byte[] decimalFormating(BigDecimal bigDecimal, Charset characterSet, int fieldLength, int sizeDecimalPart) {

		return textPadding(bigDecimal.setScale(sizeDecimalPart, RoundingMode.HALF_UP).toPlainString(), characterSet, fieldLength, ALIGN_RIGHT);
	}

	public static byte[] doubleFormating(Double doubleNum, Charset characterSet, int fieldLength, int sizeDecimalPart) throws java.io.UnsupportedEncodingException {

		int sizeWholePart = fieldLength - (sizeDecimalPart > 0 ? (sizeDecimalPart + 1) : 0);

		StringBuffer format = new StringBuffer(fieldLength);

		for (int i = 0; i < sizeWholePart; i++) {

			format.append("#");
		}

		if (sizeDecimalPart > 0) {

			format.append(".");

			for (int i = 0; i < sizeDecimalPart; i++) {

				format.append("0");
			}
		}

		DecimalFormat df = new DecimalFormat(format.toString());

		return textPadding(df.format(doubleNum.doubleValue()).toString(), characterSet, fieldLength, ALIGN_RIGHT);
	}

	public static boolean contains(byte[] arr, byte value) {

		boolean found = false;
		for (int i = 0; i < arr.length; i++) {

			if (arr[i] == value) {

				found = true;
				break;
			}
		}

		return found;
	}
}
