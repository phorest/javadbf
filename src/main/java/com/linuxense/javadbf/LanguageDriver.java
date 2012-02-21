package com.linuxense.javadbf;

import java.nio.charset.Charset;

class LanguageDriver {
	public final static Charset DEFAULT_CHARSET = Charset.forName("cp1252"); 
	
	public enum CodePage {
		DOS_USA((byte) 0x1, "cp437"), 
		DOS_MULTILINGUAL((byte) 0x2, "cp850"), 
		WINDOWS_ANSI((byte) 0x3, "cp1252"), 
		STANDARD_MACINTOSH((byte) 0x4, "MacRoman"), 
		EE_MS_DOS((byte) 0x64, "cp852"), 
		NORDIC_MS_DOS((byte) 0x65, "cp865"), 
		RUSSIAN_MS_DOS((byte) 0x66, "cp866"), 
		ICELANDIC_MS_DOS((byte) 0x67, null), 
		KAMENICKY_MS_DOS((byte) 0x68, null), 
		MAZOVIA_MS_DOS((byte) 0x69, null), 
		GREEK_MS_DOS((byte) 0x6A, null), 
		TURKISH_MS_DOS((byte) 0x6B, null), 
		RUSSIAN_MACINTOSH((byte) 0x96, null), 
		EASTERN_EUROPEAN_MACINTOSH((byte) 0x97, null), 
		GREEK_MACINTOSH((byte) 0x98, null), 
		WINDOWS_EE((byte) 0xc8, "cp1250"), 
		RUSSIAN_WINDOWS((byte) 0xc9, null), 
		TURKISH_WINDOWS((byte) 0xca, null), 
		GREEK_WINDOWS((byte) 0xcb, null);

		private final byte code;
		private final Charset charset;

		private CodePage(byte code, String charsetName) {
			this.code = code;
			this.charset = charsetName == null ? null : Charset.forName(charsetName);
		}

		public byte getCode() {
			return code;
		}

		public Charset getCharset() {
			return charset;
		}

		public static LanguageDriver.CodePage fromCode(byte code) {
			for (LanguageDriver.CodePage cp : CodePage.values()) {
				if (cp.getCode() == code) {
					return cp;
				}
			}

			throw new IllegalArgumentException();
		}

		/**
		 * @param charsetName
		 * @return
		 * @throw NullPointerException if the given charset name is null
		 * @throw IllegalCharsetNameException if the given charset name is illegal or there is no corresponding code page
		 * @throw UnsupportedCharsetException if no support for the named charset is available in this instance of the Java virtual machine
		 */
		public static LanguageDriver.CodePage fromCharsetName(String charsetName) {
			if (charsetName == null) {
				throw new NullPointerException();
			}
			return fromCharset(Charset.forName(charsetName));
		}

		/**
		 * @param charset
		 * @return
		 * @throw NullPointerException if the given charset is null
		 * @throw IllegalArgumentException - if the is no corresponding code page
		 */
		public static LanguageDriver.CodePage fromCharset(Charset charset) {
			if (charset == null) {
				throw new NullPointerException();
			}
			for (LanguageDriver.CodePage cp : CodePage.values()) {
				if (charset.equals(cp.getCharset())) {
					return cp;
				}
			}

			throw new IllegalArgumentException();
		}
	}

	private final byte code;

	public LanguageDriver(byte code) {
		this.code = code;
	}

	public byte getCode() {
		return code;
	}

	public Charset getCharset() {
		try {
			return CodePage.fromCode(code).getCharset();
		} catch (IllegalArgumentException ex) {
			return DEFAULT_CHARSET;
		}
	}

	public static LanguageDriver fromCharsetName(String charsetName) {
		return new LanguageDriver(CodePage.fromCharsetName(charsetName).getCode());
	}

	public static LanguageDriver fromCharset(Charset charset) {
		return new LanguageDriver(CodePage.fromCharset(charset).getCode());
	}
}