package com.linuxense.javadbf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

class FPTMemoFile extends MemoFile {

	private final static int PICTURE = 0x0; // Picture. This normally indicates that file is produced on a MacIntosh, since pictures on the DOS/Windows platform are "objects".
	private final static int MEMO = 0x1; // Memo
	private final static int OBJECT = 0x2; // Object

	public FPTMemoFile(File file, String mode, Charset charset) throws IOException {
		super(file, mode, charset);

		ByteBuffer buff = ByteBuffer.allocate(512);

		channel.read(buff);

		buff.order(ByteOrder.BIG_ENDIAN);

		buff.flip();

		nextAvailableBlockNumber = buff.getInt();

		buff.position(buff.position() + 2);

		sizeOfBlocks = buff.getShort();
	}

	@Override
	public String getMemo(int address) throws IOException {
		channel.position(address * sizeOfBlocks);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		ByteBuffer buff = ByteBuffer.allocate(sizeOfBlocks);
		buff.order(ByteOrder.BIG_ENDIAN);

		channel.read(buff);
		buff.flip();

		int recordType = buff.getInt();
		if (recordType != MEMO) {
			throw new DBFException("Unsupported record type");
		}

		int len = buff.getInt();
		int remaining = len;
		while (remaining > 0) {
			byte data[] = new byte[buff.remaining()];

			buff.get(data);

			baos.write(data);

			remaining -= data.length;
			if (remaining > 0) {
				buff.clear();
				channel.read(buff);
				buff.flip();
			}
		}

		String result = new String(baos.toByteArray(), 0, len, charset);

		return result;
	}
}
