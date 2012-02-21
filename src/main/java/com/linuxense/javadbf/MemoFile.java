package com.linuxense.javadbf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

abstract class MemoFile implements Closeable {
	protected final FileChannel channel;	
	protected final Charset charset;
	
	protected int nextAvailableBlockNumber = 0;
	protected int sizeOfBlocks = 512;


	public MemoFile(File file, String mode, Charset charset) throws IOException {
		this.charset = charset;		
		this.channel = new RandomAccessFile(file, mode).getChannel();		
	}

	public abstract String getMemo(int address) throws IOException;
	
	public void close() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}
}
