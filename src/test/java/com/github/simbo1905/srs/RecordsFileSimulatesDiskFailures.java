package com.github.simbo1905.srs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.github.simbo1905.srs.RecordsFile;
import com.github.simbo1905.srs.RecordsFileException;

public class RecordsFileSimulatesDiskFailures extends RecordsFile {

	public RecordsFileSimulatesDiskFailures(String dbPath, int initialSize, WriteCallback wc)
			throws IOException, RecordsFileException {
		super(dbPath, initialSize);
		File f = new File(dbPath);
		this.file = new InterceptedRandomAccessFile(new RandomAccessFile(f, "rw"),wc);
	}

}
