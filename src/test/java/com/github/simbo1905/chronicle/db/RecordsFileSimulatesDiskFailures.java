package com.github.simbo1905.chronicle.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RecordsFileSimulatesDiskFailures extends RecordsFile {

	public RecordsFileSimulatesDiskFailures(String dbPath, int initialSize, WriteCallback wc)
			throws IOException, RecordsFileException {
		super(dbPath, initialSize);
		File f = new File(dbPath);
		this.file = new InterceptedRandomAccessFile(new RandomAccessFile(f, "rw"),wc);
	}

}
