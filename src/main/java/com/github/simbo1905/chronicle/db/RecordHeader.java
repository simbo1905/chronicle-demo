package com.github.simbo1905.chronicle.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordHeader {

	@Override
	public String toString() {
		return "RecordHeader [dataPointer=" + dataPointer + ", dataCount="
				+ dataCount + ", dataCapacity=" + dataCapacity
				+ ", indexPosition=" + indexPosition + "]";
	}

	/**
	 * File pointer to the first byte of record data (8 bytes).
	 */
	protected long dataPointer;

	/**
	 * Actual number of bytes of data held in this record (4 bytes).
	 */
	protected int dataCount;

	/**
	 * Number of bytes of data that this record can hold (4 bytes).
	 */
	protected int dataCapacity;

	/**
	 * Indicates this header's position in the file index.
	 */
	protected int indexPosition;

	protected RecordHeader() {
	}

	protected RecordHeader(long dataPointer, int dataCapacity) {
		if (dataCapacity < 1) {
			throw new IllegalArgumentException("Bad record size: "
					+ dataCapacity);
		}
		this.dataPointer = dataPointer;
		this.dataCapacity = dataCapacity;
		this.dataCount = 0;
	}

	protected int getIndexPosition() {
		return indexPosition;
	}

	protected void setIndexPosition(int indexPosition) {
		this.indexPosition = indexPosition;
	}

	protected int getDataCapacity() {
		return dataCapacity;
	}

	protected int getFreeSpace() {
		return dataCapacity - dataCount;
	}

	/**
	 * Read as a single operation to avoid corruption
	 */
	protected void read(DataInput in) throws IOException {
		byte[] header = new byte[16];
		in.readFully(header);
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.put(header);
		buffer.flip();

		dataPointer = buffer.getLong();
		dataCapacity = buffer.getInt();
		dataCount = buffer.getInt();
	}

	/**
	 * in order to improve the likelihood of not corrupting the header write as
	 * a single operation
	 */
	protected void write(DataOutput out) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.putLong(dataPointer);
		buffer.putInt(dataCapacity);
		buffer.putInt(dataCount);
		out.write(buffer.array(), 0, 16);
	}

	protected static RecordHeader readHeader(DataInput in) throws IOException {
		RecordHeader r = new RecordHeader();
		r.read(in);
		return r;
	}

	/**
	 * Returns a new record header which occupies the free space of this record.
	 * Shrinks this record size by the size of its free space.
	 */
	protected RecordHeader split() throws RecordsFileException {
		long newFp = dataPointer + (long) dataCount;
		RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace());
		dataCapacity = dataCount;
		return newRecord;
	}

}
