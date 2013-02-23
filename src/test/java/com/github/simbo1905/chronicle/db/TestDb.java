package com.github.simbo1905.chronicle.db;

import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.io.OptionalDataException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestDb {
	
	private static final class StackCollectingWriteCallback implements
			WriteCallback {
		private final List<List<String>> writeStacks;

		private StackCollectingWriteCallback(List<List<String>> writeStacks) {
			this.writeStacks = writeStacks;
		}

		@Override
		public void onWrite() {
			List<String> stack = new ArrayList<String>();
			writeStacks.add(stack);
			StackTraceElement[] st = Thread.currentThread().getStackTrace();
			for( int index = 2; index < st.length; index++){
				String s = st[index].toString();
				stack.add(s);
				if( !s.contains("com.github.simbo1905")) {
					break;
				}
			}
		}
	}

	private static final class CrashAtWriteCallback implements WriteCallback {
		final int crashAtIndex;
		int calls = 0;

		private CrashAtWriteCallback(int index) {
			crashAtIndex = index;
		}

		@Override
		public void onWrite() throws IOException {
			if( crashAtIndex == calls++){
				throw new IOException("simulated crash at call index: "+crashAtIndex);
			}
		}
	}

	static final String TMP = System.getProperty("java.io.tmpdir");

	private final static Logger LOGGER = Logger.getLogger(TestDb.class.getName()); 
	
	String fileName;
	int initialSize;
	
	public TestDb() {
		LOGGER.setLevel(Level.ALL);
		init(TMP+"junit.records",0);
	}
	
	public void init(final String fileName, final int initialSize) {
		this.fileName = fileName;
		this.initialSize = initialSize;		
		File db = new File(this.fileName);
		if( db.exists() ){
			db.delete();
		} 
		db.deleteOnExit();
	}
	
	RecordsFile recordsFile = null;
	
	@After
	public void deleteDb() throws Exception {
		File db = new File(this.fileName);
		if( db.exists() ){
			db.delete();
		} 
	}

	@Test
	public void originalTest() throws Exception {
		recordsFile = new RecordsFile(fileName, initialSize);
		
		LOGGER.info("creating records file...");

		LOGGER.info("adding a record...");
		RecordWriter rw = new RecordWriter("foo.lastAccessTime");
		rw.writeObject(new Date());
		recordsFile.insertRecord(rw);

		LOGGER.info("reading record...");
		RecordReader rr = recordsFile.readRecord("foo.lastAccessTime");
		Date d = (Date) rr.readObject();
		System.out.println("\tlast access was at: " + d.toString());

		LOGGER.info("updating record...");
		rw = new RecordWriter("foo.lastAccessTime");
		rw.writeObject(new Date());
		recordsFile.updateRecord(rw);

		LOGGER.info("reading record...");
		rr = recordsFile.readRecord("foo.lastAccessTime");
		d = (Date) rr.readObject();
		System.out.println("\tlast access was at: " + d.toString());

		LOGGER.info("deleting record...");
		recordsFile.deleteRecord("foo.lastAccessTime");
		if (recordsFile.recordExists("foo.lastAccessTime")) {
			throw new Exception("Record not deleted");
		} else {
			LOGGER.info("record successfully deleted.");
		}

		LOGGER.info("test completed.");
	}
	
	@Test
	public void testInsertOneRecord() throws Exception {
		// given
		recordsFile = new RecordsFile(fileName, initialSize);
		List<UUID> uuids = createUuid(1);
		Object uuid = uuids.get(0);
		RecordWriter rw = new RecordWriter(uuid.toString());
		rw.writeObject(uuids.get(0));
		
		// when
		this.recordsFile.insertRecord(rw);
		RecordReader record = this.recordsFile.readRecord(uuid.toString());
		
		// then
		Assert.assertThat((UUID)record.readObject(), is(uuids.get(0)));
	}
	
	@Test
	public void testInsertOneRecordWithCrashes() throws Exception {
		final List<List<String>> writeStacks = new ArrayList<List<String>>();
		
		WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

		List<UUID> uuids = createUuid(1);
		UUID uuid = uuids.get(0);
		
		final List<String> localFileNames = new ArrayList<String>();
		final String recordingFile = fileName("record");
		localFileNames.add(recordingFile);
		interceptedInsertOneRecord(collectsWriteStacks, recordingFile,uuids);
		
		for(int index = 0; index < writeStacks.size(); index++){
			final List<String> stack = writeStacks.get(index);
			final WriteCallback crashAt = new CrashAtWriteCallback(index);
			final String localFileName = fileName("crash"+index);
			localFileNames.add(localFileName);
			try { 
				interceptedInsertOneRecord(crashAt, localFileName,uuids);
			} catch( IOException ioe ) {
				try {
					RecordsFile possiblyCorruptedFile = new RecordsFile(localFileName, "r");
					int count = possiblyCorruptedFile.getNumRecords();
					if( count > 0 ){
						RecordReader rr = possiblyCorruptedFile.readRecord(uuid.toString());
						UUID uuidDb = (UUID) rr.readObject();
						Assert.assertThat("should not exist but what does it look like?", uuidDb, is(uuid));
						Assert.assertThat(String.format("crash site %s found record %s at %s",index, uuidDb, stackToString(stack)), count,is(0));
					}
				} catch (Exception e ){
					removeFiles(localFileNames);
					final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
					throw new RuntimeException(msg,e);
				}
			}
		}
		removeFiles(localFileNames);		
	}

	
	@Test
	public void testInsertTwoRecords() throws Exception {
		// given
		recordsFile = new RecordsFile(fileName, initialSize);
		List<UUID> uuids = createUuid(2);
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		// when
		this.recordsFile.insertRecord(rw0);
		this.recordsFile.insertRecord(rw1);
		RecordReader rr = this.recordsFile.readRecord(uuid0.toString());
		RecordReader rr1 = this.recordsFile.readRecord(uuid1.toString());
		
		// then
		Assert.assertThat((UUID)rr.readObject(), is(uuid0));
		Assert.assertThat((UUID)rr1.readObject(), is(uuid1));
	}
	
	@Test
	public void testInsertTwoRecordsWithCrashes() throws Exception {
		final List<List<String>> writeStacks = new ArrayList<List<String>>();
		
		WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

		List<UUID> uuids = createUuid(2);
		UUID uuid0 = uuids.get(0);
		UUID uuid1 = uuids.get(1);
		
		final List<String> localFileNames = new ArrayList<String>();
		final String recordingFile = fileName("record");
		localFileNames.add(recordingFile);
		interceptedInsertTwoRecord(collectsWriteStacks, recordingFile,uuids);
		
		for(int index = 0; index < writeStacks.size(); index++){
			final List<String> stack = writeStacks.get(index);
			final WriteCallback crashAt = new CrashAtWriteCallback(index);
			final String localFileName = fileName("crash"+index);
			localFileNames.add(localFileName);
			try { 
				interceptedInsertTwoRecord(crashAt, localFileName,uuids);
			} catch( IOException ioe ) {
				try {
					RecordsFile possiblyCorruptedFile = new RecordsFile(localFileName, "r");
					int count = possiblyCorruptedFile.getNumRecords();
					if( count > 0 ){
						{
							RecordReader rr = possiblyCorruptedFile.readRecord(uuid0.toString());
							UUID uuidDb = (UUID) rr.readObject();
							Assert.assertThat("should not exist but what does it look like?", uuidDb, is(uuid0));
							Assert.assertThat(String.format("crash site %s found record %s at %s",index, uuidDb, stackToString(stack)), count,is(0));
						}
						{
							RecordReader rr = possiblyCorruptedFile.readRecord(uuid1.toString());
							UUID uuidDb = (UUID) rr.readObject();
							Assert.assertThat("should not exist but what does it look like?", uuidDb, is(uuid1));
							Assert.assertThat(String.format("crash site %s found record %s at %s",index, uuidDb, stackToString(stack)), count,is(0));
						}
					}
				} catch (Exception e ){
					removeFiles(localFileNames);
					final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
					throw new RuntimeException(msg,e);
				}
			}
		}
		removeFiles(localFileNames);		
	}
	
	
	
	private void removeFiles(List<String> localFileNames) {
		for( String file : localFileNames ){
			File f = new File(file);
			f.delete();
		}
	}

	private String stackToString(List<String> stack) {
		StringBuilder sb = new StringBuilder();
		for( String s : stack ){
			sb.append("\\n\\t");
			sb.append(s);
		}
		return sb.toString();
	}

	private String fileName(String base) {
		String fileName = TMP+base;
		File file = new File(fileName);
		file.deleteOnExit();
		return fileName;
	}

	private void interceptedInsertOneRecord(WriteCallback wc, String newFileName, List<UUID> uuids)
			throws IOException, RecordsFileException, OptionalDataException,
			ClassNotFoundException {
		LOGGER.info(String.format("writing to: "+newFileName));
		
		// given
		recordsFile = new RecordsFileSimulatesDiskFailures(newFileName, initialSize, wc);
		
		Object uuid = uuids.get(0);
		RecordWriter rw = new RecordWriter(uuid.toString());
		rw.writeObject(uuid);
		
		// when
		this.recordsFile.insertRecord(rw);
	}
	
	private void interceptedInsertTwoRecord(WriteCallback wc, String newFileName, List<UUID> uuids)
			throws IOException, RecordsFileException, OptionalDataException,
			ClassNotFoundException {
		// given
		recordsFile = new RecordsFile(fileName, initialSize);
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		// when
		this.recordsFile.insertRecord(rw0);
		this.recordsFile.insertRecord(rw1);
	}

	private List<UUID> createUuid(int count) {
		List<UUID> uuids = new ArrayList<UUID>(count);
		for( int index = 0; index < count; index++ ) {
			uuids.add(UUID.randomUUID());
		}
		return uuids;
	}
}
