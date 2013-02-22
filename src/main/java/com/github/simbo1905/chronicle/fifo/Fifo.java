package com.github.simbo1905.chronicle.fifo;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.datamodel.DataStore;
import com.higherfrequencytrading.chronicle.datamodel.ModelMode;
import com.higherfrequencytrading.chronicle.datamodel.Wrapper;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * A first in first out structure backed by a memory mapped file for
 * interprocess communication. 
 * 
 * @author simbo
 */
public class Fifo<T> {
	static final String TMP = System.getProperty("java.io.tmpdir");

	private final String name;

	protected final int maxMessageSize;
	protected final Class<T> clazz;
	protected final int buffersize;
	protected final Chronicle chronicle;

	private FifoHead head;
	private FifoTail tail;

	public Fifo(String name, Chronicle chronicle, Class<T> clazz, int maxMessageSize, int bufferSize){
		this.name = name;
		this.chronicle = chronicle;
		this.maxMessageSize = maxMessageSize;
		this.clazz = clazz;
		this.buffersize = bufferSize;
	}

	public class FifoHead implements Closeable {
		
		protected final DataStore dataStore;

		final protected FileLock fileLock;
		
		@Override
		public void close() throws IOException {
			dataStore.close();
			this.fileLock.release();
		}

		public FifoHead(FileLock fileLock) throws IOException {
			this.fileLock = fileLock;
			dataStore = new DataStore(chronicle, ModelMode.MASTER);
		}
		
		public void add(T t) {
			checkWritable();
			Excerpt excerpt = dataStore.startExcerpt(
					maxMessageSize + 2 + name.length() + 1, name);
			excerpt.writeObject(t);
			excerpt.finish();
		}

		void checkWritable() {
			dataStore.checkWritable();
		}

		public void start() {
			dataStore.start();
		}
	}
	
    public final static EventFactory<MutableSlot> EVENT_FACTORY = new EventFactory<MutableSlot>()
    {
        public MutableSlot newInstance()
        {
            return new MutableSlot();
        }
    };
	
	public static final class MutableSlot
	{
		private Object value;
		private long sequence;

		public synchronized long getSequence() {
			return sequence;
		}

		public synchronized Object getValue()
	    {
	        return value;
	    }

	    public synchronized void setValue(final Object value)
	    {
	        this.value = value;
	    }

		public synchronized void setSequence(long sequence) {
			this.sequence = sequence;
		}

	}

	public class FifoTail implements Wrapper, Closeable {
		
		private final DataStore dataStore;
		
		private final ExecutorService executor;
		
		private final RingBuffer<MutableSlot> ringBuffer;
		
		private final Disruptor<MutableSlot> disruptor; 
		
		ArrayBlockingQueue<Object> buffer = new ArrayBlockingQueue<Object>(buffersize, true);
		
		@Override
		public void close() {
			dataStore.close();
	        disruptor.shutdown();
	        executor.shutdown();
		}
		
		@SuppressWarnings("unchecked")
		public FifoTail() throws IOException {
			dataStore = new DataStore(chronicle, ModelMode.READ_ONLY);
			dataStore.add(name, this);
			executor = Executors.newSingleThreadExecutor();
			disruptor =
					  new Disruptor<MutableSlot>(EVENT_FACTORY, executor, 
					                            new SingleThreadedClaimStrategy(buffersize),
					                            new SleepingWaitStrategy());
			
			EventHandler<MutableSlot> eventHandler = new EventHandler<Fifo.MutableSlot>() {
	            public void onEvent(final MutableSlot event, final long sequence, final boolean endOfBatch) throws Exception {
	            	buffer.put(event.getValue());
	            }
	        };
			disruptor.handleEventsWith(eventHandler);	        
			ringBuffer = disruptor.start();	   
		}

		@SuppressWarnings("unchecked")
		public T remove() {
			Object value = buffer.poll();
			while( value == null ) {
				Thread.yield();
				value = buffer.poll();
			}
			return (T) value;
		}

		public void start() {
			dataStore.start();
		}
		
		@Override
		public void onExcerpt(Excerpt excerpt) {
			@SuppressWarnings("unchecked")
			T t = (T) excerpt.readObject();
			excerpt.finish();
			long sequence = ringBuffer.next();
			MutableSlot event = ringBuffer.get(sequence);
			event.setValue(t);
			event.setSequence(sequence);
			ringBuffer.publish(sequence);   
		}
		
		@Override
		public void notifyOff(boolean notifyOff) {
			// noop
		}
	}
	
	public FifoHead head() throws IOException, IllegalAccessException {
		final FileLock fileLock = obtainFileLock(name);
		head = new FifoHead(fileLock);
		return head;
	}

	public FifoTail tail() throws IOException, IllegalAccessException {
		tail = new FifoTail();
		return tail;
	}

	private FileLock obtainFileLock(String name)
			throws FileNotFoundException, IOException,
			IllegalAccessException {
		final File f = new File(name);
		final RandomAccessFile file = new RandomAccessFile(f, "rw");
		final FileLock fileLock = file.getChannel().tryLock();
		if (null == fileLock) {
			throw new IllegalAccessException(
					"could not obtain FileLock at " + f.getCanonicalPath());
		}
		return fileLock;
	}
}
