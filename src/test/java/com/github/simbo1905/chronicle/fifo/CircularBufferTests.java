package com.github.simbo1905.chronicle.fifo;


import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CyclicBarrier;

import org.junit.Test;

public class CircularBufferTests {

	private static final int TRIALS_COUNT = 2<<9;
	private static final int BUFFER_SIZE = 2<<4;

	interface Fifo<T> { 
		T get() throws Exception; // blocks
		T poll(); // none blocking read
		void put(T t) throws Exception; // blocks
		void offer(T t); // non blocking add
		int size();

	}
	
	interface Factory<T> { 
		T newInstance();
	}
	
	static class AbqFifo implements Fifo<Long> {
		final ArrayBlockingQueue<Long> abq = new ArrayBlockingQueue<Long>(BUFFER_SIZE, true);
		
		@Override
		public Long get() throws Exception {
			return abq.take();
		}

		@Override
		public Long poll() {
			return abq.poll();
		}

		@Override
		public void offer(Long t) {
			abq.offer(t);
		}

		@Override
		public void put(Long t) throws Exception {
			abq.put(t);
		}

		@Override
		public int size() {
			return abq.size();
		}};
		
	static class LongFactory implements Factory<Long> {
			long counter = 0;
			@Override
			public Long newInstance() {
				return counter++;
			}};
	
	@Test
	public void testRandomRepeatable() throws Exception {
		Random random1 = new Random(0);
		Random random2 = new Random(0);
		for( int i = 0; i < (2<<8); i++ ){
			assertThat(random1.nextInt(), is(random2.nextInt()));
		}
	}
			
	@Test
	public void testAbqBlocking() throws Exception {
		// given
		Fifo<Long> fifo = new AbqFifo();
			
		// when 
		List<Long> results = this.<Long>doManyBlockingAdds(TRIALS_COUNT,fifo,new LongFactory());
		
		// then
		this.<Long>assertContiguous(results, TRIALS_COUNT,new LongFactory());
	}

	private <T> void assertContiguous(final List<T> results, final int count, final Factory<T> source) {
		assertNotNull("results must not be null", results);
		assertThat(results.size(), is(count));
		for( int i = 0; i < count; i++ ){
			T expected = source.newInstance();
			T actual = results.get(i);
			assertThat( actual, is(expected));
		}
	}
	
	private <T> List<T> doManyBlockingAdds(final int count, final Fifo<T> fifo, final Factory<T> source) throws Exception {
		
		final CyclicBarrier gate = new CyclicBarrier(3);
		final List<T> fifoOutput = new ArrayList<T>();
		
		Thread producer = new Thread( new Runnable() {
			final Random random = new Random(0);
			@Override
			public void run() {
				try {
					// preload queue
					for( int index = 0; index < BUFFER_SIZE; index++){
						T t = source.newInstance();
						fifo.put(t);
					}
					
					gate.await();
					
					for( int c = 0; c < count; c++){
						T t = source.newInstance();
						fifo.put(t);
						Thread.sleep(0, (int) (random.nextDouble()*100));
						System.out.println("p size "+fifo.size());
					}
				} catch (Exception e) {
					throw new RuntimeException("could not put", e);
				}
			}
		});
		
		Thread consumer = new Thread( new Runnable() {
			final Random random = new Random(count);
			@Override
			public void run() {
				try {
					gate.await();
				} catch( Exception e) {
					throw new RuntimeException("could not await gate", e);
				}
				for( int c = 0; c < count; c++){
					try {
						T t = fifo.get();
						fifoOutput.add(t);
						Thread.sleep(0, (int) (random.nextDouble()*100));
						System.out.println("c size "+fifo.size());
					} catch (Exception e) {
						throw new RuntimeException("could not put", e);
					}
				}
			}
		});
		
		producer.start();
		consumer.start();
		
		gate.await(); // go!
		
		producer.join();
		consumer.join();
		
		return fifoOutput;
	}
}
