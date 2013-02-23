package com.github.simbo1905.chronicle.fifo;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;
import com.higherfrequencytrading.chronicle.tools.ChronicleTools;

@Ignore
public class FifoTests {
	
	static final String TMP = System.getProperty("java.io.tmpdir");
	
	private static String location = TMP+"/"+FifoTests.class.getSimpleName();
	
	static { 
		ChronicleTools.deleteOnExit(location);
	}
	
	int payloadSize = -1;
	int some = -1;

	private IndexedChronicle chronicle;	
	
	@Before
	public void setup() throws Exception {
		payloadSize =UUID.randomUUID().toString().getBytes("UTF8").length+1;
		some = 2 << 4;
		chronicle = new IndexedChronicle(location);
	}

	@Test
	public void testFifo() throws Exception {
		Fifo<String> fifo = new Fifo<String>("fifo1", chronicle,String.class,payloadSize, 2 << 2);
		Fifo<String>.FifoHead head = fifo.head();
		head.start();
		Fifo<String>.FifoTail tail = fifo.tail();
		tail.start();
		
		List<String> addedToHead = new ArrayList<String>();
		List<String> removedFromTail = new ArrayList<String>();
		
		for( int index = 0; index < some; index++ ){
			String guid = UUID.randomUUID().toString();
			head.add(guid);
			addedToHead.add(guid);
			System.out.println(String.format("generated [%s] %s %s", Thread.currentThread().getId(), index, guid));
		}

		for( int index = 0; index < some; index++ ){
			String back = tail.remove();
			removedFromTail.add(back);
		}
		
		for( int index = 0; index < some; index++ ){
			String expected = addedToHead.get(index);
			String actual =  removedFromTail.get(index);
			System.out.println(String.format("expected %s %s", index, expected));
			System.out.println(String.format("actual %s %s", index, actual));
			if( !expected.equals(actual)){
				System.err.println("");
			}
			Assert.assertThat( expected, is( actual ) );
		}
		
		head.close();
		tail.close();
	}
	
	@Test
	public void testFifoNoChronicle() throws Exception {
		Fifo<String> fifo = new Fifo<String>("fifo2", chronicle ,String.class,payloadSize, 2 << 2);
		final Fifo<String>.FifoTail tail = fifo.tail();
		
		final List<String> uuids = new ArrayList<String>();
		List<String> removedFromTail = new ArrayList<String>();
		
		for( int index = 0; index < some; index++ ){
			String guid = UUID.randomUUID().toString();
			uuids.add(guid);
			System.out.println(String.format("generated [%s] %s %s", Thread.currentThread().getId(), index, guid));
		}

		// mock the chronicle stuff
		final Excerpt excerpt = Mockito.mock(Excerpt.class);
		final AtomicInteger counter = new AtomicInteger();
		when(excerpt.readObject()).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				return uuids.get(counter.getAndIncrement());
			}
		});
		
		// push it straight to the tail
		Thread pusher = new Thread(){
			@Override
			public void run() {
				for( int index = 0; index < some; index++ ){
					tail.onExcerpt(excerpt);
				}
			}
		};
		
		pusher.start();

		for( int index = 0; index < some; index++ ){
			String back = tail.remove();
			removedFromTail.add(back);
		}
		
		for( int index = 0; index < some; index++ ){
			String expected = uuids.get(index);
			String actual =  removedFromTail.get(index);
			System.out.println(String.format("expected %s %s", index, expected));
			System.out.println(String.format("actual %s %s", index, actual));
			if( !expected.equals(actual)){
				System.err.println("");
			}
			Assert.assertThat( expected, is( actual ) );
		}
	}
}
