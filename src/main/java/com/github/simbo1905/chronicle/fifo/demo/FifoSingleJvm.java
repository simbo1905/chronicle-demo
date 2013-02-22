package com.github.simbo1905.chronicle.fifo.demo;

import java.io.IOException;
import java.util.Date;

import com.github.simbo1905.chronicle.fifo.Fifo;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

/**
 * Checks that the fifo exceeds memory by writing 4x max memory so recommended 
 * as run with -Xmx10m
 * @author simbo
 */
public class FifoSingleJvm {
	static final String TMP = System.getProperty("java.io.tmpdir");

	public static int MANY = 1000000;
	
	public static void main(String[] args) throws IOException, IllegalAccessException {

		final String nameRoot = (args.length == 0)?"fifotestrunner":args[0];
		
		long maxMemory = Runtime.getRuntime().maxMemory();
		
		System.out.println(String.format("maxMemory: %s", maxMemory));
		
		String guid = "3F2504E0-4F89-11D3-9A0C-0305E82C3301";
		
		final int payloadSize = guid.getBytes("UTF8").length+1;
		
		final int many = (int) (2*maxMemory/payloadSize);

		System.out.print(String.format("data count: %s, total data size: %s\n", many,(many*payloadSize)));

		IndexedChronicle chronicle = new IndexedChronicle(TMP+nameRoot);
		//chronicle.useUnsafe(true);
		
		Fifo<String> fifo = new Fifo<String>("singlejvmfifo", chronicle,String.class,payloadSize, 2 << 14);
		Fifo<String>.FifoHead head = fifo.head();
		head.start();
		Fifo<String>.FifoTail tail = fifo.tail();
		tail.start();
		
		System.out.println(new Date());
		
		long startMs = System.currentTimeMillis();
		
		for( int index = 0; index < many; index++ ){
			head.add(guid);
		}

		for( int index = 0; index < many; index++ ){
			String back = tail.remove();
			if( back == null ) {
				Thread.yield();
			}
			if( !back.equals(guid)){
				throw new AssertionError(String.format("%s != %s", back, guid));
			}
		}

		long endMs = System.currentTimeMillis();
		
		System.out.println(new Date());
		
		System.out.print(String.format("time: %s ms, count: %s\n", (endMs - startMs),many));
		Double timems = Double.valueOf(endMs) - Double.valueOf(startMs);
		Object pms = many / timems;
		System.out.print(String.format("per millisecond: %s\n", pms));
		Object pmms = many / (1000*timems);
		System.out.print(String.format("per microsecond: %s\n", pmms));
		
		head.close();
		tail.close();
	}
}
