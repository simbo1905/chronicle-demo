package com.github.simbo1905.chronicle.fifo.demo;

import static com.github.simbo1905.chronicle.fifo.demo.FifoSingleJvm.MANY;

import java.io.IOException;
import java.util.Date;

import com.github.simbo1905.chronicle.fifo.Fifo;

public class FifoSlave {
	
	static final String TMP = System.getProperty("java.io.tmpdir");
	
	public static void main(String[] args) throws IOException, IllegalAccessException {
		String guid = "3F2504E0-4F89-11D3-9A0C-0305E82C3301";
		final int payloadSize = guid.getBytes("UTF8").length+1;
		Fifo<String> fifo = FifoMaster.createFifo(args, payloadSize);
		Fifo<String>.FifoTail tail = fifo.tail();
		tail.start();
		
		System.out.println("awaiting first guid...");
		
		tail.remove(); // await first
		
		System.out.println(new Date());
		
		long startMs = System.currentTimeMillis();
		
		for( int index = 0; index < MANY -1; index++ ){
			String uuid2 = tail.remove();
			while( uuid2 == null ) {
				Thread.yield();
				uuid2 = tail.remove();
			}
		}
		
		long endMs = System.currentTimeMillis();
		
		System.out.println(new Date());
		
		System.out.print(String.format("time: %s ms, count: %s\n", (endMs - startMs),MANY));
		Double timems = Double.valueOf(endMs) - Double.valueOf(startMs);
		Object pms = MANY / timems;
		System.out.print(String.format("per millisecond: %s\n", pms));
		Object pmms = MANY / (1000*timems);
		System.out.print(String.format("per microsecond: %s\n", pmms));
		
		tail.close();
	}
}
