package com.github.simbo1905.chronicle.slavelist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.datamodel.DataStore;
import com.higherfrequencytrading.chronicle.datamodel.ListListener;
import com.higherfrequencytrading.chronicle.datamodel.ListWrapper;
import com.higherfrequencytrading.chronicle.datamodel.ModelMode;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

public class ListWriterSlave {
	static final String TMP = System.getProperty("java.io.tmpdir");

	public static void main(String[] args) throws Exception {
		String name = TMP + "/chronicle";
		System.out.println("file is "+name);
		Chronicle chronicle = new IndexedChronicle(name);
		DataStore dataStore = new DataStore(chronicle, ModelMode.READ_ONLY);
		List<String> underlying = new ArrayList<String>();
		int maxMessageSize = 128;
		ListWrapper<String> list = new ListWrapper<String>(dataStore,
				"testlist", String.class, underlying, maxMessageSize);
		
		final AtomicInteger addCounter = new AtomicInteger();
		final AtomicInteger removeCounter = new AtomicInteger();
		
		list.addListener(new ListListener<String>() {
			
			@Override
			public void eventStart(long eventId, String name) {
			}
			
			@Override
			public void eventEnd(boolean lastEvent) {
			}
			
			@Override
			public void remove(String e) {
				removeCounter.getAndIncrement();
			}
			
			@Override
			public void add(String e) {
				addCounter.getAndIncrement();
			}
			
			@Override
			public void set(int index, String oldElement, String element) {
			}
			
			@Override
			public void remove(int index, String element) {
				removeCounter.getAndIncrement();
				if( removeCounter.get() == 2*1000000-1){
					System.out.println("notifying...");
					synchronized (ListWriterSlave.class) {
						ListWriterSlave.class.notifyAll();	
					}					
				}
			}
			
			@Override
			public void add(int index, String element) {
				addCounter.getAndIncrement();
			}
		});
		System.out.println("starting...");
		dataStore.start();
		System.out.println("waiting...");
		synchronized (ListWriterSlave.class) {
			ListWriterSlave.class.wait(30000);	
		}
		System.out.println("addCounter:"+addCounter.get());
		System.out.println("removeCounter:"+removeCounter.get());
		System.out.println("list.size(): "+list.size());
		System.out.println("list: "+list);
		System.out.println("The End.");
		
	}

}
