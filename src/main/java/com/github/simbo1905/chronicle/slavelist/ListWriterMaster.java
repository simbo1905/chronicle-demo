package com.github.simbo1905.chronicle.slavelist;

import java.util.ArrayList;
import java.util.List;

import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.datamodel.DataStore;
import com.higherfrequencytrading.chronicle.datamodel.ListWrapper;
import com.higherfrequencytrading.chronicle.datamodel.ModelMode;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

public class ListWriterMaster {
	static final String TMP = System.getProperty("java.io.tmpdir");

	public static void main(String[] args) throws Exception {
		String name = TMP + "/chronicle";
		Chronicle chronicle = new IndexedChronicle(name);
		DataStore dataStore = new DataStore(chronicle, ModelMode.MASTER);
		List<String> underlying = new ArrayList<String>();
		int maxMessageSize = 128;
		ListWrapper<String> list = new ListWrapper<String>(dataStore,
				"testlist", String.class, underlying, maxMessageSize);
		dataStore.start();
		for( int i = 0; i < 1000000; i++ ){ 
			list.add(0, "hello"+i);
			list.add(1, "world"+i);
			list.remove(0);
			list.remove(1); // does nothing until second loop and leaves one elements
		}
		System.out.println("list.size(): "+list.size());
		System.out.println("list: "+list);
		chronicle.close();
	}
}
