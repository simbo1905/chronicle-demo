package com.github.simbo1905.chronicle.db;

import java.io.IOException;

public interface WriteCallback {

	void onWrite() throws IOException;

}
