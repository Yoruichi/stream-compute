package td.olap.computer.persist;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class TestMultiThreadLevel {
	public static void main(String[] args) {
		Options options = new Options();
		options.createIfMissing(true);
		options.cacheSize(100 * 1048576);
		for (int i = 0; i < 5; i++)
			new T("example", options).start();
	}
}

class T extends Thread {

	private String dbName;
	private static Options options;

	static {
		options = new Options();
		options.createIfMissing(true);
		options.cacheSize(100 * 1048576);
	}

	public T(String dbName, Options options) {
		this.dbName = dbName;
	}

	@Override
	public void run() {
		DB db = null;
		synchronized (options) {
			try {
				db = JniDBFactory.factory.open(new File(dbName), options);
				for (int i = 0; i < 10000; i++)
					db.put(JniDBFactory.bytes("test" + i),
							JniDBFactory.bytes("test" + i));
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (db != null)
					try {
						db.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}
	}
}