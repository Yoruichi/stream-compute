package td.olap.computer.persist;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class LevelDBBeanch {

	public static void main(String[] args) {
		Options options = new Options();
		try {
			DB db = JniDBFactory.factory.open(new File("example"), options);
			long start = System.currentTimeMillis();
			for(int i=0; i<100000000; i++) {
				db.put(JniDBFactory.bytes(UUID.randomUUID().toString()), JniDBFactory.bytes("" + i));
			}
			String stats = db.getProperty("leveldb.stats");
			System.out.println(stats);
			System.out.println(JniDBFactory.asString(db.get(JniDBFactory.bytes("London"))));
			System.out.println("use time : " + (System.currentTimeMillis() - start) + " ms.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
