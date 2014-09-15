package td.olap.computer.persist;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.*;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

public class LevelDBOperator {

	public static void main(String[] args) {

		Options options = new Options();

		DBComparator comparator = new DBComparator() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return asString(arg0).compareTo(asString(arg1)) * -1;
			}

			@Override
			public String name() {
				return "leveldb.BytewiseComparator";
			}

			@Override
			public byte[] findShortestSeparator(byte[] start, byte[] limit) {
				return start;
			}

			@Override
			public byte[] findShortSuccessor(byte[] key) {
				return key;
			}
		};
//		options.comparator(comparator);

		options.createIfMissing(true);
		options.cacheSize(100 * 1048576); // 100MB cache
		DB db = null;
		WriteBatch batch = null;
		DBIterator iterator = null;
		try {
			db = factory.open(new File("example"), options);
			String stats = db.getProperty("leveldb.stats");
			System.out.println(stats);
			db.put(bytes("Tampa"), bytes("rocks"));
			String value = asString(db.get(bytes("Tampa")));
			System.out.println(value);
			db.delete(bytes("Tampa"));
			// batch write
			batch = db.createWriteBatch();
			batch.delete(bytes("Denver"));
			batch.put(bytes("Tampa"), bytes("green"));
			batch.put(bytes("London"), bytes("red"));
			db.write(batch);
			System.out.println(asString(db.get(bytes("London"))));
			System.out.println(asString(db.get(bytes("Denver"))));
			// iterator
			iterator = db.iterator();
			for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
				String k = asString(iterator.peekNext().getKey());
				String v = asString(iterator.peekNext().getValue());
				System.out.println(k + " = " + v);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (iterator != null)
					iterator.close();
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			iterator = null;
			try {
				if (batch != null)
					batch.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			batch = null;
			try {
				if (db != null)
					db.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			db = null;
		}
	}
}
