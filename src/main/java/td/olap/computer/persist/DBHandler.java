package td.olap.computer.persist;

public abstract class DBHandler {

	public abstract void open() throws Exception;
	
	public abstract void close() throws Exception;
	
	public abstract void setKey(String key, String value);
	
	public abstract void setKey(String key, byte[] value);
	
	public abstract void delKey(String key);
	
	public abstract String getStringValue(String key)  throws Exception;
	
	public abstract byte[] getByteWiseValue(String key) throws Exception;
}
