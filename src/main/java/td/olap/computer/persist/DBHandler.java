package td.olap.computer.persist;

import java.util.Set;

public abstract class DBHandler {

    public abstract void open() throws Exception;

    public abstract void close() throws Exception;

    public abstract void setKey(String key, String value);

    public abstract void setKey(String key, byte[] value);

    public abstract void hSetKey(String key, String field, String value);

    public abstract void hSetKey(String key, String field, byte[] value);

    public abstract String hGetStringValue(String key, String field);

    public abstract byte[] hGetWiseValue(String key, String field);

    public abstract void hDelField(String key, String field);

    public abstract Set<String> hGetFields(String key);

    public abstract void delKey(String key);

    public abstract String getStringValue(String key) throws Exception;

    public abstract byte[] getByteWiseValue(String key) throws Exception;
}
