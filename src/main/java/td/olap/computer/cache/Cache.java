package td.olap.computer.cache;

public interface Cache<K, V> {

	/**
	 * Put a value into cache with key
	 * @param key
	 * @return if this key exists return the old value else return current value
	 */
	public V put(K key, V value);
	
	/**
	 * Get the value in cache under the key
	 * @param key
	 * @return the value maybe null
	 */
	public V get(K key);
	
}
