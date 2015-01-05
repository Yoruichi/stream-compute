package td.olap.computer.data;

import java.io.Serializable;

public class Tuple<K, V> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4384212878429421440L;
	private K field;
	private V[] messages;

	public Tuple(K field, V... messages) {
		setField(field);
		setMessages(messages);
	}

	public K getField() {
		return field;
	}

	public void setField(K field) {
		this.field = field;
	}

	public V getMessage(int index) {
		return messages[index];
	}

	public V[] getMessages() {
		return messages;
	}

	public void setMessages(V[] messages) {
		this.messages = messages;
	}

}
