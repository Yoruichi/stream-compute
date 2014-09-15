package td.olap.computer.data;

import java.io.Serializable;

public class Tuple {

	private String field;
	private Serializable[] messages;

	public Tuple(String field, Serializable... messages) {
		setField(field);
		setMessages(messages);
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Serializable[] getMessages() {
		return messages;
	}

	public void setMessages(Serializable[] messages) {
		this.messages = messages;
	}

}
