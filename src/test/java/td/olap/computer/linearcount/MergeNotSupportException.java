package td.olap.computer.linearcount;

public class MergeNotSupportException extends Exception {

	public MergeNotSupportException(String message) {
		super(message);
	}

	public MergeNotSupportException(Exception e) {
		super(e);
	}

}
