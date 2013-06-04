package org.apache.camel.component.avro;

public class AvroComponentException extends Exception {

	private static final long serialVersionUID = 8915917806189741165L;
	
	public AvroComponentException() {
		super();
	}

	public AvroComponentException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public AvroComponentException(String message, Throwable cause) {
		super(message, cause);
	}

	public AvroComponentException(String message) {
		super(message);
	}

	public AvroComponentException(Throwable cause) {
		super(cause);
	}

}
