package org.apache.camel.component.avro;

public interface AvroResponder {
	
	void register(String messageName, AvroConsumer consumer) throws AvroComponentException;
	
	boolean unregister(String messageName);

}
