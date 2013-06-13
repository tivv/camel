package org.apache.camel.component.avro;

import java.io.IOException;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;

public class AvroReflectRequestor extends ReflectRequestor {

	public AvroReflectRequestor(Class<?> iface, Transceiver transceiver) throws IOException {
		super(iface, transceiver);
	}

	public AvroReflectRequestor(Protocol protocol, Transceiver transceiver) throws IOException {
		super(protocol, transceiver);
	}
}
