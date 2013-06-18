package org.apache.camel.component.avro;

import static org.apache.camel.component.avro.AvroConstants.AVRO_HTTP_TRANSPORT;
import static org.apache.camel.component.avro.AvroConstants.AVRO_NETTY_TRANSPORT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.specific.SpecificData;
import org.apache.camel.Exchange;
import org.apache.camel.util.ExchangeHelper;

public class AvroResponderUtil {

	/**
	 * Initializes and starts http or netty server on basis of transport protocol from configuration.
	 * 
	 * @param server	http or netty server to initialize and start  
	 * @param responder AvroResponder
	 * @param consumer	AvroConsumer
	 * @return			Initialized and started server
	 * @throws IOException
	 */
	static Server initAndStartServer(Server server, Responder responder, AvroConsumer consumer) throws IOException {
		AvroConfiguration configuration = consumer.getEndpoint().getConfiguration();
        
        if(AVRO_HTTP_TRANSPORT.equalsIgnoreCase(configuration.getTransport()))
        	server = new HttpServer(responder, configuration.getPort());
        
        if(AVRO_NETTY_TRANSPORT.equalsIgnoreCase(configuration.getTransport()))
            server = new NettyServer(responder, new InetSocketAddress(configuration.getHost(), configuration.getPort()));

        server.start();
        
        return server;
	}
	
	/**
	 * Creates exchange and processes it.
	 * 
	 * @param consumer	Holds processor and exception handler
	 * @param message	Message on which exchange is created
	 * @param params	Params of exchange
	 * @return			Response of exchange processing
	 */
	static Object processExchange(AvroConsumer consumer, Protocol.Message message, Object params) {
		Object response;
		Exchange exchange = consumer.getEndpoint().createExchange(message, params);

        try {
        	consumer.getProcessor().process(exchange);
        } catch (Throwable e) {
        	consumer.getExceptionHandler().handleException(e);
        }

        if (ExchangeHelper.isOutCapable(exchange)) {
            response = exchange.getOut().getBody();
        } else {
            response = null;
        }

        boolean failed = exchange.isFailed();
        if (failed) {
            if (exchange.getException() != null) {
                response = exchange.getException();
            } else {
                // failed and no exception, must be a fault
                response = exchange.getOut().getBody();
            }
        }
		return response;
	}
	
	/**
	 * Extracts parameters from RPC call to List or converts to object of appropriate type
	 * if only one parameter set.
	 *  
	 * @param	message Avro message
	 * @param	request Avro request
	 * @return	Parameters of RPC method invocation
	 */
	static <T extends SpecificData> Object extractParams(Protocol.Message message, Object request) {
		int numParams = message.getRequest().getFields().size();
        
        if(numParams == 1) {
        	Object param;
        	Field field = message.getRequest().getFields().get(0);
        	Class<T> paramType = T.get().getClass(field.schema());
        	if(!paramType.isPrimitive() && ((GenericRecord) request).get(field.name()) != null)
        		param = paramType.cast(((GenericRecord) request).get(field.name()));
        	else
        		param = ((GenericRecord) request).get(field.name());
        	return param;
        } else {
        	List<Object> params =  new ArrayList<Object>();
			for (Schema.Field param : message.getRequest().getFields()) {
				params.add(((GenericRecord) request).get(param.name()));
			}
			return params;
        }
	}
}
