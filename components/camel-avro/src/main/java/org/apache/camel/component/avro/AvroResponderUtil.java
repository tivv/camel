package org.apache.camel.component.avro;

import static org.apache.camel.component.avro.AvroConstants.AVRO_HTTP_TRANSPORT;
import static org.apache.camel.component.avro.AvroConstants.AVRO_NETTY_TRANSPORT;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
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
	public static Server initAndStartServer(Server server, Responder responder, AvroConsumer consumer) throws IOException {
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
	 * @return			Response of exchanhge processing
	 */
	public static Object processExchange(AvroConsumer consumer, Protocol.Message message, Object[] params) {
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
}
