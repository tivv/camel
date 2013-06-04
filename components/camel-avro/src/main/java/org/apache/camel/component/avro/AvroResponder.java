/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.avro;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.specific.SpecificData;

import org.apache.camel.Exchange;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.lang.StringUtils;
import static org.apache.camel.component.avro.AvroConstants.*;

public class AvroResponder extends SpecificResponder {

	private ConcurrentMap<String, AvroConsumer> consumerRegistry = new ConcurrentHashMap<String, AvroConsumer>();
    private AvroConsumer defaultConsumer;
    private Server server;
    
    public AvroResponder(AvroConsumer consumer)  throws Exception {
        super(consumer.getEndpoint().getProtocol(), null);
        AvroConfiguration configuration = consumer.getEndpoint().getConfiguration();
        
        if(AVRO_HTTP_TRANSPORT.equalsIgnoreCase(configuration.getTransport()))
        	server = new HttpServer(this, configuration.getPort());
        
        if(AVRO_NETTY_TRANSPORT.equalsIgnoreCase(configuration.getTransport()))
            server = new NettyServer(this, new InetSocketAddress(configuration.getHost(), configuration.getPort()));

        server.start();
    }

    @Override
    public Object respond(Protocol.Message message, Object request) {
        Object response;
        int numParams = message.getRequest().getFields().size();
        Object[] params = new Object[numParams];
        Class<?>[] paramTypes = new Class[numParams];
        int i = 0;
        for (Schema.Field param : message.getRequest().getFields()) {
            params[i] = ((GenericRecord) request).get(param.name());
            paramTypes[i] = SpecificData.get().getClass(param.schema());
            i++;
        }

        AvroConsumer consumer = this.defaultConsumer;
        if(!StringUtils.isEmpty(message.getName()) && this.consumerRegistry.get(message.getName()) != null)
        	consumer = this.consumerRegistry.get(message.getName());
        
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
     * Registers consumer by appropriate message name as key in registry.
     *  
     * @param messageName	message name
     * @param consumer		avro consumer
     * @throws AvroComponentException
     */
    public void register(String messageName, AvroConsumer consumer) throws AvroComponentException {       
    	if (messageName == null) {
    		if(this.defaultConsumer != null)
    			throw new AvroComponentException("Consumer default consumer already registrered for uri: " + consumer.getEndpoint().getEndpointUri());
    		this.defaultConsumer = consumer;
    	} else {
    		if (consumerRegistry.putIfAbsent(messageName, consumer)!=null) {
    			throw new AvroComponentException("Consumer already registrered for message: " + messageName + " and uri: " + consumer.getEndpoint().getEndpointUri());
    		}
    	}
    }
    
    /**
     * Unregisters consumer by message name.
     * Stops server in case if all consumers are unregistered and default consumer is absent or stopped. 
     * 
     * @param messageName message name
     * @return
     */
    public boolean unregister(String messageName) {
    	if(!StringUtils.isEmpty(messageName)) consumerRegistry.remove(messageName);
    	if((defaultConsumer == null || StringUtils.isEmpty(messageName)) && consumerRegistry.isEmpty()) {
            if (server != null) {
                server.close();
            }
    		return true; 
    	}
    	return false;
    }
}
