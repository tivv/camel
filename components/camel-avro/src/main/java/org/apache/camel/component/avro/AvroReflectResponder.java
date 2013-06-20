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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang.StringUtils;
import org.mortbay.log.Log;

public class AvroReflectResponder extends ReflectResponder implements
		AvroResponder {
	
	private ConcurrentMap<String, AvroConsumer> consumerRegistry = new ConcurrentHashMap<String, AvroConsumer>();
    private AvroConsumer defaultConsumer;
	private Server server;
	
	public AvroReflectResponder(AvroConsumer consumer)  throws Exception {
        super(consumer.getEndpoint().getProtocol(), null);
        server = AvroResponderUtil.initAndStartServer(server, this, consumer);
    }
	
	@Override
    public Object respond(Protocol.Message message, Object request) throws AvroComponentException {
    	if((defaultConsumer==null) && (!getLocal().getMessages().containsKey(message.getName())))
        	throw new AvroComponentException("No message with name: " + message.getName() + " mapped.");
        
        AvroConsumer consumer = this.defaultConsumer;
        if(!StringUtils.isEmpty(message.getName()) && this.consumerRegistry.containsKey(message.getName()))
        	consumer = this.consumerRegistry.get(message.getName());
        
        if(consumer == null) throw new AvroComponentException("No consumer defined for message: " + message.getName());
        
        Object params = AvroResponderUtil.<ReflectData>extractParams(message, request, consumer.getEndpoint().getConfiguration().getSingleParameter(), ReflectData.get());
        
        return AvroResponderUtil.processExchange(consumer, message, params);
    }

    /**
     * Registers consumer by appropriate message name as key in registry.
     *  
     * @param messageName	message name
     * @param consumer		avro consumer
     * @throws AvroComponentException
     */
    @Override
    public void register(String messageName, AvroConsumer consumer) throws AvroComponentException {       
    	if (messageName == null) {
    		if(this.defaultConsumer != null)
    			throw new AvroComponentException("Default consumer already registered for uri: " + consumer.getEndpoint().getEndpointUri());
    		this.defaultConsumer = consumer;
    	} else {
    		if (consumerRegistry.putIfAbsent(messageName, consumer) != null) {
    			throw new AvroComponentException("Consumer already registered for message: " + messageName + " and uri: " + consumer.getEndpoint().getEndpointUri());
    		}
    	}
    }
    
    /**
     * Unregisters consumer by message name.
     * Stops server in case if all consumers are unregistered and default consumer is absent or stopped. 
     * 
     * @param messageName message name
     * @return true if all consumers are unregistered and defaultConsumer is absent or null.
     *         It means that this responder can be unregistered. 
     */
    @Override
    public boolean unregister(String messageName) {
    	if(!StringUtils.isEmpty(messageName)) {
    		if(consumerRegistry.remove(messageName) == null)
    			Log.warn("Consumer with message name " + messageName + " was already unregistered.");
    	}
    	else defaultConsumer = null;
    	
    	if((defaultConsumer == null) && (consumerRegistry.isEmpty())) {
            if (server != null) {
                server.close();
            }
    		return true; 
    	}
    	return false;
    }
}
