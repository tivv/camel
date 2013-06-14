package org.apache.camel.component.avro;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.collections.MapUtils;
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
    	if(MapUtils.isNotEmpty(getLocal().getMessages()) && !getLocal().getMessages().containsKey(message.getName()))
        	throw new AvroComponentException("No message with name: " + message.getName() + " defined in protocol.");
    	
        int numParams = message.getRequest().getFields().size();
        Object[] params = new Object[numParams];
        Class<?>[] paramTypes = new Class[numParams];
        int i = 0;
        for (Schema.Field param : message.getRequest().getFields()) {
            params[i] = ((GenericRecord) request).get(param.name());
            paramTypes[i] = ReflectData.get().getClass(param.schema());
            i++;
        }
        
        AvroConsumer consumer = this.defaultConsumer;
        if(!StringUtils.isEmpty(message.getName()) && this.consumerRegistry.get(message.getName()) != null)
        	consumer = this.consumerRegistry.get(message.getName());
        
        if(consumer == null) throw new AvroComponentException("No consumer defined for message: " + message.getName());
        
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
    			throw new AvroComponentException("Consumer default consumer already registrered for uri: " + consumer.getEndpoint().getEndpointUri());
    		this.defaultConsumer = consumer;
    	} else {
    		if (consumerRegistry.putIfAbsent(messageName, consumer) != null) {
    			throw new AvroComponentException("Consumer already registrered for message: " + messageName + " and uri: " + consumer.getEndpoint().getEndpointUri());
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
