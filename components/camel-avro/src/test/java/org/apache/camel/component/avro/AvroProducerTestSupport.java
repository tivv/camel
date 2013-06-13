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

import java.io.IOException;

import junit.framework.Assert;

import org.apache.avro.ipc.Server;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.avro.generated.Key;
import org.apache.camel.avro.generated.Value;
import org.apache.camel.avro.impl.KeyValueProtocolImpl;
import org.apache.camel.avro.test.TestReflectionImpl;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Test;

public abstract class AvroProducerTestSupport extends AvroTestSupport {

	protected int avroPort = setupFreePort("avroort");
	protected int avroPortReflection = setupFreePort("avroPortReflection");
	
    Server server;
    Server serverReflection;
    KeyValueProtocolImpl keyValue = new KeyValueProtocolImpl();
    TestReflectionImpl testReflectionImpl = new TestReflectionImpl();

    protected abstract void initializeServer() throws IOException;

    @Override
    public void setUp() throws Exception {
        initializeServer();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (server != null) {
            server.close();
        }
        
        if (serverReflection != null) {
            serverReflection.close();
        }
    }

    @Test
    public void testInOnly() throws InterruptedException {
        Key key = Key.newBuilder().setKey("1").build();
        Value value = Value.newBuilder().setValue("test value").build();
        Object[] request = {key, value};
        template.sendBodyAndHeader("direct:in", request, AvroConstants.AVRO_MESSAGE_NAME, "put");
        Assert.assertEquals(value, keyValue.getStore().get(key));
    }
    
    @Test
    public void testInOnlyWithMessageNameInRoute() throws InterruptedException {
        Key key = Key.newBuilder().setKey("1").build();
        Value value = Value.newBuilder().setValue("test value").build();
        Object[] request = {key, value};
        template.sendBody("direct:in-message-name", request);
        Assert.assertEquals(value, keyValue.getStore().get(key));
    }
    
    @Test
    public void testInOnlyReflection() throws InterruptedException {
        String name = "Chuck";
        Object[] request = {name};
        template.sendBody("direct:in-reflection", request);
        Assert.assertEquals(name, testReflectionImpl.getName());
    }
    
    @Test(expected=CamelExecutionException.class)
    public void testInOnlyWithWrongMessageNameInMessage() throws InterruptedException {
        Key key = Key.newBuilder().setKey("1").build();
        Value value = Value.newBuilder().setValue("test value").build();
        Object[] request = {key, value};
        template.sendBodyAndHeader("direct:in-message-name", request, AvroConstants.AVRO_MESSAGE_NAME, "/get");
        Assert.assertEquals(value, keyValue.getStore().get(key));
    }

    @Test
    public void testInOut() throws InterruptedException {
        keyValue.getStore().clear();
        Key key = Key.newBuilder().setKey("2").build();
        Value value = Value.newBuilder().setValue("test value").build();
        keyValue.getStore().put(key, value);

        MockEndpoint mock = getMockEndpoint("mock:result-inout");
        mock.expectedMessageCount(1);
        mock.expectedBodiesReceived(value);
        template.sendBodyAndHeader("direct:inout", key, AvroConstants.AVRO_MESSAGE_NAME, "get");
        mock.assertIsSatisfied(10000);
    }
    
    @Test
    public void testInOutMessageNameInRoute() throws InterruptedException {
        keyValue.getStore().clear();
        Key key = Key.newBuilder().setKey("2").build();
        Value value = Value.newBuilder().setValue("test value").build();
        keyValue.getStore().put(key, value);

        MockEndpoint mock = getMockEndpoint("mock:result-inout-message-name");
        mock.expectedMessageCount(1);
        mock.expectedBodiesReceived(value);
        template.sendBody("direct:inout-message-name", key);
        mock.assertIsSatisfied(10000);
    }
    
    @Test(expected=CamelExecutionException.class)
    public void testInOutWrongMessageNameInRoute() throws InterruptedException {
        keyValue.getStore().clear();
        Key key = Key.newBuilder().setKey("2").build();
        Value value = Value.newBuilder().setValue("test value").build();
        keyValue.getStore().put(key, value);

        MockEndpoint mock = getMockEndpoint("mock:result-inout-message-name");
        mock.expectedMessageCount(0);
        mock.expectedBodiesReceived(value);
        template.sendBody("direct:inout-wrong-message-name", key);
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        AvroConfiguration configuration = new AvroConfiguration();
        AvroComponent component = new AvroComponent(context);
        component.setConfiguration(configuration);
        context.addComponent("avro", component);
        return context;
    }
}