package org.apache.camel.component.avro.processors;

import static org.apache.camel.component.avro.AvroConsumerTestSupport.REFLECTION_TEST_NAME;
import junit.framework.Assert;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class ReflectionInOnlyProcessor implements Processor {

	@Override
	public void process(Exchange exchange) throws Exception {
        Object body = exchange.getIn().getBody();
        if (body instanceof Object[]) {
            Object[] args = (Object[]) body;
            if (args.length == 1 && args[0] instanceof String) {
            	Assert.assertEquals(REFLECTION_TEST_NAME, args[0]);
            }
        }
    }

}
