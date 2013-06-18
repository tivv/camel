package org.apache.camel.component.avro.processors;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.avro.test.TestReflection;
import org.apache.camel.avro.test.TestReflectionImpl;

public class ReflectionInOutProcessor implements Processor {

	private TestReflection testReflection = new TestReflectionImpl();
	
	public ReflectionInOutProcessor(TestReflection testReflection) {
		this.testReflection = testReflection;  
	}
	
	@Override
	public void process(Exchange exchange) throws Exception {
        Object body = exchange.getIn().getBody();
        if (body instanceof Object) {
            exchange.getOut().setBody(testReflection.increaseAge((Integer) body));
        }
        
        if (body instanceof List<?>) {
            List<Object> args = (List<Object>) body;
            if (args.size() == 1 && args.get(0) instanceof Integer) {
            	exchange.getOut().setBody(testReflection.increaseAge((Integer) args.get(0)));
            }
        }
    }
}
