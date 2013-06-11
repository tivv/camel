package org.apache.camel.avro.test;

public class TestReflectionImpl implements TestReflection {

	String name = "";
	int age = 0;
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int getAge() {
		return age;
	}

	@Override
	public void setAge(int age) {
		this.age = age;
	}

}
