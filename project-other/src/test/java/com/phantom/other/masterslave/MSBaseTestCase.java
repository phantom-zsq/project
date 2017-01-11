package com.phantom.other.masterslave;

import org.springframework.test.AbstractDependencyInjectionSpringContextTests;

public class MSBaseTestCase extends AbstractDependencyInjectionSpringContextTests {

	@Override
	protected String[] getConfigLocations() {
		return new String[] { "classpath*:/spring-ms-test.xml" };
	}
}
