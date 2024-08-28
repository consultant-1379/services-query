package com.ericsson.eniq.events.server.query;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ericsson.eniq.events.server.common.EventDataSourceType;

public class EventDataSourceTypeTest {
	
	// TODO: the TR_ vars will be refactored to something more readable.
	// 
	@Test
	public void testEnumToVelocityTemplateParameterMapping() {
		assertEquals("TR_1", EventDataSourceType.RAW.getValue());
		assertEquals("TR_2", EventDataSourceType.AGGREGATED_1MIN.getValue());
		assertEquals("TR_3", EventDataSourceType.AGGREGATED_15MIN.getValue());
		assertEquals("TR_4", EventDataSourceType.AGGREGATED_DAY.getValue());
	}

}
