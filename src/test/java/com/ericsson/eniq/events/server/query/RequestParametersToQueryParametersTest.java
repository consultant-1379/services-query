/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Map;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.RATDescriptionMappingUtils;

/**
 * @author eemecoy
 *
 */
public class RequestParametersToQueryParametersTest extends BaseJMockUnitTest {

    private RequestParametersToQueryParameters requestParametersToQueryParameters;

    RATDescriptionMappingUtils ratService;

    @Before
    public void setup() {
        setUpMockedRatService();
        requestParametersToQueryParameters = new RequestParametersToQueryParameters();
    }

    @Test
    public void test() {
        final String cell = "00";
        final String controller = "ONRM_RootMo_R:RNC01:RNC01";
        final String vendor = "Ericsson";
        final String node = cell + ",," + controller + "," + vendor + "," + "3G";
        final Map<String, QueryParameter> result = requestParametersToQueryParameters.mapNodeParameterToCell(node,
                ratService);
        assertThat(result.size(), is(4));
        assertThat((String) result.get(CELL_SQL_NAME).getValue(), is(cell));
        assertThat((String) result.get(BSC_SQL_NAME).getValue(), is(controller));
        assertThat((String) result.get(VENDOR_PARAM_UPPER_CASE).getValue(), is(vendor));
        assertThat((String) result.get(RAT_PARAM).getValue(), is("1"));
    }

    private void setUpMockedRatService() {
        ratService = mockery.mock(RATDescriptionMappingUtils.class);
        mockery.checking(new Expectations() {
            {
                allowing(ratService).getRATIntegerValue("3G");
                will(returnValue("1"));
            }
        });

    }

}
