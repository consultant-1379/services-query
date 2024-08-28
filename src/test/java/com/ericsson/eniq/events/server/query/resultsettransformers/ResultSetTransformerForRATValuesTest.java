/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query.resultsettransformers;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_COLUMN_NAME;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_DESC_COLUMN_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

/**
 * @author eemecoy
 *
 */
public class ResultSetTransformerForRATValuesTest extends BaseJMockUnitTest {

    private static final String RAT_VALUE_FOR_GSM = "0";

    protected static final String RAT_VALUE_FOR_WCDMA = "1";

    private ResultSetTransformerForRATValues objToTest;

    @Before
    public void setup() {
        objToTest = new ResultSetTransformerForRATValues();
    }

    @Test
    public void testTransformReturnsMappingOfRATValuesForNetworkTypes() throws SQLException {
        final ResultSet mockedResultSet = mockery.mock(ResultSet.class);
        final String ratStringValueForGSM = "GSM";
        final String ratStringValueForWCDMA = "WCDMA";
        setUpMockedResultSetToReturnData(ratStringValueForGSM, ratStringValueForWCDMA, mockedResultSet);
        final Map<String, String> result = objToTest.transform(mockedResultSet);
        assertTrue(result.containsKey(RAT_VALUE_FOR_GSM));
        assertThat(result.get(RAT_VALUE_FOR_GSM), is(ratStringValueForGSM));
        assertTrue(result.containsKey(RAT_VALUE_FOR_WCDMA));
        assertThat(result.get(RAT_VALUE_FOR_WCDMA), is(ratStringValueForWCDMA));
    }

    private void setUpMockedResultSetToReturnData(final String ratStringValueForGSM,
            final String ratStringValueForWCDMA, final ResultSet mockedResultSet) throws SQLException {
        mockery.checking(new Expectations() {
            {
                one(mockedResultSet).next();
                will(returnValue(true));
                one(mockedResultSet).getString(RAT_COLUMN_NAME);
                will(returnValue(RAT_VALUE_FOR_GSM));
                one(mockedResultSet).getString(RAT_DESC_COLUMN_NAME);
                will(returnValue(ratStringValueForGSM));

                one(mockedResultSet).next();
                will(returnValue(true));
                one(mockedResultSet).getString(RAT_COLUMN_NAME);
                will(returnValue(RAT_VALUE_FOR_WCDMA));
                one(mockedResultSet).getString(RAT_DESC_COLUMN_NAME);
                will(returnValue(ratStringValueForWCDMA));

                //and ensure transformer doesn't loop over result set forever
                one(mockedResultSet).next();
                will(returnValue(false));
            }
        });

    }
}
