/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * @author eemecoy
 *
 */
public class SQLQueryLoggerTest {

    private static final String STRING_PARAMETER_NAME = "city";

    private static final String STRING_PARAMETER_VALUE = "London";

    private static final String SINGLE_QUOTE = "'";

    private static final String INTEGER_PARAMETER_NAME = "number_of_councils";

    private static final int INTEGER_PARAMETER_VALUE = 2;

    private static final long LONG_PARAMETER_VALUE = 343434L;

    private static final String LONG_PARAMETER_NAME = "population";

    private SQLQueryLogger sqlQueryLogger;

    @Before
    public void setup() {
        sqlQueryLogger = new SQLQueryLogger();
    }

    @Test
    public void testLoggingQueryHandlesSingleCharacterParameterName() {
        final String staticQuery = "select from some table where something = ";
        final String singleCharacterParameterName = "x";
        final String query = staticQuery + COLON + singleCharacterParameterName;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(singleCharacterParameterName, QueryParameter.createIntParameter(INTEGER_PARAMETER_VALUE));
        final String tracedQuery = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedTrace = staticQuery + INTEGER_PARAMETER_VALUE;
        assertThat(tracedQuery, is(expectedTrace));
    }

    @Test
    public void testLoggingQueryHandlesParameterAtStartOfQuery() {
        final String staticQuery = " from some table";
        final String query = COLON + INTEGER_PARAMETER_NAME + staticQuery;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(INTEGER_PARAMETER_NAME, QueryParameter.createIntParameter(INTEGER_PARAMETER_VALUE));
        final String tracedQuery = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedTrace = INTEGER_PARAMETER_VALUE + staticQuery;
        assertThat(tracedQuery, is(expectedTrace));
    }

    @Test
    public void testLoggingQueryIgnoresSemiColonsThatAreGenuineSQLSyntax() {
        final String query = "some query with some SQL string conversion || ':' ||";
        final String tracedQuery = sqlQueryLogger.injectQueryParameters(query, null);
        assertThat(tracedQuery, is(query));
    }

    @Test
    public void testLoggingQueryWithInjectedLongQueryParameter() {
        final String staticQuery = "select  * from table where population = ";
        final String query = staticQuery + COLON + LONG_PARAMETER_NAME;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(LONG_PARAMETER_NAME, QueryParameter.createLongParameter(LONG_PARAMETER_VALUE));
        final String result = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedResult = staticQuery + LONG_PARAMETER_VALUE;
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testLoggingQueryWithInjectedNullQueryParameter() {
        final String staticQuery = "select  * from table where some null value = ";
        final String query = staticQuery + COLON + INTEGER_PARAMETER_NAME;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(INTEGER_PARAMETER_NAME, QueryParameter.createNullParameter(INTEGER_PARAMETER_VALUE));
        final String result = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedResult = staticQuery + INTEGER_PARAMETER_VALUE;
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testLoggingQueryWithInjectedIntegerQueryParameter() {
        final String staticQuery = "select  * from table where number_of_councils = ";
        final String query = staticQuery + COLON + INTEGER_PARAMETER_NAME;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(INTEGER_PARAMETER_NAME, QueryParameter.createIntParameter(INTEGER_PARAMETER_VALUE));
        final String result = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedResult = staticQuery + INTEGER_PARAMETER_VALUE;
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testLoggingQueryWithInjectedStringQueryParameter() {
        final String staticQuery = "select  * from table where city = ";
        final String query = staticQuery + COLON + STRING_PARAMETER_NAME;
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(STRING_PARAMETER_NAME, QueryParameter.createStringParameter(STRING_PARAMETER_VALUE));
        final String result = sqlQueryLogger.injectQueryParameters(query, queryParameters);
        final String expectedResult = staticQuery + SINGLE_QUOTE + STRING_PARAMETER_VALUE + SINGLE_QUOTE;
        assertThat(result, is(expectedResult));
    }

}
