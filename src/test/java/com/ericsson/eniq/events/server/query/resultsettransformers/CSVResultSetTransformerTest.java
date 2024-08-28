/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query.resultsettransformers;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.common.ApplicationConstants;

/**
 * @author ericker
 * @since 2010
 *
 */
public class CSVResultSetTransformerTest extends BaseJMockUnitTest {

    @Test
    public void transformToCSV() throws SQLException {
        final List<Integer> timeColumnIndexes = null;
        final String tzOffset = "+0000";
        final String expected = "\"A\",\"B\",\"C\"\n";

        final ResultSetTransformer<String> csvTransformer = ResultSetTransformerFactory.getCSVStreamTransformer(
                timeColumnIndexes, tzOffset);

        final ResultSet mockedResultSet = mockery.mock(ResultSet.class);
        final ResultSetMetaData mockedResultSetMetaData = mockery.mock(ResultSetMetaData.class);

        mockery.checking(new Expectations() {
            {
                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                exactly(4).of(mockedResultSetMetaData).getColumnCount();
                will(returnValue(3));
                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                one(mockedResultSetMetaData).getColumnType(1);
                will(returnValue(12));
                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                one(mockedResultSetMetaData).getColumnType(2);
                will(returnValue(12));
                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                one(mockedResultSetMetaData).getColumnType(3);
                will(returnValue(12));
                one(mockedResultSet).getString(1);
                will(returnValue("A"));
                one(mockedResultSet).getString(2);
                will(returnValue("B"));
                one(mockedResultSet).getString(3);
                will(returnValue("C"));
            }
        });
        final String results = csvTransformer.transform(mockedResultSet);
        Assert.assertEquals(expected, results);
    }

    @Test
    public void transformToCSVForCauseCode() throws SQLException {
        final String timeColumn = null;
        final String tzOffset = "+0000";
        final String expected = "\"val11\",\"val12\",\"val13\",\"201\",\"val5\",\"val6\",\"val7\",\"this is your help\",\"val9\",\"val10\"\n";

        final ResultSetTransformer<String> csvTransformer = ResultSetTransformerFactory
                .getCSVStreamTransformerForCauseCode(timeColumn, tzOffset);

        final ResultSet mockedResultSet = setUpResultSetExpectationsForCauseCode();

        final String results = csvTransformer.transform(mockedResultSet);
        Assert.assertEquals(expected, results);
    }

    @Test
    public void transformToCSVForCauseCodeWithExceptions() throws SQLException {
        final String timeColumn = null;
        final String tzOffset = "+0000";
        final String expected = "\"val11\",\"val12\",\"val13\",\"201\",\"val5\",\"val6\",\"val7\",\"##203##201##205##[|this is your help|]##32##[|this is not your help|]\",\"val9\",\"val10\"\n";

        final ResultSetTransformer<String> csvTransformer = ResultSetTransformerFactory
                .getCSVStreamTransformerForCauseCode(timeColumn, tzOffset);

        final ResultSet mockedResultSet = setUpResultSetExpectationsForCauseCodeWithExceptions();

        final String results = csvTransformer.transform(mockedResultSet);
        Assert.assertEquals(expected, results);
    }

    private ResultSet setUpResultSetExpectationsForCauseCode() throws SQLException {
        final ResultSet mockedResultSet = mockery.mock(ResultSet.class);
        final ResultSetMetaData mockedResultSetMetaData = mockery.mock(ResultSetMetaData.class);
        mockery.checking(new Expectations() {
            {
                one(mockedResultSet).findColumn(ApplicationConstants.SCC_HELP_SQL_NAME);
                will(returnValue(8));
                one(mockedResultSet).findColumn(ApplicationConstants.CC_SQL_NAME);
                will(returnValue(4));

                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                one(mockedResultSetMetaData).getColumnCount();
                will(returnValue(10));

                one(mockedResultSet).getString(1);
                will(returnValue("val11"));
                one(mockedResultSet).getString(2);
                will(returnValue("val12"));
                one(mockedResultSet).getString(3);
                will(returnValue("val13"));
                exactly(2).of(mockedResultSet).getString(4);
                will(returnValue("201"));
                one(mockedResultSet).getString(5);
                will(returnValue("val5"));
                one(mockedResultSet).getString(6);
                will(returnValue("val6"));
                one(mockedResultSet).getString(7);
                will(returnValue("val7"));
                one(mockedResultSet).getString(8);
                will(returnValue("##203##201##205##[|this is your help|]##32##[|this is not your help|]"));
                one(mockedResultSet).getString(9);
                will(returnValue("val9"));
                one(mockedResultSet).getString(10);
                will(returnValue("val10"));

            }
        });
        return mockedResultSet;
    }

    private ResultSet setUpResultSetExpectationsForCauseCodeWithExceptions() throws SQLException {
        final ResultSet mockedResultSet = mockery.mock(ResultSet.class);
        final ResultSetMetaData mockedResultSetMetaData = mockery.mock(ResultSetMetaData.class);
        mockery.checking(new Expectations() {
            {
                one(mockedResultSet).findColumn(ApplicationConstants.SCC_HELP_SQL_NAME);
                will(throwException(new SQLException()));
                one(mockedResultSet).findColumn(ApplicationConstants.CC_SQL_NAME);
                will(throwException(new SQLException()));

                one(mockedResultSet).getMetaData();
                will(returnValue(mockedResultSetMetaData));
                one(mockedResultSetMetaData).getColumnCount();
                will(returnValue(10));

                one(mockedResultSet).getString(1);
                will(returnValue("val11"));
                one(mockedResultSet).getString(2);
                will(returnValue("val12"));
                one(mockedResultSet).getString(3);
                will(returnValue("val13"));
                exactly(1).of(mockedResultSet).getString(4);
                will(returnValue("201"));
                one(mockedResultSet).getString(5);
                will(returnValue("val5"));
                one(mockedResultSet).getString(6);
                will(returnValue("val6"));
                one(mockedResultSet).getString(7);
                will(returnValue("val7"));
                one(mockedResultSet).getString(8);
                will(returnValue("##203##201##205##[|this is your help|]##32##[|this is not your help|]"));
                one(mockedResultSet).getString(9);
                will(returnValue("val9"));
                one(mockedResultSet).getString(10);
                will(returnValue("val10"));

            }
        });
        return mockedResultSet;
    }
}
