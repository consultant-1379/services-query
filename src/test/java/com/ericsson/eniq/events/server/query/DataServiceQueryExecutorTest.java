/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import com.ericsson.eniq.events.server.datasource.DBConnectionManager;
import com.ericsson.eniq.events.server.datasource.DataSourceConfigurationException;
import com.ericsson.eniq.events.server.query.resultsettransformers.ResultSetTransformer;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author EEMECOY
 */

public class DataServiceQueryExecutorTest extends BaseJMockUnitTest {

    private static final String SAMPLE_REQUEST_ID = "sampleRequestID";

    RequestIdMappingService requestIdMappingService;

    DBConnectionManager mockedDbConnectionManager;

    private DataServiceQueryExecutor objToTest;

    @Before
    public void setup() {
        objToTest = new DataServiceQueryExecutor();
        requestIdMappingService = mockery.mock(RequestIdMappingService.class);
        this.objToTest.setRequestIdMappings(requestIdMappingService);
        mockedDbConnectionManager = mockery.mock(DBConnectionManager.class);
        objToTest.setDbConnectionManager(mockedDbConnectionManager);
    }

    public void setRequestIdMappingService(final RequestIdMappingService requestIdMappingService) {
        this.requestIdMappingService = requestIdMappingService;
    }

    @Test
    public void testGetDataWithOneQuery() throws Exception {
        final String query = "some sql query";
        final ResultSet mockedResultSet = mockery.mock(ResultSet.class);
        setUpDatabaseExpectations(mockedResultSet, query);
        setupExpectationsOnRequestIdMappingService(1);
        final ResultSetTransformer<String> transformer = mockery.mock(ResultSetTransformer.class);
        final String expectedJSONResult = expect1Transform(transformer, mockedResultSet);
        final String result = objToTest.getData(SAMPLE_REQUEST_ID, query, null, transformer, null);
        assertThat(result, is(expectedJSONResult));
    }

    @Test
    public void testGetDataWithMultipleQueries() throws Exception {
        final String query1 = "1st sql query";
        final String query2 = "2nd sql query";
        final List<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        final List<ResultSet> mockedResultSets = setUpDatabaseExpectationsForMultipleQueries(queries);
        setupExpectationsOnRequestIdMappingService(2);
        final ResultSetTransformer<String> transformer = mockery.mock(ResultSetTransformer.class);
        final String expectedJSONResult = expectMultipleTransforms(transformer, mockedResultSets);
        final String result = objToTest.getDataForMultipleQueries(SAMPLE_REQUEST_ID, queries, null, transformer, null);
        assertThat(result, is(expectedJSONResult));
    }

    private List<ResultSet> setUpDatabaseExpectationsForMultipleQueries(final List<String> queries)
            throws SQLException, DataSourceConfigurationException {
        final List<ResultSet> mockedResultSets = new ArrayList<ResultSet>();
        for (int i = 0; i < queries.size(); i++) {

            final String query = queries.get(i);
            final Connection mockedConnection = mockery.mock(Connection.class, "connectionForQuery" + query);
            final PreparedStatement mockedStatement = mockery
                    .mock(PreparedStatement.class, "statementForQuery" + query);
            final ResultSet mockedResultSet = mockery.mock(ResultSet.class, "resultset for query" + query);

            mockedResultSets.add(mockedResultSet);

            mockery.checking(new Expectations() {
                {
                    one(mockedDbConnectionManager).getConnection(null);
                    will(returnValue(mockedConnection));
                    one(mockedConnection).prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    will(returnValue(mockedStatement));
                    one(mockedStatement).executeQuery();
                    will(returnValue(mockedResultSet));

                    one(mockedResultSet).close();
                    one(mockedStatement).close();
                    one(mockedConnection).close();

                }
            });
        }

        return mockedResultSets;

    }

    private String expectMultipleTransforms(final ResultSetTransformer<String> transformer,
                                            final List<ResultSet> listOfResultSets) throws SQLException {
        final String resultOfJsonTransform = "some json";
        mockery.checking(new Expectations() {
            {
                one(transformer).transform(listOfResultSets);
                will(returnValue(resultOfJsonTransform));
            }
        });
        return resultOfJsonTransform;
    }

    private String expect1Transform(final ResultSetTransformer<String> transformer, final ResultSet mockedResultSet)
            throws SQLException {
        final String resultOfJsonTransform = "some json";
        mockery.checking(new Expectations() {
            {
                one(transformer).transform(mockedResultSet);
                will(returnValue(resultOfJsonTransform));
            }
        });
        return resultOfJsonTransform;

    }

    private void setupExpectationsOnRequestIdMappingService(final int numberExpectedInvocations) {
        mockery.checking(new Expectations() {
            {
                exactly(numberExpectedInvocations).of(requestIdMappingService)
                        .isCancelFailedForReqId(SAMPLE_REQUEST_ID);
                exactly(numberExpectedInvocations).of(requestIdMappingService).put(with(equal(SAMPLE_REQUEST_ID)),
                        with(any(NamedParameterStatement.class)));
                exactly(numberExpectedInvocations).of(requestIdMappingService).containsKey(SAMPLE_REQUEST_ID);
                will(returnValue(true));
                allowing(requestIdMappingService).remove(SAMPLE_REQUEST_ID);
                allowing(requestIdMappingService).removeFailedCancelReqId(SAMPLE_REQUEST_ID);
            }
        });

    }

    private ResultSet setUpDatabaseExpectations(final ResultSet mockedResultSet, final String sqlQuery)
            throws SQLException, DataSourceConfigurationException {
        final Connection mockedConnection = mockery.mock(Connection.class, "connectionForQuery" + sqlQuery);
        final PreparedStatement mockedStatement = mockery.mock(PreparedStatement.class, "statementForQuery" + sqlQuery);
        mockery.checking(new Expectations() {
            {
                one(mockedDbConnectionManager).getConnection(null);
                will(returnValue(mockedConnection));
                one(mockedConnection).prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                will(returnValue(mockedStatement));
                one(mockedStatement).executeQuery();
                will(returnValue(mockedResultSet));
                one(mockedStatement).close();
                one(mockedConnection).close();
                one(mockedStatement).getMoreResults();
                will(returnValue(false));
                one(mockedResultSet).close();

            }
        });

        return mockedResultSet;

    }
}
