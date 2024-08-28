/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks.timerangequeries.impl;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicy;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicyFactory;
import com.ericsson.eniq.events.server.query.DataServiceQueryExecutor;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.query.resultsettransformers.ResultSetTransformer;
import com.ericsson.eniq.events.server.templates.mappingengine.TemplateMappingEngine;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.QueryUtils;
import com.ericsson.eniq.events.server.utils.config.latency.TechPackTechnologies;

/**
 * @author eemecoy
 */
public class EventsTechPackTimerangeQuerierTest extends BaseJMockUnitTest {

    final String startDateTime = "2010-10-11 13:00";

    final String endDateTime = "2010-10-11 14:00";

    private EventsTechPackTimerangeQuerier timerangeQuerier;

    private TemplateUtils mockedTemplateUtils;

    private QueryUtils mockedQueryUtils;

    private TemplateMappingEngine mockedTemplateMappingEngine;

    private FormattedDateTimeRange mockedFormattedDateTimeRange;

    private DataServiceQueryExecutor mockedDataServiceQueryExecutor;

    private LoadBalancingPolicyFactory mockedLoadBalancingPolicyFactory;

    private TechPackTechnologies mockedTechPackTechnologies;

    @Before
    public void setup() {
        timerangeQuerier = new EventsTechPackTimerangeQuerier();
        mockedTemplateUtils = mockery.mock(TemplateUtils.class);
        timerangeQuerier.setTemplateUtils(mockedTemplateUtils);
        mockedQueryUtils = mockery.mock(QueryUtils.class);
        timerangeQuerier.setQueryUtils(mockedQueryUtils);
        mockedTemplateMappingEngine = mockery.mock(TemplateMappingEngine.class);
        timerangeQuerier.setTemplateMappingEngine(mockedTemplateMappingEngine);
        mockedFormattedDateTimeRange = mockery.mock(FormattedDateTimeRange.class);
        mockedDataServiceQueryExecutor = mockery.mock(DataServiceQueryExecutor.class);
        timerangeQuerier.setDataServiceQueryExecutor(mockedDataServiceQueryExecutor);
        mockedLoadBalancingPolicyFactory = mockery.mock(LoadBalancingPolicyFactory.class);
        timerangeQuerier.setLoadBalancingPolicyFactory(mockedLoadBalancingPolicyFactory);
        mockedTechPackTechnologies = mockery.mock(TechPackTechnologies.class);
        timerangeQuerier.setTechPackTechnologies(mockedTechPackTechnologies);
    }

    @Test
    public void testGetLatestTables_CFA_RAW() {
        final String view = EVENT_E_RAN_CFA_RAW;
        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TIMERANGE_VIEW, view + "_TIMERANGE");
        parameters.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);
        expectCallsOnMockedObjectsForLatestTablesVolumeBasedTechPacks(view, EventsTechPackTimerangeQuerier.GET_LATEST_RAW_TABLES, parameters, result,
                true);
        assertThat(timerangeQuerier.getLatestTablesUsingQuery(view), is(result));
    }

    @Test
    public void testGetLatestTables_SGEH_ERR_RAW() {
        final String view = EVENT_E_SGEH_ERR_RAW;
        final List<String> expected = new ArrayList<String>();
        expected.add("EVENT_E_SGEH_ERR_RAW_01");

        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TIMERANGE_VIEW, view + "_TIMERANGE");
        parameters.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);
        expectCallsOnMockedObjectsForLatestTablesVolumeBasedTechPacks(view, EventsTechPackTimerangeQuerier.GET_LATEST_RAW_TABLES, parameters, result,
                true);
        assertThat(timerangeQuerier.getLatestTablesUsingQuery(view), is(expected));
    }

    @Test
    public void testGetLatestTables_SGEH_RAW() {
        final String view = EVENT_E_SGEH_RAW;
        final List<String> expected = new ArrayList<String>();
        expected.add("EVENT_E_SGEH_RAW_01");

        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TIMERANGE_VIEW, view + "_TIMERANGE");
        parameters.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, true);
        expectCallsOnMockedObjectsForLatestTablesVolumeBasedTechPacks(view, EventsTechPackTimerangeQuerier.GET_LATEST_RAW_TABLES, parameters, result,
                true);
        assertThat(timerangeQuerier.getLatestTablesUsingQuery(view), is(expected));
    }

    @Test
    public void testGetRAWTablesUsingQuery() {
        final String view = "EVENT_E_RAN_ERR";
        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TIMERANGE_VIEW, view + "_TIMERANGE");
        expectCallsOnMockedObjectsForRawTablesVolumeBasedTechPacks(view, EventsTechPackTimerangeQuerier.GET_RAW_TABLES, parameters, result, true);
        assertThat(timerangeQuerier.getRAWTablesUsingQuery(mockedFormattedDateTimeRange, view), is(result));
    }

    @Test
    public void testGetLatestTablesUsingQuery_DC_Z_ALARM_INFO() {
        final String rawTable = "DIM_Z_ALARM_INFO";
        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TYPE, rawTable);
        expectCallsOnMockedObjectsForLatestTablesTimeBasedTechPacks(rawTable, EventsTechPackTimerangeQuerier.GET_LATEST_RAW_TABLES_NO_TIMERANGE,
                parameters, result, false);
        assertThat(timerangeQuerier.getLatestTablesUsingQuery(rawTable), is(result));
    }

    @Test
    public void testGetRAWTablesUsingQuery_DC_Z_ALARM_INFO() {
        final String rawTable = "DIM_Z_ALARM_INFO";
        final List<String> result = new ArrayList<String>();
        final Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(RAW_TYPE, rawTable);
        expectCallsOnMockedObjectsForRawTablesTimeBasedTechPacks(rawTable, EventsTechPackTimerangeQuerier.GET_RAW_TABLES_NO_TIMERANGE, parameters,
                result, false);
        assertThat(timerangeQuerier.getRAWTablesUsingQuery(mockedFormattedDateTimeRange, rawTable), is(result));
    }

    @SuppressWarnings("unchecked")
    private void expectCallsOnMockedObjectsForLatestTablesVolumeBasedTechPacks(final String view, final String pathName,
                                                                               final Map<String, Object> parameters, final List<String> result,
                                                                               final boolean isTimeRange) {
        final String templateFile = " the template file";
        final LoadBalancingPolicy loadBalancingPolicy = mockery.mock(LoadBalancingPolicy.class);
        final String query = "an sql query";
        mockery.checking(new Expectations() {
            {
                one(mockedTechPackTechnologies).usesVolumeBasedRawPartitions(view);
                will(returnValue(isTimeRange));
                one(mockedTemplateMappingEngine).getTemplate(with(same(pathName)), with(any(MultivaluedMap.class)), with(same((String) null)));
                will(returnValue(templateFile));
                one(mockedTemplateUtils).getQueryFromTemplate(templateFile, parameters);
                will(returnValue(query));
                one(mockedLoadBalancingPolicyFactory).getDefaultLoadBalancingPolicy();
                will(returnValue(loadBalancingPolicy));
                one(mockedDataServiceQueryExecutor).getData(with(same(CANCEL_REQ_NOT_SUPPORTED)), with(same(query)), with(any(Map.class)),
                        with(any(ResultSetTransformer.class)), with(same(loadBalancingPolicy)));
                will(returnValue(result));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void expectCallsOnMockedObjectsForRawTablesVolumeBasedTechPacks(final String view, final String pathName,
                                                                            final Map<String, Object> parameters, final List<String> result,
                                                                            final boolean isTimeRange) {
        final String templateFile = " the template file";
        final LoadBalancingPolicy loadBalancingPolicy = mockery.mock(LoadBalancingPolicy.class);
        final String query = "an sql query";
        mockery.checking(new Expectations() {
            {
                one(mockedTechPackTechnologies).usesVolumeBasedRawPartitions(view);
                will(returnValue(isTimeRange));
                one(mockedTemplateMappingEngine).getTemplate(with(same(pathName)), with(any(MultivaluedMap.class)), with(any(String.class)));
                will(returnValue(templateFile));
                one(mockedTemplateUtils).getQueryFromTemplate(templateFile, parameters);
                will(returnValue(query));
                allowing(mockedQueryUtils).mapDateParameters(mockedFormattedDateTimeRange);
                will(returnValue(new HashMap<String, QueryParameter>()));
                one(mockedLoadBalancingPolicyFactory).getDefaultLoadBalancingPolicy();
                will(returnValue(loadBalancingPolicy));
                one(mockedDataServiceQueryExecutor).getData(with(same(CANCEL_REQ_NOT_SUPPORTED)), with(same(query)), with(any(Map.class)),
                        with(any(ResultSetTransformer.class)), with(same(loadBalancingPolicy)));
                will(returnValue(result));
                allowing(mockedFormattedDateTimeRange).getRangeInMinutes();
                will(returnValue((long)60));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void expectCallsOnMockedObjectsForLatestTablesTimeBasedTechPacks(final String view, final String pathName,
                                                                             final Map<String, Object> parameters, final List<String> result,
                                                                             final boolean isTimeRange) {
        final String templateFile = " the template file";
        final String query = "an sql query";
        mockery.checking(new Expectations() {
            {
                one(mockedTechPackTechnologies).usesVolumeBasedRawPartitions(view);
                will(returnValue(isTimeRange));
                one(mockedTemplateMappingEngine).getTemplate(with(same(pathName)), with(any(MultivaluedMap.class)), with(same((String) null)));
                will(returnValue(templateFile));
                one(mockedTemplateUtils).getQueryFromTemplate(templateFile, parameters);
                will(returnValue(query));
                one(mockedDataServiceQueryExecutor).getDataFromRepdb(with(same(CANCEL_REQ_NOT_SUPPORTED)), with(same(query)), with(any(Map.class)),
                        with(any(ResultSetTransformer.class)));
                will(returnValue(result));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void expectCallsOnMockedObjectsForRawTablesTimeBasedTechPacks(final String view, final String pathName,
                                                                          final Map<String, Object> parameters, final List<String> result,
                                                                          final boolean isTimeRange) {
        final String templateFile = " the template file";
        final String query = "an sql query";
        mockery.checking(new Expectations() {
            {
                one(mockedTechPackTechnologies).usesVolumeBasedRawPartitions(view);
                will(returnValue(isTimeRange));
                one(mockedTemplateMappingEngine).getTemplate(with(same(pathName)), with(any(MultivaluedMap.class)), with(any(String.class)));
                will(returnValue(templateFile));
                one(mockedTemplateUtils).getQueryFromTemplate(templateFile, parameters);
                will(returnValue(query));
                allowing(mockedQueryUtils).mapDateParameters(mockedFormattedDateTimeRange);
                will(returnValue(new HashMap<String, QueryParameter>()));
                one(mockedDataServiceQueryExecutor).getDataFromRepdb(with(same(CANCEL_REQ_NOT_SUPPORTED)), with(same(query)), with(any(Map.class)),
                        with(any(ResultSetTransformer.class)));
                will(returnValue(result));
                allowing(mockedFormattedDateTimeRange).getRangeInMinutes();
                will(returnValue((long)60));
            }
        });
    }
}
