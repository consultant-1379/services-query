/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.query;

import static junit.framework.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.kpi.KPI;
import com.ericsson.eniq.events.server.kpi.KPIQueryfactory;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeHelper;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ericker
 */
public class KPIQueryGeneratorTest extends BaseJMockUnitTest {

    private KPIQueryGenerator kpiQueryGenerator;

    private KPIQueryfactory kpiQueryfactory;

    private DateTimeHelper dateTimeHelper;

    private TechPackList techPackList;

    private KPI mockedKpi;

    private ApplicationConfigManager applicationConfigManagerMock;

    List<KPI> kpiList;

    @Before
    public void setUp() {
        kpiQueryfactory = mockery.mock(KPIQueryfactory.class);
        techPackList = mockery.mock(TechPackList.class);
        mockedKpi = mockery.mock(KPI.class);
        applicationConfigManagerMock = createAndIgnore(ApplicationConfigManager.class);

        kpiQueryGenerator = new KPIQueryGenerator();
        kpiQueryGenerator.setKpiQueryfactory(kpiQueryfactory);
        dateTimeHelper = new DateTimeHelper();
        dateTimeHelper.setApplicationConfigManager(applicationConfigManagerMock);
        kpiList = new ArrayList<KPI>();
    }

    @Test
    public void testGetSGEHQueryAPN() throws Exception {
        final QueryGeneratorParameters queryGeneratorParameters = getQueryGeneratorParameters(getRequestParametersAPN());

        setUpExpectations();
        kpiQueryGenerator.getQuery(queryGeneratorParameters);
    }

    @Test
    public void testGetParametersAPN() throws Exception {
        final QueryGeneratorParameters queryGeneratorParameters = getQueryGeneratorParameters(getRequestParametersAPN());
        final String timerange = EventDataSourceType.AGGREGATED_DAY.getValue();
        final Map<String, Object> templateParameters = kpiQueryGenerator.getTemplateParameters(
                queryGeneratorParameters, timerange);
        final Map<String, Object> expectedParameters = getExpectedAPNTemplateParams();
        for (final String s : templateParameters.keySet()) {
            assertEquals(expectedParameters.get(s), templateParameters.get(s));
        }
    }

    @Test
    public void testGetParametersSGSN() throws Exception {
        final QueryGeneratorParameters queryGeneratorParameters = getQueryGeneratorParameters(getRequestParametersSGSN());
        final String timerange = EventDataSourceType.AGGREGATED_DAY.getValue();
        final Map<String, Object> templateParameters = kpiQueryGenerator.getTemplateParameters(
                queryGeneratorParameters, timerange);
        final Map<String, Object> expectedParameters = getExpectedSGSNTemplateParams();
        for (final String s : templateParameters.keySet()) {
            assertEquals(expectedParameters.get(s), templateParameters.get(s));
        }
    }

    private QueryGeneratorParameters getQueryGeneratorParameters(final MultivaluedMap<String, String> requestParameters) {
        final String templatePath = "";
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        final List<String> techPacks = getSGEHTechPacks();
        final FormattedDateTimeRange dateTimeRange = dateTimeHelper.translateDateTimeParameters(requestParameters,
                techPacks);
        final String drillDownTypeForService = "";
        final int maxResultSetSize = 500;
        kpiList.add(mockedKpi);

        return new QueryGeneratorParameters(templatePath, requestParameters, serviceSpecificTemplateParameters,
                dateTimeRange, drillDownTypeForService, maxResultSetSize, techPackList, kpiList, false, false);
    }

    private List<String> getSGEHTechPacks() {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(ApplicationConstants.EVENT_E_SGEH_TPNAME);
        return techPacks;
    }

    private MultivaluedMap<String, String> getRequestParametersAPN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(ApplicationConstants.DATE_FROM, "01112011");
        requestParameters.add(ApplicationConstants.DATE_TO, "02112011");
        requestParameters.add(ApplicationConstants.TIME_FROM_QUERY_PARAM, "0200");
        requestParameters.add(ApplicationConstants.TIME_TO_QUERY_PARAM, "0200");
        requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
        requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.APN);
        requestParameters.add(ApplicationConstants.NODE_PARAM, "SampleAPN");
        requestParameters.add(ApplicationConstants.MAX_ROWS, "500");
        return requestParameters;
    }

    private MultivaluedMap<String, String> getRequestParametersSGSN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(ApplicationConstants.DATE_FROM, "01112011");
        requestParameters.add(ApplicationConstants.DATE_TO, "02112011");
        requestParameters.add(ApplicationConstants.TIME_FROM_QUERY_PARAM, "0200");
        requestParameters.add(ApplicationConstants.TIME_TO_QUERY_PARAM, "0200");
        requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
        requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.TYPE_SGSN);
        requestParameters.add(ApplicationConstants.NODE_PARAM, "SGSN1");
        requestParameters.add(ApplicationConstants.MAX_ROWS, "500");
        return requestParameters;
    }

    private void setUpExpectations() {

        mockery.checking(new Expectations() {
            {
                one(kpiQueryfactory).getSGEHKPIQuery(with(any(Map.class)), with(kpiList), with(techPackList));
            }
        });
    }

    private Map<String, Object> getExpectedAPNTemplateParams() {
        final Map<String, Object> templateParams = new HashMap<String, Object>();
        templateParams.put("count", 500);
        templateParams.put("timerange", "TR_4");
        templateParams.put("type", "APN");
        return templateParams;
    }

    private Map<String, Object> getExpectedSGSNTemplateParams() {
        final Map<String, Object> templateParams = new HashMap<String, Object>();
        templateParams.put("count", 500);
        templateParams.put("timerange", "TR_4");
        templateParams.put("type", "SGSN");
        return templateParams;
    }
}
