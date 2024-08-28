/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Expectations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.kpi.KPI;
import com.ericsson.eniq.events.server.templates.mappingengine.TemplateMappingEngine;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeHelper;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eriwals
 * @since 2011
 */
public class QueryGeneratorTest extends BaseJMockUnitTest {

    private QueryGenerator queryGenerator;

    private DateTimeHelper dateTimeHelper;

    private ApplicationConfigManager mockApplicationConfigManager;

    TemplateMappingEngine templateMappingEngine;

    private TemplateUtils templateUtils;

    private TimeRangeSelector timeRangeSelector;

    private static final String DRILLTYPE = null;

    @Before
    public void setUp() {
        mockApplicationConfigManager = createAndIgnore(ApplicationConfigManager.class);
        dateTimeHelper = new DateTimeHelper();
        dateTimeHelper.setApplicationConfigManager(mockApplicationConfigManager);
        templateMappingEngine = mockery.mock(TemplateMappingEngine.class);
        templateUtils = new TemplateUtils();
        timeRangeSelector = new TimeRangeSelector();
        try {
            templateUtils.applicationStartup();
        } catch (final Exception e) {
            e.printStackTrace();
        }
        queryGenerator = new QueryGenerator();
        queryGenerator.setDateTimeHelper(dateTimeHelper);
        queryGenerator.setTemplateMappingEngine(templateMappingEngine);
        queryGenerator.setTemplateUtils(templateUtils);
        queryGenerator.setTimeRangeSelector(timeRangeSelector);
    }

    @After
    public void tearDown() {
        templateUtils.applicationShutdown();
    }

    @Test
    public void testGetQueryForValidPath() {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();

        requestParameters.add(ApplicationConstants.TIME_QUERY_PARAM, "10080");
        requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
        requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.APN);
        requestParameters.add(ApplicationConstants.KEY_PARAM, ApplicationConstants.KEY_TYPE_ERR);

        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();

        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();

        aggregationViews.put(ApplicationConstants.APN, new AggregationTableInfo("table",
                EventDataSourceType.AGGREGATED_15MIN));

        final FormattedDateTimeRange dateTimeRange = dateTimeHelper
                .translateDateTimeParameters(requestParameters, null);

        assertNotNull(dateTimeRange);
        expectCallOnTemplateMappingEngine(ApplicationConstants.EVENT_ANALYSIS, requestParameters, null, DAY_VIEW);

        final String query = queryGenerator.getQuery(new QueryGeneratorParameters(ApplicationConstants.EVENT_ANALYSIS,
                requestParameters, serviceSpecificTemplateParameters, dateTimeRange, DRILLTYPE, 500, null,
                new ArrayList<KPI>(), false, false));

        assertNotNull(query);

    }

    private void expectCallOnTemplateMappingEngine(final String templatePath,
            final MultivaluedMap<String, String> requestParameters, final String drillType, final String timeRange) {
        mockery.checking(new Expectations() {
            {
                one(templateMappingEngine).getTemplate(templatePath, requestParameters, drillType, timeRange);
                will(returnValue("common/q_event_analysis_details.vm"));
            }
        });

    }
}
