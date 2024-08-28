/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.utils.techpacks;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.*;
import com.ericsson.eniq.events.server.utils.techpacks.timerangequeries.TimerangeQuerier;
import com.ericsson.eniq.events.server.utils.techpacks.timerangequeries.TimerangeQuerierFactory;

public class RawTableFetcherTest extends BaseJMockUnitTest {

    private static final String EVENT_E_DVTP_DT_RAW = "EVENT_E_DVTP_DT_RAW";

    private static final String EVENT_E_SGEH_ERR_RAW_01 = "EVENT_E_SGEH_ERR_RAW_01";

    private final String startDateTime = "2010-10-11 13:00";

    private final String endDateTime = "2010-10-11 14:00";

    private RawTableFetcher objToTest;

    private RMIEngineUtils mockedRmiEngineUtils;

    private TimerangeQuerierFactory timerangeQuerierFactory;

    private FormattedDateTimeRange mockedFormattedDateTimeRange;

    @Before
    public void setup() {
        objToTest = new RawTableFetcher();
        mockedRmiEngineUtils = mockery.mock(RMIEngineUtils.class);
        objToTest.setRmiEngineUtils(mockedRmiEngineUtils);
        timerangeQuerierFactory = mockery.mock(TimerangeQuerierFactory.class);
        objToTest.setTimerangeQuerierFactory(timerangeQuerierFactory);
        mockedFormattedDateTimeRange = mockery.mock(FormattedDateTimeRange.class);
        allowGetStartAndEndDateTimeOnDateTimeRange();
    }

    @Test
    public void testGetRawSucTables() throws ParseException {
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("EVENT_E_SGEH_SUC_RAW_01");
        expectCallOnEngineGetTableNames(new String[] { EVENT_E_SGEH_SUC_RAW }, listOfTables);
        final List<String> result = objToTest.getRawSucTables(mockedFormattedDateTimeRange, EVENT_E_SGEH);
        assertThat(result, is(listOfTables));
    }

    @Test
    public void testGetRawErrTables() throws ParseException {
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("EVENT_E_LTE_ERR_RAW_01");
        expectCallOnEngineGetTableNames(new String[] { EVENT_E_LTE_ERR_RAW }, listOfTables);
        final List<String> result = objToTest.getRawErrTables(mockedFormattedDateTimeRange, EVENT_E_LTE);
        assertThat(result, is(listOfTables));
    }

    @Test
    public void testFetchRawTablesForTechPackAndKey() throws ParseException {
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("EVENT_E_GSM_CFA_ERR_RAW_01");
        expectCallOnEngineGetTableNames(new String[] { EVENT_E_GSM_CFA_ERR_RAW }, listOfTables);
        final List<String> result = objToTest.fetchRawTablesForTechPackAndKey(mockedFormattedDateTimeRange, EVENT_E_GSM_CFA, ERR);
        assertThat(result, is(listOfTables));
    }

    @Test
    public void testFetchRawTablesForTechPackAndKeyCFA() throws ParseException {
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("EVENT_E_RAN_CFA_ERR_RAW_01");
        expectCallOnEngineGetTableNames(new String[] { EVENT_E_RAN_CFA_RAW }, listOfTables);
        final List<String> result = objToTest.fetchRawTablesForTechPackAndKey(mockedFormattedDateTimeRange, EVENT_E_RAN_CFA, ERR);
        assertThat(result, is(listOfTables));
    }

    @Test
    public void testFetchRawTablesForTechPackForDC_Z_ALARM_INFO() throws ParseException {
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("DC_Z_ALARM_INFO_RAW_01");
        expectCallOnEngineGetTableNames(new String[] { DC_Z_ALARM_INFO_RAW }, listOfTables);
        final List<String> result = objToTest.fetchRawTablesForTechPackAndKey(mockedFormattedDateTimeRange, DC_Z_ALARM, INFO);
        assertThat(result, is(listOfTables));
    }

    @Test
    public void testgetViewNamesForTemplateKey() {

        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_DT, RAW_DT_TABLES), is(new String[] { EVENT_E_DVTP_DT_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_ERR, RAW_NON_LTE_TABLES), is(new String[] { EVENT_E_SGEH_ERR_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_SUM, RAW_TABLES), is(new String[] { EVENT_E_SGEH_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_TOTAL, RAW_TABLES), is(new String[] { EVENT_E_SGEH_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_ERR, RAW_ALL_ERR_TABLES), is(new String[] { EVENT_E_SGEH_ERR_RAW,
                EVENT_E_LTE_ERR_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_SUC, RAW_ALL_SUC_TABLES), is(new String[] { EVENT_E_SGEH_SUC_RAW,
                EVENT_E_LTE_SUC_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_ERR, RAW_LTE_TABLES), is(new String[] { EVENT_E_LTE_ERR_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_ERR, RAW_ERR_TABLES), is(new String[] { EVENT_E_SGEH_ERR_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_SUC, RAW_ALL_SUC_TABLES), is(new String[] { EVENT_E_SGEH_SUC_RAW,
                EVENT_E_LTE_SUC_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_ERR, RAW_LTE_ERR_TABLES), is(new String[] { EVENT_E_LTE_ERR_RAW }));
        assertThat(objToTest.getViewNamesForTemplateKey(KEY_TYPE_SUC, RAW_LTE_SUC_TABLES), is(new String[] { EVENT_E_LTE_SUC_RAW }));
    }

    @Test
    public void testgetLatestTablesFromEngineSGEH_WithSGEHSpecificKey() {
        final List<String> sgehRawTables = new ArrayList<String>();
        sgehRawTables.add(EVENT_E_SGEH_ERR_RAW_01);
        final String viewName = "EVENT_E_SGEH_ERR_RAW";
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewName);
                will(returnValue(sgehRawTables));
            }
        });
        final List<String> result = objToTest.getLatestTablesFromEngine(viewName);
        assertThat(result, is(sgehRawTables));
    }

    @Test
    public void testgetLatestTablesFromEngineSGEH() {
        final List<String> sgehRawTables = new ArrayList<String>();
        sgehRawTables.add(EVENT_E_SGEH_ERR_RAW_01);
        final String viewName = "EVENT_E_SGEH_ERR_RAW";
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewName);
                will(returnValue(sgehRawTables));
            }
        });
        final List<String> result = objToTest.getLatestTablesFromEngine(viewName);
        assertThat(result, is(sgehRawTables));
    }

    @Test
    public void testGetRAWTablesForLTEAndSGEHTechPacksLTEAndNONLTE() throws ParseException {
        final String key = KEY_TYPE_ERR;
        final List<String> listOfTables = new ArrayList<String>();
        listOfTables.add("rawLteTable1");
        listOfTables.add("rawTable2");
        final Map<String, String> parameterMapForOrdinaryTables = new HashMap<String, String>();
        parameterMapForOrdinaryTables.put(KEY_PARAM, KEY_TYPE_ERR);

        final Map<String, String> parameterMapForLTETables = new HashMap<String, String>();
        parameterMapForLTETables.put(KEY_PARAM, KEY_TYPE_ERR);
        parameterMapForLTETables.put(IS_LTE_VIEW, "true");

        final String[] viewNames = new String[] { "EVENT_E_SGEH_ERR_RAW", "EVENT_E_LTE_ERR_RAW" };
        expectCallOnEngineGetTableNames(viewNames, listOfTables);
        final FormattedDateTimeRange dateTimeRange = mockedFormattedDateTimeRange;
        final List<String> result = objToTest.getRAWTables(dateTimeRange, key, RAW_ALL_ERR_TABLES);
        assertThat(result, is(listOfTables));
    }

    private void expectCallOnEngineGetTableNames(final String[] viewNames, final List<String> listOfTables) throws ParseException {
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getTableNames(new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(startDateTime).getTime()),
                        new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(endDateTime).getTime()), viewNames);
                will(returnValue(listOfTables));
            }
        });
    }

    @Test
    public void testgetRAWTablesForLTEAndNONLTEQueriesDatabase() throws Exception {
        final List<String> listOfSgehTables = new ArrayList<String>();
        listOfSgehTables.add("rawSgehTable1");
        listOfSgehTables.add("rawSgehTable2");
        final List<String> listOfLteTables = new ArrayList<String>();
        listOfLteTables.add("rawLteTable1");
        listOfLteTables.add("rawLteTable2");

        final Map<String, String> parameterMapForSgehQuery = new HashMap<String, String>();
        parameterMapForSgehQuery.put(RAW_TIMERANGE_VIEW, EVENT_E_SGEH_ERR_RAW_TIMERANGE);
        final Map<String, String> parameterMapForLteQuery = new HashMap<String, String>();
        parameterMapForLteQuery.put(RAW_TIMERANGE_VIEW, EVENT_E_LTE_ERR_RAW_TIMERANGE);

        final String sgehView = "EVENT_E_SGEH_ERR_RAW";
        final String lteView = "EVENT_E_LTE_ERR_RAW";
        final String[] viewNames = new String[] { sgehView, lteView };

        final List<String> emptyListOfTables = new ArrayList<String>();
        expectCallOnEngineGetTableNames(viewNames, emptyListOfTables);

        final TimerangeQuerier timerangeQuerier = mockery.mock(TimerangeQuerier.class);
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewNames);
                will(returnValue(new ArrayList<String>()));
                one(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));
                one(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));
                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, sgehView);
                will(returnValue(listOfSgehTables));
                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, lteView);
                will(returnValue(listOfLteTables));
            }
        });
        final FormattedDateTimeRange dateTimeRange = mockedFormattedDateTimeRange;
        final List<String> result = objToTest.getRAWTables(dateTimeRange, KEY_TYPE_ERR, RAW_ALL_ERR_TABLES);
        final List<String> expectedResult = new ArrayList<String>();
        expectedResult.addAll(listOfSgehTables);
        expectedResult.addAll(listOfLteTables);
        assertThat(result, is(expectedResult));

    }

    @Test
    public void testGetRAWTablesForRawNonLteTAbles() throws Exception {
        final String key = KEY_TYPE_ERR;
        final Map<String, String> parameterMapForOrdinaryTables = new HashMap<String, String>();
        parameterMapForOrdinaryTables.put(KEY_PARAM, KEY_TYPE_ERR);

        final List<String> listOfNonLteTables = new ArrayList<String>();
        listOfNonLteTables.add("rawTable2");
        final String[] viewNames = new String[] { EVENT_E_SGEH_ERR_RAW };
        expectCallOnEngineGetTableNames(viewNames, listOfNonLteTables);

        final List<String> result = objToTest.getRAWTables(mockedFormattedDateTimeRange, key, RAW_NON_LTE_TABLES);
        assertThat(result, is(listOfNonLteTables));
    }

    @Test
    public void testgetRAWTablesForRawAllTablesGoesToDatabaseForLatestTablesWhenRMIFailsAndFirstQueryOnDatabaseReturnsNothing() throws Exception {
        final List<String> emptyList = new ArrayList<String>();

        final Map<String, Object> templateParameterMapForFirstQueryOnSGEHTimerangeView = new HashMap<String, Object>();
        templateParameterMapForFirstQueryOnSGEHTimerangeView.put(RAW_TIMERANGE_VIEW, EVENT_E_SGEH_ERR_RAW_TIMERANGE);
        final Map<String, Object> templateParameterMapForFirstQueryOnLTETimerangeView = new HashMap<String, Object>();
        templateParameterMapForFirstQueryOnLTETimerangeView.put(RAW_TIMERANGE_VIEW, EVENT_E_LTE_ERR_RAW_TIMERANGE);
        final Map<String, Object> templateParameterMapForQueryForLatestSgehTables = new HashMap<String, Object>();
        templateParameterMapForQueryForLatestSgehTables.put(RAW_TIMERANGE_VIEW, EVENT_E_SGEH_ERR_RAW_TIMERANGE);
        templateParameterMapForQueryForLatestSgehTables.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);
        final Map<String, Object> templateParameterMapForQueryForLatestLteTables = new HashMap<String, Object>();
        templateParameterMapForQueryForLatestLteTables.put(RAW_TIMERANGE_VIEW, EVENT_E_LTE_ERR_RAW_TIMERANGE);
        templateParameterMapForQueryForLatestLteTables.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);

        final List<String> latestLteRawTables = new ArrayList<String>();
        latestLteRawTables.add("EVENT_E_LTE_ERR_RAW_01");
        final List<String> latestSgehTables = new ArrayList<String>();
        latestSgehTables.add("EVENT_E_SGEH_ERR_RAW_02");
        final String[] viewNames = new String[] { EVENT_E_SGEH_ERR_RAW, EVENT_E_LTE_ERR_RAW };
        expectCallOnEngineGetTableNames(viewNames, emptyList);
        final TimerangeQuerier timerangeQuerier = mockery.mock(TimerangeQuerier.class);
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewNames);
                will(returnValue(new ArrayList<String>()));
                exactly(2).of(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));
                exactly(2).of(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));
                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, EVENT_E_SGEH_ERR_RAW);
                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, EVENT_E_LTE_ERR_RAW);
                one(timerangeQuerier).getLatestTablesUsingQuery(EVENT_E_SGEH_ERR_RAW);
                will(returnValue(latestSgehTables));
                one(timerangeQuerier).getLatestTablesUsingQuery(EVENT_E_LTE_ERR_RAW);
                will(returnValue(latestLteRawTables));
            }
        });
        final FormattedDateTimeRange dateTimeRange = mockedFormattedDateTimeRange;
        final List<String> result = objToTest.getRAWTables(dateTimeRange, KEY_TYPE_ERR, RAW_ALL_ERR_TABLES);
        final List<String> expectedResult = new ArrayList<String>();
        expectedResult.addAll(latestSgehTables);
        expectedResult.addAll(latestLteRawTables);
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testgetRAWTablesForRawLteTablesGoesToDatabaseForLatestTablesWhenRMIFailsAndFirstQueryOnDatabaseReturnsNothing() throws Exception {
        final List<String> emptyList = new ArrayList<String>();

        final Map<String, String> templateParameterMapForFirstQuery = new HashMap<String, String>();
        templateParameterMapForFirstQuery.put(RAW_TIMERANGE_VIEW, EVENT_E_LTE_ERR_RAW_TIMERANGE);
        final Map<String, Object> templateParameterMapForQueryForLatestTables = new HashMap<String, Object>();
        templateParameterMapForQueryForLatestTables.put(RAW_TIMERANGE_VIEW, EVENT_E_LTE_ERR_RAW_TIMERANGE);
        templateParameterMapForQueryForLatestTables.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);

        final List<String> lteRawTables = new ArrayList<String>();
        lteRawTables.add("EVENT_E_LTE_ERR_RAW_01");
        final String viewName = "EVENT_E_LTE_ERR_RAW";
        expectCallOnEngineGetTableNames(new String[] { viewName }, emptyList);
        final TimerangeQuerier timerangeQuerier = mockery.mock(TimerangeQuerier.class);
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewName);
                will(returnValue(new ArrayList<String>()));

                allowing(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));

                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, EVENT_E_LTE_ERR_RAW);
                one(timerangeQuerier).getLatestTablesUsingQuery(EVENT_E_LTE_ERR_RAW);
                will(returnValue(lteRawTables));
            }
        });
        final FormattedDateTimeRange dateTimeRange = mockedFormattedDateTimeRange;
        final List<String> result = objToTest.getRAWTables(dateTimeRange, KEY_TYPE_ERR, RAW_LTE_TABLES);
        assertThat(result, is(lteRawTables));

    }

    @Test
    public void testGetTablesReturnsEmptyListWhenNoTablesFound() throws Exception {
        final List<String> emptyList = new ArrayList<String>();
        final Map<String, Object> templateParameterMapForFirstQuery = new HashMap<String, Object>();
        templateParameterMapForFirstQuery.put(RAW_TIMERANGE_VIEW, EVENT_E_SGEH_ERR_RAW_TIMERANGE);
        final Map<String, Object> templateParameterMapForQueryForLatestTables = new HashMap<String, Object>();
        templateParameterMapForQueryForLatestTables.put(RAW_TIMERANGE_VIEW, EVENT_E_SGEH_ERR_RAW_TIMERANGE);
        templateParameterMapForQueryForLatestTables.put(SELECT_BOTH_LATEST_ERR_AND_SUC_TABLES, false);

        final List<String> rawTables = new ArrayList<String>();
        final String viewName = "EVENT_E_SGEH_ERR_RAW";
        expectCallOnEngineGetTableNames(new String[] { viewName }, emptyList);
        final TimerangeQuerier timerangeQuerier = mockery.mock(TimerangeQuerier.class);
        mockery.checking(new Expectations() {
            {
                one(mockedRmiEngineUtils).getLatestTableNames(viewName);
                will(returnValue(new ArrayList<String>()));

                allowing(timerangeQuerierFactory).getTimerangeQuerier();
                will(returnValue(timerangeQuerier));

                one(timerangeQuerier).getRAWTablesUsingQuery(mockedFormattedDateTimeRange, viewName);
                one(timerangeQuerier).getLatestTablesUsingQuery(viewName);
            }
        });
        final FormattedDateTimeRange dateTimeRange = mockedFormattedDateTimeRange;
        final List<String> result = objToTest.getRAWTables(dateTimeRange, KEY_TYPE_ERR, RAW_ERR_TABLES);
        assertThat(result, is(rawTables));

    }

    private void allowGetStartAndEndDateTimeOnDateTimeRange() {
        mockery.checking(new Expectations() {
            {
                allowing(mockedFormattedDateTimeRange).getStartDateTime();
                will(returnValue(startDateTime));
                allowing(mockedFormattedDateTimeRange).getEndDateTime();
                will(returnValue(endDateTime));
                allowing(mockedFormattedDateTimeRange).getRangeInMinutes();
                will(returnValue((long)60));
            }
        });

    }

}
