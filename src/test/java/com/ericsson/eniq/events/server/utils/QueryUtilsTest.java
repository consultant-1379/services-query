/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/

package com.ericsson.eniq.events.server.utils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.*;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.time.DateUtils;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.common.HashIdCreator;
import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.exception.ServiceException;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.query.QueryParameterType;
import com.ericsson.eniq.events.server.test.common.ApplicationTestConstants;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.QueryUtils.NodeValuesByType;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeWhiteList;
import com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class QueryUtilsTest extends BaseJMockUnitTest {

    private static final String GSM_RAT_VALUE = "0";

    private static final String GSM = "GSM";

    private static final String WCDMA = "WCDMA";

    private static final String WCDMA_RAT_VALUE = "1";

    private static final String FAKE_RAT_DESCRIPTION = "some unknown rat description";

    private static final String FAKE_RAT_INTEGER_VALUE = "332";

    private final QueryUtils queryUtils = new QueryUtils();

    HashIdCreator hashIdCreator;

    HashUtilities mockedHashUtilities;

    RATDescriptionMappingUtils mockedRatDescriptionMappingUtils;

    @Before
    public void setUp() {
    mockedRatDescriptionMappingUtils = mockery.mock(RATDescriptionMappingUtils.class);
    queryUtils.setRatDescriptionMappingUtils(mockedRatDescriptionMappingUtils);
    queryUtils.setParameterChecker(new ParameterChecker());
    mockedHashUtilities = mockery.mock(HashUtilities.class);
    queryUtils.setHashUtilities(mockedHashUtilities);
    allowCallsForRatMappings();
    }

    private void allowCallsForRatMappings() {
    mockery.checking(new Expectations() {
        {
        allowing(mockedRatDescriptionMappingUtils).isaRATValue(GSM_RAT_VALUE);
        will(returnValue(true));
        allowing(mockedRatDescriptionMappingUtils).isaRATValue(WCDMA_RAT_VALUE);
        will(returnValue(true));
        allowing(mockedRatDescriptionMappingUtils).getRATDescription(GSM_RAT_VALUE);
        will(returnValue(GSM));
        allowing(mockedRatDescriptionMappingUtils).getRATDescription(WCDMA_RAT_VALUE);
        will(returnValue(WCDMA));
        allowing(mockedRatDescriptionMappingUtils).getRATDescription(FAKE_RAT_INTEGER_VALUE);
        will(returnValue(FAKE_RAT_INTEGER_VALUE));
        allowing(mockedRatDescriptionMappingUtils).getRATIntegerValue(FAKE_RAT_DESCRIPTION);
        will(returnValue(FAKE_RAT_DESCRIPTION));
        allowing(mockedRatDescriptionMappingUtils).getRATIntegerValue(GSM);
        will(returnValue(GSM_RAT_VALUE));
        allowing(mockedRatDescriptionMappingUtils).getRATIntegerValue(WCDMA);
        will(returnValue(WCDMA_RAT_VALUE));
        allowing(mockedRatDescriptionMappingUtils).getRATIntegerValue(null);
        will(returnValue(null));
        }
    });

    }

    @Test
    public void testWhiteSpaceInNodeIsTrimmed() {
    final String apn = "my.apn";
    final String apnWithWhiteSpace = apn + "  ";
    final NodeValuesByType nodeValuesByType = queryUtils.getNodeValuesByTypeInstance(apnWithWhiteSpace, TYPE_APN);
    assertThat(nodeValuesByType.getApn(), is(apn));
    }

    @Test
    public void testMapRequestParametersWithUnknownRatJustMapsToTheStringValue() {
    final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
    requestParameters.putSingle(RAT_PARAM, FAKE_RAT_DESCRIPTION);
    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(requestParameters, null);
    assertTrue(result.containsKey(RAT_PARAM));
    final QueryParameter resultForRATParam = result.get(RAT_PARAM);
    assertEquals(FAKE_RAT_DESCRIPTION, resultForRATParam.getValue());
    }

    @Test
    public void testgetQueryParameters() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();

        final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String tenplatePath = "common/q_event_analysis_aggregation_summary.vm";
    requestParameters.add(ApplicationConstants.TIME_QUERY_PARAM, "10080");
    requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
    requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.APN);
    requestParameters.add(ApplicationConstants.KEY_PARAM, ApplicationConstants.KEY_TYPE_ERR);

    final DateTimeWhiteList datetimeWhiteList = new DateTimeWhiteList();
    datetimeWhiteList.applicationStartup();
    queryUtils.setDatetimeWhiteList(datetimeWhiteList);

    final Map<String, QueryParameter> result = queryUtils.getQueryParameters(requestParameters,
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
                    DateTimeUtils.formattedDateTime(dateTo, timeTo)), tenplatePath);
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    }

    @Test
    public void testgetQueryParametersOneWeekRange() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();

        final String timeFrom = "0000";
    final String timeTo = "0000";
    final String dateFrom = "17081987";
    final String dateTo = "25081987";
    final String tenplatePath = "common/q_event_analysis_aggregation_summary.vm";
    requestParameters.add(ApplicationConstants.TIME_QUERY_PARAM, "11520");
    requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
    requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.APN);
    requestParameters.add(ApplicationConstants.KEY_PARAM, ApplicationConstants.KEY_TYPE_ERR);

    final DateTimeWhiteList datetimeWhiteList = new DateTimeWhiteList();
    datetimeWhiteList.applicationStartup();
    queryUtils.setDatetimeWhiteList(datetimeWhiteList);

    final Map<String, QueryParameter> result = queryUtils.getQueryParameters(requestParameters,
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
                    DateTimeUtils.formattedDateTime(dateTo, timeTo)), tenplatePath);
    assertNotNull(result);
    assertEquals(6, result.size());
    assertEquals("1987-08-17 00:00", result.get(DATE_FROM).getValue());
    assertEquals("1987-08-25 00:00", result.get(DATE_TO).getValue());
    assertEquals("1987-08-17", result.get(LOCAL_DATE_FROM).getValue());
    assertEquals("1987-08-25", result.get(LOCAL_DATE_TO).getValue());
    assertEquals("1987-08-16 23:00", result.get(ADJUSTED_DATE_FROM).getValue());
    assertEquals("1987-08-24 23:00", result.get(ADJUSTED_DATE_TO).getValue());
    }
    

    @Test
    public void testgetQueryParametersAdjustedOneWeekRange() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();

        final String timeFrom = "0000";
    final String timeTo = "0000";
    final String dateFrom = "17081987";
    final String dateTo = "25081987";
    final String tenplatePath = "common/q_event_analysis_aggregation_summary.vm";
    requestParameters.add(ApplicationConstants.TIME_QUERY_PARAM, "11520");
    requestParameters.add(ApplicationConstants.TZ_OFFSET, "+0100");
    requestParameters.add(ApplicationConstants.TYPE_PARAM, ApplicationConstants.APN);
    requestParameters.add(ApplicationConstants.KEY_PARAM, ApplicationConstants.KEY_TYPE_ERR);

    final DateTimeWhiteList datetimeWhiteList = new DateTimeWhiteList();
    datetimeWhiteList.applicationStartup();
    queryUtils.setDatetimeWhiteList(datetimeWhiteList);

    final Map<String, QueryParameter> result = queryUtils.getQueryParameters(requestParameters,
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
                    DateTimeUtils.formattedDateTime(dateTo, timeTo)), tenplatePath);
    assertNotNull(result);
    assertEquals(6, result.size());
    assertEquals("1987-08-17 00:00", result.get(DATE_FROM).getValue());
    assertEquals("1987-08-25 00:00", result.get(DATE_TO).getValue());
    assertEquals("1987-08-17", result.get(LOCAL_DATE_FROM).getValue());
    assertEquals("1987-08-25", result.get(LOCAL_DATE_TO).getValue());
    assertEquals("1987-08-16 23:00", result.get(ADJUSTED_DATE_FROM).getValue());
    assertEquals("1987-08-24 23:00", result.get(ADJUSTED_DATE_TO).getValue());
    }

    @Test
    public void testMapRequestParametersWithWCDMARat() {
    final String bsc = "ONRM_RootMo_R:RNC01:RNC01";
    final String hier2 = "heir2";
    final String cell = "Cell00";
    final String vendor = "Ericsson";
    final String rat = WCDMA;
    expectCallOnHashUtilitiesForCell(rat, bsc, hier2, cell, vendor);
    final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
    requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
    requestParameters.putSingle(NODE_PARAM, cell + "," + hier2 + "," + bsc + "," + vendor + "," + rat);
    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(requestParameters, null);
    assertTrue(result.containsKey(RAT_PARAM));
    final QueryParameter resultForRATParam = result.get(RAT_PARAM);
    assertEquals(WCDMA_RAT_VALUE, resultForRATParam.getValue().toString());
    }

    private long expectCallOnHashUtilitiesForCell(final String rat, final String controller, final String hier2,
        final String cell, final String vendor) {
    final long hashedId = generateRandomLongValue();
    mockery.checking(new Expectations() {
        {
        one(mockedHashUtilities).createHashIDForCell(rat, controller, hier2, cell, vendor);
        will(returnValue(hashedId));
        }
    });
    return hashedId;

    }

    @Test
    public void testMapRequestParametersWithGSMRat() {
    final String rat = GSM;
    final String vendor = "Ericsson";
    final String controller = "ONRM_RootMo_R:RNC01:RNC01";
    final String hier2 = "heir2";
    final String cell = "Cell00";
    expectCallOnHashUtilitiesForCell(rat, controller, hier2, cell, vendor);
    final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
    requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
    requestParameters.putSingle(NODE_PARAM, cell + "," + hier2 + "," + controller + "," + vendor + "," + rat);
    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(requestParameters, null);
    assertTrue(result.containsKey(RAT_PARAM));
    final QueryParameter resultForRATParam = result.get(RAT_PARAM);
    assertEquals(GSM_RAT_VALUE, resultForRATParam.getValue().toString());
    }

    @Test
    public void testGetRATFromNodeValuesByTypeGSM() {
    final NodeValuesByType nodeValuesByType = queryUtils.getNodeValuesByTypeInstance("someNode", "someType");
    nodeValuesByType.setRat("GSM");
    final String result = nodeValuesByType.getAndConvertRatToIntegerValue(mockedRatDescriptionMappingUtils);
    assertThat(result, is(GSM_RAT_VALUE));
    }

    @Test
    public void testGetRATFromNodeValuesByTypeWCDMA() {
    final NodeValuesByType nodeValuesByType = queryUtils.getNodeValuesByTypeInstance("someNode", "someType");
    nodeValuesByType.setRat("WCDMA");
    final String result = nodeValuesByType.getAndConvertRatToIntegerValue(mockedRatDescriptionMappingUtils);
    assertThat(result, is("1"));
    }

    @Test
    public void testGetRATFromNodeValuesByTypeUnknownRATDescriptionJustReturnsIntegerValue() {
    final NodeValuesByType nodeValuesByType = queryUtils.getNodeValuesByTypeInstance("someNode", "someType");
    nodeValuesByType.setRat(FAKE_RAT_DESCRIPTION);
    final String result = nodeValuesByType.getAndConvertRatToIntegerValue(mockedRatDescriptionMappingUtils);
    assertThat(result, is(FAKE_RAT_DESCRIPTION));
    }

    @Test
    public void test_getEventDataSourceTypeForGrid() throws WebApplicationException {
    FormattedDateTimeRange fdtr;
    EventDataSourceType expectedType;
    EventDataSourceType actualType;

    // <=5 min => RAW
    fdtr = getTimeRange(5);
    expectedType = EventDataSourceType.RAW;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);

    // 30min < t <=minutes in a week => 15MIN
    fdtr = getTimeRange(MINUTES_IN_2_WEEKS + 1);
    expectedType = EventDataSourceType.AGGREGATED_DAY;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);
    }

    @Test
    public void test_getEventDataSourceTypeForGrid_one_minute_aggregation_flag_true() throws WebApplicationException {
    FormattedDateTimeRange fdtr;
    EventDataSourceType expectedType;
    EventDataSourceType actualType;

    // 1 min or less => RAW
    fdtr = getTimeRange(1);
    expectedType = EventDataSourceType.RAW;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);

    // 1min < t < minutes in a day => 1MIN
    fdtr = getTimeRange(MINUTES_IN_A_DAY);
    expectedType = EventDataSourceType.AGGREGATED_15MIN;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);

    // minutes in a day < t < minutes in a week => 15MIN
    fdtr = getTimeRange(MINUTES_IN_2_WEEKS + 1);
    expectedType = EventDataSourceType.AGGREGATED_DAY;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);

    // minutes in a week < t => 15MIN
    fdtr = getTimeRange(MINUTES_IN_2_WEEKS);
    expectedType = EventDataSourceType.AGGREGATED_DAY;
    actualType = queryUtils.getEventDataSourceType(fdtr);
    assertEquals(expectedType, actualType);
    }

    @Test
    public void test_getParametersByType_bsc_type() {
    final String rat = WCDMA;
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    expectCallOnHashUtilitiesForController(rat, bsc, vendor);

    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = bsc + "," + vendor + "," + rat;

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_BSC);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(5, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(vendor, result.get(VENDOR_PARAM.toUpperCase()).getValue());
    assertEquals(bsc, result.get(BSC_SQL_NAME).getValue());
    assertEquals("1", result.get(RAT_PARAM).getValue().toString());
    }

    public static FormattedDateTimeRange createTimeRange(final String startDateTime, final String endDateTime) {
    return DateTimeRange.getFormattedDateTimeRange(startDateTime, endDateTime,
        ApplicationConfigManager.ENIQ_EVENTS_TIME_DELAY_1MIN_DATA_DEFAULT_MINUTES,
        ApplicationConfigManager.ENIQ_EVENTS_TIME_DELAY_15MIN_DATA_DEFAULT_MINUTES,
        ApplicationConfigManager.ENIQ_EVENTS_TIME_DELAY_DAY_DATA_DEFAULT_MINUTES);

    }

    private long expectCallOnHashUtilitiesForController(final String rat, final String controller, final String vendor) {
    final long hashedId = generateRandomLongValue();
    mockery.checking(new Expectations() {
        {
        one(mockedHashUtilities).createHashIDForControllerAsLong(rat, controller, vendor);
        will(returnValue(hashedId));
        }
    });
    return hashedId;

    }

    protected long generateRandomLongValue() {
    return new Random().nextLong();
    }

    @Test
    public void test_getParametersByType_bsc_type_hashId() {
    final String rat = WCDMA;
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    final long expectedHashedId = expectCallOnHashUtilitiesForController(rat, bsc, vendor);
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = bsc + "," + vendor + "," + rat;
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_BSC);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(expectedHashedId, result.get(CONTROLLER_SQL_ID).getValue());
    }

    @Test
    public void test_getParametersByType_msc_type_hashId() {
    final String node = "MSS_1";
    final long expectedHashId = expectCallOnHashUtilitiesForMSC(node);
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_MSC);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(expectedHashId, result.get(EVENT_SOURCE_SQL_ID).getValue());
    }

    private long expectCallOnHashUtilitiesForMSC(final String msc) {
    final long hashedId = generateRandomLongValue();
    mockery.checking(new Expectations() {
        {
        one(mockedHashUtilities).createHashIDForMSC(msc);
        will(returnValue(hashedId));
        }
    });
    return hashedId;

    }

    @Test
    public void test_getParametersByType_cell_type() {
    final String cell = "cell1";
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    final String rat = WCDMA;
    final String hier2 = "";
    expectCallOnHashUtilitiesForCell(rat, bsc, hier2, cell, vendor);
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = cell + ",," + bsc + "," + vendor + "," + rat;

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_CELL);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(6, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(cell, result.get(CELL_SQL_NAME).getValue());
    assertEquals(vendor, result.get(VENDOR_PARAM.toUpperCase()).getValue());
    assertEquals(bsc, result.get(BSC_SQL_NAME).getValue());
    assertEquals("1", result.get(RAT_PARAM).getValue().toString());
    }

    @Test
    public void test_getParametersByType_cell_type_hashId() {
    final String rat = WCDMA;
    final String cell = "cell1";
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    final String hier2 = EMPTY_STRING;
    final long expectedHashedId = expectCallOnHashUtilitiesForCell(rat, bsc, hier2, cell, vendor);
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = cell + ",," + bsc + "," + vendor + "," + rat;
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_CELL);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(expectedHashedId, result.get(CELL_SQL_ID).getValue());
    }

    @Test
    public void test_getParametersByType_tac_type() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = "MODULE ITI MMG: VMC-1, VMM-1, Aqua Si,35458000";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_TAC);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals("35458000", result.get(TAC_PARAM.toUpperCase()).getValue().toString());
    }

    @Test
    public void test_getParametersByGroup_imsi_type() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String groupName = "gomeGroup";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(GROUP_NAME_PARAM, groupName);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    }

    @Test
    public void test_getParametersByNode_imsi_type() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String imsiValue = "anImsi";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(IMSI_PARAM, imsiValue);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(imsiValue, result.get(IMSI_PARAM.toUpperCase()).getValue());
    }

    @Test
    public void test_getParameters_NodeAndGroupSet_imsi_type() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);

    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(IMSI_PARAM, "abc");
    map.putSingle(GROUP_NAME_PARAM, "def");

    try {
        queryUtils.mapRequestParameters(
            map,
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
                DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    } catch (final ServiceException e) {
        assertTrue(e.getMessage().contains("one must be null"));
    }
    }

    @Test
    public void test_getParametersByType_tac_type_hashId() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String node = "MODULE ITI MMG: VMC-1, VMM-1, Aqua Si,35458000";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_TAC);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    final long expectedHashId = 35458000l;
    assertEquals(expectedHashId, result.get(TAC_PARAM.toUpperCase()).getValue());
    }

    @Test
    public void test_getParametersByGroup_imsi_type_hashId() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String groupName = "gomeGroup";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(GROUP_NAME_PARAM, groupName);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    }

    @Test
    public void test_getParametersByNode_imsi_type_hashId() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";
    final String imsiValue = "anImsi";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(IMSI_PARAM, imsiValue);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(3, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    assertEquals(imsiValue, result.get(IMSI_PARAM.toUpperCase()).getValue());
    }

    @Test
    public void test_getParameters_NodeAndGroupSet_imsi_type_hashId() {
    final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_SUM);

    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(IMSI_PARAM, "abc");
    map.putSingle(GROUP_NAME_PARAM, "def");

    try {
        queryUtils.mapRequestParametersForHashId(
            map,
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
                DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    } catch (final ServiceException e) {
        assertTrue(e.getMessage().contains("one must be null"));
    }
    }

    private static FormattedDateTimeRange getTimeRange(final int rangeInMinutes) {
    final Date from = new Date();
    final Date to = DateUtils.addMinutes(from, rangeInMinutes);
    return createTimeRange(DateTimeUtils.formattedDateTime(from, GSM_RAT_VALUE),
        DateTimeUtils.formattedDateTime(to, GSM_RAT_VALUE));
    }

    @Test
    public void testGetDrillType_APN() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_APN);
    assertEquals(TYPE_APN, queryUtils.getDrillType(map));
    }

    @Test
    public void testGetDrillType_BSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(APN_PARAM, "apn");
    map.putSingle(SGSN_PARAM, "sgsn");
    map.putSingle(VENDOR_PARAM, "vendor");
    map.putSingle(BSC_PARAM, "bsc");
    map.putSingle(RAT_PARAM, "1");
    assertEquals(TYPE_BSC, queryUtils.getDrillType(map));
    }

    @Test
    public void testGetDrillType_SGSN() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(APN_PARAM, "apn");
    map.putSingle(SGSN_PARAM, "sgsn");
    assertEquals(TYPE_SGSN, queryUtils.getDrillType(map));
    }

    @Test
    public void testGetDrillType_CELL() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(APN_PARAM, "apn");
    map.putSingle(SGSN_PARAM, "sgsn");
    map.putSingle(VENDOR_PARAM, "vendor");
    map.putSingle(BSC_PARAM, "bsc");
    map.putSingle(CELL_PARAM, "cell");
    map.putSingle(RAT_PARAM, "1");
    assertEquals(TYPE_CELL, queryUtils.getDrillType(map));
    }

    @Test
    public void testGetDrillType_EVENTS() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(APN_PARAM, "apn");
    map.putSingle(SGSN_PARAM, "sgsn");
    map.putSingle(VENDOR_PARAM, "vendor");
    map.putSingle(BSC_PARAM, "bsc");
    map.putSingle(CELL_PARAM, "cell");
    map.putSingle(TYPE_CAUSE_CODE, "2");
    map.putSingle(TYPE_SUB_CAUSE_CODE, "4");
    map.putSingle(RAT_PARAM, "1");
    assertEquals(EVENTS_DRILL_TYPE_PARAM, queryUtils.getDrillType(map));
    }

    @Test
    public void testGetDrillTypeForMss_MSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc1");
    assertEquals(TYPE_MSC, queryUtils.getDrillTypeForMss(map));
    }

    @Test
    public void testGetDrillTypeForMssLocationService_MSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_MSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsOriginating_MSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_MSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsTerminating_MSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_TERMINATING_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_MSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMss_BSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    assertEquals(TYPE_BSC, queryUtils.getDrillTypeForMss(map));
    }

    @Test
    public void testGetDrillTypeForMssLocationService_BSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_BSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsOriginating_BSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_BSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsTerminating_BSC() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_TERMINATING_EVENT_ID);
    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(TYPE_BSC, drillType);
    }

    @Test
    public void testGetDrillTypeForMss_CELL() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    assertEquals(TYPE_CELL, queryUtils.getDrillTypeForMss(map));
    }

    @Test
    public void testGetDrillTypeForMss_EVENTS() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    map.putSingle(FAULT_CODE_PARAM, "faultCode");
    assertEquals(EVENTS_DRILL_TYPE_PARAM, queryUtils.getDrillTypeForMss(map));

    map.clear();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    map.putSingle(FAULT_CODE_PARAM, "faultCode");
    map.putSingle(TYPE_INTERNAL_CAUSE_CODE, "internalCauseCode");
    assertEquals(EVENTS_DRILL_TYPE_PARAM, queryUtils.getDrillTypeForMss(map));
    }

    @Test
    public void testGetDrillTypeForMssLocationService_EVENTS() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);

    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(EVENTS_DRILL_TYPE_PARAM, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsOriginating_EVENTS() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);

    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(EVENTS_DRILL_TYPE_PARAM, drillType);
    }

    @Test
    public void testGetDrillTypeForMssSMSmsTerminating_EVENTS() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(EVENT_SOURCE_SQL_ID, "msc");
    map.putSingle(CONTROLLER_SQL_ID, "bsc");
    map.putSingle(CELL_SQL_ID, "cell");
    map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_TERMINATING_EVENT_ID);

    final String drillType = queryUtils.getDrillTypeForMss(map);
    assertEquals(EVENTS_DRILL_TYPE_PARAM, drillType);
    }

    @Test
    public void testCheckValidValue() {
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_TAC);
    map.putSingle(NODE_PARAM, "NOKIA,123123");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_APN);
    map.putSingle(NODE_PARAM, "blaa.enet");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_APN);
    map.putSingle(NODE_PARAM, "");
    assertEquals(false, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_BSC);
    map.putSingle(NODE_PARAM, "BSC_1,ERICSSON,1");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_CELL);
    map.putSingle(NODE_PARAM, "CELL1,,BSC2,ERICSSON,0");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_SGSN);
    map.putSingle(NODE_PARAM, "SGSN1");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_MSC);
    map.putSingle(NODE_PARAM, "MSC1");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, TYPE_IMSI);
    map.putSingle(IMSI_PARAM, "123123123123123");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(GROUP_NAME_PARAM, "group1-2a");
    assertEquals(true, queryUtils.checkValidValue(map));

    map.clear();
    map.putSingle(TYPE_PARAM, SUBBI_FAILURE);
    map.putSingle(NODE_PARAM, "ATTACH,0");
    assertEquals(true, queryUtils.checkValidValue(map));
    }

    @Test(expected = RuntimeException.class)
    public void testcreateQueryParameterNoParameterTypeDefined() {
    queryUtils.createQueryParameter("non existent parameter", null);
    }

    @Test
    public void testcreateQueryParameterForLong() {
    final String parameterValue = "123456";
    final QueryParameter result = queryUtils.createQueryParameter(TAC_PARAM, parameterValue);
    assertThat(result.getType(), is(QueryParameterType.LONG));
    assertThat((Long) result.getValue(), is(Long.valueOf(parameterValue)));
    }

    @Test
    public void testcreateQueryParameterForInt() {
    final String parameterValue = "3";
    final QueryParameter result = queryUtils.createQueryParameter(EVENT_ID_PARAM, parameterValue);
    assertThat(result.getType(), is(QueryParameterType.INT));
    assertThat((Integer) result.getValue(), is(Integer.valueOf(parameterValue)));
    }

    @Test
    public void testcreateQueryParameterForString() {
    final String parameterValue = "RNC01";
    final QueryParameter result = queryUtils.createQueryParameter(RNC_ID_PARAM, parameterValue);
    assertThat(result.getType(), is(QueryParameterType.STRING));
    assertThat((String) result.getValue(), is(parameterValue));
    }

    @Test
    public void testmapDateParameters() {
        final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";

    final Map<String, QueryParameter> result = queryUtils.mapDateParameters(
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(2, result.size());

    assertEquals("2009-11-14 16:00", result.get(DATE_FROM).getValue());
    assertEquals("2009-11-14 16:01", result.get(DATE_TO).getValue());
    }
    
    @Test
    public void testAdjustedmapDateParameters() {
        final String timeFrom = "0000";
    final String timeTo = "0000";
    final String dateFrom = "14112009";
    final String dateTo = "21112009";
    String utcOffset = DateTimeUtils.getUTCOffset();
    String formattedStartTime = DateTimeUtils.formattedDateTime(dateFrom, timeFrom);
    String formattedEndTime = DateTimeUtils.formattedDateTime(dateTo, timeTo);
    
    String expectedStartDate = DateTimeUtils.getRawOffsetAdjustedTime(formattedStartTime, utcOffset);
    String expectedEndDate = DateTimeUtils.getRawOffsetAdjustedTime(formattedEndTime, utcOffset);    

    final Map<String, QueryParameter> result = queryUtils.mapAdjustedDateParameters(
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    Object actualStartDate =result.get(DATE_FROM).getValue();
    Object atualEndDate = result.get(DATE_TO).getValue();

    assertNotNull(result);
    assertEquals(2, result.size());

    assertEquals(expectedStartDate, actualStartDate);
    assertEquals(expectedEndDate, atualEndDate);
    }

    @Test
    public void testmapDateParametersForApnRetention() {
        final String timeFrom = "1600";
    final String timeTo = "1601";
    final String dateFrom = "14112009";
    final String dateTo = "14112009";

    final Map<String, QueryParameter> result = queryUtils.mapDateParametersForApnRetention(
            createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),DateTimeUtils.formattedDateTime(dateTo, timeTo)));
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("2009-11-14 16:00", result.get("dateFromForApnRetention").getValue());
    }

    @Test
    public void test_getParametersByType_cell_type_9days() {
    final String cell = "cell1";
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    final String rat = WCDMA;
    final String hier2 = "";
    expectCallOnHashUtilitiesForCell(rat, bsc, hier2, cell, vendor);
    final String timeFrom = "0000";
    final String timeTo = "0000";
    final String dateFrom = "06092014";
    final String dateTo = "15092014";
    final String node = cell + ",," + bsc + "," + vendor + "," + rat;

    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_CELL);
    map.putSingle(TZ_OFFSET, ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParameters(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(9, result.size());

    assertEquals("2014-09-05 18:30", result.get(DATE_FROM).getValue());
    assertEquals("2014-09-14 18:30", result.get(DATE_TO).getValue());
    assertEquals(cell, result.get(CELL_SQL_NAME).getValue());
    assertEquals(vendor, result.get(VENDOR_PARAM.toUpperCase()).getValue());
    assertEquals(bsc, result.get(BSC_SQL_NAME).getValue());
    assertEquals("1", result.get(RAT_PARAM).getValue().toString());

    assertEquals("2014-09-06", result.get(LOCAL_DATE_FROM).getValue());
    assertEquals("2014-09-15", result.get(LOCAL_DATE_TO).getValue());
    assertEquals("true", result.get(ApplicationTestConstants.IS_WEEK_OVERRIDE_APPLIED).getValue());
    }


    @Test
    public void test_getParametersByType_cell_type_hashId_9days() {
    final String rat = WCDMA;
    final String cell = "cell1";
    final String bsc = "bsc1";
    final String vendor = "vendor1";
    final String hier2 = EMPTY_STRING;
    final long expectedHashedId = expectCallOnHashUtilitiesForCell(rat, bsc, hier2, cell, vendor);
    final String timeFrom = "0000";
    final String timeTo = "0000";
    final String dateFrom = "06092014";
    final String dateTo = "15092014";
    final String node = cell + ",," + bsc + "," + vendor + "," + rat;
    final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
    map.putSingle(TYPE_PARAM, TYPE_CELL);
    map.putSingle(TZ_OFFSET, ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
    map.putSingle(TIME_FROM_QUERY_PARAM, timeFrom);
    map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
    map.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
    map.putSingle(DATE_TO_QUERY_PARAM, dateTo);
    map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
    map.putSingle(NODE_PARAM, node);

    final Map<String, QueryParameter> result = queryUtils.mapRequestParametersForHashId(
        map,
        createTimeRange(DateTimeUtils.formattedDateTime(dateFrom, timeFrom),
            DateTimeUtils.formattedDateTime(dateTo, timeTo)));

    assertNotNull(result);
    assertEquals(6, result.size());

    assertEquals("2014-09-05 18:30", result.get(DATE_FROM).getValue());
    assertEquals("2014-09-14 18:30", result.get(DATE_TO).getValue());
    assertEquals(expectedHashedId, result.get(CELL_SQL_ID).getValue());

    assertEquals("2014-09-06", result.get(LOCAL_DATE_FROM).getValue());
    assertEquals("2014-09-15", result.get(LOCAL_DATE_TO).getValue());
    assertEquals("true", result.get(ApplicationTestConstants.IS_WEEK_OVERRIDE_APPLIED).getValue());
    }
}
