/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.TechPackRepresentation;
import com.ericsson.eniq.events.server.common.exception.CannotAccessLicensingServiceException;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.RMIEngineUtils;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeHelper;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 */
public class TechPackListFactoryMockTest extends BaseJMockUnitTest {

    private TechPackListFactory objToTest;

    FormattedDateTimeRange mockedFormattedDateTimeRange;

    final String startDateTime = "2010-10-11 13:00";

    final String endDateTime = "2010-10-11 14:00";

    protected RMIEngineUtils mockedRmiEngineUtils;

    RawTableFetcher mockedRawTableFetcher;

    DateTimeHelper mockedDateTimeHelper;

    TechPackLicensingService mockedTechPackLicensingService;

    @Before
    public void setup() {
        mockedRmiEngineUtils = mockery.mock(RMIEngineUtils.class);
        objToTest = new TechPackListFactory();
        mockedRawTableFetcher = mockery.mock(RawTableFetcher.class);
        objToTest.setRawTableFetcher(mockedRawTableFetcher);
        mockedDateTimeHelper = mockery.mock(DateTimeHelper.class);
        objToTest.setDateTimeHelper(mockedDateTimeHelper);
        mockedTechPackLicensingService = mockery.mock(TechPackLicensingService.class);
        objToTest.setTechPackLicensingService(mockedTechPackLicensingService);

        mockedFormattedDateTimeRange = mockery.mock(FormattedDateTimeRange.class);

        allowGetStartAndEndDateTimeOnDateTimeRange();

    }

    private void expectGetEventDataSourceTypeOnDateTimeHelper(final EventDataSourceType eventDataSourceTypeToReturn) {
        mockery.checking(new Expectations() {
            {
                allowing(mockedDateTimeHelper).getEventDataSourceType(mockedFormattedDateTimeRange);
                will(returnValue(eventDataSourceTypeToReturn));
            }
        });

    }

    private void allowGetStartAndEndDateTimeOnDateTimeRange() {
        mockery.checking(new Expectations() {
            {
                allowing(mockedFormattedDateTimeRange).getStartDateTime();
                will(returnValue(startDateTime));
                allowing(mockedFormattedDateTimeRange).getEndDateTime();
                will(returnValue(endDateTime));
            }
        });

    }

    @Test
    public void testCreateTechPackTable_TechPackThatDoesntHaveErrOrSucTables() throws Exception {
        final List<String> lteErrTables = new ArrayList<String>();
        lteErrTables.add("EVENT_E_RAN_CFA_ERR_RAW_01");
        expectCallOnFetcherForErrTables(EVENT_E_RAN_CFA, lteErrTables);
        final List<String> lteSucTables = new ArrayList<String>();
        lteSucTables.add("EVENT_E_RAN_CFA_SUC_RAW_01");
        expectCallOnFetcherForSucTables(EVENT_E_RAN_CFA, lteSucTables);
        final List<String> lteRawTables = new ArrayList<String>();
        lteRawTables.add("EVENT_E_RAN_CFA_RAW_01");
        expectCallOnFetcherForRawTables(EVENT_E_RAN_CFA, lteRawTables);

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_RAN_CFA, true);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_RAN_CFA);

        final AggregationTableInfo aggregationView = new AggregationTableInfo(VEND_HIER3,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackList(techPacksToCreate, mockedFormattedDateTimeRange,
                aggregationView);

        final List<String> expectedPlainAggregationViews = new ArrayList<String>();
        expectedPlainAggregationViews.add(EVENT_E_RAN_CFA + UNDERSCORE + VEND_HIER3 + "_15MIN");
        assertThat(result.getAllPlainAggregationViews(), is(expectedPlainAggregationViews));

    }

    @Test
    public void testCreateTechPackTableUsingKey_TechPackThatHasErrTables() throws Exception {
        final List<String> errTables = new ArrayList<String>();
        errTables.add("EVENT_E_GSM_CFA_ERR_RAW_01");
        expectCallOnFetcherForTechPackAndKeyForErrTables(EVENT_E_GSM_CFA, errTables, ERR);
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(ERR);

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_GSM_CFA, true);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_GSM_CFA);

        final AggregationTableInfo aggregationView = new AggregationTableInfo(VEND_HIER3,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackListWithKeys(techPacksToCreate, rawTableKeys,
                mockedFormattedDateTimeRange, aggregationView);

        final List<String> expectedPlainAggregationViews = new ArrayList<String>();
        expectedPlainAggregationViews.add(EVENT_E_GSM_CFA + UNDERSCORE + VEND_HIER3 + "_15MIN");
        assertThat(result.getAllPlainAggregationViews(), is(expectedPlainAggregationViews));

    }

    @Test
    public void testCreateTechPackTable_allTechPacksLicensed() throws Exception {
        final List<String> lteErrTables = new ArrayList<String>();
        lteErrTables.add("EVENT_E_LTE_ERR_RAW_01");
        expectCallOnFetcherForErrTables(EVENT_E_LTE, lteErrTables);
        final List<String> lteSucTables = new ArrayList<String>();
        lteSucTables.add("EVENT_E_LTE_SUC_RAW_01");
        expectCallOnFetcherForSucTables(EVENT_E_LTE, lteSucTables);
        final List<String> lteRawTables = new ArrayList<String>();
        lteRawTables.add("EVENT_E_LTE_RAW_01");
        expectCallOnFetcherForRawTables(EVENT_E_LTE, lteRawTables);

        final List<String> sgehErrTables = new ArrayList<String>();
        sgehErrTables.add("EVENT_E_SGEH_ERR_RAW_03");
        expectCallOnFetcherForErrTables(EVENT_E_SGEH, sgehErrTables);
        final List<String> sgehSucTables = new ArrayList<String>();
        sgehSucTables.add("EVENT_E_SGEH_SUC_RAW_01");
        expectCallOnFetcherForSucTables(EVENT_E_SGEH, sgehSucTables);
        final List<String> sgehRawTables = new ArrayList<String>();
        sgehRawTables.add("EVENT_E_SGEH_RAW_01");
        expectCallOnFetcherForRawTables(EVENT_E_SGEH, sgehRawTables);

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_SGEH, true);
        expectGetOnTechPackLicensingService(EVENT_E_LTE, true);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_LTE);
        techPacksToCreate.add(EVENT_E_SGEH);
        final AggregationTableInfo aggregationView = new AggregationTableInfo(APN_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackList(techPacksToCreate, mockedFormattedDateTimeRange,
                aggregationView);

        assertThat(result.shouldQueryUseAggregationTables(), is(true));

        final List<String> expectedRawErrTables = new ArrayList<String>();
        expectedRawErrTables.addAll(lteErrTables);
        expectedRawErrTables.addAll(sgehErrTables);
        assertThat(result.getAllRawErrTables(), is(expectedRawErrTables));
        final List<String> expectedRawSucTables = new ArrayList<String>();
        expectedRawSucTables.addAll(lteSucTables);
        expectedRawSucTables.addAll(sgehSucTables);
        assertThat(result.getAllRawSucTables(), is(expectedRawSucTables));

        final List<String> expectedRawTables = new ArrayList<String>();
        expectedRawTables.addAll(lteRawTables);
        expectedRawTables.addAll(sgehRawTables);
        assertThat(result.getAllRawTables(), is(expectedRawTables));

        final List<String> expectedErrAggregationViews = new ArrayList<String>();
        expectedErrAggregationViews.add("EVENT_E_LTE_APN_EVENTID_ERR_15MIN");
        expectedErrAggregationViews.add("EVENT_E_SGEH_APN_EVENTID_ERR_15MIN");
        assertThat(result.getAllErrAggregationViews(), is(expectedErrAggregationViews));
        final List<String> expectedSucAggregationViews = new ArrayList<String>();
        expectedSucAggregationViews.add("EVENT_E_LTE_APN_EVENTID_SUC_15MIN");
        expectedSucAggregationViews.add("EVENT_E_SGEH_APN_EVENTID_SUC_15MIN");
        assertThat(result.getAllSucAggregationViews(), is(expectedSucAggregationViews));

        final TechPackRepresentation techPack = result.getTechPack(EVENT_E_SGEH);
        assertThat(techPack.getName(), is(EVENT_E_SGEH));
        assertThat(techPack.getStaticLookupTechpack(), is(DIM_E_SGEH));

        final Collection<TechPackRepresentation> techPacks = result.getTechPacks();
        assertThat(techPacks.size(), is(2));
    }

    private void expectCallOnFetcherForRawTables(final String techPackName, final List<String> tablesToReturn) {
        mockery.checking(new Expectations() {
            {
                one(mockedRawTableFetcher).getRawTables(mockedFormattedDateTimeRange, techPackName);
                will(returnValue(tablesToReturn));
            }
        });

    }

    private void expectGetOnTechPackLicensingService(final String techPackName, final boolean isTechPackLicensed)
            throws CannotAccessLicensingServiceException {
        mockery.checking(new Expectations() {
            {
                one(mockedTechPackLicensingService).isTechPackLicensed(techPackName);
                will(returnValue(isTechPackLicensed));
            }
        });

    }

    @Test
    public void testCreateTechPackTable_NoTechPacksLicensed() throws Exception {

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_SGEH, false);
        expectGetOnTechPackLicensingService(EVENT_E_LTE, false);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_LTE);
        techPacksToCreate.add(EVENT_E_SGEH);
        final AggregationTableInfo aggregationView = new AggregationTableInfo(APN_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackList(techPacksToCreate, mockedFormattedDateTimeRange,
                aggregationView);

        final Collection<TechPackRepresentation> techPacks = result.getTechPacks();
        assertThat(techPacks.isEmpty(), is(true));
    }

    @Test
    public void testCreateTechPackTable_only1SGEHTechPackLicensed() throws Exception {

        final List<String> sgehErrTables = new ArrayList<String>();
        sgehErrTables.add("EVENT_E_SGEH_ERR_RAW_03");
        expectCallOnFetcherForErrTables(EVENT_E_SGEH, sgehErrTables);
        final List<String> sgehSucTables = new ArrayList<String>();
        sgehSucTables.add("EVENT_E_SGEH_SUC_RAW_01");
        expectCallOnFetcherForSucTables(EVENT_E_SGEH, sgehSucTables);
        expectCallOnFetcherForPlainRawTables(EVENT_E_SGEH);

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_SGEH, true);
        expectGetOnTechPackLicensingService(EVENT_E_LTE, false);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_LTE);
        techPacksToCreate.add(EVENT_E_SGEH);
        final AggregationTableInfo aggregationView = new AggregationTableInfo(APN_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackList(techPacksToCreate, mockedFormattedDateTimeRange,
                aggregationView);

        final Collection<TechPackRepresentation> techPacks = result.getTechPacks();
        assertThat(techPacks.size(), is(1));
        final TechPackRepresentation techPack = result.getTechPack(EVENT_E_SGEH);
        assertThat(techPack.getName(), is(EVENT_E_SGEH));
    }

    private void expectCallOnFetcherForPlainRawTables(final String techPackName) {
        mockery.checking(new Expectations() {
            {
                one(mockedRawTableFetcher).getRawTables(mockedFormattedDateTimeRange, techPackName);
            }
        });
    }

    @Test
    public void testCreateTechPackTable_only1LTETechPackLicensed() throws Exception {

        final List<String> sgehErrTables = new ArrayList<String>();
        sgehErrTables.add("EVENT_E_LTE_ERR_RAW_03");
        expectCallOnFetcherForErrTables(EVENT_E_LTE, sgehErrTables);
        final List<String> sgehSucTables = new ArrayList<String>();
        sgehSucTables.add("EVENT_E_LTE_SUC_RAW_01");
        expectCallOnFetcherForSucTables(EVENT_E_LTE, sgehSucTables);
        expectCallOnFetcherForPlainRawTables(EVENT_E_LTE);

        expectGetEventDataSourceTypeOnDateTimeHelper(EventDataSourceType.AGGREGATED_15MIN);

        expectGetOnTechPackLicensingService(EVENT_E_SGEH, false);
        expectGetOnTechPackLicensingService(EVENT_E_LTE, true);

        final List<String> techPacksToCreate = new ArrayList<String>();
        techPacksToCreate.add(EVENT_E_LTE);
        techPacksToCreate.add(EVENT_E_SGEH);
        final AggregationTableInfo aggregationView = new AggregationTableInfo(APN_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY);

        final TechPackList result = objToTest.createTechPackList(techPacksToCreate, mockedFormattedDateTimeRange,
                aggregationView);

        final Collection<TechPackRepresentation> techPacks = result.getTechPacks();
        assertThat(techPacks.size(), is(1));
        final TechPackRepresentation techPack = result.getTechPack(EVENT_E_LTE);
        assertThat(techPack.getName(), is(EVENT_E_LTE));
    }

    private void expectCallOnFetcherForSucTables(final String techPackName, final List<String> tablesToReturn) {
        mockery.checking(new Expectations() {
            {
                one(mockedRawTableFetcher).getRawSucTables(mockedFormattedDateTimeRange, techPackName);
                will(returnValue(tablesToReturn));
            }
        });

    }

    private void expectCallOnFetcherForErrTables(final String techPackName, final List<String> tablesToReturn) {
        mockery.checking(new Expectations() {
            {
                one(mockedRawTableFetcher).getRawErrTables(mockedFormattedDateTimeRange, techPackName);
                will(returnValue(tablesToReturn));
            }
        });

    }

    private void expectCallOnFetcherForTechPackAndKeyForErrTables(final String techPackName,
                                                                  final List<String> tablesToReturn, final String key) {
        mockery.checking(new Expectations() {
            {
                one(mockedRawTableFetcher).fetchRawTablesForTechPackAndKey(mockedFormattedDateTimeRange, techPackName,
                        key);
                will(returnValue(tablesToReturn));
            }
        });
    }

    @Test
    public void testshouldQueryUseAggregationViewWhereThereIsntAggregationGroup() {
        final AggregationTableInfo aggregationView = new AggregationTableInfo("some agg table",
                EventDataSourceType.AGGREGATED_DAY);
        assertThat(objToTest.shouldQueryUseAggregationView(EventDataSourceType.AGGREGATED_15MIN.toString(),
                aggregationView), is(false));
    }

    @Test
    public void testshouldQueryUseAggregationViewWhereThereIsAggregationGroup() {
        final AggregationTableInfo aggregationView = new AggregationTableInfo("some agg table",
                EventDataSourceType.AGGREGATED_DAY);
        assertThat(
                objToTest.shouldQueryUseAggregationView(EventDataSourceType.AGGREGATED_DAY.toString(), aggregationView),
                is(true));
    }

    @Test
    public void testshouldQueryUseAggregationViewWhereThereIsntAggregationGroupWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo("some agg table", EventDataSourceType.AGGREGATED_DAY));
        assertThat(objToTest.shouldQueryUseAggregationView(TYPE_APN, EventDataSourceType.AGGREGATED_15MIN.toString(),
                aggregationViews), is(false));

    }

    @Test
    public void testshouldQueryUseAggregationViewWhereThereIsAggregationGroupWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo("some agg table", EventDataSourceType.AGGREGATED_DAY));
        assertThat(objToTest.shouldQueryUseAggregationView(TYPE_APN, EventDataSourceType.AGGREGATED_DAY.toString(),
                aggregationViews), is(true));
    }

    @Test
    public void testgetErrorAggregationViewWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        final String aggregationKey = "VEND";
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(aggregationKey, EventDataSourceType.AGGREGATED_15MIN));
        final String techPackName = EVENT_E_SGEH;
        final String timeRange = "_15MIN";
        final String expectedResult = techPackName + UNDERSCORE + aggregationKey + UNDERSCORE + KEY_TYPE_ERR
                + timeRange;
        assertThat(objToTest.getErrorAggregationView(TYPE_APN, timeRange, techPackName, aggregationViews),
                is(expectedResult));
    }

    @Test
    public void testgetSuccessAggregationViewWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        final String aggregationKey = "VEND";
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(aggregationKey, EventDataSourceType.AGGREGATED_15MIN));
        final String techPackName = EVENT_E_SGEH;
        final String timeRange = "_15MIN";
        final String expectedResult = techPackName + UNDERSCORE + aggregationKey + UNDERSCORE + KEY_TYPE_SUC
                + timeRange;
        assertThat(objToTest.getSuccessAggregationView(TYPE_APN, timeRange, techPackName, aggregationViews),
                is(expectedResult));
    }

    @Test
    public void testshouldQueryUseMultipleAggregationViewWhereThereIsntAggregationGroup() {
        final AggregationTableInfo aggregationView = new AggregationTableInfo("some agg table",
                "another - some agg table", EventDataSourceType.AGGREGATED_DAY);
        assertThat(objToTest.shouldQueryUseAggregationView(EventDataSourceType.AGGREGATED_15MIN.toString(),
                aggregationView), is(false));
    }

    @Test
    public void testshouldQueryUseMultipleAggregationViewWhereThereIsAggregationGroup() {
        final AggregationTableInfo aggregationView = new AggregationTableInfo("some agg table",
                "another - some agg table", EventDataSourceType.AGGREGATED_DAY);
        assertThat(
                objToTest.shouldQueryUseAggregationView(EventDataSourceType.AGGREGATED_DAY.toString(), aggregationView),
                is(true));
    }

    @Test
    public void testshouldQueryUseMultipleAggregationViewWhereThereIsntAggregationGroupWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo("some agg table", "another - some agg table",
                EventDataSourceType.AGGREGATED_DAY));
        assertThat(objToTest.shouldQueryUseAggregationView(TYPE_APN, EventDataSourceType.AGGREGATED_15MIN.toString(),
                aggregationViews), is(false));
    }

    @Test
    public void testshouldQueryUseMultipleAggregationViewWhereThereIsAggregationGroupWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo("some agg table", "another - some agg table",
                EventDataSourceType.AGGREGATED_DAY));
        assertThat(objToTest.shouldQueryUseAggregationView(TYPE_APN, EventDataSourceType.AGGREGATED_DAY.toString(),
                aggregationViews), is(true));

    }

    @Test
    public void testgetErrorMultipleAggregationViewWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        final String aggregationKey = "HIER3_CELL";
        final String aggregationKeyAlternative = "THIER3_CELL";
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(aggregationKey, aggregationKeyAlternative,
                EventDataSourceType.AGGREGATED_15MIN));
        final String techPackName = EVENT_E_RAN_HFA;
        final String timeRange = "_15MIN";
        final String expectedResult = techPackName + UNDERSCORE + aggregationKey + UNDERSCORE + KEY_TYPE_ERR
                + timeRange;
        assertThat(objToTest.getErrorAggregationView(TYPE_APN, timeRange, techPackName, aggregationViews),
                is(expectedResult));
    }

    @Test
    public void testgetSuccessMultipleAggregationViewWithMap() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        final String aggregationKey = "HIER3_CELL";
        final String aggregationKeyAlternative = "THIER3_CELL";
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(aggregationKey, aggregationKeyAlternative,
                EventDataSourceType.AGGREGATED_15MIN));
        final String techPackName = EVENT_E_RAN_HFA;
        final String timeRange = "_15MIN";
        final String expectedResult = techPackName + UNDERSCORE + aggregationKey + UNDERSCORE + KEY_TYPE_SUC
                + timeRange;
        assertThat(objToTest.getSuccessAggregationView(TYPE_APN, timeRange, techPackName, aggregationViews),
                is(expectedResult));
    }
}
