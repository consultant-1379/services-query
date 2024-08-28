/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;
import static com.ericsson.eniq.events.server.utils.techpacks.ViewTypeSelector.*;

import java.util.*;

import javax.ejb.*;

import com.ericsson.eniq.events.server.common.*;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeHelper;

/**
 * Class responsible for creating the TechPackList object, containing information about the raw and aggregation tables applicable for given tech packs
 * and given time ranges and user input parameters
 */
@Stateless
@Local
public class TechPackListFactory {
    private final static Map<String, String> typeToTPAggregationTableMapping = new HashMap<String, String>();

    static {
        typeToTPAggregationTableMapping.put(APN_EVENTID, TYPE_APN);
        typeToTPAggregationTableMapping.put(EVNTSRC_EVENTID, TYPE_SGSN);
        typeToTPAggregationTableMapping.put(TYPE_TAC, "TERM");
        typeToTPAggregationTableMapping.put(MANUF_TAC_EVENTID, "TERM");
        typeToTPAggregationTableMapping.put(NO_TABLE, NO_TABLE);
    }

    @EJB
    private RawTableFetcher rawTableFetcher;

    @EJB
    private DateTimeHelper dateTimeHelper;

    @EJB
    private TechPackLicensingService techPackLicensingService;

    @EJB
    private ApplicationConfigManager applicationConfigManager;

    /**
     * Determine whether given query should use the aggregation or raw tables for query
     * <p/>
     * is to be obsoleted
     */
    public boolean shouldQueryUseAggregationView(final String timerange, final AggregationTableInfo aggregationTableInfo) {
        if (EventDataSourceType.AGGREGATED_1MIN.toString().equalsIgnoreCase(timerange) && !applicationConfigManager.getOneMinuteAggregation()) {
            return false;
        }
        return aggregationTableInfo != null && aggregationTableInfo.hasAggregationTimeRangeFor(timerange);
    }

    /**
     * Create the success aggregation view for the provided parameters.
     * <p/>
     * This method has public access for backward compatibility only - it is used by the BaseResource class, which is to be obsoleted
     */
    private String getSuccessAggregationView(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo) {
        if (techPackName.startsWith(EVENT_E_DVTP_DT_TPNAME)) {
            return getPlainAggregationView(time, techPackName, aggregationTableInfo);
        }
        return getAggregationView(time, techPackName, aggregationTableInfo, KEY_TYPE_SUC);
    }

    /**
     * Gets the full name of the aggregation view
     * 
     * @param time one of _RAW, _1MIN, _15MIN or _DAY
     * @param techPackName name of the tech pack eg EVENT_E_SGEH
     * @param aggregationTableInfo map of aggregation views available for this tech pack
     * @param key one of ERR, SUC
     * @return the full name of the aggregation view
     */
    private String getAggregationView(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo, final String key) {
        final String aggregationViewForQueryType = aggregationTableInfo.getAggregationView();
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(techPackName);
        stringBuilder.append(UNDERSCORE);
        stringBuilder.append(aggregationViewForQueryType);
        if (key != null && key.length() > 0) {
            stringBuilder.append(UNDERSCORE);
            stringBuilder.append(key);
        }
        stringBuilder.append(time);
        return stringBuilder.toString();
    }

    /**
     * Gets the map of lists of the full names of aggregation views based on measurement type (the key are aggregation sets)
     * 
     * @param time one of _RAW, _1MIN, _15MIN or _DAY
     * @param techPackName name of the tech pack eg EVENT_E_SGEH
     * @param aggregationTableInfo map of aggregation views available for this tech pack
     * @param measurementTypes list of measurement types
     * @param key one of ERR, SUC
     * 
     * @return the map of lists of the full names of aggregation views based on measurement type (the key are aggregation sets)
     */
    private Map<String, List<String>> getAggregationViewsWithMeasurementTypes(final String time, final String techPackName,
                                                                              final AggregationTableInfo aggregationTableInfo,
                                                                              final List<String> measurementTypes, final String key) {

        final Map<String, List<String>> aggregationViewsWithMeasurementTypesMap = new HashMap<String, List<String>>();
        final List<String> allAggregationViewsList = aggregationTableInfo.getAggregationViewsList();
        for (final String aggregationViewForQueryType : allAggregationViewsList) {
            final List<String> aggregationViewsWithMeasurementTypesList = new ArrayList<String>();
            for (final String measurementType : measurementTypes) {
                final StringBuilder stringBuilder = new StringBuilder(); //NOPMD
                stringBuilder.append(techPackName);
                stringBuilder.append(UNDERSCORE);
                stringBuilder.append(measurementType);
                stringBuilder.append(UNDERSCORE);
                stringBuilder.append(aggregationViewForQueryType);
                if (key != null && key.length() > 0) {
                    stringBuilder.append(UNDERSCORE);
                    stringBuilder.append(key);
                }
                stringBuilder.append(time);
                aggregationViewsWithMeasurementTypesList.add(stringBuilder.toString());
            }

            aggregationViewsWithMeasurementTypesMap.put(aggregationViewForQueryType, aggregationViewsWithMeasurementTypesList);
        }

        return aggregationViewsWithMeasurementTypesMap;
    }

    /**
     * Create the error aggregation view for the provided parameters.
     * <p/>
     * This method has public access for backward compatibility only - it is used by the BaseResource class, which is to be obsoleted
     */
    private String getErrorAggregationView(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo) {
        if (techPackName.startsWith(EVENT_E_DVTP_DT_TPNAME)) {
            return getPlainAggregationView(time, techPackName, aggregationTableInfo);
        }
        return getAggregationView(time, techPackName, aggregationTableInfo, KEY_TYPE_ERR);
    }

    /**
     * Determine the raw and aggregation tables to use for this query Note that this method currently fetches the raw tables for every query
     * regardless of whether the query requires the raw tables
     * 
     * @return a TechPackList object containing the names of the raw and aggregation tables to query for this request
     */
    public TechPackList createTechPackList(final List<String> techPacks, final FormattedDateTimeRange dateTimeRange,
                                           final AggregationTableInfo aggregationTableInfo) {
        return createTechPackListWithSpecifiedAggregation(techPacks, dateTimeRange, aggregationTableInfo,
                dateTimeHelper.getEventDataSourceType(dateTimeRange));
    }

    /**
     * @param techPacks
     * @param dateTimeRange
     * @param aggregationTableInfo
     * @param dataSourceType
     * @return
     */
    public TechPackList createTechPackListWithSpecifiedAggregation(final List<String> techPacks, final FormattedDateTimeRange dateTimeRange,
                                                                   final AggregationTableInfo aggregationTableInfo,
                                                                   final EventDataSourceType dataSourceType) {
        final TechPackList techPackTables = new TechPackList();
        final String timeRange = dataSourceType.getValue();
        techPackTables.setShouldQueryUseAggregationView(shouldQueryUseAggregationView(timeRange, aggregationTableInfo));
        final String err_time = returnErrorAggregateViewType(dataSourceType);
        for (final String techPackName : techPacks) {
            final TechPackRepresentation techPack = new TechPackRepresentation(techPackName);
            if (isTechPackLicensed(techPack.getLicencedName())) {
                final String suc_time = returnSuccessAggregateViewType(dataSourceType, techPackName);
                techPack.setErrAggregationView(getErrorAggregationView(err_time, techPackName, aggregationTableInfo));
                techPack.setSucAggregationView(getSuccessAggregationView(suc_time, techPackName, aggregationTableInfo));
                techPack.setPlainAggregationView(getPlainAggregationView(err_time, techPackName, aggregationTableInfo));
                techPack.setErrRawTables(rawTableFetcher.getRawErrTables(dateTimeRange, techPackName));
                techPack.setSucRawTables(rawTableFetcher.getRawSucTables(dateTimeRange, techPackName));
                techPack.setRawTables(rawTableFetcher.getRawTables(dateTimeRange, techPackName));
                techPack.setStaticLookUpTable(getMatchingDIMTechPack(techPackName));
                techPackTables.addTechPack(techPackName, techPack);
            }
        }

        return techPackTables;
    }

    /**
     * Determine the raw and aggregation tables to use for this query Note that this method currently fetches the raw tables for every query
     * regardless of whether the query requires the raw tables
     * 
     * @param rawTableKeys list of the raw table keys
     * @return a TechPackList object containing the names of the raw and aggregation tables to query for this request
     */
    public TechPackList createTechPackListWithKeys(final List<String> techPacks, final List<String> rawTableKeys,
                                                   final FormattedDateTimeRange dateTimeRange, final AggregationTableInfo aggregationTableInfo) {
        final TechPackList techPackTables = new TechPackList();
        final String timeRange = dateTimeHelper.getEventDataSourceType(dateTimeRange).getValue();
        techPackTables.setShouldQueryUseAggregationView(shouldQueryUseAggregationView(timeRange, aggregationTableInfo));
        final String err_time = returnErrorAggregateViewType(dateTimeHelper.getEventDataSourceType(dateTimeRange));
        for (final String techPackName : techPacks) {
            final TechPackRepresentation techPack = new TechPackRepresentation(techPackName);
            if (isTechPackLicensed(techPack.getLicencedName())) {
                final String suc_time = returnSuccessAggregateViewType(dateTimeHelper.getEventDataSourceType(dateTimeRange), techPackName);

                techPack.setErrAggregationView(getErrorAggregationView(err_time, techPackName, aggregationTableInfo));
                techPack.setSucAggregationView(getSuccessAggregationView(suc_time, techPackName, aggregationTableInfo));
                techPack.setPlainAggregationView(getPlainAggregationView(err_time, techPackName, aggregationTableInfo));

                techPack.setStaticLookUpTable(getMatchingDIMTechPack(techPackName));
                techPack.setAllCallsAggregationView(getAllCallsAggregationView(err_time, techPackName, aggregationTableInfo));

                for (final String rawTableKey : rawTableKeys) {
                    final List<String> rawTables = rawTableFetcher.fetchRawTablesForTechPackAndKey(dateTimeRange, techPackName, rawTableKey);
                    if (rawTableKey.equals(ERR)) {
                        techPack.setErrRawTables(rawTables);
                    } else if (rawTableKey.equals(SUC)) {
                        techPack.setSucRawTables(rawTables);
                    }
                    techPack.setRawTables(rawTables);

                }
                techPackTables.addTechPack(techPackName, techPack);
            }
        }

        return techPackTables;
    }

    public TechPackList createTechPackListWithMeasuermentType(final List<String> techPacks, final List<String> measurementTypes,
                                                              final FormattedDateTimeRange dateTimeRange,
                                                              final AggregationTableInfo aggregationTableInfo, final String tableSuffixKey) {
        final TechPackList techPackTables = new TechPackList();
        final String timeRange = dateTimeHelper.getEventDataSourceType(dateTimeRange).getValue();
        techPackTables.setShouldQueryUseAggregationView(shouldQueryUseAggregationView(timeRange, aggregationTableInfo));
        final String time = ApplicationConstants.returnAggregateViewType(timeRange);
        for (final String techPackName : techPacks) {
            final TechPackRepresentation techPack = new TechPackRepresentation(techPackName);
            if (isTechPackLicensed(techPack.getLicencedName())) {
                if ((techPackName.startsWith(EVENT_E_RAN_SESSION_TPNAME) || techPackName.startsWith(EVENT_E_CORE_SESSION_TPNAME))
                        && techPacks.size() > 1) {
                    techPack.setPlainAggregationView(getPlainAggregationView(time, techPackName, aggregationTableInfo));
                    techPack.setRawTables(rawTableFetcher.getRawTables(dateTimeRange, techPackName));
                } else {
                    techPack.setAggregationViewsWithMeasurementTypes(getAggregationViewsWithMeasurementTypes(time, techPackName,
                            aggregationTableInfo, measurementTypes, tableSuffixKey));
                    techPack.setRawTablesWithMeausreTypes(rawTableFetcher.getRawTablesWithMeasurementTypesAndKeys(dateTimeRange, techPackName,
                            measurementTypes, tableSuffixKey));

                    if (techPack.getRawSucTables() == null || techPack.getRawSucTables().size() == 0) {
                        techPack.setSucRawTables(rawTableFetcher.getRawSucTables(dateTimeRange, techPackName));
                    }

                    if (techPack.getRawErrTables() == null || techPack.getRawErrTables().size() == 0) {
                        techPack.setErrRawTables(rawTableFetcher.getRawErrTables(dateTimeRange, techPackName));
                    }

                }
                techPack.setStaticLookUpTable(getMatchingDIMTechPack(techPackName));
                techPackTables.addTechPack(techPackName, techPack);
            }
        }

        return techPackTables;
    }

    private String getPlainAggregationView(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo) {
        if (techPackName.startsWith(EVENT_E_DVTP_DT_TPNAME)) {
            return getPlainAggregationViewForDVTP(time, techPackName, aggregationTableInfo);
        }
        return getAggregationView(time, techPackName, aggregationTableInfo, EMPTY_STRING);
    }

    /**
     * Method to cater for the fact that GSN tables need special handling. ie, no _1Min tables and also some types are different see
     * typeToTPAggregationTableMapping
     * 
     * @param time The time view, eg _1Min, _15Min _Day _Raw. _1Min will be converted to _15Min
     * @param techPackName The Techpack Name
     * @param aggregationTableInfo The default Aggregation Table Info. Table names will be modified to cater for GSN tables
     * @return
     */
    private String getPlainAggregationViewForDVTP(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo) {
        final String tempTime = time;
        if (ONE_MINUTE_VIEW.equals(time)) {
            return "";
        }
        final AggregationTableInfo tableInfo = new AggregationTableInfo(
                typeToTPAggregationTableMapping.get(aggregationTableInfo.getAggregationView()), aggregationTableInfo.getAggregationTimeRanges());

        return getAggregationView(tempTime, techPackName, tableInfo, EMPTY_STRING);
    }

    private boolean isTechPackLicensed(final String techPackName) {
        return techPackLicensingService.isTechPackLicensed(techPackName);
    }

    /**
     * @param rawTableFetcher the rawTableFetcher to set
     */
    public void setRawTableFetcher(final RawTableFetcher rawTableFetcher) {
        this.rawTableFetcher = rawTableFetcher;
    }

    /**
     * @param dateTimeHelper the dateTimeHelper to set
     */
    public void setDateTimeHelper(final DateTimeHelper dateTimeHelper) {
        this.dateTimeHelper = dateTimeHelper;
    }

    public String getMatchingDIMTechPack(final String techPackName) {
        return TechPackData.DIM_E_TECHPACKS.get(techPackName);
    }

    public void setTechPackLicensingService(final TechPackLicensingService techPackLicensingService) {
        this.techPackLicensingService = techPackLicensingService;
    }

    public String getErrorAggregationView(final String type, final String time, final String techPackName,
                                          final Map<String, AggregationTableInfo> aggregationViews) {
        final AggregationTableInfo aggregationTableInfo = aggregationViews.get(type);
        return getErrorAggregationView(time, techPackName, aggregationTableInfo);
    }

    public String getSuccessAggregationView(final String type, final String time, final String techPackName,
                                            final Map<String, AggregationTableInfo> aggregationViews) {
        final AggregationTableInfo aggregationTableInfo = aggregationViews.get(type);
        return getSuccessAggregationView(time, techPackName, aggregationTableInfo);
    }

    public boolean shouldQueryUseAggregationView(final String type, final String timerange, final Map<String, AggregationTableInfo> aggregationViews) {
        final AggregationTableInfo aggregationTableInfo = aggregationViews.get(type);
        return shouldQueryUseAggregationView(timerange, aggregationTableInfo);
    }

    /**
     * @param applicationConfigManager the applicationConfigManager to set
     */
    public void setApplicationConfigManager(final ApplicationConfigManager applicationConfigManager) {
        this.applicationConfigManager = applicationConfigManager;
    }

    /**
     * Create the all aggregation view for the provided parameters.
     */
    private String getAllCallsAggregationView(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo) {
        return getAggregationViewAllCalls(time, techPackName, aggregationTableInfo, KEY_TYPE_ALLCALLS);
    }

    /**
     * Gets the full name of the aggregation view without the aggregationTableInfo
     * 
     * @param time one of _RAW, _1MIN, _15MIN or _DAY
     * @param techPackName name of the tech pack eg EVENT_E_SGEH
     * @param aggregationTableInfo the aggregation view
     * @param key one of ERR, SUC, ALLCALLS
     * @return the full name of the aggregation view
     */
    private String getAggregationViewAllCalls(final String time, final String techPackName, final AggregationTableInfo aggregationTableInfo,
                                              final String key) {
        final String aggregationViewForQueryType = aggregationTableInfo.getSecondaryAggregationView();
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(techPackName);
        stringBuilder.append(UNDERSCORE);
        stringBuilder.append(aggregationViewForQueryType);
        System.out.println("aggregationViewForQueryType: " + aggregationViewForQueryType);
        if (key != null && key.length() > 0) {
            stringBuilder.append(UNDERSCORE);
            stringBuilder.append(key);
        }
        stringBuilder.append(time);
        return stringBuilder.toString();
    }

}
