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

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.ejb.*;

import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.utils.*;
import com.ericsson.eniq.events.server.utils.techpacks.timerangequeries.TimerangeQuerier;
import com.ericsson.eniq.events.server.utils.techpacks.timerangequeries.TimerangeQuerierFactory;

/**
 * Class responsible for fetching the raw tables to use for a given raw view and time range This class fetches this information from the following
 * sources:
 * <ol>
 * <li>Query the ENIQ engine to fetch the raw tables applicable for the given raw view and time range</li>
 * <li>If this call to the ENIQ engine fails, or if the call returns an empty list of tables, this class will then query the ENIQ engine for the
 * latest tables for this raw view</li>
 * <li>If this call to the ENIQ engine fails, or if the call returns an empty list of tables, this class will run an SQL query on the time range view
 * for the given tech pack. This SQL query selects the raw tables for the given time range from the appropriate time range view</li>
 * <li>If this SQL query returns no tables, this class will then run an SQL query on the time range view to fetch the latest tables</li>
 * </ol>
 */
@Stateless
@Local
public class RawTableFetcher {

    @EJB
    private RMIEngineUtils rmiEngineUtils;

    /**
     * @return the rmiEngineUtils
     */
    public RMIEngineUtils getRmiEngineUtils() {
        return this.rmiEngineUtils;
    }

    @EJB
    private TimerangeQuerierFactory timerangeQuerierFactory;

    /**
     * Query for the SUC tables for this tech pack and this time range eg for the EVENT_E_SGEH tech pack, this metho will look for the
     * EVENT_E_SGEH_SUC_RAW tables
     * 
     * @param dateTimeRange date time range to query for
     * @param techPackName the tech pack to query for eg EVENT_E_SGEH
     * @return list of raw tables for this tech pack that are applicable for this time range
     */
    public List<String> getRawSucTables(final FormattedDateTimeRange dateTimeRange, final String techPackName) {
        return fetchRawTablesForTechPackAndKeyThatHaveRawErrOrSucTables(dateTimeRange, techPackName, KEY_TYPE_SUC);
    }

    private List<String> fetchRawTablesForTechPackAndKeyThatHaveRawErrOrSucTables(final FormattedDateTimeRange dateTimeRange,
                                                                                  final String techPackName, final String typeOfTable) {
        if (getTechPacksThatHaveRawErrOrSucTables().contains(techPackName)) {
            return fetchRawTablesForTechPackAndKey(dateTimeRange, techPackName, typeOfTable);
        }
        return new ArrayList<String>();
    }

    public List<String> fetchRawTablesForTechPackAndKey(final FormattedDateTimeRange dateTimeRange, final String techPackName,
                                                        final String typeOfTable) {
        final String viewName = putTogetherViewName(techPackName, typeOfTable);
        return fetchRawTables(dateTimeRange, viewName);
    }

    /**
     * Query for the ERR tables for this tech pack and this time range eg for the EVENT_E_SGEH tech pack, this method will look for the
     * EVENT_E_SGEH_ERR_RAW tables
     * 
     * @param dateTimeRange date time range to query for
     * @param techPackName the tech pack to query for eg EVENT_E_SGEH
     * @return list of raw tables for this tech pack that are applicable for this time range
     */
    public List<String> getRawErrTables(final FormattedDateTimeRange dateTimeRange, final String techPackName) {
        return fetchRawTablesForTechPackAndKeyThatHaveRawErrOrSucTables(dateTimeRange, techPackName, KEY_TYPE_ERR);
    }

    private List<String> getTechPacksThatHaveRawErrOrSucTables() {
        return TechPackData.listOfTechPacksThatHaveRawErrOrSucTables;
    }

    /**
     * This function is used for getting the raw tables which could replace a raw view for a particular time range The function first uses the RMI
     * engine, if it fails then it uses the SQL query
     * <p/>
     * Currently it has been used for timeout issues in Ranking(#MultipleRankingResource) and Event Analysis(#EventAnalysisResource).
     * 
     * @param dateTimeRange the date time range
     * @param rawTableType to differentiate which RAW tables we need (e.g. error, success)
     * @param templateKeyForRawTablesList key passed to template eg rawAllErrTables
     * @return the raw table names
     * @deprecated the methods in this class that take in a dateTimeRange and a techpackName should be used instead ie the getRawTables(),
     *             getRawErrTables() or getRawSucTables() methods
     */
    @Deprecated
    public List<String> getRAWTables(final FormattedDateTimeRange dateTimeRange, final String rawTableType, final String templateKeyForRawTablesList) {
        final String[] viewNames = getViewNamesForTemplateKey(rawTableType, templateKeyForRawTablesList);
        return fetchRawTables(dateTimeRange, viewNames);
    }

    private List<String> fetchRawTables(final FormattedDateTimeRange dateTimeRange, final String... viewNames) {
        List<String> rawtables = new ArrayList<String>();
        try {
            rawtables = getTablesFromEngine(dateTimeRange, viewNames);
        } catch (final ParseException e) {
            ServicesLogger.warn(getClass().toString(), "getRAWTables",
                    "Exception thrown while trying to parse the date in order to get the table names from the engine cache. " + e.getMessage());
        }

        if (rawtables == null || rawtables.isEmpty()) {
            rawtables = getLatestTablesFromEngine(viewNames);
            if (rawtables.isEmpty()) {
                // if RMI fails then use SQL query to get the table names and log the event
                for (final String view : viewNames) {
                    rawtables.addAll(getTimerangeQuerier().getRAWTablesUsingQuery(dateTimeRange, view));
                }
                if (rawtables.isEmpty()) {
                    for (final String view : viewNames) {
                        rawtables.addAll(getTimerangeQuerier().getLatestTablesUsingQuery(view));
                    }
                }
            }
        }
        return rawtables;
    }

    private TimerangeQuerier getTimerangeQuerier() {
        return timerangeQuerierFactory.getTimerangeQuerier();
    }

    String[] getViewNamesForTemplateKey(final String rawTableType, final String templateKeyForRawTablesList) {
        final String[] viewNames;
        if (isGetAllTables(templateKeyForRawTablesList)) {
            viewNames = new String[] { getViewName(EVENT_E_SGEH, rawTableType), getViewName(EVENT_E_LTE, rawTableType) };
        } else if (isLTETable(templateKeyForRawTablesList)) {
            viewNames = new String[] { getViewName(EVENT_E_LTE, rawTableType) };
        } else if (isGetDTTables(templateKeyForRawTablesList)) {
            viewNames = new String[] { EVENT_E_DVTP_DT_RAW };
        } else {
            viewNames = new String[] { getViewName(EVENT_E_SGEH, rawTableType) };
        }
        return viewNames;
    }

    private String getViewName(final String techPackName, final String rawTableType) {
        if (rawTableType.equals(KEY_TYPE_SUM) || rawTableType.equals(KEY_TYPE_TOTAL)) {
            return techPackName + RAW_VIEW;
        }
        return putTogetherViewName(techPackName, rawTableType);
    }

    private String putTogetherViewName(final String techPackName, final String key) {
        return techPackName + UNDERSCORE + key + RAW_VIEW;
    }

    /**
     * Checks if is get dt tables.
     * 
     * @param templateKey the template key
     * @return true, if checks if is get dt tables
     * @deprecated included here for backwards compatiblity only
     */
    @Deprecated
    private boolean isGetDTTables(final String templateKey) {
        return templateKey.equals(RAW_DT_TABLES);
    }

    /**
     * return true if template key for table name eg rawLteErrTables contains the text lte (not case sensitive) and doesn't contain the text nonlte
     * false otherwise.
     * 
     * @return true if templateKey indicates an lte table, false otherwise
     * @deprecated included here for backwards compatiblity only
     */
    @Deprecated
    private boolean isLTETable(final String templateKey) {
        return templateKey.toUpperCase().contains("LTE") && !templateKey.toUpperCase().contains("NONLTE");
    }

    /**
     * Checks if is get all tables.
     * 
     * @param templateKey the template key
     * @return true, if checks if is get all tables
     * @deprecated included here for backwards compatiblity only
     */
    @Deprecated
    private boolean isGetAllTables(final String templateKey) {
        return templateKey.toUpperCase().contains("ALL");
    }

    /**
     * Gets the latest tables from engine.
     * 
     * @param viewNames key passed to template eg rawAllErrTables
     * @return
     */
    List<String> getLatestTablesFromEngine(final String... viewNames) {
        return rmiEngineUtils.getLatestTableNames(viewNames);
    }

    /**
     * Gets the tables from engine.
     * 
     * @param newDateTimeRange the new date time range
     * @param key the key
     * @param templateKey the template key
     * @param viewNames
     * @return the tables from engine
     * @throws ParseException the parse exception
     */
    private List<String> getTablesFromEngine(final FormattedDateTimeRange newDateTimeRange, final String... viewNames) throws ParseException {

        Timestamp startTimeStamp;
        Timestamp endTimeStamp;
        long rangeInMinutes = newDateTimeRange.getRangeInMinutes();
        List<String> tableList;

        if (rangeInMinutes >= MINUTES_IN_A_WEEK) {
            String utcOffset = DateTimeUtils.getUTCOffset();
            String startDateWithoutOffset = newDateTimeRange.getStartDateTime();
            String endDateWithoutOffset = newDateTimeRange.getEndDateTime();
            String adjustedStartDate = DateTimeUtils.getRawOffsetAdjustedTime(startDateWithoutOffset, utcOffset);
            String adjustedEndDate = DateTimeUtils.getRawOffsetAdjustedTime(endDateWithoutOffset, utcOffset);
            startTimeStamp = new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(adjustedStartDate).getTime());
            endTimeStamp = new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(adjustedEndDate).getTime());
            tableList = getRmiEngineUtils().getTableNames(startTimeStamp, endTimeStamp, viewNames);            
        } else {

            String startDateTime = newDateTimeRange.getStartDateTime();
            String endDateTime = newDateTimeRange.getEndDateTime();
            startTimeStamp = new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(startDateTime).getTime());
            endTimeStamp = new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(endDateTime).getTime());
            tableList = getRmiEngineUtils().getTableNames(startTimeStamp, endTimeStamp, viewNames);
        }
        return tableList;
    }

    public void setRmiEngineUtils(final RMIEngineUtils rmiEngineUtils) {
        this.rmiEngineUtils = rmiEngineUtils;
    }

    /**
     * Query for the (raw, ie neither ERR or SUC) tables for this tech pack and this time range eg for the EVENT_E_RAN_CFA tech pack, this method will
     * look for the EVENT_E_RAN_CFA_RAW tables
     * 
     * @param dateTimeRange date time range to query for
     * @param techPackName the tech pack to query for eg EVENT_E_SGEH
     * @return list of raw tables for this tech pack that are applicable for this time range
     */
    public List<String> getRawTables(final FormattedDateTimeRange dateTimeRange, final String techPackName) {
        final String viewName = techPackName + RAW_VIEW;
        return fetchRawTables(dateTimeRange, viewName);
    }

    /**
     * Query for the (raw, ie neither ERR or SUC) tables for this tech pack and this time range with measurement types Eg for the EVENT_E_RAN_HFA tech
     * pack, this method will look for the EVENT_E_RAN_HFA_SOHO_ERR_RAW tables
     * 
     * @param dateTimeRange date time range to query for
     * @param techPackName the tech pack to query for eg EVENT_E_SGEH
     * @param measurementTypes the measurement type of table eg IRAT
     * @param rawTableKey they table suffix key eg _ERR
     * @return list of raw tables for this tech pack that are applicable for this time range
     */
    public List<String> getRawTablesWithMeasurementTypesAndKeys(final FormattedDateTimeRange dateTimeRange, final String techPackName,
                                                                final List<String> measurementTypes, final String rawTableKey) {
        final List<String> rawTables = new ArrayList<String>();

        for (final String type : measurementTypes) {
            final StringBuilder viewNameBuilder = new StringBuilder(); //NOPMD
            viewNameBuilder.append(techPackName);
            if (type != null) {
                viewNameBuilder.append(UNDERSCORE);
                viewNameBuilder.append(type);
            }

            if (rawTableKey != null && rawTableKey.length() > 0) {
                viewNameBuilder.append(UNDERSCORE);
                viewNameBuilder.append(rawTableKey);
            }
            viewNameBuilder.append(RAW_VIEW);
            rawTables.addAll(fetchRawTables(dateTimeRange, viewNameBuilder.toString()));
        }

        return rawTables;
    }

    /**
     * @param timerangeQuerierFactory the timerangeQuerierFactory to set
     */
    public void setTimerangeQuerierFactory(final TimerangeQuerierFactory timerangeQuerierFactory) {
        this.timerangeQuerierFactory = timerangeQuerierFactory;
    }

}
