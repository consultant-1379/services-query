/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2015
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

import java.util.HashMap;
import java.util.Map;

import javax.ejb.*;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.EventIDConstants;
import com.ericsson.eniq.events.server.query.*;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeWhiteList;
import com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker;

@DependsOn("RATDescriptionMappingsService")
@Singleton
@LocalBean
@Startup
public class QueryUtils {

    private enum TypesOfHashId {
        EVENTSRCMSC_HASH, CONTROLLER_HASH, CELL_HASH
    }

    public static final long UNSET_LONG_VALUE = 0;

    private static final int NEGATIVE_VALUE = -1;

    @EJB
    // required for converting RAT parameters between integer values and
    // string field descriptions
    private RATDescriptionMappingUtils ratDescriptionMappingUtils;

    @EJB
    private ParameterChecker parameterChecker;

    @EJB
    HashUtilities hashUtilities;

    @EJB
    private DateTimeWhiteList datetimeWhiteList;

    @EJB
    private ApplicationConfigManager applicationConfigManager;

    /**
     * Alternative to viewAggregationTimeRangeGrid Type safe and more readable. Based on time range indicate the data type source for grid data
     * whether aggregated or raw.
     * 
     * @param timeRange
     *        The time range to use
     * @return EventDataSourceType instance
     * @throws WebApplicationException
     *         Invalid time format
     */
    public EventDataSourceType getEventDataSourceType(final FormattedDateTimeRange timeRange) throws WebApplicationException {
        return getEventDataSourceType(timeRange.getRangeInMinutes());

    }

    /**
     * Gets the event data source type for grid with one min aggregated.
     * 
     * @param timeRangeInMinutes
     * @return the event data source type for grid with one min aggregated
     * @throws WebApplicationException
     *         the web application exception
     */
    private EventDataSourceType getEventDataSourceType(final long timeRangeInMinutes) throws WebApplicationException {

        EventDataSourceType result;

        if (DateTimeUtils.oneMinuteAggregationRange(timeRangeInMinutes) && applicationConfigManager.getOneMinuteAggregation()) {
            result = EventDataSourceType.AGGREGATED_1MIN;
        } else if (DateTimeUtils.fifteenMinuteAggregatedRangeForGrid(timeRangeInMinutes)) {
            result = EventDataSourceType.AGGREGATED_15MIN;
        } else if (DateTimeUtils.rawEventRange(timeRangeInMinutes)
                || (DateTimeUtils.oneMinuteAggregationRange(timeRangeInMinutes) && !applicationConfigManager.getOneMinuteAggregation())) {
            result = EventDataSourceType.RAW;
        } else {
            result = EventDataSourceType.AGGREGATED_DAY;
        }

        return result;
    }

    /**
     * Gets the drill type.
     * 
     * @param requestParameters
     *        the request parameters
     * @return the drill type
     */
    public String getDrillType(final MultivaluedMap<String, String> requestParameters) {

        final boolean checkSGSNKey = requestParameters.containsKey(SGSN_PARAM);
        final boolean checkCELLKey = requestParameters.containsKey(CELL_PARAM);
        final boolean checkBSCKey = requestParameters.containsKey(BSC_PARAM);
        final boolean checkCCKey = requestParameters.containsKey(TYPE_CAUSE_CODE);
        final boolean checkSCCKey = requestParameters.containsKey(TYPE_SUB_CAUSE_CODE);
        final boolean checkVendorKey = requestParameters.containsKey(VENDOR_PARAM);

        if (!checkSGSNKey && !checkBSCKey && !checkCCKey && !checkCELLKey && !checkSCCKey && !checkVendorKey) {

            return TYPE_APN;
        } else if (!checkBSCKey && !checkCCKey && !checkCELLKey && !checkSCCKey && !checkVendorKey) {
            return TYPE_SGSN;

        } else if (!checkCCKey && !checkCELLKey && !checkSCCKey) {
            return TYPE_BSC;
        } else if (!checkCCKey && !checkSCCKey) {
            return TYPE_CELL;
        }
        return EVENTS_DRILL_TYPE_PARAM;
    }

    /**
     * Gets the drill type for MSS
     * 
     * @param requestParameters
     *        the request parameters
     * @return the drill type
     */
    public String getDrillTypeForMss(final MultivaluedMap<String, String> requestParameters) {

        final String eventID = requestParameters.getFirst(EVENT_ID_PARAM);
        final boolean checkBSCKey = requestParameters.containsKey(CONTROLLER_SQL_ID);
        final boolean checkCELLKey = requestParameters.containsKey(CELL_SQL_ID);
        final boolean checkCCKey = requestParameters.containsKey(FAULT_CODE_PARAM);
        final boolean checkICCKey = requestParameters.containsKey(INTERNAL_CAUSE_CODE_PARAM);

        if (EventIDConstants.isMssLocationServiceEvent(eventID) || EventIDConstants.isMssSMSEvent(eventID)) {
            if (!checkBSCKey && !checkCELLKey) {
                return TYPE_MSC;
            } else if (!checkCELLKey) {
                return TYPE_BSC;
            }
            return EVENTS_DRILL_TYPE_PARAM;
        }
        if (!checkBSCKey && !checkCELLKey && !checkCCKey && !checkICCKey) {
            return TYPE_MSC;
        } else if (!checkCELLKey && !checkCCKey && !checkICCKey) {
            return TYPE_BSC;
        } else if (!checkCCKey && !checkICCKey) {
            return TYPE_CELL;
        }
        return EVENTS_DRILL_TYPE_PARAM;
    }

    public void addLocalDateParameters(final FormattedDateTimeRange dateTimeRange, final Map<String, QueryParameter> queryParameters) {

        if (this.getEventDataSourceType(dateTimeRange.getRangeInMinutes()) == EventDataSourceType.AGGREGATED_DAY) {
            queryParameters.put(LOCAL_DATE_FROM, QueryParameter.createStringParameter(dateTimeRange.getStartDateLocal()));
            queryParameters.put(LOCAL_DATE_TO, QueryParameter.createStringParameter(dateTimeRange.getEndDateLocal()));
        }
    }

    private void addAdjustedDateParameters(final FormattedDateTimeRange dateTimeRange, final Map<String, QueryParameter> queryParameters,
                                           final String tzOffSet) {
        final String dateFrom = dateTimeRange.getStartDateTime();
        final String dateTo = dateTimeRange.getEndDateTime();

        final String OffsetMinutes = DateTimeUtils.getTzOffsetInMinutes(tzOffSet);
        final String OffsetMinutesToAddWithoutSign = OffsetMinutes.substring(1);
        final int intValueOfMinutesToAdjust = (OffsetMinutes.substring(0, 1).equals(HYPHEN)) ? Integer.parseInt(OffsetMinutesToAddWithoutSign)
                : (NEGATIVE_VALUE) * Integer.parseInt(OffsetMinutesToAddWithoutSign);

        final String AdjustedDateFrom = DateTimeUtils.addMinutes(dateFrom, intValueOfMinutesToAdjust);
        final String AdjustedDateTo = DateTimeUtils.addMinutes(dateTo, intValueOfMinutesToAdjust);

        queryParameters.put(ADJUSTED_DATE_FROM, QueryParameter.createStringParameter(AdjustedDateFrom));
        queryParameters.put(ADJUSTED_DATE_TO, QueryParameter.createStringParameter(AdjustedDateTo));
    }

    /**
     * Maps request parameters to SQL query parameters.
     * 
     * @param requestParameters
     *        map of URI request parameters
     * @param dateTimeRange
     *        stringified date time range
     * @return URI paramaters mapped into query parameters
     */
    public Map<String, QueryParameter> mapRequestParameters(final MultivaluedMap<String, String> requestParameters,
                                                            final FormattedDateTimeRange dateTimeRange) {

        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();

        final class AddQueryParameter {

            void addString(final String parameter) {
                addStringToQueryParameters(requestParameters, queryParameters, parameter);
            }

            void addLong(final String parameter) {
                addLongToQueryParameters(requestParameters, queryParameters, parameter);
            }

            void addNullIfEmpty(final String parameter) {
                addStringParameterNullIfEmpty(requestParameters, queryParameters, parameter);
            }

        }
        final String node = requestParameters.getFirst(NODE_PARAM);

        final String type = requestParameters.getFirst(TYPE_PARAM);

        if (StringUtils.isNotBlank(node)) {
            final NodeValuesByType nodeValuesByType = new NodeValuesByType(node, type);
            requestParameters.putSingle(APN_PARAM, nodeValuesByType.getApn());
            requestParameters.putSingle(SGSN_PARAM, nodeValuesByType.getSgsn());
            requestParameters.putSingle(BSC_PARAM, nodeValuesByType.getBsc());
            requestParameters.putSingle(TAC_PARAM, nodeValuesByType.getTac());
            requestParameters.putSingle(CELL_PARAM, nodeValuesByType.getCell());
            requestParameters.putSingle(VENDOR_PARAM, nodeValuesByType.getVendor());
            requestParameters.putSingle(RAT_PARAM, nodeValuesByType.getAndConvertRatToIntegerValue(ratDescriptionMappingUtils));
            requestParameters.putSingle(EVENT_ID_PARAM, nodeValuesByType.getEventID());
            requestParameters.putSingle(EVENT_TYPE_PARAM, nodeValuesByType.getEventDescription());
        }

        final AddQueryParameter addQueryParameter = new AddQueryParameter();
        addQueryParameter.addLong(EVENT_ID_PARAM);
        addQueryParameter.addLong(CAUSE_CODE_PARAM);
        addQueryParameter.addLong(SUB_CAUSE_CODE_PARAM);
        addQueryParameter.addString(APN_PARAM);
        addQueryParameter.addString(SGSN_PARAM);
        addQueryParameter.addString(BSC_PARAM);
        addQueryParameter.addLong(TAC_PARAM);
        addQueryParameter.addLong(IMEISV_PARAM);
        addQueryParameter.addNullIfEmpty(CELL_PARAM);
        addQueryParameter.addNullIfEmpty(VENDOR_PARAM);
        addQueryParameter.addString(MAN_PARAM);
        addQueryParameter.addLong(IMSI_PARAM);
        addQueryParameter.addString(GROUP_NAME_PARAM);
        addQueryParameter.addLong(TYPE_CAUSE_CODE);
        addQueryParameter.addLong(TYPE_SUB_CAUSE_CODE);
        addQueryParameter.addLong(CAUSE_PROT_TYPE);
        addQueryParameter.addString(CAUSE_PROT_TYPE_HEADER);
        addQueryParameter.addLong(CAUSE_PROT_TYPE_PARAM);
        addQueryParameter.addLong(RAT_PARAM);
        addQueryParameter.addString(DAY_PARAM);
        addQueryParameter.addString(HOUR_PARAM);
        addQueryParameter.addLong(PTMSI_PARAM);
        addQueryParameter.addLong(EVENT_RESULT_PARAM);
        addQueryParameter.addLong(EVENT_TYPE_PARAM);
        addQueryParameter.addLong(MSISDN_PARAM);
        addQueryParameter.addLong(CATEGORY_ID_PARAM);

        if (dateTimeRange != null) {
            final String key = requestParameters.getFirst(KEY_PARAM);

            String startTime = dateTimeRange.getStartDateTime();
            String endTime = dateTimeRange.getEndDateTime();

            boolean isWeekOverrideApplied = false;

            /*
             * do UTC conversion, if timeRange >=1week & key=ERR, TOTAL or SUC or DV_DRILL_DOWN also allow UTC conversion of packet switch Subscriber
             * drills as Summary uses DAY tables
             */
            if ((dateTimeRange.getRangeInMinutes() > MINUTES_IN_A_WEEK) && StringUtils.isNotBlank(key)
                    && (key.equals(KEY_TYPE_ERR) || key.equals(KEY_TYPE_TOTAL) || key.equals(KEY_TYPE_SUC) || key.equals(KEY_TYPE_DV_DRILL_DOWN))) {
                startTime = DateTimeUtils.getUTCTime(dateTimeRange.getStartDateTime(), requestParameters.getFirst(TZ_OFFSET), DATE_TIME_FORMAT);
                endTime = DateTimeUtils.getUTCTime(dateTimeRange.getEndDateTime(), requestParameters.getFirst(TZ_OFFSET), DATE_TIME_FORMAT);
                isWeekOverrideApplied = true;
                queryParameters.put(ISWEEKOVERRIDE, QueryParameter.createStringParameter(String.valueOf(isWeekOverrideApplied)));
            }

            // Removes the DV_DRILL_DOWN Key used for handling
            // so that it has no effect on any other
            if (StringUtils.isNotBlank(key) && key.equals(KEY_TYPE_DV_DRILL_DOWN)) {
                requestParameters.remove(KEY_PARAM);
            }

            queryParameters.put(DATE_FROM, QueryParameter.createStringParameter(startTime));
            queryParameters.put(DATE_TO, QueryParameter.createStringParameter(endTime));

            addLocalDateParameters(dateTimeRange, queryParameters);
            if (!isWeekOverrideApplied &&(dateTimeRange.getRangeInMinutes() >= MINUTES_IN_A_WEEK) ) {
                addAdjustedDateParameters(dateTimeRange, queryParameters, requestParameters.getFirst(TZ_OFFSET));
            }

        }

        return queryParameters;
    }

    void addLongToQueryParameters(final MultivaluedMap<String, String> requestParameters, final Map<String, QueryParameter> queryParameters,
                                  final String parameter, final String parameterName) {
        if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {
            final String parameterValue = requestParameters.getFirst(parameter);
            try {
                final Long longParam = Long.valueOf(parameterValue);
                queryParameters.put(parameterName, QueryParameter.createLongParameter(longParam));
            } catch (final NumberFormatException e) {
                addStringToQueryParameters(requestParameters, queryParameters, parameter);
            }
        }

    }

    void addLongToQueryParameters(final MultivaluedMap<String, String> requestParameters, final Map<String, QueryParameter> queryParameters,
                                  final String parameter) {
        final String convertedParameterName = getConvertedParameterName(parameter);
        addLongToQueryParameters(requestParameters, queryParameters, parameter, convertedParameterName);
    }

    private void addStringToQueryParameters(final MultivaluedMap<String, String> requestParameters,
                                            final Map<String, QueryParameter> queryParameters, final String parameter, final String parameterName) {

        if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {
            final String parameterValue = requestParameters.getFirst(parameter);

            queryParameters.put(parameterName, QueryParameter.createStringParameter(parameterValue));
        }
    }

    void addStringToQueryParameters(final MultivaluedMap<String, String> requestParameters, final Map<String, QueryParameter> queryParameters,
                                    final String parameter) {
        final String convertedParameterName = getConvertedParameterName(parameter);
        addStringToQueryParameters(requestParameters, queryParameters, parameter, convertedParameterName);
    }

    void addStringParameterNullIfEmpty(final MultivaluedMap<String, String> requestParameters, final Map<String, QueryParameter> queryParameters,
                                       final String parameter) {
        if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {
            final String parameterValue = requestParameters.getFirst(parameter);
            if (parameterValue.equals(NULL)) {
                queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createNullParameter(java.sql.Types.VARCHAR));
            } else {
                queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createStringParameter(parameterValue));
            }
        }
    }

    /**
     * Maps request parameters to SQL query parameters.
     * 
     * The date/time parameters will be mapped to SQL parameters unless the templatePath is one that doesn't require date/time parameters - see
     * DateTimeWhiteList for more information on this
     * 
     * @param requestParameters
     *        map of URI request parameters
     * @param dateTimeRange
     *        stringified date time range
     * @param templatePath
     * @return URI paramaters mapped into query parameters
     */
    public Map<String, QueryParameter> getQueryParameters(final MultivaluedMap<String, String> requestParameters,
                                                          final FormattedDateTimeRange dateTimeRange, final String templatePath) {

        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();

        final class AddQueryParameter {

            void add(final String parameter) {
                if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {
                    final String parameterValue = requestParameters.getFirst(parameter);

                    queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createStringParameter(parameterValue));
                }
            }

            void addLong(final String parameter) {
                if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {
                    final String parameterValue = requestParameters.getFirst(parameter);
                    try {
                        final Long longParam = Long.valueOf(parameterValue);
                        queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createLongParameter(longParam));
                    } catch (final NumberFormatException e) {
                        add(parameter);
                    }
                }
            }

            void addNullIfEmpty(final String parameter) {
                if (StringUtils.isNotBlank(requestParameters.getFirst(parameter))) {

                    final String parameterValue = requestParameters.getFirst(parameter);

                    if (parameterValue.equals(NULL)) {
                        queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createNullParameter(java.sql.Types.VARCHAR));
                    } else {
                        queryParameters.put(getConvertedParameterName(parameter), QueryParameter.createStringParameter(parameterValue));
                    }
                }
            }
        }

        final AddQueryParameter addQueryParameter = new AddQueryParameter();
        addQueryParameter.addLong(EVENT_ID_PARAM);
        addQueryParameter.addLong(CAUSE_CODE_PARAM);
        addQueryParameter.addLong(SUB_CAUSE_CODE_PARAM);
        addQueryParameter.add(APN_PARAM);
        addQueryParameter.add(SGSN_PARAM);
        addQueryParameter.add(BSC_PARAM);
        addQueryParameter.addLong(TAC_PARAM);
        addQueryParameter.addNullIfEmpty(CELL_PARAM);
        addQueryParameter.addNullIfEmpty(VENDOR_PARAM);
        addQueryParameter.add(MAN_PARAM);
        addQueryParameter.addLong(IMSI_PARAM);
        addQueryParameter.add(GROUP_NAME_PARAM);
        addQueryParameter.add(TYPE_CAUSE_CODE);
        addQueryParameter.add(TYPE_SUB_CAUSE_CODE);
        addQueryParameter.add(CAUSE_PROT_TYPE_PARAM);
        addQueryParameter.add(CAUSE_PROT_TYPE);
        addQueryParameter.add(CAUSE_PROT_TYPE_HEADER);
        addQueryParameter.addLong(RAT_PARAM);
        addQueryParameter.add(DAY_PARAM);
        addQueryParameter.add(HOUR_PARAM);
        addQueryParameter.addLong(PTMSI_PARAM);
        addQueryParameter.add(EVENT_RESULT_PARAM);
        addQueryParameter.add(EVENT_TYPE_PARAM);
        addQueryParameter.addLong(MSISDN_PARAM);
        addQueryParameter.addLong(CATEGORY_ID_PARAM);

        if (datetimeWhiteList.queryRequiresDateTimeParameters(templatePath)) {
            queryParameters.put(DATE_FROM, QueryParameter.createStringParameter(dateTimeRange.getStartDateTime()));
            queryParameters.put(DATE_TO, QueryParameter.createStringParameter(dateTimeRange.getEndDateTime()));
            addLocalDateParameters(dateTimeRange, queryParameters);
            if ((dateTimeRange.getRangeInMinutes() >= MINUTES_IN_A_WEEK)) {
                addAdjustedDateParameters(dateTimeRange, queryParameters, requestParameters.getFirst(TZ_OFFSET));
            }

        }

        return queryParameters;
    }

    /**
     * Maps request parameters to SQL query parameters.
     * 
     * @param requestParameters
     *        map of URI request parameters
     * @param dateTimeRange
     *        stringified date time range
     * @return URI paramaters mapped into query parameters
     */
    public Map<String, QueryParameter> mapRequestParametersForHashId(final MultivaluedMap<String, String> requestParameters,
                                                                     final FormattedDateTimeRange dateTimeRange) {

        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();

        final class AddQueryParameter {
            void convertParameterNameAndAddLong(final String parameter) {
                final String convertedParameterNameForHashId = getConvertedParameterNameForHashId(parameter);
                addLongToQueryParameters(requestParameters, queryParameters, parameter, convertedParameterNameForHashId);
            }

            void addString(final String parameter) {
                final String convertedParameterName = getConvertedParameterNameForHashId(parameter);
                addStringToQueryParameters(requestParameters, queryParameters, parameter, convertedParameterName);
            }
        }

        final String node = requestParameters.getFirst(NODE_PARAM);
        final String eventName = requestParameters.getFirst(EVENT_NAME_PARAM);
        final String type = requestParameters.getFirst(TYPE_PARAM);

        if (StringUtils.isNotBlank(node) && !TYPE_IMSI.equals(type) && !TYPE_MSISDN.equals(type)) {
            final NodeValuesByType nodeValuesByType = new NodeValuesByType(node, type);
            requestParameters.putSingle(TAC_PARAM, nodeValuesByType.getTac());
            requestParameters.putSingle(EVENT_TYPE_PARAM, nodeValuesByType.getEventDescription());
            addLongToRequestParametersIfSet(requestParameters, CONTROLLER_SQL_ID, nodeValuesByType.getHier3Id());
            addLongToRequestParametersIfSet(requestParameters, CELL_SQL_ID, nodeValuesByType.getHier321Id());
            addLongToRequestParametersIfSet(requestParameters, EVENT_SOURCE_SQL_ID, nodeValuesByType.getEventSrcId());
        }

        if (StringUtils.isNotBlank(eventName)) {
            final NodeValuesByType nodeValuesByType = new NodeValuesByType(eventName, type);
            requestParameters.putSingle(EVENT_ID_PARAM, nodeValuesByType.getEventID());
        }

        final AddQueryParameter addQueryParameter = new AddQueryParameter();
        addQueryParameter.convertParameterNameAndAddLong(EVENT_ID_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(INTERNAL_CAUSE_CODE_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(TAC_PARAM);
        addQueryParameter.addString(MAN_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(IMSI_PARAM);
        addQueryParameter.addString(GROUP_NAME_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(TYPE_INTERNAL_CAUSE_CODE);
        addQueryParameter.addString(DAY_PARAM);
        addQueryParameter.addString(HOUR_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(EVENT_RESULT_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(EVENT_TYPE_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(CONTROLLER_SQL_ID);
        addQueryParameter.convertParameterNameAndAddLong(CELL_SQL_ID);
        addQueryParameter.convertParameterNameAndAddLong(EVENT_SOURCE_SQL_ID);
        addQueryParameter.convertParameterNameAndAddLong(FAULT_CODE_PARAM);
        addQueryParameter.convertParameterNameAndAddLong(MSISDN_PARAM);

        if (dateTimeRange != null) {
            final String key = requestParameters.getFirst(KEY_PARAM);
            boolean isSubscriberType = false;
            boolean isWeekOverrideApplied = false;

            String startTime = dateTimeRange.getStartDateTime();
            String endTime = dateTimeRange.getEndDateTime();
            // avoid UTC conversion for Circuit Subscriber drills as all
            // requests go to RAW tables
            if (StringUtils.isNotBlank(type)
                    && (type.equals(IMSI_PARAM_UPPER_CASE) || type.equals(MSISDN_PARAM_UPPER_CASE) || type.equals(PTMSI_PARAM_UPPER_CASE))) {
                isSubscriberType = true;
            }
            // do UTC conversion, iff timeRange >=1week & key=ERR, TOTAL or SUC
            if ((dateTimeRange.getRangeInMinutes() > MINUTES_IN_A_WEEK) && StringUtils.isNotBlank(key)
                    && (key.equals(KEY_TYPE_ERR) || key.equals(KEY_TYPE_TOTAL) || key.equals(KEY_TYPE_SUC)) && !(isSubscriberType)) {
                startTime = DateTimeUtils.getUTCTime(dateTimeRange.getStartDateTime(), requestParameters.getFirst(TZ_OFFSET), DATE_TIME_FORMAT);
                endTime = DateTimeUtils.getUTCTime(dateTimeRange.getEndDateTime(), requestParameters.getFirst(TZ_OFFSET), DATE_TIME_FORMAT);
                isWeekOverrideApplied = true;
                queryParameters.put(ISWEEKOVERRIDE, QueryParameter.createStringParameter(String.valueOf(isWeekOverrideApplied)));
            }
            queryParameters.put(DATE_FROM, QueryParameter.createStringParameter(startTime));
            queryParameters.put(DATE_TO, QueryParameter.createStringParameter(endTime));
            addLocalDateParameters(dateTimeRange, queryParameters);
            if (!isWeekOverrideApplied && (dateTimeRange.getRangeInMinutes() >= MINUTES_IN_A_WEEK )) {
                addAdjustedDateParameters(dateTimeRange, queryParameters, requestParameters.getFirst(TZ_OFFSET));
            }
        }

        return queryParameters;
    }

    private void addLongToRequestParametersIfSet(final MultivaluedMap<String, String> requestParameters, final String parameterName,
                                                 final long parameterValue) {
        if (parameterValue != UNSET_LONG_VALUE) {
            requestParameters.putSingle(parameterName, Long.toString(parameterValue));
        }
    }

    /**
     * Maps dateTime parameters to SQL query parameters.
     * 
     * @param dateTimeRange
     *        stringified date time range
     * @return dateTime parameters mapped into query parameters
     */
    public Map<String, QueryParameter> mapDateParameters(final FormattedDateTimeRange dateTimeRange) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        if (dateTimeRange != null) {
            queryParameters.put(DATE_FROM, QueryParameter.createStringParameter(dateTimeRange.getStartDateTime()));
            queryParameters.put(DATE_TO, QueryParameter.createStringParameter(dateTimeRange.getEndDateTime()));
        }
        return queryParameters;
    }

    public Map<String, QueryParameter> mapAdjustedDateParameters(final FormattedDateTimeRange dateTimeRange) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        String utcOffset = DateTimeUtils.getUTCOffset();
        if (dateTimeRange != null) {
            queryParameters.put(DATE_FROM, QueryParameter.createStringParameter(DateTimeUtils.getRawOffsetAdjustedTime(dateTimeRange.getStartDateTime(), utcOffset)));
            queryParameters.put(DATE_TO, QueryParameter.createStringParameter(DateTimeUtils.getRawOffsetAdjustedTime(dateTimeRange.getEndDateTime(), utcOffset)));
        }
        return queryParameters;
    }

    /**
     * Maps dateTime parameters to SQL query parameters.
     * 
     * @param dateTimeRange
     *        stringified date time range
     * @return dateTime parameters mapped into query parameters
     */
    public Map<String, QueryParameter> mapDateParametersForApnRetention(final FormattedDateTimeRange dateTimeRange) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        if (dateTimeRange != null) {
            queryParameters.put(DATE_FROM_FOR_APN_RETENTION, QueryParameter.createStringParameter(dateTimeRange.getStartDateTime()));
        }
        return queryParameters;
    }

    /**
     * adds dateTime parameters to SQL query parameters.
     * 
     * @param dateTimeRange
     *        stringified date time range
     * @return dateTime parameters mapped into query parameters
     */
    public Map<String, QueryParameter> addDateParametersForApnRetention(final Map<String, QueryParameter> queryParameters,
                                                                        final FormattedDateTimeRange dateTimeRange) {
        if (dateTimeRange != null) {
            queryParameters.put(DATE_FROM_FOR_APN_RETENTION, QueryParameter.createStringParameter(dateTimeRange.getStartDateTime()));
        }
        return queryParameters;
    }

    /**
     * Gets the converted parameter name.
     * 
     * @param parameter
     *        the parameter
     * @return the converted parameter name
     */
    private String getConvertedParameterName(final String parameter) {
        String newParameter = null;
        if (parameter.equals(SGSN_PARAM)) {
            newParameter = SGSN_SQL_NAME;
        } else if (parameter.equals(BSC_PARAM)) {
            newParameter = BSC_SQL_NAME;
        } else if (parameter.equals(CELL_PARAM)) {
            newParameter = CELL_SQL_NAME;
        } else if (parameter.equals(APN_PARAM) || parameter.equals(IMSI_PARAM) || parameter.equals(VENDOR_PARAM) || parameter.equals(TAC_PARAM)
                || parameter.equals(MAN_PARAM) || parameter.equals(PTMSI_PARAM) || parameter.equals(MSISDN_PARAM) || parameter.equals(IMEISV_PARAM)) {
            newParameter = parameter.toUpperCase();
        } else if (parameter.equals(TYPE_CAUSE_CODE) || parameter.equals(CAUSE_CODE_PARAM)) {
            newParameter = CC_SQL_NAME;
        } else if (parameter.equals(TYPE_SUB_CAUSE_CODE) || parameter.equals(SUB_CAUSE_CODE_PARAM)) {
            newParameter = SCC_SQL_NAME;
        } else if (parameter.equals(CAUSE_PROT_TYPE_PARAM)) {
            newParameter = TYPE_CAUSE_PROT_TYPE;
        } else if (parameter.equals(HOUR_PARAM)) {
            newParameter = HOUR_SQL_PARAM;
        } else {
            newParameter = parameter;
        }
        return newParameter;
    }

    /**
     * Gets the converted parameter name.
     * 
     * @param parameter
     *        the parameter
     * @return the converted parameter name
     */
    private String getConvertedParameterNameForHashId(final String parameter) {
        String newParameter = null;
        if (parameter.equals(IMSI_PARAM) || parameter.equals(VENDOR_PARAM) || parameter.equals(TAC_PARAM) || parameter.equals(MAN_PARAM)
                || parameter.equals(MSC_PARAM) || parameter.equals(MSISDN_PARAM)) {
            newParameter = parameter.toUpperCase();
        } else if (parameter.equals(TYPE_INTERNAL_CAUSE_CODE) || parameter.equals(INTERNAL_CAUSE_CODE_PARAM)) {
            newParameter = ICC_SQL_NAME;
        } else if (parameter.equals(HOUR_PARAM)) {
            newParameter = HOUR_SQL_PARAM;
        } else if (parameter.equals(GROUP_NAME_PARAM)) {
            newParameter = GROUP_NAME_KEY;
        } else if (parameter.equals(EVENT_ID_PARAM)) {
            newParameter = EVENT_ID_SQL_PARAM;
        } else if (parameter.equals(FAULT_CODE_PARAM)) {
            newParameter = FAULT_CODE_SQL_NAME;
        } else if (parameter.equals(HOUR_PARAM)) {
            newParameter = HOUR_SQL_PARAM;
        } else {
            newParameter = parameter;
        }
        return newParameter;
    }

    /**
     * Check valid value. Returns false if the parameters aren't valid, some examples: Entry for the IMSI field isn't valid Entry of 321 for the CELL
     * field isn't valid Node should not be blank for type SOMETYPE Otherwise returns true if all parameters are deemed valid
     * 
     * @param requestParameters
     *        the request parameters
     * @return true, if valid value, false if not valid
     */
    public boolean checkValidValue(final MultivaluedMap<String, String> requestParameters) {
        return parameterChecker.checkValidValueOfParameter(requestParameters);
    }

    /**
     * present for test purposes
     * 
     * @param ratDescriptionMappingUtils
     */
    public void setRatDescriptionMappingUtils(final RATDescriptionMappingUtils ratDescriptionMappingUtils) {
        this.ratDescriptionMappingUtils = ratDescriptionMappingUtils;
    }

    /**
     * present for test purposes
     * 
     * @return the ratDescriptionMappingUtils
     */
    public RATDescriptionMappingUtils getRatDescriptionMappingUtils() {
        return ratDescriptionMappingUtils;
    }

    public String getRATValueAsInteger(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(RAT_PARAM)) {
            // if the RAT parameter is included in the parameters, its just the
            // integer RAT value that's specified
            return requestParameters.getFirst(RAT_PARAM);
        }
        // whereas if the RAT is included in the overall NODE parameter, its as
        // the
        // RAT description
        final NodeValuesByType nodeValuesByType = new NodeValuesByType(requestParameters.getFirst(NODE_PARAM), requestParameters.getFirst(TYPE_PARAM));
        return nodeValuesByType.getAndConvertRatToIntegerValue(ratDescriptionMappingUtils);
    }

    NodeValuesByType getNodeValuesByTypeInstance(final String node, final String type) {
        return new NodeValuesByType(node, type);
    }

    /**
     * This method will generate the Access Area hashId for Subscriber BI cell analysis and support the below format
     * 
     * <HIERARCHY_1>,<HIERARCHY_2>,<HIERARCHY_3>,<VENDOR>,<RAT> For example cell1,,controller1,Ericsson,GSM
     * 
     * @param cell
     *        topology string representing the Access Area/Cell
     * @return hashId representing the Access Area in database
     */

    public long createHashIdForCell(final String cell) {
        return hashUtilities.createHashIdForHier321Id(cell);
    }

    /**
     * The Class NodeValuesByType.
     */
    public class NodeValuesByType {

        private String apn;

        private String sgsn;

        private String msc;

        private String cell;

        private String bsc;

        private String tac;

        private String vendor;

        private String rat;

        private String eventID;

        private String eventDescription;

        private String hier2;

        private long hier3_id;

        private long hier321_id;

        private long eventSrc_id;

        /**
         * The Constructor.
         * 
         * @param node
         *        the node
         * @param type
         *        the type
         */
        public NodeValuesByType(final String node, final String type) {
            init(node, type);
        }

        /**
         * Init.
         * 
         * @param node
         * @param type
         */
        private void init(final String node, final String type) {
            final String trimmedNode = node.trim();
            if (type.equals(TYPE_APN)) {
                setApn(trimmedNode);
            } else if (type.equals(TYPE_SGSN)) {
                setSgsn(trimmedNode);
            } else if (type.equals(TYPE_MSC)) {
                setMsc(trimmedNode);
                setEventSrcId(createHashId(TypesOfHashId.EVENTSRCMSC_HASH));
            } else if (type.equals(TYPE_TAC)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setTac(value[value.length - 1]);
            } else if (type.equals(TYPE_BSC)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setBsc(value[0]);
                setVendor(value[1]);
                setRat(value[2]);
                setHier3Id(createHashId(TypesOfHashId.CONTROLLER_HASH));
            } else if (type.equals(TYPE_CELL)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setCell(value[0]);
                setHier2(value[1]);
                setBsc(value[2]);
                setVendor(value[3]);
                setRat(value[4]);
                setHier321Id(createHashId(TypesOfHashId.CELL_HASH));
            } else if (type.equals(SUBBI_FAILURE)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setEventID(value[1]);
            } else if (type.equals(TYPE_IMSI) || type.equals(TYPE_MSISDN)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setEventID(value[1]);
            } else if (type.equals(SUBBI_TAU)) {
                final String[] value = trimmedNode.split(DELIMITER);
                setTac(value[0]);
                setEventDescription(value[1]);
            } else if (type.equals(SUBBI_HANDOVER)) {
                final String[] value = trimmedNode.split(DELIMITER);
                if (value.length == 0) {
                    setCell(NULL);
                    setVendor(NULL);
                } else {
                    setCell(value[0].length() == 0 ? NULL : value[0]);
                    setVendor(value[1].length() == 0 ? NULL : value[1]);
                }
            }
        }

        private long createHashId(final TypesOfHashId typeOfHashId) {
            switch (typeOfHashId) {
                case CONTROLLER_HASH:
                    return hashUtilities.createHashIDForControllerAsLong(rat, bsc, vendor);
                case CELL_HASH:
                    return hashUtilities.createHashIDForCell(rat, bsc, hier2, cell, vendor);
                case EVENTSRCMSC_HASH:
                    return hashUtilities.createHashIDForMSC(msc);
                default:
                    throw new RuntimeException("No hashing functionality defined for type " + typeOfHashId);
            }

        }

        /**
         * Sets the apn.
         * 
         * @param apn
         *        the apn
         */
        public void setApn(final String apn) {
            this.apn = apn;
        }

        /**
         * Sets the sgsn.
         * 
         * @param sgsn
         *        the sgsn
         */
        public void setSgsn(final String sgsn) {
            this.sgsn = sgsn;
        }

        public void setMsc(final String msc) {
            this.msc = msc;
        }

        /**
         * Sets the cell.
         * 
         * @param cell
         *        the cell
         */
        public void setCell(final String cell) {
            this.cell = cell;
        }

        /**
         * Sets the hier2 string.
         * 
         * @param hier2
         *        the hier2
         */
        public void setHier2(final String hier2) {
            this.hier2 = hier2;
        }

        public void setHier3Id(final long id) {
            hier3_id = id;
        }

        public void setHier321Id(final long id) {
            hier321_id = id;
        }

        public void setEventSrcId(final long id) {
            eventSrc_id = id;
        }

        /**
         * Sets the bsc.
         * 
         * @param bsc
         *        the bsc
         */
        public void setBsc(final String bsc) {
            this.bsc = bsc;
        }

        /**
         * Sets the tac.
         * 
         * @param tac
         *        the tac
         */
        public void setTac(final String tac) {
            this.tac = tac;
        }

        /**
         * Sets the vendor.
         * 
         * @param vendor
         *        the vendor
         */
        public void setVendor(final String vendor) {
            this.vendor = vendor;
        }

        public void setRat(final String rat) {
            this.rat = rat;
        }

        /**
         * Gets the apn.
         * 
         * @return the apn
         */
        public String getApn() {
            return apn;
        }

        /**
         * Gets the sgsn.
         * 
         * @return the sgsn
         */
        public String getSgsn() {
            return sgsn;
        }

        public String getMsc() {
            return msc;
        }

        /**
         * Gets the cell.
         * 
         * @return the cell
         */
        public String getCell() {
            return cell;
        }

        public long getHier3Id() {
            return hier3_id;
        }

        public long getHier321Id() {
            return hier321_id;
        }

        public long getEventSrcId() {
            return eventSrc_id;
        }

        /**
         * Gets the bsc.
         * 
         * @return the bsc
         */
        public String getBsc() {
            return bsc;
        }

        /**
         * Gets the tac.
         * 
         * @return the tac
         */
        public String getTac() {
            return tac;
        }

        /**
         * Gets the vendor.
         * 
         * @return the vendor
         */
        public String getVendor() {
            return vendor;
        }

        /**
         * Gets the RAT - converted into integer value found in the parameter ratDescriptionMappings
         * 
         * @param ratDescriptionMappings
         *        contains preloaded descriptions for rat values
         * 
         * @return the RAT value, converted to its integer value
         */
        public String getAndConvertRatToIntegerValue(final RATDescriptionMappingUtils ratDescriptionMappings) {
            return ratDescriptionMappings.getRATIntegerValue(rat);
        }

        /**
         * @return the eventID
         */
        public String getEventID() {
            return eventID;
        }

        /**
         * @param eventID
         *        the eventID to set
         */
        public void setEventID(final String eventID) {
            this.eventID = eventID;
        }

        /**
         * @return the eventDescription
         */
        public String getEventDescription() {
            return eventDescription;
        }

        /**
         * @param eventDescription
         *        the eventDescription to set
         */
        public void setEventDescription(final String eventDescription) {
            this.eventDescription = eventDescription;
        }
    }

    public void setParameterChecker(final ParameterChecker parameterChecker) {
        this.parameterChecker = parameterChecker;
    }

    /**
     * @param hashUtilities
     *        the hashUtilities to set
     */
    public void setHashUtilities(final HashUtilities hashUtilities) {
        this.hashUtilities = hashUtilities;
    }

    /**
     * Create a QueryParameter object for the supplied parameter name and value The type of the parameter is determined from a static class,
     * QueryParameterTypes
     * 
     * @param parameterName
     *        name of the parameter
     * @param parameterValue
     *        value of the specified parameter
     * 
     * @return a QueryParameter object holding these values
     */
    public QueryParameter createQueryParameter(final String parameterName, final String parameterValue) {
        final QueryParameterType parameterType = QueryParameterTypeMap.get(parameterName);
        switch (parameterType) {
            case STRING:
                return QueryParameter.createStringParameter(parameterValue);
            case INT:
                return QueryParameter.createIntParameter(Integer.valueOf(parameterValue));
            case LONG:
                return QueryParameter.createLongParameter(Long.valueOf(parameterValue));
            default:
                throw new UnsupportedOperationException("No mapping provided for " + parameterType);
        }

    }

    public long createHashIDForController(final String rat, final String bsc, final String vendor) {
        return hashUtilities.createHashIDForControllerAsLong(rat, bsc, vendor);
    }

    /**
     * 
     * @param controller
     *        String representation of All Controller information e.g ONRM_ROOT_MO_R:RNC01:RNC01,Ericsson,3G
     * @return HashID for the controller as a Long value
     */

    public long createHashIDFor3GController(final String controller) {
        return hashUtilities.createHashIDFor3GController(controller);
    }

    /**
     * Based on the given parameters create a hashing ID for the cell.
     * 
     * @param rat
     *        The Radio Access Technology
     * @param bsc
     *        The fdn of the controller node.
     * @param hier2
     * @param cell
     *        the cell id
     * @param vendor
     *        the vendor
     * @return
     */
    public long createHashIDForCell(final String rat, final String bsc, final String hier2, final String cell, final String vendor) {
        return hashUtilities.createHashIDForCell(rat, bsc, hier2, cell, vendor);
    }

    /**
     * Based on the given parameters create a hashing ID for the cell.
     * 
     * @param rat
     *        The Radio Access Technology
     * @param bsc
     *        The fdn of the controller node.
     * @param cell
     *        the cell id
     * @param vendor
     *        the vendor
     * @return
     */
    public long createHashIDFor3GCell(final String rat, final String bsc, final String cell, final String vendor) {
        return hashUtilities.createHashIDFor3GCell(rat, bsc, cell, vendor);
    }

    public void setDatetimeWhiteList(final DateTimeWhiteList datetimeWhiteList) {
        this.datetimeWhiteList = datetimeWhiteList;
    }

    /**
     * @param applicationConfigManager
     *        the applicationConfigManager to set
     */
    public void setApplicationConfigManager(final ApplicationConfigManager applicationConfigManager) {
        this.applicationConfigManager = applicationConfigManager;
    }
}
