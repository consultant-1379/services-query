package com.ericsson.eniq.events.server.query.resultsettransformers;

import static com.ericsson.eniq.events.server.common.UserPreferencesType.*;

import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.common.UserPreferencesType;
import com.ericsson.eniq.events.server.utils.Calculator;
import com.ericsson.eniq.events.server.utils.RATDescriptionMappingUtils;
import com.ericsson.eniq.events.server.utils.json.JSONCauseCodeUtils;
import com.ericsson.eniq.events.server.utils.json.JSONLiveLoadUtils;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * Transformations for result set data for various JSON response encodings and other export formats.
 * 
 * @since 2010
 */
public final class ResultSetTransformerFactory {

    /**
     * Gets the json grid data transformer.
     * 
     * @param timestampFrom
     *        the timestamp from
     * @param timestampTo
     *        the timestamp to
     * @return the JSON grid data transformer
     */
    public static ResultSetTransformer<String> getJSONGridDataTransformer(final String timestampFrom, final String timestampTo,
                                                                          final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONGridDataObject(rs, timestampFrom, timestampTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONGridDataObject(results, timestampFrom, timestampTo, timeColumn, tzOffset);
            }
        };
    }

    /**
     * Gets the json chart data transformer for charting the Groups Most Frequent Signalling Graph. It is specific because the transformer needs to
     * tell the JSONUtils to calculate the Y_AXIS_MAX and MIN differently - this is the only graph that returns values that are not used by the UI to
     * plot the graph (they are used for the toggle to grid)
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getGroupsMostFreqSignalJSONChartDataTransformer(final String dateTimeFrom, final String dateTimeTo,
                                                                                               final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {

            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toGroupsMostFreqSignalJSONChartDataObject(rs, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toGroupsMostFreqSignalJSONChartDataObject(results, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }
        };
    }

    /**
     * Gets the json grid data transformer.
     * 
     * @param timestampFrom
     *        the timestamp from
     * @param timestampTo
     *        the timestamp to
     * @param list
     *        of time column indexes
     * @return the JSON grid data transformer
     */
    public static ResultSetTransformer<String> getJSONGridDataTransformer(final String timestampFrom, final String timestampTo,
                                                                          final List<Integer> timeColumns, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONGridDataObject(rs, timestampFrom, timestampTo, timeColumns, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONGridDataObject(results, timestampFrom, timestampTo, timeColumns, tzOffset);
            }
        };
    }

    public static ResultSetTransformer<String> getJSONGridDataTransformerForAppendingRows(final String timestampFrom, final String timestampTo,
                                                                                          final List<Integer> timeColumns, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONGridDataObjectAppendRows(rs, timestampFrom, timestampTo, timeColumns, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONGridDataObjectAppendRows(results, timestampFrom, timestampTo, timeColumns, tzOffset);
            }
        };
    }

    /**
     * Gets the json chart data transformer.
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getJSONChartDataTransformer(final String xaxis, final String secondYaxis, final String dateTimeFrom,
                                                                           final String dateTimeTo, final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONChartDataObject(rs, xaxis, secondYaxis, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONChartDataObject(results, xaxis, secondYaxis, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }
        };
    }

    /**
     * Gets the json chart data transformer.
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getJSONChartDataTransformerWithAppendedRows(final String xaxis, final String secondYaxis,
                                                                                           final String dateTimeFrom, final String dateTimeTo,
                                                                                           final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONChartDataObjectWithAppendedRows(rs, xaxis, secondYaxis, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONChartDataObjectWithAppendedRows(results, xaxis, secondYaxis, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }
        };
    }

    /**
     * Gets the kpi chart data transformer.
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @param kpiTimeList
     *        the kpi time list
     * @return the JSONKPI chart data transformer
     */
    public static ResultSetTransformer<String> getJSONSamplingChartDataTransformer(final String xaxis, final String secondYaxis,
                                                                                   final String[] chartTimeList, final String dateTimeFrom,
                                                                                   final String dateTimeTo, final String timeColumn,
                                                                                   final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONSamplingChartDataObject(rs, xaxis, secondYaxis, chartTimeList, dateTimeFrom, dateTimeTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONSamplingChartDataObject(xaxis, secondYaxis, chartTimeList, dateTimeFrom, dateTimeTo, timeColumn, tzOffset,
                        results);
            }
        };
    }

    /**
     * Gets the chart data with title and timetick interval transformer.
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @param chartTimeList
     *        the chartTimeList
     * @param timestampFrom
     * @param timestampTo
     * @param timeColumn
     * @param tzOffset
     * @param chartTitle
     * @param timeInterval
     *        interval between two time tick on xaxis
     * @return the JSONKPI chart data transformer
     */
    public static ResultSetTransformer<String> getJSONChartWithTitleAndTimeIntervalDataTransformer(final String xaxis, final String secondYaxis,
                                                                                                   final String[] chartTimeList,
                                                                                                   final String dateTimeFrom,
                                                                                                   final String dateTimeTo, final String timeColumn,
                                                                                                   final String tzOffset, final String chartTitle,
                                                                                                   final int timeInterval) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONChartWithTitleAndTimeIntervalDataObject(rs, xaxis, secondYaxis, chartTimeList, dateTimeFrom, dateTimeTo,
                        timeColumn, tzOffset, chartTitle, timeInterval);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONChartWithTitleAndTimeIntervalDataObject(xaxis, secondYaxis, chartTimeList, dateTimeFrom, dateTimeTo,
                        timeColumn, tzOffset, results, chartTitle, timeInterval);
            }
        };
    }

    /**
     * Gets the kpi chart data transformer.
     * 
     * @param xaxis
     *        the xaxis
     * @param secondYaxis
     *        the second yaxis
     * @param kpiTimeList
     *        the kpi time list
     * @return the JSONKPI chart data transformer
     */
    public static ResultSetTransformer<String> getJSONSamplingChartDataTransformerWithCalculator(final String xaxis, final String secondYaxis,
                                                                                                 final String[] chartTimeList,
                                                                                                 final String timeColumn, final String tzOffset,
                                                                                                 final Calculator calc) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONSamplingChartDataObjectWithCalculator(rs, xaxis, secondYaxis, chartTimeList, timeColumn, tzOffset, calc);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONSamplingChartDataObjectWithCalculator(xaxis, secondYaxis, chartTimeList, timeColumn, tzOffset, results, calc);
            }
        };
    }

    /**
     * Gets the json name list data transformer.
     * 
     * @return the JSON name list data transformer
     */
    public static ResultSetTransformer<String> getJSONNameListDataTransformer() {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONNameListDataObject(rs);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json name list data transformer.
     * 
     * @return the JSON name list data transformer
     */
    public static ResultSetTransformer<String> getJSONNameListMultipleValuesDataTransformer() {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONNameListMultipleValueDataObject(rs);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json name list data transformer without time information.
     * 
     * @return the JSON name list data transformer
     */
    public static ResultSetTransformer<String> getJSONNameListDataTransformerWithoutTimeInfo() {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONDataObjectWithoutTimeInfo(rs);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json live load transformer.
     * 
     * @param liveLoadType
     *        the live load type
     * @param callbackID
     *        the callback id
     * @param pagingIndex
     *        start index in result
     * @param pagingLimit
     *        maximum number of rows requested for this result
     * @return the JSON live load transformer
     */
    public static ResultSetTransformer<String> getJSONLiveLoadTransformer(final String liveLoadType, final String callbackID,
                                                                          final String pagingLimit, final String pagingIndex,
                                                                          final RATDescriptionMappingUtils ratDescriptionMappings) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONLiveLoadUtils.toJSONLiveLoad(liveLoadType, callbackID, pagingLimit, pagingIndex, rs);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json metadata ui transformer.
     * 
     * @param servicesUrl
     *        the services url
     * @param handsetPath
     *        the handset path
     * @return the JSON metadata ui transformer
     */
    public static ResultSetTransformer<String> getJSONMetadataUITransformer(final String servicesUrl, final String handsetPath) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONLiveLoadUtils.toJSONLiveLoadHandsetsMetadataUI(rs, servicesUrl, handsetPath);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the csv transformer.
     * 
     * @return the CSV transformer
     */
    public static ResultSetTransformer<String> getCSVTransformer(final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return ResultSetHelper.toCSV(rs, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the csv transformer.
     * 
     * @return the CSV transformer
     */
    public static ResultSetTransformer<String> getCSVStreamTransformer(final List<Integer> timeColumnIndexes, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return ResultSetHelper.singleRowToCSV(rs, timeColumnIndexes, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the csv transformer which includes extraction of the Sub Cause Code Help Text, based on Cause Code ID. Added for TR HN63122.
     * 
     * @return the CSV transformer
     */
    public static ResultSetTransformer<String> getCSVStreamTransformerForCauseCode(final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return ResultSetHelper.singleRowToCSVForCauseCode(rs, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the transformer for transforming the result of the list connected cells to a SAC query
     * 
     * @return the SAC transformer
     */
    public static ResultSetTransformer<String> getSACConnectedCellsTransformer() {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONForListCellsConnectedToSACResults(rs);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json chart data transformer for SUB BI busy day/hour.
     * 
     * @param xaxis
     *        the xaxis
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getJSONChartDataTransformerForSUBBIBusy(final String busyKey, final String dateTimeFrom,
                                                                                       final String dateTimeTo, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONChartDataObjectForSubBIBusy(rs, busyKey, dateTimeFrom, dateTimeTo, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json chart data transformer for SUB BI busy day/hour.
     * 
     * @param xaxis
     *        the xaxis
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getJSONChartDataTransformerForSUBBIBusyWithAppendedRows(final String busyKey,
                                                                                                       final String dateTimeFrom,
                                                                                                       final String dateTimeTo, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONChartDataObjectForSubBIBusyWithAppendedRows(rs, busyKey, dateTimeFrom, dateTimeTo, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONChartDataObjectForSubBIBusyWithAppendedRows(results, busyKey, dateTimeFrom, dateTimeTo, tzOffset);
            }
        };
    }

    /**
     * Gets the json grid data transformer for SUB BI busy day.
     * 
     * @return the JSON chart data transformer
     */
    public static ResultSetTransformer<String> getJSONGridDataTransformerForSUBBIBusyDay(final String dateFrom, final String dateTo,
                                                                                         final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toJSONGridDataObjectForSubBIBusyDay(rs, dateFrom, dateTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the data transformer to check if the ResultSet size is exactly 1
     * 
     * @return true if the result set contains exactly one row
     */
    public static ResultSetTransformer<Boolean> getTransformerToCheckResultSizeIsOne() {
        return new ResultSetTransformerToCheckResultSizeIsOne();
    }

    /**
     * Gets the raw table names. Put non-null raw table names into a list.
     * 
     * @return the raw table names
     */
    public static ResultSetTransformer<List<String>> getRawTableNamesTransformer() {
        return new ResultSetTransformerForRawTables();
    }

    /**
     * Gets the transformer for converting data in RAT table to Map of strings
     * 
     * @return transformer
     */
    public static ResultSetTransformer<Map<String, String>> getRATValuesTransformer() {
        return new ResultSetTransformerForRATValues();
    }

    /**
     * Gets the json grid data transformer which includes extraction of the Sub Cause Code Help Text, based on Cause Code ID.
     * <p/>
     * Added for TR HN63122.
     * 
     * @param timestampFrom
     *        the timestamp from
     * @param timestampTo
     *        the timestamp to
     * @return the JSON grid data transformer
     */
    public static ResultSetTransformer<String> getCauseCodeHelpTextTransformer(final String timestampFrom, final String timestampTo,
                                                                               final String timeColumn, final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return JSONCauseCodeUtils.toJSONGridDataObjectForCauseCode(rs, timestampFrom, timestampTo, timeColumn, tzOffset);
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets the json grid data transformer for roaming analysis
     * 
     * @param timestampFrom
     *        the timestamp from
     * @param timestampTo
     *        the timestamp to
     * @return the JSON grid data transformer
     */
    public static ResultSetTransformer<String> getRoamingHelpTextTransformer(final String timestampFrom, final String timestampTo,
                                                                             final String tzOffset) {
        return new ResultSetTransformer<String>() {
            @Override
            public String transform(final ResultSet rs) throws SQLException {
                return null;
            }

            @Override
            public String transform(final List<ResultSet> results) throws SQLException {
                return JSONUtils.toJSONChartDataObjectForRoamingAppendedRows(results, "", tzOffset);
            }
        };
    }

    // this is a singleton.

    /**
     * The Constructor.
     */
    private ResultSetTransformerFactory() {
    }

    /**
     * Get transformer for the query that fetches the license numbers for tech packs from the database
     * 
     * @return the transformer that produces a Map of strings and list of strings
     */
    public static ResultSetTransformer<Map<String, List<String>>> getTechPackLicenseNumbersTransformer() {
        return new ResultSetTransformer<Map<String, List<String>>>() {
            @Override
            public Map<String, List<String>> transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toMap(rs);
            }

            @Override
            public Map<String, List<String>> transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Get transformer for the query that fetches rows with the String values only
     * 
     * @return the transformer that produces a List of List of Strings
     */
    public static ResultSetTransformer<List<List<String>>> getStringResultsTransformer() {
        return new ResultSetTransformer<List<List<String>>>() {
            @Override
            public List<List<String>> transform(final ResultSet rs) throws SQLException {
                return JSONUtils.toList(rs);
            }

            @Override
            public List<List<String>> transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    /**
     * Gets transformer for the result set of UserPreferences data
     * 
     * @return the transformer that produces com.ericsson.eniq.events.server.serviceprovider.UserPreferencesType objects
     */
    public static ResultSetTransformer<UserPreferencesType> getUserPreferencesTransformer() {
        return new ResultSetTransformer<UserPreferencesType>() {
            @Override
            public UserPreferencesType transform(final ResultSet rs) throws SQLException {
                return rs.next() ? new UserPreferencesType(rs.getString(USERNAME_COLUMN), rs.getInt(VERSION_COLUMN), readClob(rs, SETTINGS_COLUMN)
                        .toString()) : null;
            }

            private String readClob(final ResultSet rs, final String columnName) throws SQLException {
                final Reader clobReader = rs.getCharacterStream(columnName);
                final char buf[] = new char[256];
                int len = -1;
                final StringBuilder sb = new StringBuilder();
                try {
                    while ((len = clobReader.read(buf)) != -1) {
                        sb.append(new String(buf, 0, len));
                    }
                } catch (final IOException e) {
                    throw new SQLException(e);
                }
                return sb.toString();
            }

            @Override
            public UserPreferencesType transform(final List<ResultSet> results) throws SQLException {
                return null;
            }
        };
    }

    public static ResultSetTransformer<Object> getNullResultSetTransformer() {
        return new ResultSetTransformer<Object>() {

            @Override
            public Object transform(final List<ResultSet> results) throws SQLException {
                return null;
            }

            @Override
            public Object transform(final ResultSet rs) throws SQLException {
                return null;
            }
        };
    }

}
