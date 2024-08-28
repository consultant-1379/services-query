package com.ericsson.eniq.events.server.query.resultsettransformers;

import static com.ericsson.eniq.events.server.common.utils.JSONUtilsConstants.*;
import static java.sql.Types.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.utils.DateTimeUtils;

/**
 * ResultSet helper functions.
 * 
 * @author edeccox
 * @since 2010
 *
 */
public final class ResultSetHelper {
    private static final String CSV_SEPARATOR = ",";

    private static final String NEW_LINE = "\n";

    private static final char QUOTATION = '"';

    private final static int MATCHER_POSITION = 4;

    private final static int MATCHER_OFFSET = 2;

    /**
     * Writes a ResultSet to a CSV formatted string.
     * This is a very basic CSV writer. It has the following 
     * limitations:
     * 1. only comma separated lists supported
     * 2. no escaping of values is catered for
     * 3. no quoting of values with special characters is done
     * 
     * The usage is based on simple grid data conversions
     * with special characters.
     * TODO: use third party CSV API when possible. 
     * 
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs the ResultSet
     * @throws java.sql.SQLException
     */
    public static String toCSV(final ResultSet rs, final String timeColumn, final String tzOffset) throws SQLException {
        final StringBuilder sb = new StringBuilder();
        final String columnNames = listToCSV(getColumnNames(rs));

        if (columnNames != null) {
            sb.append(columnNames);
        }
        List<Integer> timeColumnIndexes = null;
        try {
            final int timeCol = Integer.parseInt(timeColumn);
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(timeCol);
        } catch (final NumberFormatException numEx) {
            ServicesLogger.detailed(Level.WARNING, "ResultSetHelper", "toCSV", numEx);
        }
        while (rs.next()) {
            final String line = listToCSV(getColumnValues(rs, timeColumnIndexes, tzOffset));
            if (line != null) {
                sb.append(line);
            }
        }

        return sb.toString();
    }

    /**
     * Writes a ResultSet to a CSV formatted string.
     * This is a very basic CSV writer. It has the following 
     * limitations:
     * 1. only comma separated lists supported
     * 2. no escaping of values is catered for
     * 3. no quoting of values with special characters is done
     * 
     * The usage is based on simple grid data conversions
     * with special characters.
     * TODO: use third party CSV API when possible. 
     * 
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs the ResultSet
     * @param timeColumn
     * @param tzOffset the timezone offset to apply
     * @throws java.sql.SQLException
     */
    public static String singleRowToCSV(final ResultSet rs, final List<Integer> timeColumnIndexes, final String tzOffset)
            throws SQLException {
        final String line = listToCSV(getColumnValues(rs, timeColumnIndexes, tzOffset));
        return line == null ? NEW_LINE : line;
    }

    /**
     * Modified variant of singleRowToCSV.
     * This calls getColumnValuesForCauseCode instead of getColumnValues.
     * Added for TR HN63122.
     * Rest of documentation unchanged:
     * 
     * Writes a ResultSet to a CSV formatted string.
     * This is a very basic CSV writer. It has the following 
     * limitations:
     * 1. only comma separated lists supported
     * 2. no escaping of values is catered for
     * 3. no quoting of values with special characters is done
     * 
     * The usage is based on simple grid data conversions
     * with special characters.
     * TODO: use third party CSV API when possible. 
     * 
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs the ResultSet
     * @param timeColumn
     * @param tzOffset the timezone offset to apply
     * @throws java.sql.SQLException
     */
    public static String singleRowToCSVForCauseCode(final ResultSet rs, final String timeColumn, final String tzOffset)
            throws SQLException {

        final String line = listToCSV(getColumnValuesForCauseCode(rs, timeColumn, tzOffset));
        return line == null ? NEW_LINE : line;
    }

    /**
     * Get ResultSet column names as a list of strings.
     * @param rs the ResultSet
     * @return list of column names as strings
     * @throws SQLException
     */
    public static List<String> getColumnNames(final ResultSet rs) throws SQLException {
        final List<String> names = new ArrayList<String>();
        final ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            names.add(metadata.getColumnName(i + 1));
        }
        return names;
    }

    /**
     * Return a single row of values as a list of strings. 
     * Each value is a an entry in the list. 
     * 
     * @param rs the ResultSet
     * @return column values for a single row as a list
     * @throws SQLException
     */
    public static List<String> getColumnValues(final ResultSet rs, final List<Integer> timeColumnIndexes,
            final String tzOffset) throws SQLException {
        final List<String> values = new ArrayList<String>();
        final ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            if (timeColumnIndexes != null && timeColumnIndexes.contains(i + 1)) {

                values.add(DateTimeUtils.getLocalTime(rs.getString(i + 1), tzOffset,
                        ApplicationConstants.RECEIVED_DATE_FORMAT));

            } else {
                if (doesColumnContainFloatingPointValues(rs.getMetaData().getColumnType(i + 1))) {
                    values.add(String.valueOf(rs.getFloat(i + 1)));
                } else {
                    values.add(rs.getString(i + 1));
                }
            }
        }
        return values;
    }

    /**
     * Method to check if that specified columnType is one of FLOAT, REAL or DOUBLE
     * @param columnType
     * @return
     */
    private static boolean doesColumnContainFloatingPointValues(final int columnType) {
        return columnType == FLOAT || columnType == REAL;
    }

    /**
     * Modified variant of getColumnValues.
     * 
     * Extracts the relevant Sub Cause Code Help text from
     * the "SUBCAUSE_CODE_HELP" column, based on the ID in
     * "CAUSE_CODE".
     * Added for TR HN63122.
     * 
     * Return a single row of values as a list of strings. 
     * Each value is a an entry in the list. 
     * 
     * @param rs                the ResultSet
     * @param timeColumn        position of time column
     * @param tZOffset          timezone offset used to calculate local time
     * @return column values for a single row as a list
     * @throws SQLException
     */
    public static List<String> getColumnValuesForCauseCode(final ResultSet rs, final String timeColumn,
            final String tzOffset) throws SQLException {

        int subCauseCodeHelpColumnIndex;
        int causeCodeColumnIndex;

        try {
            subCauseCodeHelpColumnIndex = rs.findColumn(ApplicationConstants.SCC_HELP_SQL_NAME);
        } catch (final SQLException e) {
            subCauseCodeHelpColumnIndex = -1;
        }

        try {
            causeCodeColumnIndex = rs.findColumn(ApplicationConstants.CC_SQL_NAME);
        } catch (final SQLException e) {
            causeCodeColumnIndex = -1;
        }

        final List<String> values = new ArrayList<String>();
        final ResultSetMetaData metadata = rs.getMetaData();
        final int numberColumns = metadata.getColumnCount();

        for (int i = 0; i < numberColumns; i++) {

            // if current column is subcause code help column
            if (i == subCauseCodeHelpColumnIndex - 1) {

                final String causeCodeID = rs.getString(causeCodeColumnIndex);
                final String causeCodeHelpText = rs.getString(subCauseCodeHelpColumnIndex);

                final Matcher matcher;
                final Pattern pattern;

                final String value;

                pattern = Pattern.compile("((.*)(" + HASH + HASH + causeCodeID + HASH + HASH + ")[" + HASH + HASH
                        + "\\d*" + HASH + HASH + "]*(" + "\\" + ARRAY_START + "\\" + PIPE + ".*?" + "\\" + PIPE + "\\"
                        + ARRAY_END + ")(.*))");
                matcher = pattern.matcher(causeCodeHelpText);
                if (matcher.matches()) {
                    value = matcher.group(MATCHER_POSITION).substring(MATCHER_OFFSET,
                            matcher.group(MATCHER_POSITION).length() - MATCHER_OFFSET);
                } else {
                    value = "";
                }

                // append help text
                values.add((value));
            } else {
                if (timeColumn != null && i == Integer.parseInt(timeColumn) - 1) {

                    values.add(DateTimeUtils.getLocalTime(rs.getString(i + 1), tzOffset,
                            ApplicationConstants.RECEIVED_DATE_FORMAT));

                } else {
                    values.add(rs.getString(i + 1));
                }
            }
        }
        return values;
    }

    /**
     * Converts list of values to CSV
     * If one of the values contains a comma then wrap it in quotations
     * @param values
     */
    public static String listToCSV(final List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        final StringBuilder lineBuilder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i != 0) {
                lineBuilder.append(CSV_SEPARATOR);
            }

            final String nextElement = values.get(i);
            if (nextElement == null) {
                continue;
            }

            lineBuilder.append(QUOTATION); //NOPMD (ericker 11/11/10, necessary evil)
            lineBuilder.append(nextElement);
            lineBuilder.append(QUOTATION);
        }

        lineBuilder.append(NEW_LINE);
        return lineBuilder.toString();
    }

    // prevent instantation
    private ResultSetHelper() {
    }
}
