/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.Map;
import java.util.logging.Level;

import com.ericsson.eniq.events.server.logging.ServicesLogger;

/**
 * Log SQL queries and inject SQL parameters into traced query
 * This class scans a given SQL query and replaces all instances of :parameterName with the value of that parameter
 * provided in the queryParameters argument 
 * The result in an SQL query that run directly against a database without any need for string replacement 
 *
 * @author eemecoy
 *
 */
public class SQLQueryLogger {

    String injectQueryParameters(final String query, final Map<String, QueryParameter> queryParameters) {
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < query.length(); i++) {
            if (isCharacterAParameter(query, i)) {
                final String queryParameterName = getParameterName(query, i);
                i += queryParameterName.length();
                result.append(getParameterValue(queryParameters, queryParameterName));
            } else {
                final char character = query.charAt(i);
                result.append(character);
            }
        }
        return result.toString();
    }

    private String getParameterName(final String query, final int i) {
        int j = i + 2;
        while (j < query.length() && Character.isJavaIdentifierPart(query.charAt(j))) {
            //find end of parameter name
            j++;
        }
        final String queryParameterName = query.substring(i + 1, j);
        return queryParameterName;
    }

    private Object getParameterValue(final Map<String, QueryParameter> queryParameters, final String queryParameterName) {
        final QueryParameter queryParameter = queryParameters.get(queryParameterName);
        if (queryParameter == null) {
            return null;
        }
        return formatParameterValue(queryParameter);
    }

    private Object formatParameterValue(final QueryParameter queryParameter) {
        final Object parameterValue = queryParameter.getValue();
        if (queryParameter.getType().equals(QueryParameterType.STRING)) {
            return SINGLE_QUOTE + parameterValue + SINGLE_QUOTE;
        }
        return parameterValue;
    }

    private boolean isCharacterAParameter(final String query, final int position) {
        final char character = query.charAt(position);
        char precedingCharacter = WHITE_SPACE;
        if (position > 0) { //if not at start of string 
            precedingCharacter = query.charAt(position - 1);
        }
        return character == COLON_CHARACTER && precedingCharacter == WHITE_SPACE;
    }

    /**
     * Log the SQL query provided and substitute the parameter values in the parameters map for the parameters
     * referenced by :parameterName in the SQL query
     * The query parameters are still traced out at the end of the query.
     * 
     * @param level             level to trace at
     * @param className         name of class invoking trace
     * @param methodName        name of method invoking trace
     * @param query             SQL query 
     * @param parameters        list of the parameters to the SQL query
     */
    public static void detailed(final Level level, final String className, final String methodName, final String query,
            final Map<String, QueryParameter> parameters) {
        final String queryWithParametersInjected = new SQLQueryLogger().injectQueryParameters(query, parameters);
        ServicesLogger.detailed(level, className, methodName, queryWithParametersInjected, parameters);
    }

}
