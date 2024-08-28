/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import java.sql.SQLException;
import java.util.Map;

import com.ericsson.eniq.events.server.logging.ServicesLogger;

/**
 *
 * @author ehaoswa
 * @author etomcor
 * @since  Apr 2010
 */
public final class QueryParameter {

    private final QueryParameterType type;

    private final Object value;

    public static QueryParameter createStringParameter(final String value) {
        return new QueryParameter(QueryParameterType.STRING, value);
    }

    public static QueryParameter createLongParameter(final Long value) {
        return new QueryParameter(QueryParameterType.LONG, value);
    }

    public static QueryParameter createIntParameter(final Integer value) {
        return new QueryParameter(QueryParameterType.INT, value);
    }

    public static QueryParameter createNullParameter(final Integer value) {
        return new QueryParameter(QueryParameterType.NULL, value);
    }
    
    public static QueryParameter createDBNullParameter() {
        return new QueryParameter(QueryParameterType.DBNULL, null);
    }

    public static QueryParameter createClobParameter(final String value) {
        return new QueryParameter(QueryParameterType.CLOB, value);
    }

    private QueryParameter(final QueryParameterType type, final Object value) {
        super();
        this.type = type;
        this.value = value;
    }

    public QueryParameterType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return ((type != null ? type.toString() : "<type unknown>") + ":" + value);
    }

    /**
     * Given a NamedParameterStatement insert named parameters 
     * appropriately according to supported types. Default to object if type is not supported.
     *  
     * @param stmt
     * @param parameters HashMap of named parameter and the corresponding QueryParameter (value & type)
     * @see NamedParameterStatement 
     * @return
     * @throws SQLException
     */
    public static NamedParameterStatement setParameters(final NamedParameterStatement stmt,
            final Map<String, QueryParameter> parameters) throws SQLException {

        if (stmt == null) {
            throw new IllegalArgumentException("Statement may not be null");
        }

        if (parameters == null || parameters.isEmpty()) {
            return stmt; // nothing to do
        }
        
        if (stmt.getNumberOfParameters() != parameters.size()) {
			ServicesLogger.warn(NamedParameterStatement.class.getName(),
					"setParameters", "All parameters are not set");
			ServicesLogger.info(NamedParameterStatement.class.getName(),
					"NumberOfNamedParameters:", stmt.getNumberOfParameters());
			ServicesLogger.info(NamedParameterStatement.class.getName(),
					"NumberOfQueryParameters:", parameters.size());
		}
        for (final String keyParameterName : parameters.keySet()) {
            final QueryParameter queryParameter = parameters.get(keyParameterName);
            switch (queryParameter.type) {
            case STRING:
                stmt.setString(keyParameterName, (String) queryParameter.value);
                break;
            case LONG:
                stmt.setLong(keyParameterName, (Long) queryParameter.value);
                break;
            case INT:
                stmt.setInt(keyParameterName, (Integer) queryParameter.value);
                break;

            case NULL:
                stmt.setNull(keyParameterName, (Integer) queryParameter.value);
                break;
            case DBNULL:
                stmt.setDBNull(keyParameterName);
                break;
            case CLOB:
                stmt.setClob(keyParameterName, (String) queryParameter.value);
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + queryParameter.type);
            }
        }

        return stmt;
    }

}
