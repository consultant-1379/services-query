/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping for the types for query parameters
 * 
 * @author EEMECOY
 *
 */
public abstract class QueryParameterTypeMap {

    private static Map<String, QueryParameterType> queryParameterTypes = new HashMap<String, QueryParameterType>();

    public static QueryParameterType get(final String parameterName) {
        final QueryParameterType type = queryParameterTypes.get(parameterName);
        if (type == null) {
            throw new RuntimeException("No type defined for the parameter " + parameterName);
        }
        return type;
    }

    static {
        queryParameterTypes.put(RNC_ID_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(RNCID, QueryParameterType.INT);
        queryParameterTypes.put(RNC_ALTERNATIVE_FDN_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(RAN_VENDOR_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(EVENT_ID_PARAM, QueryParameterType.INT);
        queryParameterTypes.put(TAC_PARAM, QueryParameterType.LONG);
        queryParameterTypes.put(TAC, QueryParameterType.LONG);
        queryParameterTypes.put(CELL_ID_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(EXTENDED_CAUSE_VALUE_COLUMN, QueryParameterType.INT);
        queryParameterTypes.put(CAUSE_VALUE, QueryParameterType.INT);
        queryParameterTypes.put(CELL_ID_COLUMN, QueryParameterType.INT);
        queryParameterTypes.put(IMSI_PARAM, QueryParameterType.LONG);
        queryParameterTypes.put(IMSI_PARAM_UPPER_CASE, QueryParameterType.LONG);
        queryParameterTypes.put(GROUP_NAME_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(GROUP_NAME_KEY, QueryParameterType.STRING);
        queryParameterTypes.put(MARKETING_NAME_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(ALTERNATIVE_FDN_PARAM, QueryParameterType.STRING);
        queryParameterTypes.put(HIER3_ID, QueryParameterType.LONG);
    }

}
