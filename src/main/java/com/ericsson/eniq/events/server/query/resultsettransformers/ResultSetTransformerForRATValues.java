/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query.resultsettransformers;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result Set Transformer specifically for pulling RAT values and descriptions out of database
 * Produces map of strings - key is integer value, eg 0, entry is description for that RAT type, eg GSM
 * @author eemecoy
 *
 */
public class ResultSetTransformerForRATValues implements ResultSetTransformer<Map<String, String>> {

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.query.ResultSetTransformer#transform(java.sql.ResultSet)
     */
    @Override
    public Map<String, String> transform(final ResultSet rs) throws SQLException {
        final Map<String, String> ratValues = new HashMap<String, String>();
        while (rs != null && rs.next()) {
            ratValues.put(rs.getString(RAT_COLUMN_NAME), rs.getString(RAT_DESC_COLUMN_NAME));
        }
        return ratValues;
    }

    @Override
    public Map<String, String> transform(final List<ResultSet> results) throws SQLException {
        return null;
    }

}
