/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query.resultsettransformers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * the data transformer to check if the ResultSet size is exactly 1
 * 
 * @author ejoegaf
 * @since 2011
 *
 */
public class ResultSetTransformerToCheckResultSizeIsOne implements
		ResultSetTransformer<Boolean> {
	
	/**
	 * @return true if the result set contains exactly one row
	 */
    @Override
    public Boolean transform(final ResultSet rs) throws SQLException {
    	rs.last();
    	final int lastRowNumber = rs.getRow();
        return lastRowNumber == 1;//is the first one also the last one.
    }

    @Override
    public Boolean transform(final List<ResultSet> results) throws SQLException {
        return null;
    }
}
