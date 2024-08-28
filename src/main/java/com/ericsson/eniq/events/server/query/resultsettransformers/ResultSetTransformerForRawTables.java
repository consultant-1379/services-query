package com.ericsson.eniq.events.server.query.resultsettransformers;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAW_TABLE_NAME_COLUMN;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Conversion of ResultSet to a RAW Table type. 
 * 
 * @author eavidat
 * @since 2010
 *
 */

public class ResultSetTransformerForRawTables implements ResultSetTransformer<List<String>> {
    @Override
    public List<String> transform(final ResultSet rs) throws SQLException {
        final List<String> tableNames = new ArrayList<String>();
        while (rs != null && rs.next()) {
        	final String string = rs.getString(RAW_TABLE_NAME_COLUMN);
			tableNames.add(string);
        }
        return tableNames;
    }
    

	@Override
	public List<String> transform(final List<ResultSet> results)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}    
}
