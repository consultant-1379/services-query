package com.ericsson.eniq.events.server.query.resultsettransformers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Abstraction for conversion of ResultSet to a specified 
 * type. 
 * 
 * @author edeccox
 * @since 2010
 *
 * @param <T>
 */
public interface ResultSetTransformer<T> {
    T transform(List<ResultSet> results) throws SQLException;
    T transform(ResultSet rs) throws SQLException;
}
