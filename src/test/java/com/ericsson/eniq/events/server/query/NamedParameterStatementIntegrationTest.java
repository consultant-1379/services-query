/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.database.DatabaseConnectionHelper;

/**
 * @author etomcor
 * @since May 2010
 *
 */
public class NamedParameterStatementIntegrationTest {

    @Test
    public void testNamedParameterStatement() throws Exception {

        final Map<String, QueryParameter> map = new HashMap<String, QueryParameter>();
        map.put(ApplicationConstants.DATE_FROM, QueryParameter.createStringParameter("2010-05-26 15:38:30"));
        map.put(ApplicationConstants.DATE_TO, QueryParameter.createStringParameter("2010-05-26 15:39:30"));

        final Connection connection = DatabaseConnectionHelper.getDBConnection();
        final TemplateUtils templateUtils = new TemplateUtils();
        templateUtils.applicationStartup();
        final String query = templateUtils.getQueryFromTemplate("q_named_parameter_test.vm", map);
        System.out.println(query);
        final NamedParameterStatement namedParameterStatement = QueryParameter.setParameters(
                new NamedParameterStatement(connection, query), map);
        namedParameterStatement.execute();

    }

    @Test
    public void testNamedParameterStatementLocalDate() throws Exception {

        final Map<String, QueryParameter> map = new HashMap<String, QueryParameter>();
        map.put(ApplicationConstants.LOCAL_DATE_FROM, QueryParameter.createStringParameter("2010-05-26"));
        map.put(ApplicationConstants.LOCAL_DATE_TO, QueryParameter.createStringParameter("2010-05-26"));

        final Connection connection = DatabaseConnectionHelper.getDBConnection();
        final TemplateUtils templateUtils = new TemplateUtils();
        templateUtils.applicationStartup();
        final String query = templateUtils.getQueryFromTemplate("q_named_parameter_test_local_date.vm", map);
        System.out.println(query);
        final NamedParameterStatement namedParameterStatement = QueryParameter.setParameters(
                new NamedParameterStatement(connection, query), map);
        namedParameterStatement.execute();

    }

}
