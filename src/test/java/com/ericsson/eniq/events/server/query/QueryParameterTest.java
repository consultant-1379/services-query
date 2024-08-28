package com.ericsson.eniq.events.server.query;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;

/**
 * 
 * @author edeccox
 * 
 */
public class QueryParameterTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterFailNullStatement() throws Exception {
        QueryParameter.setParameters(null, new HashMap<String, QueryParameter>());
    }

    @Test
    public void testSetParameters() throws Exception {
        final Mockery context = new Mockery() {
            {
                setImposteriser(ClassImposteriser.INSTANCE);
            }
        };
        final NamedParameterStatement stmt = context.mock(NamedParameterStatement.class);

        final Map<String, QueryParameter> parameters = new HashMap<String, QueryParameter>();

        final Object obj = new Object();
        final String text = "this is text";

        parameters.put("1", QueryParameter.createStringParameter("this is text"));
        parameters.put("2", QueryParameter.createIntParameter(Integer.MIN_VALUE));
        parameters.put("3", QueryParameter.createLongParameter(Long.MAX_VALUE));

        context.checking(new Expectations() {
            {
                one(stmt).setString("1", text);
                one(stmt).setInt("2", Integer.MIN_VALUE);
                one(stmt).setLong("3", Long.MAX_VALUE);
                allowing(stmt).getNumberOfParameters();
                will(returnValue(3));
            }
        });

        QueryParameter.setParameters(stmt, parameters);
    }

    @Test
    public void testSetParametersParamsUnEqual() throws Exception {
        final Mockery context = new Mockery() {
            {
                setImposteriser(ClassImposteriser.INSTANCE);
            }
        };
        final NamedParameterStatement stmt = context.mock(NamedParameterStatement.class);

        Map<String, QueryParameter> parameters = new HashMap<String, QueryParameter>();

        final Object obj = new Object();
        final String text = "this is text";

        parameters.put("1", QueryParameter.createStringParameter("this is text"));
        parameters.put("2", QueryParameter.createIntParameter(Integer.MIN_VALUE));
        parameters.put("3", QueryParameter.createLongParameter(Long.MAX_VALUE));

        context.checking(new Expectations() {
            {
                one(stmt).setString("1", text);
                one(stmt).setInt("2", Integer.MIN_VALUE);
                one(stmt).setLong("3", Long.MAX_VALUE);
                allowing(stmt).getNumberOfParameters();
                will(returnValue(2));
            }
        });

        QueryParameter.setParameters(stmt, parameters);
    }
    
    @Test
    public void testCreateStringParameter() {
        QueryParameter p;
        p = QueryParameter.createStringParameter("this is text");
        assertNotNull(p);
        assertEquals(QueryParameterType.STRING, p.getType());
        assertEquals("this is text", p.getValue());
        assertEquals("STRING:this is text", p.toString());
    }

    @Test
    public void testCreateIntegerParameter() {
        QueryParameter p;
        p = QueryParameter.createIntParameter(23);
        assertNotNull(p);
        assertEquals(QueryParameterType.INT, p.getType());
        assertEquals(23, p.getValue());
        assertEquals("INT:23", p.toString());
    }

}
