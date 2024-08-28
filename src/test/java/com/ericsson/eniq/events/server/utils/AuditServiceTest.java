/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.ejb.SessionContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.eniq.events.server.logging.audit.ServicesAuditLogger;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author EEMECOY
 * @author eromsza
 *
 */
@RunWith(JMock.class)
public class AuditServiceTest extends BaseJMockUnitTest {

    private static final String SUBSCRIBER_RANKING_URI = "http://localhost:8081/SUBSCRIBER/RANKING";

    private static final String TERMINAL_RANKING_URI = "http://localhost:8081/TERMINAL/RANKING";

    private static final String principalName = "somePrincipal";

    private AuditService auditService;

    private SessionContext mockedSessionContext;

    private UriInfo uriInfo;

    private ServicesAuditLogger servicesAuditLogger;

    private HttpHeaders httpHeaders;

    private final Principal dummyPrincipal = new Principal() {
        @Override
        public String getName() {
            return principalName;
        }
    };

    @Before
    public void setup() {
        auditService = new AuditService();
        servicesAuditLogger = mockery.mock(ServicesAuditLogger.class);
        auditService.setServicesAuditLogger(servicesAuditLogger);
        mockedSessionContext = mockery.mock(SessionContext.class);
        auditService.setSessionContext(mockedSessionContext);
        uriInfo = mockery.mock(UriInfo.class);
        httpHeaders = mockery.mock(HttpHeaders.class);
    }

    @Test
    public void testLogAuditEntryForQueryWithoutHttpHeadersAsParameter_Terminal() {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.add(REQUEST_URI, TERMINAL_RANKING_URI);
        final String query = "a query";
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        expectDummyPrincipal(dummyPrincipal);
        auditService.logAuditEntryForQuery(parameters, query, queryParameters);
    }

    @Test
    public void testLogAuditEntryForURIWithoutHttpHeadersAsParameter_Terminal() {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.add(REQUEST_URI, TERMINAL_RANKING_URI);
        final String ipAddress = "0.0.0.0";
        final List<String> listOfIpAddresses = new ArrayList<String>();
        listOfIpAddresses.add(ipAddress);
        parameters.put(IP_ADDRESS_PARAM, listOfIpAddresses);
        expectDummyPrincipal(dummyPrincipal);
        auditService.logAuditEntryForURI(parameters);
    }

    @Test
    public void testLogAuditEntryForQueryWithoutHttpHeadersAsParameter_Subscriber() {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.add(REQUEST_URI, SUBSCRIBER_RANKING_URI);
        final String query = "a query";
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        expectFINEOnAuditLogger(query, queryParameters);
        auditService.logAuditEntryForQuery(parameters, query, queryParameters);
    }

    @Test
    public void testLogAuditEntryForURIWithoutHttpHeadersAsParameter_Subscriber() {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.add(REQUEST_URI, SUBSCRIBER_RANKING_URI);
        final String ipAddress = "0.0.0.0";
        final List<String> listOfIpAddresses = new ArrayList<String>();
        listOfIpAddresses.add(ipAddress);
        parameters.put(IP_ADDRESS_PARAM, listOfIpAddresses);
        expectINFOOnServicesAuditLogger(listOfIpAddresses, SUBSCRIBER_RANKING_URI);
        auditService.logAuditEntryForURI(parameters);
    }

    @Test
    public void testAuditNotLoggedForTerminalQuery() throws Exception {
        final Map<String, QueryParameter> queryParameters = null;
        final String query = null;
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        expectGetRequestURIOnURIInfo(new URI(TERMINAL_RANKING_URI));
        expectOnHttpHeaders(new ArrayList<String>());
        expectDummyPrincipal(dummyPrincipal);
        auditService.logAuditEntryForQuery(uriInfo, requestParameters, query, queryParameters, httpHeaders);
    }

    @Test
    public void testAuditLoggedForQueryWithPTMSIParam() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_PTMSI);
        runTestForQueryAuditLogging(TERMINAL_RANKING_URI, requestParameters);
    }

    @Test
    public void testAuditLoggedForQueryWithIMSIParam() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        runTestForQueryAuditLogging(TERMINAL_RANKING_URI, requestParameters);
    }

    @Test
    public void testAuditLoggedForSubscriberQuery() throws Exception {
        final MultivaluedMap<String, String> requestParameters = null;
        runTestForQueryAuditLogging(SUBSCRIBER_RANKING_URI, requestParameters);
    }

    private void runTestForQueryAuditLogging(final String uri, final MultivaluedMap<String, String> requestParameters)
            throws URISyntaxException {
        final URI uriObject = new URI(uri);
        expectGetRequestURIOnURIInfo(uriObject);
        expectOnHttpHeaders(new ArrayList<String>());
        final String query = "some query";
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put("SOME_PARAM", QueryParameter.createIntParameter(2));
        expectFINEOnAuditLogger(query, queryParameters);
        auditService.logAuditEntryForQuery(uriInfo, requestParameters, query, queryParameters, httpHeaders);
    }

    @Test
    public void testAuditNotLoggedForTerminalURI() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        expectGetRequestURIOnURIInfo(new URI(TERMINAL_RANKING_URI));
        expectOnHttpHeaders(new ArrayList<String>());
        expectDummyPrincipal(dummyPrincipal);
        auditService.logAuditEntryForURI(uriInfo, requestParameters, httpHeaders);
    }

    @Test
    public void testAuditLoggedForURIWithPTMSIParameter() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_PTMSI);
        runTestForURIAuditLogging(TERMINAL_RANKING_URI, requestParameters);
    }

    @Test
    public void testAuditLoggedForURIWithIMSIParameter() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        runTestForURIAuditLogging(TERMINAL_RANKING_URI, requestParameters);
    }

    @Test
    public void testAuditLoggedForSubscriberURI() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        runTestForURIAuditLogging(SUBSCRIBER_RANKING_URI, requestParameters);
    }

    private void runTestForURIAuditLogging(final String uri, final MultivaluedMap<String, String> requestParameters)
            throws URISyntaxException {
        final URI uriObject = new URI(uri);
        expectGetRequestURIOnURIInfo(uriObject);
        final String ipAddress = "0.0.0.0";
        final List<String> listOfIpAddresses = new ArrayList<String>();
        listOfIpAddresses.add(ipAddress);
        expectOnHttpHeaders(listOfIpAddresses);
        expectINFOOnServicesAuditLogger(listOfIpAddresses, uriObject.toString());
        expectDummyPrincipal(dummyPrincipal);

        auditService.logAuditEntryForURI(uriInfo, requestParameters, httpHeaders);
    }

    private void expectDummyPrincipal(final Principal principal) {
        mockery.checking(new Expectations() {
            {
                allowing(mockedSessionContext).getCallerPrincipal();
                will(returnValue(principal));
            }
        });
    }

    private void expectFINEOnAuditLogger(final String query, final Map<String, QueryParameter> queryParameters) {
        mockery.checking(new Expectations() {
            {
                one(servicesAuditLogger).detailed(Level.FINE, query, queryParameters);
                allowing(mockedSessionContext).getCallerPrincipal();
                will(returnValue(null));
            }
        });
    }

    private void expectINFOOnServicesAuditLogger(final List<String> listOfIpAddresses, final String uri) {
        mockery.checking(new Expectations() {
            {
                one(servicesAuditLogger).detailed(Level.INFO, new ArrayList<String>(), listOfIpAddresses,
                        new Object[] { uri });
                allowing(mockedSessionContext).getCallerPrincipal();
                will(returnValue(null));
            }
        });

    }

    private void expectOnHttpHeaders(final List<String> listOfIpAddresses) {

        mockery.checking(new Expectations() {
            {
                one(httpHeaders).getRequestHeader("srcIpAddress");
                will(returnValue(listOfIpAddresses));
            }
        });

    }

    private void expectGetRequestURIOnURIInfo(final URI uri) {
        mockery.checking(new Expectations() {
            {
                allowing(uriInfo).getRequestUri();
                will(returnValue(uri));
            }
        });
    }

}
