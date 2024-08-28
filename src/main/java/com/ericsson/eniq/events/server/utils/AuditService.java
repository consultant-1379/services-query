/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.SessionContext;
import javax.ejb.Stateless;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import com.ericsson.eniq.events.server.logging.audit.ServicesAuditLogger;
import com.ericsson.eniq.events.server.query.QueryParameter;

/**
 * Class that performs Audit Logging for services
 * Acts as bridge between the GenericService layer (and deprecated BaseResource class) and the ServiceAuditLogger layer
 *
 * Methods provided for the legacy BaseResource class accept the HttpHeaders and URIInfo object, and will extract
 * the audit information from these objects
 * Methods provided for the newer GenericService class accept a map of parameters instead - this map should contain
 * all required parameters ie IP_ADDRESS_PARAM and REQUEST_URI
 *
 * @author EEMECOY
 *
 */
@Stateless
@Local
@DeclareRoles(DECLARED_ROLE)
public class AuditService {

    @EJB
    private ServicesAuditLogger servicesAuditLogger;

    @Resource
    SessionContext sessionContext;

    /**
     * Check and create info audit log entry for uri.
     *
     * @param uriInfo
     * @param requestParameters the request parameters
     * @param httpHeaders       the http headers - will contain information on user name and ip address
     *
     * @deprecated - use the logAuditEntryForURI that takes a map of parameter values instead
     */
    @Deprecated
    public void logAuditEntryForURI(final UriInfo uriInfo, final MultivaluedMap<String, String> requestParameters,
            final HttpHeaders httpHeaders) {
        final String requestUri = getRequestUriFromURIInfo(uriInfo);
        final List<String> userNames = getPrincipalNameAsList();
        final List<String> ipAddresses = getIpAddressesFromHttpHeader(httpHeaders);
        logAuditEntryForURI(requestUri, userNames, ipAddresses, requestParameters);
    }

    /**
     * Check and create info audit log entry for uri.
     *
     * @param parameters          parameters from resource layer, containing the request URI, user name, ip address
     */
    public void logAuditEntryForURI(final MultivaluedMap<String, String> parameters) {
        final String requestUri = parameters.getFirst(REQUEST_URI);
        final List<String> ipAddresses = parameters.get(IP_ADDRESS_PARAM);
        logAuditEntryForURI(requestUri, getPrincipalNameAsList(), ipAddresses, parameters);

    }

    private List<String> getIpAddressesFromHttpHeader(final HttpHeaders httpHeaders) {
        return httpHeaders.getRequestHeader(IP_ADDRESS_PARAM);
    }

    private List<String> getPrincipalNameAsList() {
        final List<String> userNames = new ArrayList<String>();
        final Principal principal = sessionContext.getCallerPrincipal();
        if (principal != null) {
            userNames.add(principal.getName());
        }
        return userNames;
    }

    private String getRequestUriFromURIInfo(final UriInfo uriInfo) {
        return uriInfo.getRequestUri().toString();
    }

    private void logAuditEntryForURI(final String requestUri, final List<String> userNames,
            final List<String> ipAddresses, final MultivaluedMap<String, String> requestParameters) {
        if (checkAuditLogParams(requestUri, requestParameters)) {
            createAuditLogEntry(userNames, ipAddresses, Level.INFO, requestUri);
        }

    }

    public void logAuditEntryForQuery(final UriInfo uriInfo, final MultivaluedMap<String, String> requestParameters,
            final List<String> queries, final Map<String, QueryParameter> queryParameters, final HttpHeaders httpHeaders) {
        final String requestUri = getRequestUriFromURIInfo(uriInfo);
        final List<String> userNames = getPrincipalNameAsList();
        final List<String> ipAddresses = getIpAddressesFromHttpHeader(httpHeaders);

        if (checkAuditLogParams(requestUri, requestParameters)) {
            createAuditLogEntry(userNames, ipAddresses, Level.FINE, queries, queryParameters);
        }
    }

    /**
     * If conditions exist for creating an audit log entry, then create a FINE
     * audit log entry
     *
     * @param requestParameters         request parameters from URI
     * @param query                     SQL query
     * @param queryParameters           parameters to SQL query
     * @param httpHeaders               http headers, containing user name and ip address
     *
     * @deprecated use the logAuditEntryForQuery() that takes a map of parameters instead
      */
    @Deprecated
    public void logAuditEntryForQuery(final UriInfo uriInfo, final MultivaluedMap<String, String> requestParameters,
            final String query, final Map<String, QueryParameter> queryParameters, final HttpHeaders httpHeaders) {
        final String requestUri = getRequestUriFromURIInfo(uriInfo);
        final List<String> userNames = getPrincipalNameAsList();
        final List<String> ipAddresses = getIpAddressesFromHttpHeader(httpHeaders);
        logAuditEntryForQuery(requestUri, userNames, ipAddresses, requestParameters, query, queryParameters);
    }

    /**
     * If conditions exist for creating an audit log entry, then create a FINE
     * audit log entry
     *
     * @param parameters                parameters from resource layer, containing the request URI, user name, ip address
     * @param query                     SQL query
     * @param queryParameters           parameters to SQL query
     *
      */
    public void logAuditEntryForQuery(final MultivaluedMap<String, String> parameters, final String query,
            final Map<String, QueryParameter> queryParameters) {
        final String requestUri = parameters.getFirst(REQUEST_URI);
        final List<String> ipAddresses = parameters.get(IP_ADDRESS_PARAM);

        logAuditEntryForQuery(requestUri, getPrincipalNameAsList(), ipAddresses, parameters, query, queryParameters);
    }

    private void logAuditEntryForQuery(final String requestUri, final List<String> userNames,
            final List<String> ipAddresses, final MultivaluedMap<String, String> requestParameters, final String query,
            final Map<String, QueryParameter> queryParameters) {
        if (checkAuditLogParams(requestUri, requestParameters)) {
            createAuditLogEntry(userNames, ipAddresses, Level.FINE, query, queryParameters);
        }

    }

    /**
     * The method calls the Audit Logger to create a log entry.
     *
    * @param logLevel
    *          the log level at which the entry will be made
    * @param info
    *          the content of the log message
     * @param httpHeaders
    */
    private void createAuditLogEntry(final List<String> userNames, final List<String> ipAddresses,
            final Level logLevel, final Object... info) {
        if (logLevel.equals(Level.INFO)) {
            // in this case, we know that info array contains only one object
            servicesAuditLogger.detailed(logLevel, userNames, ipAddresses, info);
        } else {
            servicesAuditLogger.detailed(logLevel, info);
        }
    }

    /**
     * This method checks if the conditions for creating an Audit Log entry are
     * met.
     *
     * @param requestUri
     *          the Services URI requested
     * @param requestParameters
     *          the parameters supplied with the URI
      * @return true if the conditions for creating an Audit Log entry are met
      */
    private boolean checkAuditLogParams(final String requestUri, final MultivaluedMap<String, String> requestParameters) {
        if (requestUri.contains(SUBSCRIBER_SERVICES)
                || (requestParameters.containsKey(TYPE_PARAM) && requestParameters.get(TYPE_PARAM).contains(TYPE_IMSI))
                || (requestParameters.containsKey(TYPE_PARAM) && requestParameters.get(TYPE_PARAM).contains(TYPE_PTMSI))
                || (requestParameters.containsKey(IMSI_PARAM))) {
            return true;
        }
        return false;
    }

    /**
     * @param servicesAuditLogger the servicesAuditLogger to set
     */
    public void setServicesAuditLogger(final ServicesAuditLogger servicesAuditLogger) {
        this.servicesAuditLogger = servicesAuditLogger;
    }

    /**
     * @param servicesAuditLogger the servicesAuditLogger to set
     */
    public void setSessionContext(final SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

}
