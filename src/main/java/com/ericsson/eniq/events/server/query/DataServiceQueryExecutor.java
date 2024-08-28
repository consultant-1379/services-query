/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.logging.performance.ServicesPerformanceThreadLocalHolder.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.exception.ServiceException;
import com.ericsson.eniq.events.server.common.exception.ServiceUserInfoException;
import com.ericsson.eniq.events.server.datasource.DBConnectionManager;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicy;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicyFactory;
import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.query.resultsettransformers.ResultSetTransformer;

/**
 * Class that handles running queries against databases
 * Also responsible for managing request ids - ie checking that a request hasn't been cancelled prior to
 * running the query
 *
 * @author eemecoy
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class DataServiceQueryExecutor {

    @EJB
    private DBConnectionManager dbConnectionManager;

    @EJB
    private RequestIdMappingService requestIdMappings;

    @EJB
    private LoadBalancingPolicyFactory loadBalancingPolicyFactory;

    public <T> T getDataForMultipleQueries(final String requestID, final List<String> queries,
            final Map<String, QueryParameter> parameters, final ResultSetTransformer<T> transformer,
            final LoadBalancingPolicy loadBalancingPolicy) {
        NamedParameterStatement pstmt = null;

        Connection conn = null; //NOPMD (ejoegaf 20/6/2011 database connection closed in another method)
        final List<Connection> connectionsList = new ArrayList<Connection>();
        final List<NamedParameterStatement> statementsList = new ArrayList<NamedParameterStatement>();
        final List<ResultSet> resultsSetsList = new ArrayList<ResultSet>();
        try {
            setQueryExecutionStartTime(Calendar.getInstance().getTimeInMillis());
            for (final String query : queries) {
                SQLQueryLogger.detailed(Level.FINE, getClass().getName(), "getData", query, parameters);
                conn = this.dbConnectionManager.getConnection(loadBalancingPolicy);
                connectionsList.add(conn);
                pstmt = QueryParameter.setParameters(new NamedParameterStatement(conn, query), parameters);
                statementsList.add(pstmt);
                if (requestIdMappings.isCancelFailedForReqId(requestID)) {
                    return null;
                }
                if (requestID == null || requestID.isEmpty()) {
                    throw new ServiceException("Request ID is null/empty");
                }
                if (!requestID.equalsIgnoreCase(CANCEL_REQ_NOT_SUPPORTED)) {
                    ServicesLogger.detailed(Level.FINE, getClass().getName(), "getDataForAnyTransformerReturnType()",
                            "Added requestId::" + requestID);
                    requestIdMappings.put(requestID, pstmt);
                }
                final ResultSet resultSet = pstmt.executeQuery(); //NOPMD (eemecoy 17/10/2011, resultSet objects closed in closeConnections())
                if (!requestID.equalsIgnoreCase(CANCEL_REQ_NOT_SUPPORTED) && !requestIdMappings.containsKey(requestID)) {
                    return null;
                }
                resultsSetsList.add(resultSet);
            }
            return transformer.transform(resultsSetsList);
        } catch (final SQLException sqlEx) {
            final Exception wrappedEx = sqlEx.getNextException();
            if (wrappedEx != null) {
                final String wrappedExMsg = wrappedEx.getMessage();
                if ((StringUtils.contains(wrappedExMsg, DATABASE_IO_EXCEPTION_CODE))) {
                    //the database query has timed out
                    throw new ServiceUserInfoException(E_DATABASE_TIMEOUT);
                }
            }
            throw new ServiceException(sqlEx);
        } catch (final Exception e) {
            throw new ServiceException(e);
        } finally {
            closeConnections(connectionsList, statementsList, resultsSetsList);
            removeRequestID(requestID);
            setQueryExecutionEndTime(Calendar.getInstance().getTimeInMillis());
        }
    }

    private void closeConnections(final List<Connection> connectionsList,
            final List<NamedParameterStatement> statementsList, final List<ResultSet> resultsSetsList) {
        //we need to catch the exceptions individually
        //   to ensure the database connection gets closed
        for (final ResultSet rs : resultsSetsList) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (final Exception e) {
                    ServicesLogger.warn(getClass().getName(), "closeConnections", e);
                }
            }
        }
        for (final NamedParameterStatement pstmt : statementsList) {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (final Exception e) {
                    ServicesLogger.warn(getClass().getName(), "closeConnections", e);
                }
            }
        }
        for (final Connection conn : connectionsList) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (final Exception e) {
                    ServicesLogger.warn(getClass().getName(), "closeConnections", e);
                }
            }
        }
    }

    private void closeConnections(final Connection conn, final NamedParameterStatement pstmt, final ResultSet rs) {
        //we need to catch the exceptions individually
        //   to ensure the database connection gets closed
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (final Exception e) {
            ServicesLogger.warn(getClass().getName(), "closeConnections", e);
        }
        try {
            if (pstmt != null) {
                while (pstmt.getMoreResults()) {
                    pstmt.getResultSet().close();
                }
                pstmt.close();
            }
        } catch (final Exception e) {
            ServicesLogger.warn(getClass().getName(), "closeConnections", e);
        }
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (final SQLException e) {
            ServicesLogger.warn(getClass().getName(), "closeConnections", e);
        }
    }

    public <T> T getData(final String requestID, final String query, final Map<String, QueryParameter> parameters,
            final ResultSetTransformer<T> transformer, final LoadBalancingPolicy loadBalancingPolicy) {
        try {
            final Connection connectionToDwh = this.dbConnectionManager.getConnection(loadBalancingPolicy); //NOPMD eemecoy 20/7/11 connection is closed by the private getData() method
            return runQuery(requestID, query, parameters, transformer, connectionToDwh);
        } catch (final Exception e) {
            throw new ServiceException(e);
        }
    }

    /**
     * Returns result set of type specified by the ResultSetTransformer
     *
     * @param query SQL query to be prepared
     * @param parameters Query Parameters
     * @param transformer The transformer used to conver the SQL ResultSet to any object
     * @return result set as transformed by the specified transformer, and with type specified in the ResultSetTransformer
     * @return conn     connection to database to use
     */
    private <T> T runQuery(final String requestID, final String query, final Map<String, QueryParameter> parameters,
            final ResultSetTransformer<T> transformer, final Connection conn) {
        NamedParameterStatement pstmt = null; //NOPMD (ejoegaf 20/6/2011 database connection closed in another method)
        ResultSet rs = null; //NOPMD (ejoegaf 20/6/2011 database connection closed in another method)
        try {
            SQLQueryLogger.detailed(Level.FINE, getClass().getName(), "getData", query, parameters);
            setQueryExecutionStartTime(Calendar.getInstance().getTimeInMillis());
            pstmt = QueryParameter.setParameters(new NamedParameterStatement(conn, query), parameters);
            if (requestIdMappings.isCancelFailedForReqId(requestID)) {
                return null;
            }
            if (requestID == null || requestID.isEmpty()) {
                throw new ServiceException("Request ID is null/empty");
            }
            if (!requestID.equalsIgnoreCase(CANCEL_REQ_NOT_SUPPORTED)) {
                requestIdMappings.put(requestID, pstmt);
                ServicesLogger.detailed(Level.FINE, getClass().getName(), "getDataForAnyTransformerReturnType()",
                        "Added requestId::" + requestID);
            }
            rs = pstmt.executeQuery();
            if (!requestID.equalsIgnoreCase(CANCEL_REQ_NOT_SUPPORTED) && !requestIdMappings.containsKey(requestID)) {
                return null;
            }
            return transformer.transform(rs);
        } catch (final SQLException sqlEx) {
            final Exception wrappedEx = sqlEx.getNextException();
            if (wrappedEx != null) {
                final String wrappedExMsg = wrappedEx.getMessage();
                if ((StringUtils.contains(wrappedExMsg, DATABASE_IO_EXCEPTION_CODE))) {
                    //the database query has timed out
                    throw new ServiceUserInfoException(E_DATABASE_TIMEOUT);
                }
            }
            throw new ServiceException(sqlEx);
        } catch (final Exception e) {
            throw new ServiceException(e);
        } finally {
            closeConnections(conn, pstmt, rs);
            setQueryExecutionEndTime(Calendar.getInstance().getTimeInMillis());
            removeRequestID(requestID);
        }
    }

    private void removeRequestID(final String requestID) {
        requestIdMappings.remove(requestID);
        requestIdMappings.removeFailedCancelReqId(requestID);
        ServicesLogger.detailed(Level.FINE, getClass().getName(), "getDataForAnyTransformerReturnType()",
                "Removed cancel RequestID=" + requestID, "from RequestIdMappingService");
    }

    /**
     * Run query against the repdb database (contains meta data for ENIQ EVENTS)
     *
     * @param requestID         request id of query
     * @param query             SQL query to run
     * @param parameters        query parameters
     * @param transformer       transformer to use on result set
     */
    public <T> T getDataFromRepdb(final String requestID, final String query,
            final Map<String, QueryParameter> parameters, final ResultSetTransformer<T> transformer) {
        try {
            final Connection connectionToDwh = this.dbConnectionManager.getDwhrepConnection(); //NOPMD (eemecoy 17/10/11 connection closed in getData() method)
            return runQuery(requestID, query, parameters, transformer, connectionToDwh);
        } catch (final Exception e) {
            throw new ServiceException(e);
        }
    }

    /**
     * Inserts/Updates data in the repdb database
     * @param requestID request id of query
     * @param query SQL statement to run
     * @param parameters statement parameters
     */
    public void updateDataInRepdb(final String requestID, final String query,
            final Map<String, QueryParameter> parameters) {
        try {
            final Connection connectionToDwh = this.dbConnectionManager.getDwhrepConnection(); //NOPMD (eemecoy 17/10/11 connection closed in getData() method)
            updateData(requestID, query, parameters, connectionToDwh);
        } catch (final Exception e) {
            throw new ServiceException(e);
        }
    }

    private void updateData(final String requestID, final String query, final Map<String, QueryParameter> parameters,
            final Connection conn) {
        NamedParameterStatement pstmt = null; //NOPMD
        try {
            SQLQueryLogger.detailed(Level.FINE, getClass().getName(), "updateData", query, parameters);
            setQueryExecutionStartTime(Calendar.getInstance().getTimeInMillis());
            pstmt = QueryParameter.setParameters(new NamedParameterStatement(conn, query), parameters);
            if (requestIdMappings.isCancelFailedForReqId(requestID)) {
                return;
            }
            if (!requestID.equalsIgnoreCase(CANCEL_REQ_NOT_SUPPORTED)) {
                requestIdMappings.put(requestID, pstmt);
                ServicesLogger.detailed(Level.FINE, getClass().getName(), "updateData()", "Added requestId::"
                        + requestID);
            }
            pstmt.executeUpdate();
        } catch (final SQLException sqlEx) {
            final Exception wrappedEx = sqlEx.getNextException();
            if (wrappedEx != null) {
                final String wrappedExMsg = wrappedEx.getMessage();
                if ((StringUtils.contains(wrappedExMsg, DATABASE_IO_EXCEPTION_CODE))) {
                    //the database query has timed out
                    throw new ServiceUserInfoException(E_DATABASE_TIMEOUT);
                }
            }
            throw new ServiceException(sqlEx);
        } catch (final Exception e) {
            throw new ServiceException(e);
        } finally {
            closeConnections(conn, pstmt, null);
            setQueryExecutionEndTime(Calendar.getInstance().getTimeInMillis());
            removeRequestID(requestID);
        }
    }

    public <T> T getData(final String query, final ResultSetTransformer<T> transformer) {
        return getData(CANCEL_REQ_NOT_SUPPORTED, query, null, transformer,
                loadBalancingPolicyFactory.getDefaultLoadBalancingPolicy());
    }

    /**
     * for junit test case
     *
     * @param dbConnectionManager the dbConnectionManager to set
     */
    public void setDbConnectionManager(final DBConnectionManager dbConnectionManager) {
        this.dbConnectionManager = dbConnectionManager;
    }

    /**
     * for junit test case
     * @param requestIdMappings the requestIdMappings to set
     */
    public void setRequestIdMappings(final RequestIdMappingService requestIdMappings) {
        this.requestIdMappings = requestIdMappings;
    }

    /**
     * for junit test case
     *
     * @param loadBalancingPolicyFactory the loadBalancingPolicyFactory to set
     */
    public void setLoadBalancingPolicyFactory(final LoadBalancingPolicyFactory loadBalancingPolicyFactory) {
        this.loadBalancingPolicyFactory = loadBalancingPolicyFactory;
    }
}
