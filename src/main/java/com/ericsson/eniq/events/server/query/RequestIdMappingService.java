/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;

/**
 * Class which maintains a map of request ID values to NamedParameterStatement
 * The request Id will be added to the map when a new request is received by
 * services, and stored to cancel the DB request for a particular request ID
 * Class also provides access methods for this map
 *
 * @author echchik
 */

@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.WRITE)
public class RequestIdMappingService {

    /**
     * Map of Request Id values to prepared statement execution object
     */
    private final Map<String, NamedParameterStatement> requestIdMappings = new ConcurrentHashMap<String, NamedParameterStatement>();

    /**
     * List of request ID that has no preparedstatement value when the
     * cancel request was received
     */
    private final Map<String, Long> failedCancelReqId = new ConcurrentHashMap<String, Long>();

    private long timeOutInMillis;

    @EJB
    private ApplicationConfigManager applicationManager;

    /**
     * This method fetches the time out to be used for retaining failed cancel
     * request ID
     */
    @PostConstruct
    public void getTimeOutInMillis() {
        final long timeOutInMin = applicationManager.getCancelRequestTimeOut();
        timeOutInMillis = timeOutInMin * SECONDS_IN_1_MIN * MILLISECOND_IN_1_SECOND;
    }

    @PreDestroy
    public void clearMap() {
        requestIdMappings.clear();
    }

    /*
     * This method returns
     * true : if requestID exist in the requestIdMappings map
     */
    public boolean containsKey(final String requestID) {
        if (requestID == null) {
            return false;
        }
        return requestIdMappings.containsKey(requestID);
    }

    /*
     * Removes the requestID entry from the requestIdMappings map
     */
    public void remove(final String requestID) {
        if (requestID == null) {
            return;
        }
        synchronized (requestIdMappings) {
            requestIdMappings.remove(requestID);
            requestIdMappings.notifyAll();
        }

    }

    /*
     * return true if requestIdMappings map is empty else false
     */
    @Lock(LockType.READ)
    public boolean isEmpty() {
        return requestIdMappings.isEmpty();
    }

    /*
     * Adds the entry into requestIdMappings map with key as requestID and
     * value as pstmt
     */
    public boolean put(final String requestID, final NamedParameterStatement pstmt) {
        if (requestID == null || pstmt == null) {
            return false;
        }
        requestIdMappings.put(requestID, pstmt);
        return true;
    }

    /*
     * returns the NamedParameterStatement corresponding to requestID
     * null if requestID key does not exists in the requestIdMappings map
     */
    public NamedParameterStatement get(final String requestID) {
        if (requestID == null) {
            return null;
        }
        synchronized (requestIdMappings) {
            try {
                return requestIdMappings.get(requestID);
            } finally {
                requestIdMappings.notifyAll();
            }
        }
    }

    /*
     * Checks if any entry exists in failedCancelReqId map for requestID key
     */
    public boolean isCancelFailedForReqId(final String requestID) {
        if (requestID == null) {
            return false;
        }
        return failedCancelReqId.containsKey(requestID);
    }

    /**
     * Adds the entry into failedCancelReqId map with key as requestID and
     * value as present time in millisecond
     *
     * @param requestId corresponds to this request for cancelling later
     */
    public void addFailedCancelReqId(final String requestID) {
        if (requestID == null) {
            return;
        }
        synchronized (failedCancelReqId) {
            removeOldEntries();
            failedCancelReqId.put(requestID, Calendar.getInstance().getTimeInMillis());
        }
    }

    /**
     * Removes the requestID entry from the failedCancelReqId map
     *
     * @param requestId corresponds to this request for cancelling later
     */
    public void removeFailedCancelReqId(final String requestID) {
        if (requestID == null) {
            return;
        }
        synchronized (failedCancelReqId) {
            failedCancelReqId.remove(requestID);
        }
    }

    /*
     * This method is used to remove old entries greater that twice the cancel
     * request time out parameter
     */
    private void removeOldEntries() {
        final long currentTimeInMillis = Calendar.getInstance().getTimeInMillis();
        final Set<String> allCancelFailedReqId = failedCancelReqId.keySet();
        for (final String requestId : allCancelFailedReqId) {
            final long cancelFailedTimeInMillis = failedCancelReqId.get(requestId);
            if (currentTimeInMillis - cancelFailedTimeInMillis >= timeOutInMillis) {
                failedCancelReqId.remove(requestId);
            }
        }
    }

    /**
     * @param applicationManager the applicationManager to set
     */
    public void setApplicationManager(final ApplicationConfigManager applicationManager) {
        this.applicationManager = applicationManager;
    }
}