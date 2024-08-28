/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks.timerangequeries;

import java.util.List;

import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;

/**
 * Code to fetch the raw tables applicable for a particular view
 * For example, given the view name EVENT_E_SGEH_ERR_RAW, this class can return either the raw tables applicable
 * for a specified time range, or the latest raw tables in use.
 * The result will be in the form of [EVENT_E_SGEH_ERR_RAW_01, EVENT_E_SGEH_ERR_RAW_03]
 * 
 * @author eemecoy
 *
 */
public interface TimerangeQuerier {

    /**
     * Query the time range view to fetch the raw tables for the specified view and dateTimeRange
     * 
     * @param dateTimeRange             specified date time range to query the time range for
     * @param view                      name of view eg EVENT_E_SGEH_ERR_RAW
     * @return list of raw tables applicable for given view and time range
     */
    List<String> getRAWTablesUsingQuery(FormattedDateTimeRange dateTimeRange, String view);

    /**
     * Query the time range view to fetch the latest raw tables for the specified view
     *
     * 
     * @param view                      name of view eg EVENT_E_SGEH_ERR_RAW
     * @return list of latest raw tables in use for given view
     */
    List<String> getLatestTablesUsingQuery(String view);

}
