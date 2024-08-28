/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import com.ericsson.eniq.events.server.common.EventDataSourceType;

/**
 * @author eeikonl
 * @since 2012
 * 
 */
@SuppressWarnings("PMD")
public class ViewTypeSelector {

    public static String returnErrorAggregateViewType(final EventDataSourceType timeRange) {
        if (timeRange != null) {
            switch (timeRange) {
            case AGGREGATED_1MIN:
                return ONE_MINUTE_VIEW;
            case AGGREGATED_15MIN:
                return FIFTEEN_MINUTES_VIEW;
            case AGGREGATED_DAY:
                return DAY_VIEW;
            case RAW:
            default:
                return RAW_VIEW;
            }
        }
        return RAW_VIEW;
    }

    /**
     * New formula for calculating the suc view type, based on the Data Tiering implementation If the Techpack is SGEH
     * or LTE and the timerange is raw or 1 minute, then uset he 15 min views instead
     */
    public static String returnSuccessAggregateViewType(final EventDataSourceType timeRange, final String tpName) {
        if (EVENT_E_SGEH_TPNAME.equalsIgnoreCase(tpName) || EVENT_E_LTE_TPNAME.equalsIgnoreCase(tpName)) {
            switch (timeRange) {
            case AGGREGATED_DAY:
                return DAY_VIEW;
            case RAW:
            case AGGREGATED_1MIN:
            case AGGREGATED_15MIN:
            default:
                return FIFTEEN_MINUTES_VIEW;
            }
        }
        return returnErrorAggregateViewType(timeRange);
    }
}
