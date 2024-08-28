/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.common.ApplicationConstants;

/**
 * @author eeikonl
 * @since 2012
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class TimeRangeSelector {

    /**
     * @param queryGeneratorParameters
     * @param timerange
     * @return
     */
    public String getTimeRangeType(final QueryGeneratorParameters queryGeneratorParameters, final String timerange) {

        if (queryGeneratorParameters.isDataTiered) {
            return getDataTieringViewType(timerange, queryGeneratorParameters.isExclusiveTacRelated);
        }

        return getAggregateViewType(timerange, queryGeneratorParameters.isExclusiveTacRelated);
    }

    public String getTimeRangeType(final String timerange, final boolean isExclusiveTacRelated,
            final boolean isDataTiered) {
        if (isDataTiered) {
            return getDataTieringViewType(timerange, isExclusiveTacRelated);

        }
        return getAggregateViewType(timerange, isExclusiveTacRelated);
    }

    private String getAggregateViewType(final String timerange, final boolean isExclusiveTacRelated) {
        if (isExclusiveTacRelated) {
            return RAW_VIEW;
        }
        return ApplicationConstants.returnAggregateViewType(timerange);
    }

    /**
     * @param timerange
     * @param isExclusiveTacRelated
     * @return
     */
    private String getDataTieringViewType(final String timerange, final boolean isExclusiveTacRelated) {
        if (isExclusiveTacRelated) {
            return RAW_VIEW;
        }
        return ApplicationConstants.returnDataTieringViewType(timerange);
    }

}
