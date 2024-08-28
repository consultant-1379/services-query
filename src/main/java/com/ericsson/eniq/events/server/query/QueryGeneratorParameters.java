/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.kpi.KPI;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;

/**
 * Holder for the parameters to generate a query with QueryGenerator
 *
 * @author EEMECOY
 */
public class QueryGeneratorParameters {

    public String templatePath;

    public MultivaluedMap<String, String> requestParameters;

    public Map<String, Object> serviceSpecificTemplateParameters;

    public FormattedDateTimeRange dateTimeRange;

    public String drillDownTypeForService;

    public int maxResultSetSize;

    public TechPackList techPackList;

    public List<KPI> kpiList;

    public boolean isExclusiveTacRelated;

    public boolean isDataTiered;

    /**
     * Holder for the parameters to generate a query with QueryGenerator
     *
     * @param templatePath            path to the template for query, used when querying templateMap.csv
     * @param requestParameters       parameters provided by user
     * @param serviceSpecificTemplateParameters
     *                                template parameters specificed by resource
     * @param dateTimeRange           date time range to use in query
     * @param drillDownTypeForService drill type for this query (can be null)
     * @param maxResultSetSize        max allowable size for the JSON result set
     */
    public QueryGeneratorParameters(final String templatePath, final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> serviceSpecificTemplateParameters, final FormattedDateTimeRange dateTimeRange,
            final String drillDownTypeForService, final int maxResultSetSize, final TechPackList techPackList,
            final List<KPI> kpiList, final boolean isExclusiveTacRelated, final boolean isDataTiered) {
        this.templatePath = templatePath;
        this.requestParameters = requestParameters;
        this.serviceSpecificTemplateParameters = serviceSpecificTemplateParameters;
        this.dateTimeRange = dateTimeRange;
        this.drillDownTypeForService = drillDownTypeForService;
        this.maxResultSetSize = maxResultSetSize;
        this.techPackList = techPackList;
        this.kpiList = kpiList;
        this.isExclusiveTacRelated = isExclusiveTacRelated;
        this.isDataTiered = isDataTiered;
    }
}