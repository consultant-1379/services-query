/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.query;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.kpi.KPIQueryfactory;
import com.ericsson.eniq.events.server.utils.RequestParametersWrapper;

import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.Stateless;
import java.util.HashMap;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

/**
 * @author ericker
 */
@Stateless
@Local(IQueryGenerator.class)
public class KPIQueryGenerator implements IQueryGenerator {

    @EJB
    KPIQueryfactory kpiQueryfactory;

    @Override
    public String getQuery(final QueryGeneratorParameters queryGeneratorParameters) {
        final String timerange = EventDataSourceType.AGGREGATED_DAY.getValue();

        final Map<String, Object> templateParameters = getTemplateParameters(queryGeneratorParameters, timerange);

        return kpiQueryfactory.getSGEHKPIQuery(templateParameters, queryGeneratorParameters.kpiList, queryGeneratorParameters.techPackList);
    }

    Map<String, Object> getTemplateParameters(final QueryGeneratorParameters queryGeneratorParameters, final String timerange) {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        addCommonTemplateParameters(templateParameters, timerange, queryGeneratorParameters);
        templateParameters.putAll(queryGeneratorParameters.serviceSpecificTemplateParameters);
        return templateParameters;
    }

    private void addCommonTemplateParameters(final Map<String, Object> templateParameters, final String timerange,
                                             final QueryGeneratorParameters queryGeneratorParameters) {
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(
                queryGeneratorParameters.requestParameters);
        final String type = requestParametersWrapper.getType();
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(COUNT_PARAM,
                requestParametersWrapper.getCountValue(queryGeneratorParameters.maxResultSetSize));
    }

    public void setKpiQueryfactory(final KPIQueryfactory kpiQueryfactory) {
        this.kpiQueryfactory = kpiQueryfactory;
    }


}
