/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.HashMap;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.templates.mappingengine.TemplateMappingEngine;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.utils.RequestParametersWrapper;
import com.ericsson.eniq.events.server.utils.datetime.DateTimeHelper;

/**
 * This class is responsible for generating SQL, given the system and user
 * inputted parameters
 *
 * @author eemecoy
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@Local(IQueryGenerator.class)
public class QueryGenerator implements IQueryGenerator {

    @EJB
    private TemplateUtils templateUtils;

    @EJB
    private DateTimeHelper dateTimeHelper;

    @EJB
    private TemplateMappingEngine templateMappingEngine;

    @EJB
    private TimeRangeSelector timeRangeSelector;

    /**
     * Given the parameters specified, generate the SQL to run
     *
     * @param queryGeneratorParameters parameters to generate query
     * @return SQL query
     */
    @Override
    public String getQuery(final QueryGeneratorParameters queryGeneratorParameters) {
        String timerange = null;
        if (queryGeneratorParameters.dateTimeRange != null) {
            timerange = dateTimeHelper.getEventDataSourceType(queryGeneratorParameters.dateTimeRange).toString();
        }

        final Map<String, Object> templateParameters = getTemplateParameters(queryGeneratorParameters, timerange);
        final String viewType = timeRangeSelector.getTimeRangeType(queryGeneratorParameters, timerange);
        final String templateFile = templateMappingEngine.getTemplate(queryGeneratorParameters.templatePath,
                queryGeneratorParameters.requestParameters, queryGeneratorParameters.drillDownTypeForService, viewType);

        return templateUtils.getQueryFromTemplate(templateFile, templateParameters);
    }

    Map<String, Object> getTemplateParameters(final QueryGeneratorParameters queryGeneratorParameters,
            final String timerange) {
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
        final String filterType = requestParametersWrapper.getFilterType();

        templateParameters.put(TECH_PACK_LIST, queryGeneratorParameters.techPackList);
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(COUNT_PARAM,
                requestParametersWrapper.getCountValue(queryGeneratorParameters.maxResultSetSize));
        templateParameters.put(FILTER_TYPE, filterType);
    }

    /**
     * @param templateMappingEngine the templateMappingEngine to set
     */
    public void setTemplateMappingEngine(final TemplateMappingEngine templateMappingEngine) {
        this.templateMappingEngine = templateMappingEngine;
    }

    /**
     * @param dateTimeHelper the dateTimeHelper to set
     */
    public void setDateTimeHelper(final DateTimeHelper dateTimeHelper) {
        this.dateTimeHelper = dateTimeHelper;
    }

    /**
     * @param templateUtils the templateUtils to set
     */
    public void setTemplateUtils(final TemplateUtils templateUtils) {
        this.templateUtils = templateUtils;
    }

    /**
     * @param timeRangeSelector2
     */
    public void setTimeRangeSelector(final TimeRangeSelector timeRangeSelector) {
        this.timeRangeSelector = timeRangeSelector;
    }

}
