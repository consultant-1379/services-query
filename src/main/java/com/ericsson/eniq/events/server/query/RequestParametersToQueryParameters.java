/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.query;

import java.util.HashMap;
import java.util.Map;

import com.ericsson.eniq.events.server.utils.RATDescriptionMappingUtils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.BSC_SQL_NAME;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.CELL_SQL_NAME;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DELIMITER;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;

/**
 * Helper class to centralize mapping the request parameters received in the URL to the query parameters
 * injected into the SQL queries
 * 
 * @author eemecoy
 *
 */
public class RequestParametersToQueryParameters {

    /**
     * Map the node parameter to the query parameters for a CELL query
     * Eg for the input "00,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G"
     * The cell is 00
     * The rbs is empty
     * The rnc is ONRM_RootMo_R:RNC01:RNC01
     * The vendor is Ericsson
     * The RAT is 3G
     * 
     * @param node                               node parameter as passed from user
     * @param ratDescriptionMappingUtils         service to look up rat mappings
     * @return          query parameters ready to be injected into query
     */
    public Map<String, QueryParameter> mapNodeParameterToCell(final String node,
            final RATDescriptionMappingUtils ratDescriptionMappingUtils) {

        final String[] tokens = node.split(DELIMITER);
        final String cell = tokens[0];
        final String rnc = tokens[2];
        final String vendor = tokens[3];
        final String rat = tokens[4];

        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(CELL_SQL_NAME, QueryParameter.createStringParameter(cell));
        queryParameters.put(BSC_SQL_NAME, QueryParameter.createStringParameter(rnc));
        queryParameters.put(VENDOR_PARAM_UPPER_CASE, QueryParameter.createStringParameter(vendor));
        queryParameters.put(RAT_PARAM,
                QueryParameter.createStringParameter(ratDescriptionMappingUtils.getRATIntegerValue(rat)));

        return queryParameters;
    }

}
