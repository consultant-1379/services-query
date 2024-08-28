/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.parameterchecking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.ParameterPatternConstants.*;
import static com.ericsson.eniq.events.server.utils.json.JSONUtils.*;
import static java.util.regex.Pattern.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.utils.techpacks.TechPackDescriptionMappingsService;

/**
 * 
 * 
 * This class is responsible for parameter checking, both checking that all required parameters exist, and that
 * the provided parameters are valid
 * 
 * @author eemecoy
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class ParameterChecker {

    private static final String IS_NOT_A_VALID_SEARCH_CRITERION_FOR = " is not a valid search criterion for ";

    /**
     * Mapping of types in the request parameters to their associated Regular Expression validation pattern
     */
    private final Map<String, String> typeToPatternMap = new HashMap<String, String>();

    @EJB
    private TechPackDescriptionMappingsService techPackDescriptions;

    {
        typeToPatternMap.put(TYPE_TAC, TAC_PATTERN);
        typeToPatternMap.put(TYPE_APN, APN_PATTERN);
        typeToPatternMap.put(TYPE_BSC, BSC_PATTERN);
        typeToPatternMap.put(TYPE_CELL, CELL_PATTERN);
        typeToPatternMap.put(TYPE_SGSN, SGSN_PATTERN);
        typeToPatternMap.put(SUBBI_FAILURE, EVENTS_PATTERN);
        typeToPatternMap.put(SUBBI_TAU, SUBBI_TAU_PATTERN);
        typeToPatternMap.put(SUBBI_HANDOVER, SUBBI_HANDOVER_PATTERN);
        typeToPatternMap.put(TYPE_MSC, MSC_PATTERN);
        typeToPatternMap.put(MSISDN_PARAM_UPPER_CASE, IMSI_PATTERN);
        typeToPatternMap.put(TYPE_TRAC, IMSI_PATTERN);
    }

    /**
     * Check that the required parameters for a URL query have been provided.
     * The subResource class will provide the required params and this would be
     * called from the subResource class.
     *This code is required to support the existing services code base.
     * @param requestParameters
     *          the request parameters
     * @param requiredParameters
     *          the required parameters
     * @return list of errors if there are missing parameters, null if all
     *         required parameters are present
     */
    List<String> checkRequiredParametersIfExists(final MultivaluedMap<String, String> requestParameters,
            final String[] requiredParameters) {
        final List<String> errors = new ArrayList<String>();
        for (final String requiredParameter : requiredParameters) {
            if (!requestParameters.containsKey(requiredParameter)) {
                errors.add(requiredParameter);
            }
        }
        return errors;
    }

    /**
     * Check valid value. Returns false if the parameters aren't valid, some
     * examples: Entry for the IMSI field isn't valid Entry of 321 for the CELL
     * field isn't valid Node should not be blank for type SOMETYPE Otherwise
     * returns true if all parameters are deemed valid.
     * 
     * @param requestParameters
     *          the request parameters
     * @return true, if valid value, false if not valid
     */
    public boolean checkValidValueOfParameter(final MultivaluedMap<String, String> requestParameters) {
        final String node = requestParameters.getFirst(NODE_PARAM);
        final String type = requestParameters.getFirst(TYPE_PARAM);
        boolean isValid = false;

        if (StringUtils.isNotBlank(node) && type != null && !TYPE_MSISDN.equals(type) && !TYPE_IMSI.equals(type)) {
            isValid = validateNodeParam(node, type);
        } else if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            isValid = validateParamForPattern(GROUPNAME_PATTERN, requestParameters.getFirst(GROUP_NAME_PARAM));
        } else if (requestParameters.containsKey(IMSI_PARAM)) {
            isValid = validateParamForPattern(IMSI_PATTERN, requestParameters.getFirst(IMSI_PARAM));
        } else if (requestParameters.containsKey(MSISDN_PARAM)) {
            isValid = validateParamForPattern(MSISDN_PATTERN, requestParameters.getFirst(MSISDN_PARAM));
        } else if (requestParameters.containsKey(PTMSI_PARAM)) {
            isValid = pTMSIParamChecker(requestParameters);
        } else if (requestParameters.containsKey(EVENT_NAME_PARAM)) {
            isValid = validateParamForPattern(EVENTS_PATTERN, node);
        } else if (requestParameters.containsKey(MSISDN_PARAM)) {
            isValid = validateParamForPattern(IMSI_PATTERN, node);
        }
        return isValid;
    }

    /**
     * Uses the node and type
     * @param node
     * @param type
     * @return
     */
    private boolean validateNodeParam(final String node, final String type) {
        node.trim();
        return compile(typeToPatternMap.get(type)).matcher(node).matches();
    }

    /**
     * 
     * @param pattern
     * @param value
     * @return
     */
    private boolean validateParamForPattern(final String pattern, final String value) {
        return compile(pattern).matcher(value).matches();
    }

    /**
     * Check valid value of PTMSI Parameter
     * @param requestParameters
     * @return true if valid value
     */
    private boolean pTMSIParamChecker(final MultivaluedMap<String, String> requestParameters) {
        boolean isValid;
        isValid = false;
        // PTMSI is a 32bit unsigned number, this is a value between 0 and (32^2
        // -1) which is 0 - 4294967295
        final String ptmsiString = requestParameters.getFirst(PTMSI_PARAM);
        long ptmsi = -1l;
        try {
            ptmsi = Long.parseLong(ptmsiString);
            if (ptmsi > -1L && ptmsi < 4294967296L) {
                isValid = true;
            }
        } catch (final NumberFormatException nfe) {
            ServicesLogger.warn(getClass().getName(), "checkValidValue", "Parameter " + PTMSI_PARAM
                    + " is not a valid number.");
        }
        return isValid;
    }

    /**
     * Validates request parameters against the list of static parameters provided
     * 
     * 
     * @param requestParameters
     * @param staticParameters
     * @return name of parameter if static parameter not present or has invalid value, null otherwise
     */
    String checkStaticParameters(final MultivaluedMap<String, String> requestParameters,
            final MultivaluedMap<String, String> staticParameters) {
        for (final Entry<String, List<String>> mapEntry : staticParameters.entrySet()) {
            final String parameterKey = mapEntry.getKey();
            if (!requestParameters.containsKey(parameterKey)) {
                //parameter hasn't been included in the request
                return parameterKey;
            }

            final boolean isParameterValidValue = mapEntry.getValue()
                    .contains(requestParameters.getFirst(parameterKey));
            if (!isParameterValidValue) {
                return parameterKey;
            }
            return null;
        }
        return null;

    }

    /**Checks if the required parameters for query are present in request parameters
     * @param requiredParametersForQuery
     * @param requestParameters
     * @return true is parameters are valid.
     */
    boolean checkIfParametersAreValid(final List<String> requiredParametersForQuery,
            final MultivaluedMap<String, String> requestParameters) {
        boolean isValid;
        isValid = true;
        for (final String requiredParameter : requiredParametersForQuery) {
            if (!requestParameters.containsKey(requiredParameter)) {
                isValid = false;
            }
        }
        return isValid;
    }

    /**
     * This method performs all the validity checking of the request parameters
     * Contrary to good design, we have multiple return points as we want to return from this method as soon as we
     * get an invalid value, rather than continue on parameter checking.
     * 
     * @param requiredParameters The parameters to check against, populated by the concrete service implementation
     * @param requestParameters Parameters to Validate
     * @param techPackNames The list of TechPacks that are applicable.
     * 
     * @return an empty String if all parameters are valid, a Json Error message otherwise.
     */
    public String performValidityChecking(final RequiredParameters requiredParameters,
            final MultivaluedMap<String, String> requestParameters, final List<String> techPackNames) {

        if (!checkIfParametersAreValid(requiredParameters.getRequiredParameters(), requestParameters)) {
            return jsonErrorInputMsg();// create an error msg in json utils.
        }

        final String invalidStaticParameter = checkStaticParameters(requestParameters,
                requiredParameters.getStaticParameters());
        if (invalidStaticParameter != null) {
            return getErrorMessageForInvalidStaticParameter(invalidStaticParameter, requestParameters, techPackNames);
        }

        if (requiredParameters.requiresParameterCheck()) {
            if (!checkValidValueOfParameter(requestParameters)) {
                return jsonErrorInputMsg();
            }
        }
        return StringUtils.EMPTY;
    }

    String getErrorMessageForInvalidStaticParameter(final String invalidStaticParameter,
            final MultivaluedMap<String, String> requestParameters, final List<String> techPackNames) {
        final String invalidValue = requestParameters.getFirst(invalidStaticParameter);
        if (invalidStaticParameter.equals(DISPLAY_PARAM)) {
            return createDisplayErrorMessage(invalidValue);
        }
        if (invalidStaticParameter.equals(TYPE_PARAM)) {
            return createTypeErrorMessage(techPackNames, invalidValue);
        }
        return jsonErrorInputMsg();
    }

    protected String createTypeErrorMessage(final List<String> techPackNames, final String invalidValue) {
        final List<String> featureNames = techPackDescriptions.getFeatureDescriptionsForTechPacks(techPackNames);
        final StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(invalidValue);
        errorMessage.append(IS_NOT_A_VALID_SEARCH_CRITERION_FOR);
        for (final String featureName : featureNames) {
            errorMessage.append(featureName);
        }
        return createJSONErrorResult(errorMessage.toString());
    }

    private String createDisplayErrorMessage(final String invalidValue) {
        return createJSONErrorResult(E_NO_SUCH_DISPLAY_TYPE + " : " + invalidValue);
    }

    /**
     * @param techPackDescriptions the techPackDescriptions to set
     */
    public void setTechPackDescriptions(final TechPackDescriptionMappingsService techPackDescriptions) {
        this.techPackDescriptions = techPackDescriptions;
    }
}
