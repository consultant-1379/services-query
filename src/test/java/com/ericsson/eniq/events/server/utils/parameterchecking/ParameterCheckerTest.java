/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.parameterchecking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.test.util.JSONTestUtils;
import com.ericsson.eniq.events.server.utils.techpacks.TechPackDescriptionMappingsService;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ebhasou
 */
public class ParameterCheckerTest extends BaseJMockUnitTest {

    private static final String VALID_STATIC_PARAMETER_VALUE = "someValidStaticParameterValue";

    private static final String SAMPLE_STATIC_PARAMETER = "someSampleStaticParameter";

    private static final String INVALID_DISPLAY_TYPE = "invalidDisplay";

    private static final String INVALID_SAMPLE_PARAMETER_VALUE = "someInvalidStaticParameterValue";

    private final String EXPECTED_ERROR_MESSAGE_FOR_INVALID_DISPLAY_TYPE = "No such display type : "
            + INVALID_DISPLAY_TYPE;

    private ParameterChecker parameterChecker;

    TechPackDescriptionMappingsService techPackDescriptionsService;

    @Before
    public void setup() {
        parameterChecker = new ParameterChecker();
        techPackDescriptionsService = mockery.mock(TechPackDescriptionMappingsService.class);
        parameterChecker.setTechPackDescriptions(techPackDescriptionsService);
    }

    @Test
    public void testgetErrorMessageForInvalidStaticParameter_Type() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_PTMSI);
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_RAN_CFA);
        final String techPackDescription = "WCDMA CFA Feature";
        final List<String> techPackDescriptions = new ArrayList<String>();
        techPackDescriptions.add(techPackDescription);
        expectCallForTechPackDescriptions(techPacks, techPackDescriptions);
        assertThat(parameterChecker.getErrorMessageForInvalidStaticParameter(TYPE_PARAM, requestParameters, techPacks),
                is(putTogetherJsonErrorResult("PTMSI is not a valid search criterion for " + techPackDescription)));
    }

    private void expectCallForTechPackDescriptions(final List<String> techPackNames,
            final List<String> techPackDescriptions) {
        mockery.checking(new Expectations() {
            {
                one(techPackDescriptionsService).getFeatureDescriptionsForTechPacks(techPackNames);
                will(returnValue(techPackDescriptions));
            }
        });

    }

    @Test
    public void testgetErrorMessageForInvalidStaticParameter_Display() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(DISPLAY_PARAM, INVALID_DISPLAY_TYPE);
        assertThat(parameterChecker.getErrorMessageForInvalidStaticParameter(DISPLAY_PARAM, requestParameters, null),
                is(putTogetherJsonErrorResult(EXPECTED_ERROR_MESSAGE_FOR_INVALID_DISPLAY_TYPE)));
    }

    private String putTogetherJsonErrorResult(final String errorMessage) {
        return "{\"success\":\"false\",\"errorDescription\":\"" + errorMessage + "\"}";
    }

    @Test
    public void testPerformValidityChecking_NoStaticParametersRequired_ReturnsEmptyString() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_RNC);
        final List<String> requiredParametersList = new ArrayList<String>();
        requiredParametersList.add(TYPE_PARAM);
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        final RequiredParameters requiredParameters = new RequiredParameters(staticParameters, requiredParametersList,
                false);
        assertThat(parameterChecker.performValidityChecking(requiredParameters, requestParameters, null), is(""));
    }

    @Test
    public void testperformValidityChecking_AllStaticParametersAreValid() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_RNC);
        requestParameters.putSingle(SAMPLE_STATIC_PARAMETER, VALID_STATIC_PARAMETER_VALUE);
        final List<String> requiredParametersList = new ArrayList<String>();
        requiredParametersList.add(TYPE_PARAM);
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(SAMPLE_STATIC_PARAMETER, VALID_STATIC_PARAMETER_VALUE);
        final RequiredParameters requiredParameters = new RequiredParameters(staticParameters, requiredParametersList,
                false);
        assertThat(parameterChecker.performValidityChecking(requiredParameters, requestParameters, null), is(""));
    }

    @Test
    public void testperformValidityChecking_InvalidDisplayParameter() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_RNC);
        requestParameters.putSingle(DISPLAY_PARAM, INVALID_DISPLAY_TYPE);
        final List<String> requiredParametersList = new ArrayList<String>();
        requiredParametersList.add(TYPE_PARAM);
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        final RequiredParameters requiredParameters = new RequiredParameters(staticParameters, requiredParametersList,
                true);
        assertThat(parameterChecker.performValidityChecking(requiredParameters, requestParameters, null),
                is(putTogetherJsonErrorResult(EXPECTED_ERROR_MESSAGE_FOR_INVALID_DISPLAY_TYPE)));
    }

    @Test
    public void testperformValidityChecking_InalidStaticParameter() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        final List<String> requiredParametersList = new ArrayList<String>();
        requiredParametersList.add(TYPE_PARAM);
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(SAMPLE_STATIC_PARAMETER, INVALID_SAMPLE_PARAMETER_VALUE);
        final String expectedErrorMessage = "{\"success\":\"false\",\"errorDescription\":\"Please input a valid value\"}";
        final RequiredParameters requiredParameters = new RequiredParameters(staticParameters, requiredParametersList,
                true);
        final String result = parameterChecker.performValidityChecking(requiredParameters, requestParameters, null);
        JSONTestUtils.isValidJson(result);
        assertThat(result, is(expectedErrorMessage));
    }

    @Test
    public void testperformValidityChecking_ValidDisplayParameters() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_APN);
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        final List<String> requiredParametersList = new ArrayList<String>();
        requiredParametersList.add(TYPE_PARAM);
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, CHART_PARAM);
        staticParameters.add(DISPLAY_PARAM, GRID_PARAM);
        final RequiredParameters requiredParameters = new RequiredParameters(staticParameters, requiredParametersList,
                false);
        assertThat(parameterChecker.performValidityChecking(requiredParameters, requestParameters, null), is(""));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidLTEERBS() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        final String validLteCell = "ERBS1,Ericsson,4G";
        requestParameters.putSingle(NODE_PARAM, validLteCell);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidLTECell() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
        final String validLteCell = "LTECELL1,,ERBS1,Ericsson,4G";
        requestParameters.putSingle(NODE_PARAM, validLteCell);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidRNCWith3GAsRATValue() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        final String validRNC = "ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G";
        requestParameters.putSingle(NODE_PARAM, validRNC);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueTypeSpecifiedButNoNode() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckInValidSpacesInIMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String invalidIMSI = "42 53 8876";
        requestParameters.putSingle(IMSI_PARAM, invalidIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidShortValueValidIMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String validIMSI = "4253";
        requestParameters.putSingle(IMSI_PARAM, validIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidLongValueInValidIMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String invalidIMSI = "74635263859483756453";
        requestParameters.putSingle(IMSI_PARAM, invalidIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidLongValueValidIMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String validIMSI = "74635263859483756";
        requestParameters.putSingle(IMSI_PARAM, validIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidIMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String invalidIMSI = "4253abc";
        requestParameters.putSingle(IMSI_PARAM, invalidIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckInValidValueInValidIMSI_Empty() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String invalidIMSI = "";
        requestParameters.putSingle(IMSI_PARAM, invalidIMSI);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidGroupName() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String validGroupName = "myGroup";
        requestParameters.putSingle(GROUP_NAME_PARAM, validGroupName);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidGroupName() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String validGroupName = "myGroup&&";
        requestParameters.putSingle(GROUP_NAME_PARAM, validGroupName);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidSubBIFailures() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, SUBBI_FAILURE);
        final String validEventValue = "ATTACH,0";
        requestParameters.putSingle(NODE_PARAM, validEventValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidSubBIFailures() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, SUBBI_FAILURE);
        final String invalidEventValue = "ATTACH";
        requestParameters.putSingle(NODE_PARAM, invalidEventValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidSGSN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_SGSN);
        final String validSGSNValue = "SGSN1";
        requestParameters.putSingle(NODE_PARAM, validSGSNValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidSGSN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_SGSN);
        final String invalidSGSNValue = "SGSN1,someotherparamthatshouldntbehere";
        requestParameters.putSingle(NODE_PARAM, invalidSGSNValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidCellWith3GRATValue() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
        final String validCellValue = "CELL1,,RNC1,ERICSSON,3G";
        requestParameters.putSingle(NODE_PARAM, validCellValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidCell() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
        final String validCellValue = "CELL1,,BSCT1,ERICSSON,WCDMA";
        requestParameters.putSingle(NODE_PARAM, validCellValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidCell() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
        final String invalidCellValueWithoutRAT = "CELL1,BSCT1,ERICSSON";
        requestParameters.putSingle(NODE_PARAM, invalidCellValueWithoutRAT);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidBSC() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        final String validBSCValue = "BSC_1234,NION,WCDMA";
        requestParameters.putSingle(NODE_PARAM, validBSCValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidBSC() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        final String invalidBSCValueWithOutVendorAndRAT = "BSC_1234";
        requestParameters.putSingle(NODE_PARAM, invalidBSCValueWithOutVendorAndRAT);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidTAC() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_TAC);
        final String validTacValue = "1100,1037200";
        requestParameters.putSingle(NODE_PARAM, validTacValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));

    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidTAC() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_TAC);
        final String validTacValue = "blah";
        requestParameters.putSingle(NODE_PARAM, validTacValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueValidAPN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        final String validApnTacValue = "cmlap";
        requestParameters.putSingle(NODE_PARAM, validApnTacValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInValidAPN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        final String invalidApnTacValue = "cmla%%p";
        requestParameters.putSingle(NODE_PARAM, invalidApnTacValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueMinPMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(PTMSI_PARAM, String.valueOf(0L));
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueMaxPTMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(PTMSI_PARAM, String.valueOf(4294967295L));
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInvalidMaxPTMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(PTMSI_PARAM, String.valueOf(4294967297L));
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInvalidPTMSI() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(PTMSI_PARAM, String.valueOf(-1L));
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInvalidPTMSINotEvenAProperNumber() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(PTMSI_PARAM, "NotANumber");
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueMSISDN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String validMSISDN = "123456789012345678";
        requestParameters.putSingle(MSISDN_PARAM, validMSISDN);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInvalidMSISDN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String invalidMSISDN = "1234567890123456789";
        requestParameters.putSingle(MSISDN_PARAM, invalidMSISDN);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValuevalidEventsNameParam() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String eventValue = "A,43452";
        final String nodeValue = "A,43452";
        requestParameters.putSingle(EVENT_NAME_PARAM, eventValue);
        requestParameters.putSingle(NODE_PARAM, nodeValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(true));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testcheckValidValueInvalidEventsNameParam() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final String eventValue = "A,43452";
        final String nodeValue = "43452";
        requestParameters.putSingle(EVENT_NAME_PARAM, eventValue);
        requestParameters.putSingle(NODE_PARAM, nodeValue);
        assertThat(parameterChecker.checkValidValueOfParameter(requestParameters), is(false));
    }

    @Test
    public void testCheckStaticParameters_ParameterNotIncluded() {
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        assertThat(parameterChecker.checkStaticParameters(new MultivaluedMapImpl(), staticParameters),
                is(DISPLAY_PARAM));
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker#checkRequiredParametersForQueryExists(javax.ws.rs.core.MultivaluedMap, java.util.List)}.
     */
    @Test
    public void testCheckStaticParametersAreValid() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);

        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        assertThat(parameterChecker.checkStaticParameters(requestParameters, staticParameters), is((String) null));
    }

    @Test
    public void testCheckStaticParametersAreInvalid() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, "XXX");

        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        assertThat(parameterChecker.checkStaticParameters(requestParameters, staticParameters), is(DISPLAY_PARAM));
    }

    @Test
    public void testcheckRequiredParametersIfExists() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, "GRID");

        final String[] requiredParameters = { DISPLAY_PARAM };
        final List<String> errorValues = parameterChecker.checkRequiredParametersIfExists(requestParameters,
                requiredParameters);
        assertThat(errorValues.size(), is(0));
    }

    @Test
    public void testcheckRequiredParametersIfExistsInvalid() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, "GRID");

        final String[] requiredParameters = { DISPLAY_PARAM };
        final List<String> errorValues = parameterChecker.checkRequiredParametersIfExists(requestParameters,
                requiredParameters);
        assertThat(errorValues.size(), is(1));
        assertThat(errorValues.get(0), is(DISPLAY_PARAM));
    }

    @Test
    public void testcheckIfParametersAreValid() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, "GRID");

        final List<String> requiredParametersForQuery = new ArrayList<String>();
        requiredParametersForQuery.add(DISPLAY_PARAM);
        assertThat(parameterChecker.checkIfParametersAreValid(requiredParametersForQuery, requestParameters), is(true));
    }

    @Test
    public void testcheckIfParametersAreValidinvalidList() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, "GRID");

        final List<String> requiredParametersForQuery = new ArrayList<String>();
        requiredParametersForQuery.add(TYPE_PARAM);
        assertThat(parameterChecker.checkIfParametersAreValid(requiredParametersForQuery, requestParameters), is(false));
    }
}
