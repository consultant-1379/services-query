/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.utils.parameterchecking;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

/**
 * Data type for parameter checking
 * @author ericker
 */
public class RequiredParameters {
    private final MultivaluedMap<String, String> staticParameters;
    private final List<String> requiredParameters;
    private final boolean requiresParameterCheck;

    public RequiredParameters(final MultivaluedMap<String, String> staticParameters, final List<String> requiredParameters, final boolean requiresParameterCheck) {
        this.staticParameters = staticParameters;
        this.requiredParameters = requiredParameters;
        this.requiresParameterCheck = requiresParameterCheck;
    }

    public MultivaluedMap<String, String> getStaticParameters() {
        return staticParameters;
    }

    public List<String> getRequiredParameters() {
        return requiredParameters;
    }

    public boolean requiresParameterCheck() {
        return requiresParameterCheck;
    }
}
