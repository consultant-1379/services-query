/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.utils.techpacks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;

import com.ericsson.eniq.events.server.logging.ServicesLogger;

/**
 * @author ericker
 */
@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.WRITE)
public class TechPackCXCMappingUtils {
    private Map<String, List<String>> techPackLicenseNumbers;

    public void setTechPackLicenseNumbers(final Map<String, List<String>> techPackLicenseNumbers) {
        this.techPackLicenseNumbers = techPackLicenseNumbers;
    }

    public List<String> getTechPackCXCNumbers(final String techPackName) {
        final List<String> cxcNumbers = techPackLicenseNumbers.get(techPackName);
        if (cxcNumbers == null) {
            ServicesLogger.warn(this.getClass().toString(), "getTechPackCXCNumbers", "TechPackLicenseNumbers contains "
                    + techPackLicenseNumbers.size() + " entries");
            return new ArrayList<String>();
        }
        return cxcNumbers;
    }
}
