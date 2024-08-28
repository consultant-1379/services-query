/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import java.util.ArrayList;
import java.util.List;

import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.common.exception.CannotAccessLicensingServiceException;
import com.ericsson.eniq.events.server.licensing.LicensingService;
import com.ericsson.eniq.events.server.logging.ServicesLogger;

/**
 * Service to provide licensing information for specified tech packs.
 * Class acts as bridge between the licensing service and tech pack information
 * 
 * @author EEMECOY
 *
 */
@Stateless
@Local
public class TechPackLicensingService {

    @EJB
    private LicensingService licensingService;

    @EJB
    private TechPackCXCMappingUtils techPackCXCMapping;

    public boolean isTechPackLicensed(final String techPackName) {
        /*
         * EVENT_E_GSN techpack names are EVENT_E_GSN_DT and EVENT_E_GSN_DTPDP, but from a licence point of view it
         * is EVENT_E_GSN. Therefore we need to use EVENT_E_GSN inplace of EVENT_E_GSN_DT and EVENT_E_GSN_DTPDP
         */

        final List<String> cxcLicensesForTechPack = techPackCXCMapping.getTechPackCXCNumbers(techPackName);
        if (cxcLicensesForTechPack.isEmpty()) {
            ServicesLogger.detailed(getClass().toString(), "isTechPackLicensed", "Tech pack  " + techPackName
                    + " doesn't exist in the VERSIONING or TPACTIVATION tables, tech pack isn't installed");
            return false;
        }
        try {
            return doesLicenseExistForAnyCXC(cxcLicensesForTechPack);
        } catch (final CannotAccessLicensingServiceException e) {
            ServicesLogger.exception(getClass().toString(), "isTechPackLicensed", techPackName, e);
            return false;
        }

    }

    private boolean doesLicenseExistForAnyCXC(final List<String> cxcLicensesForTechPack)
            throws CannotAccessLicensingServiceException {
        for (final String license : cxcLicensesForTechPack) {
            if (licensingService.hasLicense(license)) {
                return true;
            }
        }
        return false;
    }

    public void setLicensingService(final LicensingService licensingService) {
        this.licensingService = licensingService;
    }

    public void setTechPackCXCMapping(final TechPackCXCMappingUtils techPackCXCMapping) {
        this.techPackCXCMapping = techPackCXCMapping;
    }

    /**
     * Filter out unlicensed tech packs to return only licensed tech packs
     * 
     * @param techPacks         techpacks to consider
     * @return the tech packs in the input techPacks that are licensed, an empty list if non are licensed
     */
    public List<String> getLicensedTechPacks(final List<String> techPacks) {
        final List<String> licensedTechPacks = new ArrayList<String>();
        for (final String techPack : techPacks) {
            if (isTechPackLicensed(techPack)) {
                licensedTechPacks.add(techPack);
            }
        }
        return licensedTechPacks;
    }

}
