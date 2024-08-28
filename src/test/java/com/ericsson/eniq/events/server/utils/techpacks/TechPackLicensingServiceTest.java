/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.utils.techpacks;

import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.common.exception.CannotAccessLicensingServiceException;
import com.ericsson.eniq.events.server.licensing.LicensingService;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

public class TechPackLicensingServiceTest extends BaseJMockUnitTest {

    private static final String NON_EXISTENT_TECH_PACK = "NON_EXISTENT_TECH_PACK";

    private static final String _4G_LICENSE_CXC = "CXC4010933";

    private static final String _DVTP_LICENSE_CXC = "CXC4011640";

    private static final String _2G_LICENSE_CXC = "CXC4010923";

    private static final String _3G_LICENSE_CXC = "CXC4010924";

    private TechPackLicensingService techPackLicensingService;

    LicensingService licensingService;

    TechPackCXCMappingUtils techPackCXCMapping;

    @Before
    public void setup() {
        techPackLicensingService = new TechPackLicensingService();
        licensingService = mockery.mock(LicensingService.class);
        techPackLicensingService.setLicensingService(licensingService);
        techPackCXCMapping = mockery.mock(TechPackCXCMappingUtils.class);
        techPackLicensingService.setTechPackCXCMapping(techPackCXCMapping);
        setUpExpectationsOnTechPackCXCMappingService();
    }

    @Test
    public void testGetLicensedTechPacks_NoneLicensed() throws CannotAccessLicensingServiceException {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_DVTP);
        techPacks.add(EVENT_E_LTE);
        expectCallOnLicensingService(false, _4G_LICENSE_CXC, _DVTP_LICENSE_CXC);
        final List<String> result = techPackLicensingService.getLicensedTechPacks(techPacks);
        assertThat(result.size(), is(0));
    }

    @Test
    public void testGetLicensedTechPacks_SomeLicensed() throws CannotAccessLicensingServiceException {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_DVTP);
        techPacks.add(EVENT_E_LTE);
        expectCallOnLicensingService(true, _DVTP_LICENSE_CXC);
        expectCallOnLicensingService(false, _4G_LICENSE_CXC);
        final List<String> result = techPackLicensingService.getLicensedTechPacks(techPacks);
        assertThat(result.size(), is(1));
        assertThat(result.contains(EVENT_E_DVTP), is(true));
    }

    @Test
    public void testGetLicensedTechPacks_AllLicensed() throws CannotAccessLicensingServiceException {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_DVTP);
        techPacks.add(EVENT_E_LTE);
        expectCallOnLicensingService(true, _DVTP_LICENSE_CXC, _4G_LICENSE_CXC);
        final List<String> result = techPackLicensingService.getLicensedTechPacks(techPacks);
        assertThat(result, is(techPacks));
    }

    @Test
    public void testisTechPackLicensedForNonExistentTechPack() throws Exception {
        assertThat(techPackLicensingService.isTechPackLicensed(NON_EXISTENT_TECH_PACK), is(false));
    }

    @Test
    public void testisTechPackLicensedForEVENT_E_SGEH_Neither2GOr3GLicensePresent() throws Exception {
        final String licensedTechPack = "EVENT_E_SGEH";
        expectCallOnLicensingService(false, _2G_LICENSE_CXC, _3G_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(licensedTechPack), is(false));
    }

    @Test
    public void testisTechPackLicensedForEVENT_E_SGEH_2GLicensePresent() throws Exception {
        final String licensedTechPack = "EVENT_E_SGEH";
        expectCallOnLicensingService(true, _2G_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(licensedTechPack), is(true));
    }

    @Test
    public void testisTechPackLicensedForEVENT_E_SGEH_Only3GLicensePresent() throws Exception {
        final String licensedTechPack = "EVENT_E_SGEH";
        expectCallOnLicensingService(false, _2G_LICENSE_CXC);
        expectCallOnLicensingService(true, _3G_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(licensedTechPack), is(true));
    }

    @Test
    public void testisTechPackLicensedForUnLicensedTechPack() throws Exception {
        final String unlicensedTechPack = "EVENT_E_LTE";
        expectCallOnLicensingService(false, _4G_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(unlicensedTechPack), is(false));
    }

    @Test
    public void testisTechPackLicensedForLicensedTechPack() throws Exception {
        final String licensedTechPack = "EVENT_E_LTE";
        expectCallOnLicensingService(true, _4G_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(licensedTechPack), is(true));
    }

    @Test
    public void testisTechPackLicensedForLicensedTechPack_GSN() throws Exception {
        final String licensedTechPack = EVENT_E_DVTP;
        expectCallOnLicensingService(true, _DVTP_LICENSE_CXC);
        assertThat(techPackLicensingService.isTechPackLicensed(licensedTechPack), is(true));
    }

    private void expectCallOnLicensingService(final boolean hasLicense, final String... licenseCXCs) throws CannotAccessLicensingServiceException {
        for (final String license : licenseCXCs) {
            mockery.checking(new Expectations() {
                {
                    one(licensingService).hasLicense(license);
                    will(returnValue(hasLicense));
                }
            });

        }

    }

    private void setUpExpectationsOnTechPackCXCMappingService() {
        final List<String> sgehLicenses = new ArrayList<String>();
        sgehLicenses.add(_2G_LICENSE_CXC);
        sgehLicenses.add(_3G_LICENSE_CXC);

        final List<String> lteLicenses = new ArrayList<String>();
        lteLicenses.add(_4G_LICENSE_CXC);

        final List<String> gsnLicences = new ArrayList<String>();
        gsnLicences.add(_DVTP_LICENSE_CXC);

        mockery.checking(new Expectations() {
            {
                allowing(techPackCXCMapping).getTechPackCXCNumbers(EVENT_E_SGEH);
                will(returnValue(sgehLicenses));
                allowing(techPackCXCMapping).getTechPackCXCNumbers(EVENT_E_LTE);
                will(returnValue(lteLicenses));
                allowing(techPackCXCMapping).getTechPackCXCNumbers(NON_EXISTENT_TECH_PACK);
                will(returnValue(new ArrayList<String>()));
                allowing(techPackCXCMapping).getTechPackCXCNumbers(EVENT_E_DVTP);
                will(returnValue(gsnLicences));
            }
        });

    }

}
