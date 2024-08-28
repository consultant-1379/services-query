/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_LTE;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_SGEH;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class TechPackDescriptionMappingsServiceTest extends BaseJMockUnitTest {

    private static final String CXC_4G_TECH_PACK = "CXC1313131";

    private static final String CXC_2G_TECH_PACK = "CXC9999999";

    private static final String NON_EXISTENT_TECH_PACK_CXC = "CXC6666666";

    private static final Object OTHER_CXC = "not a tech pack cxc";

    private static final String NON_EXISTENT_TECH_PACK = "non existent tech pack";

    private static final String CXC_3G_TECH_PACK = "CXC987987";

    private TechPackDescriptionMappingsService techPackDescriptionMappingsService;

    public Properties fileContents;

    TechPackCXCMappingUtils techPackCXCMapping;

    @Before
    public void setup() {
        techPackDescriptionMappingsService = new StubbedTechPackDescriptionMappingsService();
        techPackCXCMapping = mockery.mock(TechPackCXCMappingUtils.class);
        techPackDescriptionMappingsService.setTechPackCXCMappingUtils(techPackCXCMapping);
    }

    @Test
    public void testReadFeatureDescriptionFile_TechPackNotInFile() throws IOException {
        fileContents = new Properties();
        techPackDescriptionMappingsService.readAndCacheFeatureDescriptions();
        expectCallOnMappingService(NON_EXISTENT_TECH_PACK, NON_EXISTENT_TECH_PACK_CXC);
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(NON_EXISTENT_TECH_PACK);
        final List<String> result = techPackDescriptionMappingsService.getFeatureDescriptionsForTechPacks(techPacks);
        assertThat(result.size(), is(1));
        assertThat(result.contains(NON_EXISTENT_TECH_PACK_CXC), is(true));
    }

    private void expectCallOnMappingService(final String techPackName, final String... techPackCxcs) {
        final List<String> listOfCXCsForTechPack = new ArrayList<String>();
        for (final String techPackCXC : techPackCxcs) {
            listOfCXCsForTechPack.add(techPackCXC);
        }
        mockery.checking(new Expectations() {
            {
                one(techPackCXCMapping).getTechPackCXCNumbers(techPackName);
                will(returnValue(listOfCXCsForTechPack));
            }
        });

    }

    @Test
    public void testGetFeatureDescriptionForTechPackWithNoCXC() {
        final List<String> techPackNames = new ArrayList<String>();
        final String techPackNotInDatabase = "a tech pack thats not in the database ie we dont know its cxc number";
        techPackNames.add(techPackNotInDatabase);
        expectCallOnMappingService(techPackNotInDatabase);
        final List<String> result = techPackDescriptionMappingsService
                .getFeatureDescriptionsForTechPacks(techPackNames);
        assertThat(result.size(), is(1));
        assertThat(result.get(0), is(techPackNotInDatabase));
    }

    @Test
    public void testNonTechPackCXCsDoesntCauseException() throws FileNotFoundException, IOException {
        fileContents = new Properties();
        fileContents.put(OTHER_CXC, ":Subscriber Business Intelligence::FAJ 121 1546");
        techPackDescriptionMappingsService.readAndCacheFeatureDescriptions();
    }

    @Test
    public void testReadFeatureDescriptionFile() throws IOException {
        fileContents = new Properties();
        final String descriptionFor4GTechPack = "Ericsson SGSN-MME 4G Event";
        final String fullStringFor4GTechPack = ":" + descriptionFor4GTechPack + " Tech Pack::FAJ 121 1533/4G";
        fileContents.put(CXC_4G_TECH_PACK, fullStringFor4GTechPack);
        final String descriptionFor2GTechPack = "Ericsson SGSN-MME 2G Event";
        final String fullStringFor2GTechPack = ":" + descriptionFor2GTechPack + " Tech Pack::FAJ 121 1533/2G";
        fileContents.put(CXC_2G_TECH_PACK, fullStringFor2GTechPack);
        final String descriptionFor3GTechPack = "Ericsson SGSN-MME 3G Event";
        final String fullStringFor3GTechPack = ":" + descriptionFor3GTechPack + " Tech Pack::FAJ 121 1533/3G";
        fileContents.put(CXC_3G_TECH_PACK, fullStringFor3GTechPack);
        techPackDescriptionMappingsService.readAndCacheFeatureDescriptions();
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_SGEH);
        techPacks.add(EVENT_E_LTE);
        expectCallOnMappingService(EVENT_E_SGEH, CXC_2G_TECH_PACK, CXC_3G_TECH_PACK);
        expectCallOnMappingService(EVENT_E_LTE, CXC_4G_TECH_PACK);
        final List<String> result = techPackDescriptionMappingsService.getFeatureDescriptionsForTechPacks(techPacks);
        assertThat(result.size(), is(3));
        assertThat(result.contains(descriptionFor4GTechPack), is(true));
        assertThat(result.contains(descriptionFor2GTechPack), is(true));
        assertThat(result.contains(descriptionFor2GTechPack), is(true));
    }

    class StubbedTechPackDescriptionMappingsService extends TechPackDescriptionMappingsService {
        /* (non-Javadoc)
         * @see com.ericsson.eniq.events.server.utils.techpacks.TechPackDescriptionMappingsService#readFile()
         */
        @Override
        Properties readFeatureDescriptionsFile() throws FileNotFoundException, IOException {
            return fileContents;
        }
    }

}
