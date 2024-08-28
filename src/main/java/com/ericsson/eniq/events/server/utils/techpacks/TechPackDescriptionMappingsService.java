/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import com.ericsson.eniq.events.server.common.ApplicationConfigConstants;

/**
 * Class responsible for reading the ENIQ feature_descriptions file on start up, and caching the feature descriptions for
 * each tech pack CXC.  If a tech pack CXC doesn't exist in this file, then the CXC number is returned as the description.
 * <p/>
 * The format of the /eniq/sw/conf/feature_descriptions file is:
 * CXC4010923::Ericsson SGSN-MME 2G Event Tech Pack::FAJ 121 1533/2G
 * CXC4010924::Ericsson SGSN-MME 3G Event Tech Pack::FAJ 121 1533/3G
 * <p/>
 * This class parses the stored description for each tech pack CXC to remove the colons preceding it, and the " Tech Pack"
 * and FAJ tokens following it ie for the example above:
 * CXC4010923 - description is "Ericsson SGSN-MME 2G Event"
 * CXC4010924 - description is "Ericsson SGSN-MME 3G Event"
 *
 * @author eemecoy
 */
@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.WRITE)
public class TechPackDescriptionMappingsService {

    @Resource(name = ApplicationConfigConstants.ENIQ_EVENT_PROPERTIES)
    private Properties eniqEventsProperties;

    private static final String TECH_PACK_TOKEN = " Tech Pack";

    private final Map<String, String> techPackDescriptionsMap = new HashMap<String, String>();

    @EJB
    private TechPackCXCMappingUtils techPackCXCMappingUtils;

    private static final String DEFAULT_FEATURE_DESCRIPTION_FILE_LOCATION = File.separator + "eniq" + File.separator
            + "sw" + File.separator + "conf" + File.separator + "feature_descriptions";

    @PostConstruct
    public void readAndCacheFeatureDescriptions() throws IOException {
        final Properties cxcFeatureDescriptions = readFeatureDescriptionsFile();
        for (final Object techPackCXC : cxcFeatureDescriptions.keySet()) {
            techPackDescriptionsMap.put((String) techPackCXC,
                    parseFeatureDescription((String) cxcFeatureDescriptions.get(techPackCXC)));
        }
    }

    @PreDestroy
    public void applicationDestroy() {
        techPackDescriptionsMap.clear();
    }

    /**
     * Read the contents of the feature descriptions file
     * Has default access in order to get under test
     *
     * @return Properties               map of the properties in the file
     */
    Properties readFeatureDescriptionsFile() throws FileNotFoundException, IOException {
        final String fileName = eniqEventsProperties.getProperty(FEATURE_DESCRIPTION_FILE_LOCATION_PROPERTY_NAME,
                DEFAULT_FEATURE_DESCRIPTION_FILE_LOCATION);
        final File file = new File(fileName);
        InputStream inputStream;
        if (file.exists()) {
            inputStream = new FileInputStream(fileName);
        } else {
            // Try to load from the classpath, used in testing
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        }
        final Properties fileContents = new Properties();
        fileContents.load(inputStream);
        return fileContents;
    }

    /**
     * Feature description as read from file will be :Data Bearer Throughput and Data Volume Tech Pack::FAJ 121 1619
     * This method will return the description token from this string ie Data Bearer Throughput and Data Volume Tech Pack
     *
     * @param featureDescriptionFromFile the string retrieved from the feature description file
     * @return the description of the tech pack, extracted from the input parameter
     */
    private String parseFeatureDescription(final String featureDescriptionFromFile) {
        final StringTokenizer stringTokenizer = new StringTokenizer(featureDescriptionFromFile, COLON);
        final String fullTechPackDescription = stringTokenizer.nextToken();
        return removeTokensAtEnd(fullTechPackDescription);
    }

    private String removeTokensAtEnd(final String fulltechPackDescription) {
        if (fulltechPackDescription.contains(TECH_PACK_TOKEN)) {
            return fulltechPackDescription.substring(0, fulltechPackDescription.indexOf(TECH_PACK_TOKEN));
        }
        return fulltechPackDescription;
    }

    /**
     * Get the feature descriptions for given tech packs
     * If the CXC number for a given tech pack can't be determined (ie if the tech pack isn't installed), then the
     * tech pack name is returned for that tech pack
     * If the feature_descriptions file doesn't contain an entry for the tech pack's CXC, then the CXC number is
     * returned for that tech pack
     * Otherwise, the description for the tech pack's CXC is read from the feature_descriptions file and returned
     *
     * @param techPackNames the tech pack names eg EVENT_E_SGEH, EVENT_E_LTE
     * @return list of feature descriptions for given tech packs
     */
    public List<String> getFeatureDescriptionsForTechPacks(final List<String> techPackNames) {
        final List<String> techPackDescriptions = new ArrayList<String>();

        for (final String techPack : techPackNames) {
            techPackDescriptions.addAll(getTechPackDescription(techPack));
        }
        return techPackDescriptions;
    }

    private List<String> getTechPackDescription(final String techPack) {
        final List<String> cxcNumbersForTechPack = getMatchingCXCNumbersForTechPack(techPack);
        if (cxcNumbersForTechPack.isEmpty()) { //ie there's no entry for this tech pack in the repdb database, its probably not installed            
            final List<String> listWithJustTechPackName = new ArrayList<String>();
            listWithJustTechPackName.add(techPack);
            return listWithJustTechPackName;
        }
        return getDescriptionsForCXCs(cxcNumbersForTechPack);
    }

    private List<String> getDescriptionsForCXCs(final List<String> cxcNumbersForTechPack) {
        final List<String> techPackDescriptions = new ArrayList<String>();
        for (final String techPackCXC : cxcNumbersForTechPack) {
            String techPackDescription;
            if (techPackDescriptionsMap.containsKey(techPackCXC)) {
                techPackDescription = techPackDescriptionsMap.get(techPackCXC);
            } else {
                techPackDescription = techPackCXC;
            }

            techPackDescriptions.add(techPackDescription);
        }
        return techPackDescriptions;
    }

    private List<String> getMatchingCXCNumbersForTechPack(final String techPack) {
        return techPackCXCMappingUtils.getTechPackCXCNumbers(techPack);
    }

    /**
     * @param techPackCXCMappingUtils the techPackCXCMapping to set
     */
    public void setTechPackCXCMappingUtils(final TechPackCXCMappingUtils techPackCXCMappingUtils) {
        this.techPackCXCMappingUtils = techPackCXCMappingUtils;
    }

}
