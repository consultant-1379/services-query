/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.common.HashIdCreator;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

/**
 * @author eemecoy
 *
 */
public class HashUtilitiesTest extends BaseJMockUnitTest {

    private HashUtilities hashUtilities;

    HashIdCreator mockedHashIdCreator;

    @Before
    public void setup() {
        hashUtilities = new StubbedHashUtilities();
        final RATDescriptionMappingUtils ratDescriptionMappingUtils = setUpMockedRatDescriptionsService();
        hashUtilities.setRatDescriptionMappingUtils(ratDescriptionMappingUtils);
        mockedHashIdCreator = mockery.mock(HashIdCreator.class);
    }

    @Test
    public void testHashingCell_OneArgMethod() throws Exception {
        final String hierarchy1 = "123";
        final String hierarchy2 = "RBS1";
        final String hierarchy3 = "RNC1";
        final String vendor = ERICSSON;
        final String ratIntegerValue = RAT_INTEGER_VALUE_FOR_3G;
        final String cell = hierarchy1 + COMMA + hierarchy2 + COMMA + hierarchy3 + COMMA + vendor + COMMA
                + ratIntegerValue;
        final long expectedHashId = expectCallOnHashingService(_3G + HashUtilities.HASH_ID_DELIMITOR + hierarchy3
                + HashUtilities.HASH_ID_DELIMITOR + hierarchy2 + HashUtilities.HASH_ID_DELIMITOR + hierarchy1
                + HashUtilities.HASH_ID_DELIMITOR + vendor);
        assertThat(hashUtilities.createHashIdForHier321Id(cell), is(expectedHashId));
    }

    @Test
    public void testHashingMSC() throws IOException {
        final String msc = "MSC1";
        final long expectedHashID = expectCallOnHashingService(msc);
        assertThat(hashUtilities.createHashIDForMSC(msc), is(expectedHashID));
    }

    @Test
    public void testHashingCell() throws IOException {
        final String ratValue = "1";
        final String bsc = "BSC1";
        final String vendor = "NOKIA";
        final String hierarchy2 = "";
        final String cell = "CELL1";
        final long expectedHashID = expectCallOnHashingService(_3G + HashUtilities.HASH_ID_DELIMITOR + bsc
                + HashUtilities.HASH_ID_DELIMITOR + hierarchy2 + HashUtilities.HASH_ID_DELIMITOR + cell
                + HashUtilities.HASH_ID_DELIMITOR + vendor);
        assertThat(hashUtilities.createHashIDForCell(ratValue, bsc, hierarchy2, cell, vendor), is(expectedHashID));
    }

    @Test
    public void testHashing3GCell() throws IOException {
        final String ratValue = "1";
        final String bsc = "BSC1";
        final String vendor = "NOKIA";
        final String cell = "CELL1";
        final long expectedHashID = expectCallOnHashingService(_3G + HashUtilities.HASH_ID_DELIMITOR + bsc
                + HashUtilities.HASH_ID_DELIMITOR + cell + HashUtilities.HASH_ID_DELIMITOR + vendor);
        assertThat(hashUtilities.createHashIDFor3GCell(ratValue, bsc, cell, vendor), is(expectedHashID));
    }

    @Test
    public void testHashingController() throws IOException {
        final String ratValue = "1";
        final String bsc = "BSC1";
        final String vendor = "NOKIA";
        final long expectedHashID = expectCallOnHashingService(_3G + HashUtilities.HASH_ID_DELIMITOR + bsc
                + HashUtilities.HASH_ID_DELIMITOR + vendor);
        assertThat(hashUtilities.createHashIDForControllerAsLong(ratValue, bsc, vendor), is(expectedHashID));
    }

    private RATDescriptionMappingUtils setUpMockedRatDescriptionsService() {
        final RATDescriptionMappingUtils ratDescriptionMappingUtils = mockery.mock(RATDescriptionMappingUtils.class);
        mockery.checking(new Expectations() {
            {
                allowing(ratDescriptionMappingUtils).getRATIntegerValue(RAT_INTEGER_VALUE_FOR_3G);
                will(returnValue(_3G));
            }
        });
        return ratDescriptionMappingUtils;
    }

    private long expectCallOnHashingService(final String string) throws IOException {
        final long hashId = string.hashCode();
        mockery.checking(new Expectations() {
            {
                one(mockedHashIdCreator).hashStringToLongId(string);
                will(returnValue(hashId));
            }
        });

        return hashId;

    }

    class StubbedHashUtilities extends HashUtilities {
        /* (non-Javadoc)
         * @see com.ericsson.eniq.events.server.utils.HashUtilities#createHashIdCreator()
         */
        @Override
        HashIdCreator createHashIdCreator() throws NoSuchAlgorithmException {
            return mockedHashIdCreator;
        }
    }

}
