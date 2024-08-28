/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import com.ericsson.eniq.common.HashIdCreator;
import com.ericsson.eniq.events.server.logging.ServicesLogger;

/**
 * @author eemecoy
 *
 */
@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.WRITE)
public class HashUtilities {

    private static final String HASH_COLUMN_PREFIX = "hier3";

    static final String HASH_ID_DELIMITOR = "|";

    @EJB
    private RATDescriptionMappingUtils ratDescriptionMappingUtils;

    public String createHashIDForController(final String rat, final String bsc, final String vendor) {
        final String ratValue = ratDescriptionMappingUtils.getRATIntegerValue(rat);
        final String topologyString = new StringBuilder(ratValue).append(HASH_ID_DELIMITOR).append(bsc)
                .append(HASH_ID_DELIMITOR).append(vendor).toString();
        return createHashId(topologyString).toString();
    }

    /**
     *
     * @param rat       RAT for the Controller
     * @param bsc       Controller
     * @param vendor    Vendor. e.g Ericsson
     * @return HashID for controller as HashID
     */
    public long createHashIDForControllerAsLong(final String rat, final String bsc, final String vendor) {
        final String ratValue = ratDescriptionMappingUtils.getRATIntegerValue(rat);
        final String topologyString = new StringBuilder(ratValue).append(HASH_ID_DELIMITOR).append(bsc)
                .append(HASH_ID_DELIMITOR).append(vendor).toString();
        return createHashId(topologyString);
    }

    /**
     *
     * @param controllerInNodeFormat Node representation of Controller - e.g ONRM_ROOT_MO_R:RNC01:RNC01,Ericsson,3G
     * @return HashID for controller as HashID
     */
    public long createHashIDFor3GController(final String controllerInNodeFormat) {
        final String[] splitString = controllerInNodeFormat.split(DELIMITER);
        final String rat = splitString[2];
        final String controller = splitString[0];
        final String vendor = splitString[1];
        return createHashIDForControllerAsLong(rat, controller, vendor);
    }

    public long createHashIDForCell(final String rat, final String bsc, final String hier2, final String cell,
            final String vendor) {
        final String ratValue = ratDescriptionMappingUtils.getRATIntegerValue(rat);
        final String topologyStr = new StringBuilder(ratValue).append(HASH_ID_DELIMITOR).append(bsc)
                .append(HASH_ID_DELIMITOR).append(hier2).append(HASH_ID_DELIMITOR).append(cell)
                .append(HASH_ID_DELIMITOR).append(vendor).toString();
        return createHashId(topologyStr);

    }

    /**
     * Hash the cell details received in the cellInNodeFormat parameter
     *
     * @param cellInNodeFormat                  the 3G cell in the format received from the UI eg
     *                                          RNC01-2-3,,ONRM_ROOT_MO_R:RNC01:RNC01,Ericsson,3G
     *
     * @return the hashed id for the 3G cell
     */
    public long createHashIDFor3GCell(final String cellInNodeFormat) {
        final String[] splitString = cellInNodeFormat.split(DELIMITER);
        final String rat = splitString[4];
        final String controller = splitString[2];
        final String cell = splitString[0];
        final String vendor = splitString[3];
        return createHashIDFor3GCellAsLong(rat, controller, cell, vendor);
    }

    public long createHashIDFor3GCell(final String rat, final String controller, final String cell, final String vendor) {
        return createHashIDFor3GCellAsLong(rat, controller, cell, vendor);
    }

    private long createHashIDFor3GCellAsLong(final String rat, final String controller, final String cell,
            final String vendor) {
        final String ratValue = ratDescriptionMappingUtils.getRATIntegerValue(rat);
        final String topologyStr = new StringBuilder(ratValue).append(HASH_ID_DELIMITOR).append(controller)
                .append(HASH_ID_DELIMITOR).append(cell).append(HASH_ID_DELIMITOR).append(vendor).toString();
        return createHashId(topologyStr);
    }

    private Long createHashId(final String topologyStr) {
        long hashId = 0;
        try {
            final HashIdCreator hashIdCreator = createHashIdCreator();
            hashId = hashIdCreator.hashStringToLongId(topologyStr);
            ServicesLogger.detailed(Level.FINE, getClass().getName(), "createHashId() ", "topologyStr::" + topologyStr,
                    "hashId::" + hashId);
        } catch (final Exception exp) {
            ServicesLogger.detailed(Level.SEVERE, getClass().getName(), "createHashId ", exp);
        }
        return hashId;
    }

    HashIdCreator createHashIdCreator() throws NoSuchAlgorithmException {
        return new HashIdCreator();
    }

    public void setRatDescriptionMappingUtils(final RATDescriptionMappingUtils ratDescriptionMappingUtils) {
        this.ratDescriptionMappingUtils = ratDescriptionMappingUtils;
    }

    public long createHashIDForMSC(final String msc) {
        return createHashId(msc);
    }

    /** This method will generate the Access Area hashId for Subscriber BI cell analysis
      * and support the below format
      *
      * <HIERARCHY_1>,<HIERARCHY_2>,<HIERARCHY_3>,<VENDOR>,<RAT>
      * For example cell1,,controller1,Ericsson,GSM
      *
      * @param cell topology string representing the Access Area/Cell
      * @return hashId representing the Access Area in database
      */

    public long createHashIdForHier321Id(final String cell) {
        final String[] values = cell.split(DELIMITER);
        final String ratValue = ratDescriptionMappingUtils.getRATIntegerValue(values[4]);
        final String topologyStr = new StringBuilder(ratValue).append(HASH_ID_DELIMITOR).append(values[2])
                .append(HASH_ID_DELIMITOR).append(values[1]).append(HASH_ID_DELIMITOR).append(values[0])
                .append(HASH_ID_DELIMITOR).append(values[3]).toString();
        return createHashId(topologyStr);
    }

    /**
     * Utility method to determine if the supplied column name is a Hashed ID column.
     * It bases this on the fact that all the
     */
    public boolean isHashedIdColumn(final String columnName) {
        return columnName.toLowerCase().startsWith(HASH_COLUMN_PREFIX);
    }

}
