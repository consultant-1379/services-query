/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.utils.techpacks.timerangequeries;

import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.Stateless;

/**
 * Factory class for creating different implementations of the TimerangeQuerier interface
 * 
 * Introduced to allow for different tech packs may have different mechanisms for querying the time range views, however
 * this wasn't implemented
 * Factory class retained in order to allow for future tech packs being used (for example stats tech packs will 
 * have different time range capabilities)
 * 
 * @author eemecoy
 *
 */
@Stateless
@Local
public class TimerangeQuerierFactory {

    @EJB
    private TimerangeQuerier eventsTechPackTimerangeQuerier;

    /**
     * Get the TimerangeQuerier object that should be used
     * 
     * @return TimerangeQuerier object
     */
    public TimerangeQuerier getTimerangeQuerier() {
        return eventsTechPackTimerangeQuerier;
    }

    /**
     * @param eventsTechPackTimerangeQuerier the eventsTechPackTimerangeQuerier to set
     */
    public void setEventsTechPackTimerangeQuerier(final TimerangeQuerier eventsTechPackTimerangeQuerier) {
        this.eventsTechPackTimerangeQuerier = eventsTechPackTimerangeQuerier;
    }

}
