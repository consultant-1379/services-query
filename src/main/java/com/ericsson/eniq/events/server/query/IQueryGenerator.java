/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.ericsson.eniq.events.server.query;

/**
 * @author ericker
 */
public interface IQueryGenerator {

    String getQuery(final QueryGeneratorParameters queryGeneratorParameters);

}
