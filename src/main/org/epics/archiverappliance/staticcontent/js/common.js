/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/

/**
 * Function to return the entire query string as a hashmap.
 * Usage: var myParam = getQueryParams()["aParticularParam"].
 * Note this does not cater to URL unencoded names etc; so please pass in a fully RFC compliant URL. 
 * @return {string} A hashmap that contains the name value pairs of the query string.
 */
function getQueryParams() {
  var result = {}, queryString = location.search.substring(1), re = /([^&=]+)=([^&]*)/g, m;

  while (m = re.exec(queryString)) {
    result[decodeURIComponent(m[1])] = decodeURIComponent(m[2]);
  }

  return result;
}

