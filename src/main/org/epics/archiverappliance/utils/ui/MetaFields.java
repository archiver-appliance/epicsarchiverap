package org.epics.archiverappliance.utils.ui;

import java.util.HashMap;

/**
 * Utility class for dealing with metafields as part of responses.
 * @author mshankar
 *
 */
public class MetaFields {

    /*
     * We typically return meta fields as a "meta" attribute in JSON objects.
     * Use this utility method to add a meta field name/value pair into a JSON object
     */
    @SuppressWarnings("unchecked")
    public static void addMetaFieldValue(HashMap<String, Object> jsonval, String fieldName, String fieldValue) {
        if(!jsonval.containsKey("meta")) {
            HashMap<String, String> metaFields = new HashMap<String, String>();
            jsonval.put("meta", metaFields);    
        }
        HashMap<String, String> meta = ((HashMap<String, String>)jsonval.get("meta"));
        if(!meta.containsKey(fieldName)) {
            meta.put(fieldName, fieldValue);
        }
    }

}
