package com.exacttarget.sml.storm.trident.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * User: ebishop
 * Date: 8/7/12
 * Time: 3:25 PM
 */
public class MongoDbStateNonTransactionalSerializer implements MongoDbStateSerializer {

    @Override
    public DBObject serialize(Object o) {
        return encodeBasicDBObject(o);
    }

    @Override
    public Object deserialize(DBObject dbo) {
        return decodeBasicDBObject(dbo);
    }

    private static Set<Class> LITERAL_TYPES = new HashSet<Class>(ImmutableList.of(
            Boolean.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Date.class, String.class
    ));

    private transient ObjectMapper mapper;

    private BasicDBObject encodeBasicDBObject(Object o) {
        BasicDBObject container = new BasicDBObject();
        if (o == null) {
            container.append("scheme", null);
            container.append("value", null);
        } else {
            Class type = o.getClass();
            if (LITERAL_TYPES.contains(type)) {
                container.append("scheme", type.getName()).append("value", o);
            } else {
                try {
                    if (mapper == null) mapper = new ObjectMapper();
                    String jsonString = mapper.writeValueAsString(o);
                    container.append("scheme", type.getName()).append("value", jsonString);
                } catch(Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        return container;
    }

    private Object decodeBasicDBObject(DBObject dbo) {
        Object scheme = dbo.get("scheme");
        if (scheme == null)
            return null;
        else if (scheme instanceof String) {
            if (scheme.equals("java.lang.Short"))
                return ((Number)dbo.get("value")).shortValue();
            else if (scheme.equals("java.lang.Integer"))
                return ((Number)dbo.get("value")).intValue();
            else if (scheme.equals("java.lang.Long"))
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.Float"))
                return ((Number)dbo.get("value")).floatValue();
            else if (scheme.equals("java.lang.Double"))
                return ((Number)dbo.get("value")).doubleValue();
            else if (scheme.equals("java.lang.String"))
                return dbo.get("value");
            else if (scheme.equals("java.util.Date"))
                return dbo.get("value");
            else {
                try {
                    if (mapper == null) mapper = new ObjectMapper();
                    String jsonText = (String)dbo.get("value");
                    Class type = Class.forName((String)scheme);
                    return mapper.readValue(jsonText, type);
                } catch(Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        throw new UnsupportedOperationException("Unsupported scheme: " + scheme);
    }

}
