package com.exacttarget.sml.storm.trident.mongo;

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

    private static Set<Class> LITERAL_TYPES = new HashSet(ImmutableList.of(
            Boolean.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Date.class
    ));

    private static Set<Class> QUOTED_TYPES = new HashSet(ImmutableList.of(
            Character.class, String.class
    ));

    private BasicDBObject encodeBasicDBObject(Object o) {
        BasicDBObject container = new BasicDBObject();
        if (o == null) {
            container.append("scheme", null);
            container.append("value", null);
        } else {
            Class type = o.getClass();
            if (LITERAL_TYPES.contains(type)) {
                container.append("scheme", type.getName()).append("value", o);
            } else if (QUOTED_TYPES.contains(type)) {
                container.append("scheme", type.getName()).append("value", String.valueOf(o));
            } else {
                throw new UnsupportedOperationException();
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
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.Integer"))
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.Long"))
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.Float"))
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.Double"))
                return ((Number)dbo.get("value")).longValue();
            else if (scheme.equals("java.lang.String"))
                return dbo.get("value");
            else if (scheme.equals("java.util.Date"))
                return dbo.get("value");
        }
        throw new UnsupportedOperationException("" + scheme);
    }

}
