package com.exacttarget.sml.storm.trident.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.io.Serializable;

/**
 * User: ebishop
 * Date: 8/7/12
 * Time: 3:35 PM
 */
public interface MongoDbStateSerializer<T> extends Serializable {
    public DBObject serialize(T t);
    public T deserialize(DBObject dbo);
}
