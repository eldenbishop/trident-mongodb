package com.exacttarget.sml.storm.trident.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import storm.trident.state.TransactionalValue;

/**
 * User: ebishop
 * Date: 8/7/12
 * Time: 3:26 PM
 */
public class MongoDbStateTransactionalSerializer implements MongoDbStateSerializer<TransactionalValue> {

    private MongoDbStateNonTransactionalSerializer base = new MongoDbStateNonTransactionalSerializer();

    @Override
    public DBObject serialize(TransactionalValue txVal) {
        DBObject result = new BasicDBObject();
        Long txid = txVal.getTxid();
        Object val = txVal.getVal();
        result.put("txid", txid);
        DBObject encodedValue = base.serialize(val);
        result.put("value", encodedValue);
        return result;
    }

    @Override
    public TransactionalValue deserialize(DBObject dbo) {
        Long txid = ((Number)dbo.get("txid")).longValue();
        Object value = base.deserialize((DBObject)dbo.get("value"));
        return new TransactionalValue(txid, value);
    }

}
