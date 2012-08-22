package com.exacttarget.sml.storm.trident.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import storm.trident.state.OpaqueValue;

/**
 * User: ebishop
 * Date: 8/7/12
 * Time: 3:26 PM
 */
public class MongoDbStateOpaqueSerializer implements MongoDbStateSerializer<OpaqueValue> {

    private MongoDbStateNonTransactionalSerializer base = new MongoDbStateNonTransactionalSerializer();

    @Override
    public DBObject serialize(OpaqueValue opqVal) {
        Long currTxId = opqVal.getCurrTxid();
        Object curr = opqVal.getCurr();
        Object prev = opqVal.getPrev();

        DBObject result = new BasicDBObject();
        result.put("txId", currTxId);
        result.put("val", base.serialize(curr));
        result.put("prev", base.serialize(prev));
        return result;
    }

    @Override
    public OpaqueValue deserialize(DBObject dbo) {
        Long currTxId = ((Number)dbo.get("txId")).longValue();
        Object val = base.deserialize((DBObject) dbo.get("val"));
        Object prev = base.deserialize((DBObject)dbo.get("prev"));
        return new OpaqueValue(currTxId, val, prev);
    }

}
