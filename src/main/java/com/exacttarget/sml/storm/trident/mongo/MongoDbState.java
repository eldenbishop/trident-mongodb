package com.exacttarget.sml.storm.trident.mongo;

import backtype.storm.tuple.Values;
import com.mongodb.*;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;

/**
 * User: ebishop
 * Date: 8/6/12
 * Time: 4:21 PM
 */
public class MongoDbState<T> implements IBackingMap<T> {

    private static final Map<StateType, MongoDbStateSerializer> DEFAULT_SERIALIZERS = new HashMap<StateType, MongoDbStateSerializer>() {{
        put(StateType.NON_TRANSACTIONAL, new MongoDbStateNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new MongoDbStateTransactionalSerializer());
        put(StateType.OPAQUE, new MongoDbStateOpaqueSerializer());
    }};

    public static StateFactory buildStateFactory(String host, Integer port, StateType stateType, String dbName, String collectionName) {
        return new Factory(host, null, stateType, new Options<Object>(dbName, collectionName, DEFAULT_SERIALIZERS.get(stateType)));
    }

    public static StateFactory nonTransactional(String host, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, null, StateType.NON_TRANSACTIONAL, new Options<Object>(dbName, collectionName, new MongoDbStateNonTransactionalSerializer()));
    }

    public static StateFactory nonTransactional(String host, int port, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, port, StateType.NON_TRANSACTIONAL, new Options<Object>(dbName, collectionName, new MongoDbStateNonTransactionalSerializer()));
    }

    public static StateFactory opaque(String host, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, null, StateType.OPAQUE, new Options<OpaqueValue>(dbName, collectionName, new MongoDbStateOpaqueSerializer()));
    }

    public static StateFactory opaque(String host, int port, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, port, StateType.OPAQUE, new Options<OpaqueValue>(dbName, collectionName, new MongoDbStateOpaqueSerializer()));
    }

    public static StateFactory transactional(String host, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, null, StateType.TRANSACTIONAL, new Options<TransactionalValue>(dbName, collectionName, new MongoDbStateTransactionalSerializer()));
    }

    public static StateFactory transactional(String host, int port, String dbName, String collectionName) throws UnknownHostException {
        return new Factory(host, port, StateType.TRANSACTIONAL, new Options<TransactionalValue>(dbName, collectionName, new MongoDbStateTransactionalSerializer()));
    }

    private DBCollection collection;
    private MongoDbStateSerializer<T> serializer;

    private MongoDbState(DBCollection collection, MongoDbStateSerializer<T> serializer) {
        System.out.println("MongoDbState.MongoDbState");
        this.collection = collection;
        this.serializer = serializer;
    }

    @Override
    public List<T> multiGet(List<List<Object>> tuples) {
        ArrayList<T> result = new ArrayList<T>();

        // *** build the keys
        List<String> keys = new ArrayList();
        for (List<Object> tuple : tuples) {
            String key = toMongoId(tuple);
            keys.add(key);
        }

        // *** prep the query result
        Map<String,Object> queryResult = new HashMap<String,Object>();

        if (keys.size() > 0) {
            // *** load the data
            BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$in", keys));
            DBCursor dbos = collection.find(query);
            while (dbos.hasNext()) {
                DBObject dbo = dbos.next();
                Object o = serializer.deserialize(dbo);
                queryResult.put((String) dbo.get("_id"), o);
            }

            // *** build the response
            for (String key : keys) {
                Object value = queryResult.get(key);
                result.add((T)value);
            }
        }

        return result;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        for (int i = 0; i < keys.size(); i++) {
            String id = toMongoId(keys.get(i));
            DBObject dbo = serializer.serialize(values.get(i));
            dbo.put("_id", id);
            collection.update(new BasicDBObject("_id", id), dbo, true, false);
        }
    }

    public static String toMongoId(List<Object> objects) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < objects.size(); i++) {
            if (i > 0) sb.append("/");
            sb.append(String.valueOf(objects.get(i)).replaceAll("/", "\\\\"));
        }
        return sb.toString();
    }

    public static class Options<T> implements Serializable {
        String dbName;
        String collectionName;
        int localCacheSize = 1000;
        String globalKey = "$GLOBAL$";

        MongoDbStateSerializer<T> serializer = null;

        public Options(String dbName, String collectionName, MongoDbStateSerializer<T> serializer) {
            this.dbName = dbName;
            this.collectionName = collectionName;
            this.serializer = serializer;
        }
    }

    protected static class Factory<T> implements StateFactory {

        private String host;
        private Integer port;
        private StateType stateType;
        private MongoOptions mongoOptions;

        private Options<T> opts;

        public Factory(String host, Integer port, StateType stateType, Options<T> opts) {
            this.host = host;
            this.port = port;
            this.stateType = stateType;
            this.opts = opts;
        }

        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            try {
                Mongo mongo = port == null ? new Mongo(host) : new Mongo(host, port);
                DB db = mongo.getDB(opts.dbName);
                DBCollection collection = db.getCollection(opts.collectionName);

                MongoDbState mongoState = new MongoDbState(collection, opts.serializer);
                CachedMap cachedMap = new CachedMap(mongoState, opts.localCacheSize);
                MapState mapState = buildMapState(stateType, cachedMap);
                SnapshottableMap snapshottableMap = new SnapshottableMap(mapState, new Values(opts.globalKey));

                return snapshottableMap;
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private MapState buildMapState(StateType type, CachedMap cachedMap) {
            switch(stateType) {
                case NON_TRANSACTIONAL:
                    return NonTransactionalMap.build(cachedMap);
                case OPAQUE:
                    return OpaqueMap.build(cachedMap);
                case TRANSACTIONAL:
                    return TransactionalMap.build(cachedMap);
                default:
                    throw new RuntimeException("Unknown state type: " + type);
            }
        }

    }

}
