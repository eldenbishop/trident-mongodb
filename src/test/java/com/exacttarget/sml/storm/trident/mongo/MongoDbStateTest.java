package com.exacttarget.sml.storm.trident.mongo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * User: ebishop
 * Date: 8/6/12
 * Time: 4:27 PM
 */
public class MongoDbStateTest extends TestCase {

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc, StateFactory state) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(state, new Count(), new Fields("count"))
                        .parallelismHint(6);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
        ;

        return topology.build();
    }

    public static void main(String[] args) throws UnknownHostException {

        StateFactory mongodb = MongoDbState.nonTransactional("localhost", "storm-mongodb-statetest-db", "statetest-coll");

        LocalDRPC drpc = new LocalDRPC();
        StormTopology topology = buildTopology(drpc, mongodb);
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tester", conf, topology);

        for(int i=0; i<100; i++) {
            System.out.println("DRPC: " + drpc.execute("words", "cat the man four"));
            Utils.sleep(1000);
        }

    }

//    private static final MemCacheDaemon<LocalCacheElement> daemon =
//            new MemCacheDaemon<LocalCacheElement>();
//
//    private static void startLocalMemcacheInstance(int port) {
//        System.out.println("Starting local memcache");
//        CacheStorage<Key, LocalCacheElement> storage =
//                ConcurrentLinkedHashMap.create(
//                        ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 100, 1024*500);
//        daemon.setCache(new CacheImpl(storage));
//        daemon.setAddr(new InetSocketAddress("localhost", port));
//        daemon.start();
//    }

}
