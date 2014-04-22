package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.SplitAndCountBolt;
import storm.starter.spout.NoSleepRandomSentenceSpout;
import storm.starter.spout.RandomSentenceSpout;

public class SplitJoinTopology {


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new NoSleepRandomSentenceSpout(), 10);

        String boltName = "splitjoinbolt";
        builder.setBolt(boltName, new SplitAndCountBolt(), 1)
                .fieldsGrouping("spout", new Fields("word"))
                .fieldsGrouping(boltName, "$words", new Fields("word"))
                .fieldsGrouping(boltName, "$counts", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        conf.setNumAckers(1);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("split-join-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }

}
