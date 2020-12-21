package org.apache.storm.starter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.starter.bolt.CountBolt;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.ParseTweetBolt;
import org.apache.storm.starter.bolt.ReportBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ObjectReader;

class TopNTweetTopology {
    private static final String DEFAULT_TOPO_NAME = "TopNTweetTopology";
    private static final String DEFAULT_CONSUMER_KEY = "";
    private static final String DEFAULT_CONSUMER_SECRET = "";
    private static final String DEFAULT_ACCESS_TOKEN = "";
    private static final String DEFAULT_ACCESS_TOKEN_SECRET = "";

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Print a help message")
            .build());

        options.addOption(Option.builder()
            .longOpt("name")
            .argName("TOPO_NAME")
            .hasArg()
            .desc("Name of the topology to run (defaults to " + DEFAULT_TOPO_NAME + ")")
            .build());

        options.addOption(Option.builder()
            .longOpt("topN")
            .argName("topN")
            .type(Number.class)
            .hasArg()
            .desc("Name of the topology to run (defaults to " + DEFAULT_TOPO_NAME + ")")
            .build());

        options.addOption(Option.builder()
            .longOpt("consumerKey")
            .argName("consumerKey")
            .required()
            .hasArg()
            .desc("Consumer key for the twitter account (defaults to " + DEFAULT_CONSUMER_KEY + ")")
            .build());

        options.addOption(Option.builder()
            .longOpt("consumerSecret")
            .argName("consumerSecret")
            .required()
            .hasArg()
            .desc("Consumer secret for the twitter account (defaults to " + DEFAULT_CONSUMER_SECRET + ")")
            .build());

        options.addOption(Option.builder()
            .longOpt("accessToken")
            .argName("accessToken")
            .required()
            .hasArg()
            .desc("Access Token secret for the twitter account (defaults to " + DEFAULT_ACCESS_TOKEN + ")")
            .build());

        options.addOption(Option.builder()
            .longOpt("accessTokenSecret")
            .argName("accessTokenSecret")
            .required()
            .hasArg()
            .desc("Access Token secret for the twitter account (defaults to " + DEFAULT_ACCESS_TOKEN_SECRET + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("keys")
            .argName("keys")
            .hasArgs()
            .desc("Access Token secret for the twitter account (defaults to " + DEFAULT_ACCESS_TOKEN_SECRET + ")")
            .build());

        String consumerKey = DEFAULT_CONSUMER_KEY;
        String consumerSecret = DEFAULT_CONSUMER_SECRET;

        String accessToken = DEFAULT_ACCESS_TOKEN;
        String accessTokenSecret = DEFAULT_ACCESS_TOKEN_SECRET;
        int topN = 50;

        String[] arguments = args.clone();
        String[] keyWords = null; //Arrays.copyOfRange(arguments, 4, arguments.length);

        String name = DEFAULT_TOPO_NAME;
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Exception commandLineException = null;

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("name")) {
                name = cmd.getOptionValue("name");
            }
            if (cmd.hasOption("topN")) {
                topN = ObjectReader.getInt(cmd.getParsedOptionValue("topN"), 10);
            }
            if (cmd.hasOption("consumerKey")) {
                consumerKey = cmd.getOptionValue("consumerKey");
            }
            if (cmd.hasOption("consumerSecret")) {
                consumerSecret = cmd.getOptionValue("consumerSecret");
            }
            if (cmd.hasOption("accessToken")) {
                accessToken = cmd.getOptionValue("accessToken");
            }
            if (cmd.hasOption("accessTokenSecret")) {
                accessTokenSecret = cmd.getOptionValue("accessTokenSecret");
            }
            if (cmd.hasOption("keys")) {
                keyWords = cmd.getOptionValues("keys");
            }
        } catch (ParseException | NumberFormatException e) {
            commandLineException = e;
        }

        if (commandLineException != null || cmd.hasOption('h')) {
            if (commandLineException != null) {
                System.err.println("ERROR " + commandLineException.getMessage());
            }
            new HelpFormatter().printHelp("TwitterHashtagStorm [options]", options);
            return;
        }

        System.out.println("Input is: %s, %s, %s,%s" + consumerKey
            + consumerSecret + accessToken + accessTokenSecret);
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // attach the tweet spout to the topology - parallelism of 1
        builder.setSpout("tweet-spout", new TwitterSampleSpout(consumerKey,
            consumerSecret, accessToken, accessTokenSecret, keyWords), 1);

        // attach the parse tweet bolt using shuffle grouping
        builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

        // attach the count bolt using fields grouping - parallelism of 15
        builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

        builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(topN), 4).fieldsGrouping("count-bolt", new Fields("word"));
        DRPCSpout spout = new DRPCSpout("topTweets");
        builder.setSpout("drpc", spout);

        builder.setBolt("total-ranker", new TotalRankingsBolt(topN)).globalGrouping("intermediate-ranker");
        // attach the report bolt using global grouping - parallelism of 1
        builder.setBolt("report-bolt", new ReportBolt(topN), 1).globalGrouping("total-ranker").globalGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 1).shuffleGrouping("report-bolt");


        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        // run it in a live cluster

        // set the number of workers for running all spout and bolt tasks
        conf.setNumWorkers(3);

        // create the topology and submit with config
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
    }
}
