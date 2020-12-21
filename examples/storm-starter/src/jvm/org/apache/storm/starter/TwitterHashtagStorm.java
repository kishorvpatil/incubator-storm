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
import org.apache.storm.starter.bolt.HashtagCounterBolt;
import org.apache.storm.starter.bolt.HashtagReaderBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {

    private static final String DEFAULT_TOPO_NAME = "TwitterHashtagStorm";
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

        String[] arguments = args.clone();
        String[] keyWords =  null; //Arrays.copyOfRange(arguments, 4, arguments.length);

        String name = DEFAULT_TOPO_NAME;
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Exception commandLineException = null;

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("name")) {
                name = cmd.getOptionValue("name");
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


        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
            consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
            .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
            .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        Thread.sleep(10000);

    }
}
