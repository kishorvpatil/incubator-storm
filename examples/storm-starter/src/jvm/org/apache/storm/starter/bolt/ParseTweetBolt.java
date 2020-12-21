package org.apache.storm.starter.bolt;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * A bolt that parses the tweet into words.
 */
public class ParseTweetBolt extends BaseRichBolt {
    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    private final String[] skipWords = {"rt", "to", "me", "la", "on", "that", "que",
        "followers", "watch", "know", "not", "have", "like", "I'm", "new", "good", "do",
        "more", "es", "te", "followers", "Followers", "las", "you", "and", "de", "my", "is",
        "en", "una", "in", "for", "this", "go", "en", "all", "no", "don't", "up", "are",
        "http", "http:", "https", "https:", "http://", "https://", "with", "just", "your",
        "para", "want", "your", "you're", "really", "video", "it's", "when", "they", "their", "much",
        "would", "what", "them", "todo", "FOLLOW", "retweet", "RETWEET", "even", "right", "like",
        "bien", "Like", "will", "Will", "pero", "Pero", "can't", "were", "Can't", "Were", "TWITTER",
        "make", "take", "This", "from", "about", "como", "esta", "follows", "followed"};

    @Override
    public void prepare(
        Map map,
        TopologyContext topologyContext,
        OutputCollector outputCollector) {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweetStatus = (Status) tuple.getValueByField("tweet");
        for (HashtagEntity hashtags : tweetStatus.getHashtagEntities()) {
            System.out.println("Hashtag: " + hashtags.getText());
            collector.emit(new Values(hashtags.getText()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        declarer.declare(new Fields("tweet-word"));
    }

}
