package org.apache.storm.starter.bolt;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * A bolt that counts the words that it receives.
 */
public class CountBolt extends BaseRichBolt {
    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Long> countMap;

    @Override
    public void prepare(
        Map map,
        TopologyContext topologyContext,
        OutputCollector outputCollector) {

        // save the collector for emitting tuples
        collector = outputCollector;

        // create and initialize the map
        countMap = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        // get the word from the 1st column of incoming tuple
        String word = tuple.getString(0);

        // check if the word is present in the map
        if (countMap.get(word) == null) {

            // not present, add the word with a count of 1
            countMap.put(word, 1L);
        } else {

            // already there, hence get the count
            Long val = countMap.get(word);

            // increment the count and save it to the map
            countMap.put(word, ++val);
        }

        // emit the word and count
        collector.emit(new Values(word, countMap.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second column 'count'
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
