package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;


public class SplitAndCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String streamID = tuple.getSourceStreamId();
        if (streamID.equalsIgnoreCase(Utils.DEFAULT_STREAM_ID)) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                basicOutputCollector.emit("$words", new Values(word));
            }
        } else if (streamID.equalsIgnoreCase("$words")) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            basicOutputCollector.emit("$counts", new Values(word, count));
        } else if (streamID.equalsIgnoreCase("$counts")) {
            String word = tuple.getString(0);
            Integer count = tuple.getInteger(1);
            basicOutputCollector.emit(new Values(word, count));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("$words", new Fields("word"));
        outputFieldsDeclarer.declareStream("$counts", new Fields("word", "count"));
    }

}
