/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.storm.starter.tools.Rankable;
import org.apache.storm.starter.tools.Rankings;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * A bolt that prints the word and count and send to drpc.
 */
public class ReportBolt extends BaseBasicBolt {

    private Rankings currentRankableList = null;
    private final int maxTopN;

    public ReportBolt(int topN) {
        this.maxTopN = topN;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.size() > 1) {
            String arg = tuple.getString(0);
            int top = 10;
            try {
                top = Integer.parseUnsignedInt(arg);
            } catch (NumberFormatException e) {
                //ignore
                e.printStackTrace();
            }

            Object retInfo = tuple.getValue(1);
            collector.emit(new Values(getCurrentResults(top), retInfo));
            return;
        }
        currentRankableList = (Rankings) tuple.getValue(0);
        String currentResults = getCurrentResults(10);
        System.out.println("Results: " + currentResults);

    }

    private String getCurrentResults(int topN) {
        List<String> results = new ArrayList<>();
        if (currentRankableList == null) {
            return "";
        }
        List<Rankable> rankList = currentRankableList.getRankings();
        List<Rankable> subList = rankList;
        if (topN >= currentRankableList.size()) {
            topN = currentRankableList.size() - 1;
        }
        if (currentRankableList.size() > 0) {
            for (Rankable r : rankList.subList(0, topN)) {
                String word = r.getObject().toString();
                Long count = r.getCount();
                String rank = String.format(" {%s : %d }", word, count);
                results.add(rank);
            }
        }
        return results.stream().collect(Collectors.joining(","));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
