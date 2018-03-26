package com.cy.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cy on 2018/3/26 22:16.
 */
public class Bolt1 implements IRichBolt{
    OutputCollector collector = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    Map<String,Long> counts = new HashMap<>();
    @Override
    public void execute(Tuple tuple) {
        String date = tuple.getStringByField("date");
        String uId = tuple.getStringByField("userId");
        Long count = counts.get(date + "_" + uId);
        if(count == null){
            count = 0L;
        }
        count ++;
        counts.put(date + "_" + uId,count);
        collector.emit(new Values(date + "_" + uId,count));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
