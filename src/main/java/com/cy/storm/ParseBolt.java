package com.cy.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by cy on 2018/3/26 21:56.
 */
public class ParseBolt implements IRichBolt{
    OutputCollector collector = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        //统计1小时pv，uv，并统计去重uv
        String line = tuple.getString(0);
        String[] arrLine = line.split("\\|");
        for (String data : arrLine){
            System.out.println("写入hbase。。。" );
        }
        String uId = arrLine[0];
        String date = "20180326011059"; //到秒
        collector.emit(new Values(date,uId));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date","userId"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
