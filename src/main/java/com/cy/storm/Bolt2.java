package com.cy.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by cy on 2018/3/26 22:22.
 */
public class Bolt2 implements IRichBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    Map<String,Long> counts = new HashMap<>();
    Map<String,Long> totalUv = new HashMap<>(); //截止当前时间总的去重uv
    @Override
    public void execute(Tuple tuple) {
        long uv = 0;//1小时不去重的
//        long distUv = 0;//1小时去重的
        //20180326230400_cy0123
        String date = "20180326";//当前日期
        String hour = "23";//当前小时
        String min = "04";//当前分钟
        String dateUId = tuple.getString(0);
        String dUid = dateUId.substring(0,9) + dateUId.substring(13);
        String date_hour = date + hour;
        Long count = tuple.getLong(1);
        if (!dateUId.startsWith(date_hour)){
        }
        Long aLong = counts.get(dateUId);
        if (aLong == null){
            counts.put(dateUId,count);
        }else {
            counts.put(dateUId,count + aLong);
        }
        Long aLong1 = totalUv.get(dUid);
        /*if(aLong1 == null){
            totalUv.put()
        }*/
        Iterator<String> i = counts.keySet().iterator();
        while (i.hasNext()){
            String key = i.next();
            if (key != null){
                if (key.startsWith(date_hour)){
                   /* distUv ++;
                    Long totalUvCnt = totalUv.get(date);//获取当前日期的总uv数
                    if (totalUvCnt == null){
                        totalUv.put(date,distUv);
                    }else {
                        totalUv.put(date,distUv + totalUvCnt);
                    }*/
                    uv += counts.get(key);
                    if ("01".equals(min)){ // <= 59分钟时 输出保存上一个小时的结果 在01分钟的时候统计上一个小时的数据
                        System.out.println("将结果保存在mysql");
                        //判断是否包含上一个小时
                        if (key.startsWith(date_hour + 1)){
                            i.remove();//删除
                        }
                    }
            }
        }

        }
//        counts
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
