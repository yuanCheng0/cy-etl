package com.cy.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cy on 2018/3/26 19:56.
 */
public class WebH5Topology {
    public static void main(String[] args) {
        String topic ="track";
        ZkHosts zkhosts  = new ZkHosts("192.168.0.120:2181");

        SpoutConfig spoutConfig = new SpoutConfig(zkhosts, topic,
                "/MyKafka", //偏移量offset的根目录
                "MyTrack");//子目录对应一个应用
        List<String> zkServers=new ArrayList<String>();
        //zkServers.add("192.168.1.107");
        //zkServers.add("192.168.1.108");
        for(String host:zkhosts.brokerZkStr.split(","))
        {
            zkServers.add(host.split(":")[0]);
        }

        spoutConfig.zkServers=zkServers;
        spoutConfig.zkPort=2181;
//        spoutConfig.forceFromStart=true;//从头开始消费，实际上是要改成false的
        spoutConfig.socketTimeoutMs=60;
        spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());//定义输出为string类型

        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig),1);//引用spout，并发度设为1
        builder.setBolt("parse-bolt", new ParseBolt(),4).shuffleGrouping("spout");

        builder.setBolt("parse-bolt",
                new SlidingWindowBolt()
//                        .withWindow()

                ,4).shuffleGrouping("spout");

        Config config =new Config();
        config.setDebug(true);//上线之前都要改成false否则日志会非常多
        if(args.length>0){

            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } /*catch (InvalidTopologyException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }*/

        }else{

            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("mytopology", config,  builder.createTopology());
            //本地模式在一个进程里面模拟一个storm集群的所有功能
        }
    }
}
