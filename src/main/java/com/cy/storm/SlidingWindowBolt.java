package com.cy.storm;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

/**
 * Created by cy on 2018/3/26 22:42.
 */
public class SlidingWindowBolt extends BaseWindowedBolt {
    @Override
    public void execute(TupleWindow inputWindow) {

    }
}
