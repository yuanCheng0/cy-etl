package com.cy.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by cy on 2018/1/9 22:49.
 */
public class MyRegionObserver extends BaseRegionObserver{
    private static byte[] fixed_rowkey = Bytes.toBytes("Jack");
    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        if(Bytes.equals(get.getRow(),fixed_rowkey)){
            KeyValue kv = new KeyValue(get.getRow(),Bytes.toBytes("time"),
                    Bytes.toBytes("time"),Bytes.toBytes(System.currentTimeMillis()));
            results.add(kv);
        }
    }
}
