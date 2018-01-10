package hbase;

import com.cy.utils.HbaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.io.IOException;

/**
 * Created by cy on 2018/1/6 18:04.
 */
public class HbaseFilterTest {
    public static void main(String[] args) throws IOException {
        //filterTest();
       pageFilterTest();
       /* ResultScanner sc = HbaseUtil.getAll("students");
        for (Result r : sc){
            System.out.println(r);
        }*/
    }
    //@Test
    public static void filterTest() throws IOException {
        Scan scan = new Scan();
        scan.setCaching(1000);
        //RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Jack")));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("J\\w+"));
        scan.setFilter(filter);
        Table table = HbaseUtil.getTable("students");
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner){
            System.out.println(res);
        }
    }
   // @Test
    public static void pageFilterTest() throws IOException {
        PageFilter filter = new PageFilter(4);
        byte[] lastRow = null;
        int pageCount = 0;//定义第几页
        Table table = HbaseUtil.getTable("students");
        while (++pageCount > 0){
            System.out.println("pageCount = " + pageCount);
            Scan scan = new Scan();
            scan.setFilter(filter);
            if(lastRow != null){
                scan.setStartRow(lastRow);
            }
            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            for (Result res : scanner){
                lastRow = res.getRow();
                if (++count > 3){
                    break;
                }
                System.out.println(res);
            }
            if(count < 3){
                break;
            }
        }
    }
}
