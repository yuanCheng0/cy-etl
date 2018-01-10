import com.cy.utils.HbaseUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

/**
 * Created by cy on 2017/12/25 23:53.
 */
public class HbaseUtilTest {
    public static void main(String[] args) throws IOException {
//        HbaseUtil.addFamilyName("students","bodyInfo");
//        ResultScanner results = HbaseUtil.getAll("students");
//        Iterator<Result> iterator = results.iterator();
//        while (iterator.hasNext()){
//            Result result = iterator.next();
//            System.out.println(Bytes.toString(result.getRow()));
//            System.out.println(result);
//            System.out.println("======================");
//        }
//        System.out.println(HbaseUtil.getRow("students","Jack"));
//        HbaseUtil.createSnapshotTable("students");
       /* Put put = new Put(Bytes.toBytes("1007"));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("height"),Bytes.toBytes("1.7m"));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("weight"),Bytes.toBytes("55kg"));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("yaowei"),Bytes.toBytes("70cm"));
        Put put1 = new Put(Bytes.toBytes("1008"));
        put1.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("height"),Bytes.toBytes("1.72m"));
        put1.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("weight"),Bytes.toBytes("52kg"));
        put1.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("yaowei"),Bytes.toBytes("72cm"));
        Put put2 = new Put(Bytes.toBytes("1009"));
        put2.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("height"),Bytes.toBytes("1.73m"));
        put2.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("weight"),Bytes.toBytes("53kg"));
        put2.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("yaowei"),Bytes.toBytes("73cm"));
        List<Put> puts = new ArrayList<>();
        puts.add(put);
        puts.add(put1);
        puts.add(put2);
        HBase.put("students",puts,true);*/
//        HbaseUtil.put("students",put);
        Admin admin = HbaseUtil.getConn().getAdmin();
        TableName tableName = TableName.valueOf("students");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("bodyInfo");
        tableDescriptor.addCoprocessor("com.cy.hbase.coprocessor.MyRegionObserver");
//        tableDescriptor.modifyFamily(hColumnDescriptor);
        admin.modifyTable(tableName,tableDescriptor);
    }}
