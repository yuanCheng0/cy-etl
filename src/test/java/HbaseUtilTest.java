import com.cy.utils.HbaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by cy on 2017/12/25 23:53.
 */
public class HbaseUtilTest {
    public static void main(String[] args) {
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
        Put put = new Put(Bytes.toBytes("1001"));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("height"),Bytes.toBytes(1.8));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("weight"),Bytes.toBytes(60));
        put.addColumn(Bytes.toBytes("bodyInfo"),Bytes.toBytes("yaowei"),Bytes.toBytes("80cm"));
        HbaseUtil.put("students",put);
        }
        }
