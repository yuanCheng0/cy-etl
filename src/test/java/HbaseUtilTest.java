import com.cy.utils.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

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
        HbaseUtil.createSnapshotTable("students");
    }
}
