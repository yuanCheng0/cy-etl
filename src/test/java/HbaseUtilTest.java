import com.cy.utils.HbaseUtil;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by 成圆 on 2017/12/25 23:53.
 */
public class HbaseUtilTest {
    public static void main(String[] args) {
        HbaseUtil.addFamilyName("students","bodyInfo");
    }
}
