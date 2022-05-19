package tencent.impala;

import org.apache.impala.analysis.*;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:
 *
 * @author zz
 * @date 2022/5/19
 */
public class ParserTest {


        public static void main (String[]args){
            //SELECT 列名称（*所有列） FROM 表名称
            //SELECT 列名称 FROM 表名称 where 条件
//            System.out.println(matchSql("select * from aaa "));
            System.out.println(matchSql("SELECT `b`.`大区` FROM (SELECT `T445`.`bu` as `所属部门` FROM `dc_tmp`.`sale_bi_account` `T445` WHERE (`T445`.`people_account` = 'xiechunhui@pudutech.com')) AS a LEFT JOIN (SELECT DISTINCT '所有' as `au`, `T1425`.`dept2_name` as `大区` FROM `dc_dwd`.`dwd_sales_erp_dtl_dd_v2` `T1425`) AS b ON (`a`.`所属部门` = `b`.`au`)  UNION SELECT `T445`.`bu` as `大区` FROM `dc_tmp`.`sale_bi_account` `T445` WHERE (`T445`.`people_account` = 'xiechunhui@pudutech.com')"));
//            //INSERT INTO 表名称 VALUES (值1, 值2,....)
//            //INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
//            System.out.println(matchSql("insert into ccc valuse(1,'neo','password')"));
//            System.out.println(matchSql("insert into ddd (id,name,password) valuses(1,'neo','password')"));
//            //UPDATE 表名称 SET 列名称 = 新值 WHERE 列名称 = 某值
//            System.out.println(matchSql("update eee set name = 'neo' where id = 1 "));
//            //DELETE FROM 表名称 WHERE 列名称 = 值
//            System.out.println(matchSql("delete from fff where id = 1 "));

        }
        /**
         * @param sql lowcase
         * @return
         */
        public static String matchSql (String sql){
            Matcher matcher = null;
            //SELECT 列名称 FROM 表名称
            //SELECT * FROM 表名称
            if (sql.startsWith("select")) {
                matcher = Pattern.compile("select\\s.+from\\s(.+)where\\s(.*)").matcher(sql);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
            //INSERT INTO 表名称 VALUES (值1, 值2,....)
            //INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
            if (sql.startsWith("insert")) {
                matcher = Pattern.compile("insert\\sinto\\s(.+)\\(.*\\)\\s.*").matcher(sql);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
            //UPDATE 表名称 SET 列名称 = 新值 WHERE 列名称 = 某值
            if (sql.startsWith("update")) {
                matcher = Pattern.compile("update\\s(.+)set\\s.*").matcher(sql);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
            //DELETE FROM 表名称 WHERE 列名称 = 值
            if (sql.startsWith("delete")) {
                matcher = Pattern.compile("delete\\sfrom\\s(.+)where\\s(.*)").matcher(sql);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
            return null;
        }
}
