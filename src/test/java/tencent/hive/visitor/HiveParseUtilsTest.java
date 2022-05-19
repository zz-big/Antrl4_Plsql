package tencent.hive.visitor;

import org.junit.Test;

/**
 * Created by lcc on 2018/12/17.
 */
public class HiveParseUtilsTest {
    @Test
    public void parseTable() throws Exception {

        //        String query = "SELECT LastName,FirstName FROM Persons;";

        String  query = "SELECT Persons.LastName, Persons.FirstName, Orders.OrderNo\n" +
                "FROM Persons\n" +
                "INNER JOIN Orders\n" +
                "ON Persons.Id_P = Orders.Id_P\n" +
                "ORDER BY Persons.LastName";

        HiveParseUtils.parseTable(query);
    }


    @Test
    public void parseTable1() throws Exception {
        String sql = "insert into table_a (id,name ) select a.id,b.name  from a inner join b on a.id =b.id ";
        HiveParseUtils.parseTable(sql);
    }


}