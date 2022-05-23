package com.zz.util;


import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.hive.ast.HiveInsertStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;

import java.util.*;


/**
 * Description:
 *
 * @author zz
 * @date 2022/5/19
 */
public class SqlUtils {


    public static List<String> getTableNameList(String sql, DbType dbType) {
        List<String> res = new ArrayList<>();
//        try {
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbType);
        for (SQLStatement sqlStatement : sqlStatements) {
            HiveSchemaStatVisitor sqlastVisitor = new HiveSchemaStatVisitor();
            if (sqlStatement instanceof SQLSelectStatement) {
                SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
                sqlSelectStatement.accept(sqlastVisitor);
            } else if (sqlStatement instanceof HiveInsertStatement) {
                HiveInsertStatement sqlInsertStatement = (HiveInsertStatement) sqlStatement;
                sqlInsertStatement.accept(sqlastVisitor);
            } else if (sqlStatement instanceof SQLDeleteStatement) {
                SQLDeleteStatement sqlDeleteStatement = (SQLDeleteStatement) sqlStatement;
                sqlDeleteStatement.accept(sqlastVisitor);
            } else if (sqlStatement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement sqlAlterTableStatement = (SQLAlterTableStatement) sqlStatement;
                sqlAlterTableStatement.accept(sqlastVisitor);
            }
            //获取表列表
            Map<TableStat.Name, TableStat> tables = sqlastVisitor.getTables();
            for (Map.Entry<TableStat.Name, TableStat> nameTableStatEntry : tables.entrySet()) {
                res.add(nameTableStatEntry.getKey().getName());
            }
            //获取列列表
//            Collection<TableStat.Column> columnCollection = sqlastVisitor.getColumns();
//                columnCollection.forEach(column->{
//                    System.out.println("tableName:{},columnName:{},iswhere:{}"+column.getTable()+column.getName()+column.isWhere());
//                });
        }
//        }
//        catch (Exception e) {
////            System.out.println("sql解析错误sql:{},错误原因：{}"+sql+e.getMessage());
//            if (e.getMessage().contains("token IDENTIFIER serdeproperties")) {
//                String tempSql = sql.replaceAll(" ","").toLowerCase(Locale.ROOT);
//                int start = tempSql.indexOf("altertable") + 10;
//                int end = tempSql.indexOf("setserdeproperties(");
//                res.add(tempSql.substring(start, end));
//            }
//        }
        return res;
    }

    public static void main(String[] args) {
        String sql = "insert overwrite table dc_ods.erp_kingdee_20201124_T_SAL_ORDERPLAN_full_ods \n" +
                "select  \n" +
                "   `FENTRYID`,\n" +
                " `FID`,\n" +
                " `FRECEIVETYPE`,\n" +
                " `FNEEDRECADVANCE`,\n" +
                " `FRECADVANCERATE`,\n" +
                " `FRECADVANCEAMOUNT`,\n" +
                " `FRECAMOUNT`,\n" +
                " `FMUSTDATE`,\n" +
                " `FRELBILLNO`,\n" +
                " `FCONTROLSEND`,\n" +
                " `FREMARK`,\n" +
                " `FSEQ`,\n" +
                " `FPREMATCHAMOUNTFOR`,\n" +
                " `FPLANMATERIALID`,\n" +
                " `FMATERIALSEQ`,\n" +
                " `FORDERENTRYID`,\n" +
                " `FASSAMOUNTFOR`,\n" +
                " `FASSDEDAMOUNTFOR`,\n" +
                " `FISOUTSTOCKBYRECAMOUNT`,\n" +
                " `FMATERIALROWID`,\n" +
                " `FAGGRECAMOUNT`,\n" +
                " `F_PUDU_DATE1`,\n" +
                " `FMATERIALTAXPRICE`,\n" +
                " `FMATERIALPRICEUNITQTY`,\n" +
                " `FMATERIALPRICEUNITID`,\n" +
                " `FOVERRECAMOUNT`,\n" +
                " `stg_update_time`        \n" +
                "from dc_stg.erp_kingdee_20201124_T_SAL_ORDERPLAN_stg";
        System.out.println(getTableNameList(sql, DbType.hive));
    }

}
