package tencent.hive.visitor;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import tencent.hive.out.HplsqlLexer;
import tencent.hive.out.HplsqlParser;

/**
 * @Author: chuanchuan.lcc
 * @CreateDate: 2018/12/17 PM7:37
 * @Version: 1.0
 * @Description: java类作用描述：
 */
public class HiveParseUtils {

    public static void parseTable(String sql ){

        HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(sql));
        HplsqlParser parser = new HplsqlParser(new CommonTokenStream(lexer));
        HiveVisitor visitor = new HiveVisitor();
        HplsqlParser.Table_nameContext table_nameContext = parser.table_name();
        HplsqlParser.StmtContext bb = parser.stmt();

        String res = visitor.visitStmt(bb);

        return  ;

    }


}
