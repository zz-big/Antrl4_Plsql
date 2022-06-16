package com.zz.constant;

/**
 * Description:
 *
 * @author zz
 * @date 2021/10/11
 */
public interface Constant {

    String SHOW_COLUMNS_HIVE="DESCRIBE dc_stg.%s";
    // String SHOW_COLUMNS = "select column_name,column_comment,data_type from information_schema.columns where table_name='%s' and table_schema='%s'";
    String SHOW_COLUMNS_MYSQL = " show FULL COLUMNS from %s";
    String SHOW_COLUMNS_CK = "select name as col_name,type as data_type,comment as col_comment from system.columns where database = '%s' and table='%s'";
    String SHOW_COLUMNS_POSTGRE = "Select a.attname as col_name,(select description from pg_catalog.pg_description where objoid=a.attrelid and objsubid=a.attnum) as col_comment ,pg_catalog.format_type(a.atttypid,a.atttypmod) as data_type from pg_catalog.pg_attribute a where 1=1 and a.attrelid=(select oid from pg_class where relname='%s' ) and a.attnum>0 and not a.attisdropped order by a.attnum";
    String SHOW_COLUMNS_SQL_SERVER = "SELECT col.name AS col_name ,  CONVERT(nvarchar(50),ISNULL(ep.[value], '')) AS col_comment  ,  t.name AS data_type FROM    dbo.syscolumns col  LEFT  JOIN dbo.systypes t ON col.xtype = t.xusertype  inner JOIN dbo.sysobjects obj ON col.id = obj.id  AND obj.xtype = 'U'  AND obj.status >= 0  LEFT  JOIN dbo.syscomments comm ON col.cdefault = comm.id  LEFT  JOIN sys.extended_properties ep ON col.id = ep.major_id  AND col.colid = ep.minor_id  AND ep.name = 'MS_Description' WHERE   obj.name = '%s' ORDER BY col.colorder";
    String TABLE_COMMENT_MYSQL = "SELECT TABLE_COMMENT  FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA='%s' and  TABLE_NAME='%s'";
    String TABLE_COMMENT_POSTGRE = "select cast(obj_description(relfilenode,'pg_class') as varchar) as table_comment from pg_class c where relname='%s'";
    String TABLE_COMMENT_SQL_SERVER = "SELECT DISTINCT  CONVERT(nvarchar(500),ISNULL(f.value , '')) as table_comment FROM syscolumns a LEFT JOIN systypes b ON a.xusertype= b.xusertype INNER JOIN sysobjects d ON a.id= d.id AND d.xtype= 'U' AND d.name = '%s' LEFT JOIN syscomments e ON a.cdefault= e.id LEFT JOIN sys.extended_properties g ON a.id= G.major_id AND a.colid= g.minor_id LEFT JOIN sys.extended_properties f ON d.id= f.major_id AND f.minor_id= 0";
    String TABLE_COMMENT_CK = "select comment as table_comment from  system.tables  where database = '%s' and name='%s'";

}
