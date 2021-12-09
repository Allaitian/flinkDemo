package com.sql.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class SqlParser {
    public static void main(String[] args) {
        String sql = "insert overwrite table dws.oldUserOrderDays partition (dt = '${day}')\n" +
                "select user_id,\n" +
                "    int(count(distinct(order_id))) as order_cnt,\n" +
                "    int(count(distinct(if(success_time=0,null,success_time)))) as repay_cnt,\n" +
                "    max(over_days) as over_days,\n" +
                "    float(((unix_timestamp('${day}', 'yyyyMMdd') * 1000 + 57600000)-min(due_time))/8.64e7) as due_days\n" +
                "    from\n" +
                "    (select user_id,a.order_id,due_time,success_time,\n" +
                "     float(if(success_time=0,(unix_timestamp('${day}', 'yyyyMMdd') * 1000 + 57600000)-due_time,success_time-due_time)/8.64e7) as over_days\n" +
                "     from dwd.order_repay a\n" +
                "     join\n" +
                "     (select order_id,max(update_time) as update_time from dwd.order_repay group by order_id) b\n" +
                "     on a.order_id=b.order_id and a.update_time=b.update_time ) t1\n" +
                "    group by user_id";
        final List<String> tableNameBySql = getTableNameBySql(sql);
        for (String s : tableNameBySql) {
            System.out.println(s);
        }
    }

    private static List<String> getTableNameBySql(String sql) {
        DbType dbType = JdbcConstants.MYSQL;
        try {
            List<String> tableNameList = new ArrayList<>();
            //格式化输出
//            String sqlResult = SQLUtils.format(sql, dbType);
//            logger.info("格式化后的sql:[{}]",sqlResult);

            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            if (CollectionUtils.isEmpty(stmtList)) {
//                logger.info("stmtList为空无需获取");
                return Collections.emptyList();
            }
            for (SQLStatement sqlStatement : stmtList) {
                MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
                sqlStatement.accept(visitor);
                Map<TableStat.Name, TableStat> tables = visitor.getTables();
//                logger.info("druid解析sql的结果集:[{}]",tables);
                Set<TableStat.Name> tableNameSet = tables.keySet();
                for (TableStat.Name name : tableNameSet) {
                    String tableName = name.getName();
                    if (StringUtils.isNotBlank(tableName)) {
                        tableNameList.add(tableName);
                    }
                }
            }
//            logger.info("解析sql后的表名:[{}]",tableNameList);
            return tableNameList;
        } catch (Exception e) {
//            logger.error("**************异常SQL:[{}]*****************\\n",sql);
//            logger.error(e.getMessage(),e);
        }
        return Collections.emptyList();
    }

}
