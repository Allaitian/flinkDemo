package com.sql.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSONObject;
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
        String tableNameBySql = getTableNameBySql(sql);
        System.out.println(tableNameBySql);
    }

    private static String getTableNameBySql(String sql) {
        DbType dbType = JdbcConstants.HIVE;
        JSONObject jsonObject = new JSONObject();
        try {
            //格式化输出 sql语句
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            if (stmtList.isEmpty()) {
//                logger.info("stmtList为空无需获取");
                throw new Exception("输入的SQL不存在HIVE表");
            }
            if (stmtList.size() != 1) {
                throw new Exception("输入的不是一条SQL语句");
            }
            SQLStatement sqlStatement = stmtList.get(0);

            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            sqlStatement.accept(visitor);
            Map<TableStat.Name, TableStat> tables = visitor.getTables();
            for (Map.Entry<TableStat.Name, TableStat> nameTableStatEntry : tables.entrySet()) {
                jsonObject.put(nameTableStatEntry.getKey().toString(), nameTableStatEntry.getValue().toString());
            }
            return jsonObject.toJSONString();

        } catch (Exception e) {
            System.out.println(e + "输入的SQL不正确");
        }

        return "";
    }

}
