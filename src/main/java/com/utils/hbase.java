package com.utils;

import com.bean.UserMovie;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 # 创建表
 create 'u_m_01' , 'u_m_r'

 # 插入数据
 put 'u_m_01', 'a,A', 'u_m_r:r' , '1'
 put 'u_m_01', 'a,B', 'u_m_r:r' , '3'
 put 'u_m_01', 'b,B', 'u_m_r:r' , '3'
 put 'u_m_01', 'b,C', 'u_m_r:r' , '4'
 put 'u_m_01', 'c,A', 'u_m_r:r' , '2'
 put 'u_m_01', 'c,C', 'u_m_r:r' , '5'
 put 'u_m_01', 'c,D', 'u_m_r:r' , '1'
 put 'u_m_01', 'd,B', 'u_m_r:r' , '5'
 put 'u_m_01', 'd,D', 'u_m_r:r' , '2'
 put 'u_m_01', 'e,A', 'u_m_r:r' , '3'
 put 'u_m_01', 'e,B', 'u_m_r:r' , '2'
 put 'u_m_01', 'f,A', 'u_m_r:r' , '1'
 put 'u_m_01', 'f,B', 'u_m_r:r' , '2'
 put 'u_m_01', 'f,D', 'u_m_r:r' , '3'
 put 'u_m_01', 'g,C', 'u_m_r:r' , '1'
 put 'u_m_01', 'g,D', 'u_m_r:r' , '4'
 put 'u_m_01', 'h,A', 'u_m_r:r' , '1'
 put 'u_m_01', 'h,B', 'u_m_r:r' , '2'
 put 'u_m_01', 'h,C', 'u_m_r:r' , '4'
 put 'u_m_01', 'h,D', 'u_m_r:r' , '5'
 */


public class hbase {
    public static void main(String[] args) {


        // 批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                        .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建用户-电影表 u_m
        TableResult tableResult = tableEnv.executeSql(
                "CREATE TABLE u_m (" +
                        " rowkey STRING," +
                        " u_m_r ROW<r STRING>," +
                        " PRIMARY KEY (rowkey) NOT ENFORCED" +
                        " ) WITH (" +
                        " 'connector' = 'hbase-2.2' ," +
                        " 'table-name' = 'default:u_m_01' ," +
                        " 'zookeeper.quorum' = 'hdp001:2181,hdp002:2181,hdp003:2181'" +
                        " )");

        // 查询是否能获取到HBase里的数据
//        Table table = tableEnv.sqlQuery("SELECT rowkey, u_m_r FROM u_m");

        //result 是 OK
//        tableResult.print();

        // 相当于 scan
        Table table = tableEnv.sqlQuery("SELECT rowkey , u_m_r.r FROM u_m");
//                tableEnv.executeSql("SELECT * FROM u_m").print();
        tableEnv.createTemporaryView("hbase_table", table);
        // 查询的结果
        TableResult executeResult = table.execute();
        // 获取查询结果
        CloseableIterator<Row> collect = executeResult.collect();

        // 输出 (执行print或者下面的 Consumer之后，数据就被消费了。两个只能留下一个)
                executeResult.print();

        List<UserMovie> userMovieList = new ArrayList<>();

        collect.forEachRemaining(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                //+I[F,A, +I[23]] row类型数据
                System.out.println(row);
                String field0 = String.valueOf(row.getField(0));
                String[] user_movie = field0.split(",");
                // Double.valueOf  String.valueOf 这些都是要判断是不是非空的
                // String.valueOf(((Row) row.getField(1)).getField(0))的原因是因为用了select * 而不是具体的
//                Double ratting = Double.valueOf(String.valueOf(((Row) row.getField(1)).getField(0)));
                Double ratting = Double.valueOf(String.valueOf( row.getField(1)));
                userMovieList.add(new UserMovie(user_movie[0], user_movie[1], ratting));
            }
        });

        System.out.println("................");

        for (UserMovie um : userMovieList) {
            System.out.println(um);
        }
    }
}

