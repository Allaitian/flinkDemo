import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class FlinkConnectKafkaDDL {
    public static void main(String[] args) throws Exception {

        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 2.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

//        String catalogName = "flink_hive";
//        String hiveDataBase = "flink";
//        String hiveConfDir = "/Users/aweqwe/Desktop/hive-conf";
//
//        // Catalog
//        HiveCatalog hiveCatalog =
//                new HiveCatalog(catalogName,hiveDataBase,hiveConfDir);
//
//        tableEnvironment.registerCatalog(catalogName , hiveCatalog);
//        tableEnvironment.useCatalog(catalogName);

        DataStream<Event> input = env.fromElements(
                new Event("1", "barfoo", 12),
                new Event("2", "start", 23),
                new Event("3", "foobar", 69)
);

        // DDL，根据kafka数据源创建表
        String kafkaTable = "person";
        String dropsql = "DROP TABLE IF EXISTS "+kafkaTable;
        String sql
                = "CREATE TABLE "+kafkaTable+" (\n" +
                "    user_id String,\n" +
                "    user_name String,\n" +
                "    age Int\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka',\n" +
//                "   'connector.version' = 'universal',\n" +
                "   'topic' = 'test',\n" +
                "   'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "   'properties.group.id' = 'testGroup',\n" +
                "   'scan.startup.mode' = 'earliest-offset',\n" +
                "   'format' = 'json'\n" +
//                "   'update-mode' = 'append'\n" +
                ")";
        tableEnvironment.executeSql(dropsql);
        tableEnvironment.executeSql(sql);

        Table table = tableEnvironment.sqlQuery("select * from person");

//        tableEnvironment. toDataStream(table , Row.class).print();
        tableEnvironment.toChangelogStream(table).print();

        env.execute("kafka");
    }
}