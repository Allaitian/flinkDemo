import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class KafkaToHive {
    public static void main(String[] args) {
        // get a TableEnvironment
        //StreamTableEnvironment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

// register Orders table
//        TableSink csvSink = new CsvTableSink("/path/to/file", ...);
        // define the field names and types
//        String[] fieldNames = {"a", "b", "c"};
//        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};

        // register the TableSink as table "CsvSinkTable"
//        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);

// compute revenue for all customers from France
        Table revenue = tableEnv.sqlQuery(
                "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

// emit or convert Table
// execute query
    }
}
