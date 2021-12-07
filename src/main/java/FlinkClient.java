import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkClient {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment
                .createRemoteEnvironment("flink-jobmanager", 8081, "/home/user/udfs.jar");

        DataSet<String> data = env.readTextFile("hdfs://path/to/file");

        data
                .filter(new FilterFunction<String>() {
                    public boolean filter(String value) {
                        return value.startsWith("http://");
                    }
                })
                .writeAsText("hdfs://path/to/result");

        env.execute();
    }
}
