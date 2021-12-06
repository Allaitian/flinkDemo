


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class KafkaMessageGenerator {

    public static void main(String[] args) throws Exception {
        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", "localhost:9092");
        //设置数据key和value的序列化处理类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int i=0;
        Event event = new Event();
        while (true){
        event.setUser_id("2"+i);
        event.setUser_name("peter"+i);
        event.setAge(1+i);
            System.out.println(event.toString());
            ProducerRecord<String, String> record = new ProducerRecord("test", JSONObject.toJSONString(event));
        producer.send(record);
        ++i;
        Thread.sleep(1000);}


    }

}
