import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
/**
 * 主要参数：
 * 1.bootstrap.servers,group.id,key.deserializer,value.deserializer
 * 2.session.timeout.ms coordinator检测失败的时间，设置为比较小的值
 * 3. max.poll.interval.ms consumer处理逻辑最大时间
 * 4. auto.offset.reset  [earliest,lastest,none]
 * 5. enable.auto.commit 是否自动提交位移，设置为false，由用户自行提交位移
 * 6. fetch.max.bytes 指定consumer单次获取数据的最大字节数
 * 7. max.poll.records 单次调用poll的最大返回消息数，默认500
 * 8. heartbeat.interval.ms  越小越好
 * 9. connections.max.idle.ms Kafka定期关闭空闲Socket的时间
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {

        String topic = "mop_data_report";
        String groupId = "test-group";

        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.0.11.13:9092");
        //必须指定有业务意义的名字
        properties.put("group.id",groupId);
        properties.put("enable.auto.commit","true");
        properties.put("auto.commit.interval.ms","1000");
        //从最早的消息开始读取
        properties.put("auto.offset.reset","earliest");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题，可以订阅多个主题，还可以使用正则表达式订阅主题
        //注意：多次订阅，会覆盖前面的
        consumer.subscribe(Collections.singletonList(topic));

        try{
            while(true){
                //1000是超时设定，如果有定时要求，可设置，否则建议设置个比较大的值
                //通常consumer拿到足够多的数据，会立即返回，否则会阻塞
                //poll返回则认为是成功消费了消息,如果发现消费慢需要分析是poll慢还是本身业务逻辑处理慢
                ConsumerRecords<String,String> records = consumer.poll(1000);
                for(ConsumerRecord<String,String> record : records){
                    System.out.printf("offset=%d, key=%s,value= %s%n",record.offset(),record.key(),record.value());
                }
            }
        }finally {
            consumer.close();
        }
    }
}