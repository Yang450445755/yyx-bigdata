import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MyProducer implements Job {
    private static KafkaProducer<String,String> producer;
 
    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","yyxdata001:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }
 
    /**
     * 第一种直接发送，不管结果
     */
    private static void sendMessageForgetResult(){
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "test","name","Forget_result"
        );
        producer.send(record);
        producer.close();
    }
 
    /**
     * 第二种同步发送，等待执行结果
     * @return
     * @throws Exception
     */
    private static RecordMetadata sendMessageSync() throws Exception{
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "test","name","sync"
        );
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
        return result;
    }
 
    /**
     * 第三种执行回调函数
     */
    private static void sendMessageCallback(){
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "kafka-study","name","callback"
        );
        producer.send(record,new MyProducerCallback());
    }
 
    //定时任务
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            sendMessageSync();
        }catch (Exception e){
            System.out.println("error:"+e);
        }
 
    }
 
    private static class MyProducerCallback implements Callback {
 
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e !=null){
                e.printStackTrace();
                return;
            }
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }
 
 
    public static void main(String[] args) throws Exception {

        sendMessageSync();
 
    }
 
 
}