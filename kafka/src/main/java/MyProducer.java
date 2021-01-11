import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.*;
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
     * 第二种同步发送，等待执行结果
     * @return
     * @throws Exception
     */
    private static RecordMetadata sendMessageSync() throws Exception{
        String json = getJson();


        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "test","name",json
        );
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
        return result;
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


    public static void main(String[] args) throws Exception {
        sendMessageSync();
    }


    /**
     * 将文件转换成byte数组
     * @param
     * @return
     */
    public static byte[] file2Byte(File tradeFile){
        byte[] buffer = null;
        try
        {
            FileInputStream fis = new FileInputStream(tradeFile);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1)
            {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
        return buffer;
    }

    public static String getJson(){
        return "{\n" +
                "\t\"uuid\": \"e0f8a52b-607a-48e4-b535-83b23e429875\",\n" +
                "\t\"ip\": \"172.16.0.142\",\n" +
                "\t\"app_info\": {\n" +
                "\t\t\"app_id\": \"com.finogeeks.club.cn2\",\n" +
                "\t\t\"name\": \"\",\n" +
                "\t\t\"app_version\": \"1.1.0\",\n" +
                "\t\t\"sdk_key\": \"22LyZEib0gLTQdU3MUauARL+C0QziSLZhCStRwak1uA=\",\n" +
                "\t\t\"sdk_version\": \"2.1.1\"\n" +
                "\t},\n" +
                "\t\"device_info\": {\n" +
                "\t\t\"device_id\": \"5ec632ef7f3b210001475d82\",\n" +
                "\t\t\"os\": \"iOS\",\n" +
                "\t\t\"os_version\": \"12.4.4\",\n" +
                "\t\t\"imei\": \"\",\n" +
                "\t\t\"mac\": \"\",\n" +
                "\t\t\"screen_size\": \"2232*1080\",\n" +
                "\t\t\"brand\": \"HONOR\",\n" +
                "\t\t\"model\": \"YAL-AL00\"\n" +
                "\t},\n" +
                "\t\"events\": [{\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 1622\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_show\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"80637733\",\n" +
                "\t\t\t\"path\": \"pages/server/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 1647\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_hide\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"204433348\",\n" +
                "\t\t\t\"path\": \"pages/index/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_show\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"175704090\",\n" +
                "\t\t\t\"path\": \"pages/index/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 912795\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_show\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"196881056\",\n" +
                "\t\t\t\"path\": \"page/component/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 3588\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_hide\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"175704090\",\n" +
                "\t\t\t\"path\": \"pages/index/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_show\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"244584028\",\n" +
                "\t\t\t\"path\": \"pages/index/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 1293335\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_hide\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"196881056\",\n" +
                "\t\t\t\"path\": \"page/component/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_show\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"233624308\",\n" +
                "\t\t\t\"path\": \"page/component/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"?????\",\n" +
                "\t\t\"event_type\": \"applet_start\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"load_time\": 309182\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}, {\n" +
                "\t\t\"id\": \"\",\n" +
                "\t\t\"event_name\": \"????\",\n" +
                "\t\t\"event_type\": \"page_hide\",\n" +
                "\t\t\"applet_id\": \"5ff3d0c6a484fc00014183c4\",\n" +
                "\t\t\"timestamp\": 1609912489000,\n" +
                "\t\t\"applet_ver\": \"\",\n" +
                "\t\t\"applet_sequence\": 0,\n" +
                "\t\t\"is_gray\": false,\n" +
                "\t\t\"payload\": {\n" +
                "\t\t\t\"page_id\": \"244584028\",\n" +
                "\t\t\t\"path\": \"pages/index/index.html\"\n" +
                "\t\t},\n" +
                "\t\t\"organ_id\": \"\",\n" +
                "\t\t\"basic_pack_version\": \"\"\n" +
                "\t}],\n" +
                "\t\"report_time\": 1609912489000\n" +
                "}";
    }
 
 
}