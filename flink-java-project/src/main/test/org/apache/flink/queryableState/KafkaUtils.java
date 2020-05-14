package org.apache.flink.queryableState;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;
import java.util.Random;

public class KafkaUtils {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "words";

    public static void writeToKafka() throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);

        /**
         * 产生一个随机字符串
         */
        String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"};
        StringBuilder str = new StringBuilder();
        for(int i = 0; i < 20;  i++){
            Random random = new Random();
            int randomInt = random.nextInt(12);
            str.append(months[randomInt]).append(" ");
        }
        String res = str.toString();

        /**
         * 将字符串写入到Kafka指定Topic中
         */
        ProducerRecord record = new ProducerRecord(topic, null, null, res);
        producer.send(record);

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException{
        while(true){
            Thread.sleep(500);
            writeToKafka();
        }
    }

}
