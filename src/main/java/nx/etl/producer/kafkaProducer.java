package nx.etl.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class kafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();

        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        String topic = "nxflink";

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        while (true) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"countryCode\":\"" + getCountryCode() + "\",\"data\":[{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"},{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"}]}";
            System.out.println(message);
            //同步的方式，往Kafka里面生产数据
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(2000);
        }
        //关闭链接


    }

    public static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getCountryCode() {
        String[] types = {"US", "TW", "HK", "PK", "KW", "SA", "IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static double getRandomScore() {
        double[] types = {0.3, 0.2, 0.1, 0.5, 0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomLevel() {
        String[] types = {"A", "A+", "B", "C", "D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
