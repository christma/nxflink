package com.nx.pro.zeye.lesson01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerDemo {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "192.168.208.51:9092");
    properties.put("acks", "all");
    properties.put("retries", 0);
    properties.put("batch.size", 16384);
    properties.put("linger.ms", 1);
    properties.put("buffer.memory", 33554432);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = null;

    try {
      producer = new KafkaProducer<String, String>(properties);
      int i = 0;
      Random r = new Random(1);
      while (true){
        String msg = "------Message " + i++;
        producer.send(new ProducerRecord<String, String>("nxflink", msg));
        System.out.println("Sent:" + msg);
        Thread.sleep(r.nextInt(1000));
      }
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      producer.close();
    }
  }
}
