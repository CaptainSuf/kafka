package com.dev;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducer extends Thread{

    private String topic;

    public KafkaProducer(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducr();
        for(int i=0;i<10;i++){
            producer.send(new KeyedMessage(topic,"message:hello world"+i));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducr(){
        /*ProducerRecord*/
        Properties properties = new Properties();
        //声明zk
        properties.put("zookeeper.connect", "192.168.99.100:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        // 声明kafka broker
        properties.put("metadata.broker.list", "192.168.99.100:9092");
        return new Producer(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new KafkaProducer("test").start();
    }
}
