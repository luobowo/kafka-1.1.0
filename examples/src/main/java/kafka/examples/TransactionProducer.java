package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * example to test transaction
 * @author XiaFeng  @ 2019-04-03
 * @version 1.0.0
 */
public class TransactionProducer
{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private volatile boolean stop = false;

    private TransactionProducer(String topic) {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "test-transactional-id1");
        props.put("enable.idempotence", true);
        props.put("acks", "all");
        producer = new KafkaProducer<>(props);
        this.topic = topic;

        Runtime.getRuntime().addShutdownHook(
            new Thread(){
                public void run(){
                    stop = true;
                    producer.abortTransaction();
                    producer.close();
                }
            }
        );
    }

    private void start() {
        producer.initTransactions();
        int messageNo = 1;
        while (!stop) {
            producer.beginTransaction();
            String messageStr = "Message_" + messageNo;
            System.out.println("send message begin");
            producer.send(new ProducerRecord<>(topic,
                messageNo,
                messageStr));
            ++messageNo;
            producer.commitTransaction();
            System.out.println(
                "message(" + messageStr + ") sent to kafka success");
            try {
                Thread.sleep(100);
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args){
        TransactionProducer producer = new TransactionProducer(KafkaProperties.TOPIC);
        producer.start();
    }
}
