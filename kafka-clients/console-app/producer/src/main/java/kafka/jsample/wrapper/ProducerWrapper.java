package kafka.jsample.wrapper;

import kafka.jsample.model.Product;
import kafka.jsample.model.User;
import kafka.jsample.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.Properties;

@Slf4j
public class ProducerWrapper {

    private Producer<String, String> producer;

    public void init() {
        log.info("Initializing kafka producer");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_ADDRESS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        log.info("Connecting with kafka broker...");
        producer = new KafkaProducer<>(properties);
    }

    public void send(User user, Product product) {
        ProducerRecord<String, String> record = new ProducerRecord<>(Constants.TOPIC_NAME, user.getUserId(), product.getProductId());
        log.info("Publishing data to kafka {}", record);
        producer.send(record, ProducerWrapper::onCompletion);
    }

    private static void onCompletion(RecordMetadata r, Exception e) {
        if (Objects.nonNull(e)) {
            log.error("Exception occurred while sending data to kafka", e);
        }
    }

    public void destroy() {
        log.info("Destroying kafka producer");
        producer.close();
    }
}