package kafka.jsample.wrapper;

import kafka.jsample.models.Product;
import kafka.jsample.models.User;
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

    private Producer<User, Product> producer;

    public void init() {
        log.info("Initializing kafka producer");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_ADDRESS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("schema.registry.url", Constants.SCHEMA_REGISTRY_ADDRESS);
        log.info("Connecting with kafka broker...");
        producer = new KafkaProducer<>(properties);
    }

    public void send(User user, Product product) {
        ProducerRecord<User, Product> record = new ProducerRecord<>(Constants.TOPIC_NAME, user, product);
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