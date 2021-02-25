package kafka.jsample.wrapper;

import kafka.jsample.models.Product;
import kafka.jsample.models.User;
import kafka.jsample.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerWrapper {

    private Consumer<User, Product> consumer;

    public void init() {
        log.info("Initializing kafka consumer");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_ADDRESS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        properties.put("specific.avro.header","true");
        properties.put("schema.registry.url", Constants.SCHEMA_REGISTRY_ADDRESS);
        consumer = new KafkaConsumer<>(properties);
    }

    public void receive() {
        log.info("Connecting with kafka broker");
        consumer.subscribe(Collections.singletonList(Constants.TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<User, Product> records = consumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                if (records.isEmpty()) {
                    log.info("No data received from kafka broker. Retrying");
                }
                for (ConsumerRecord<User, Product> record : records) {
                    log.info("Data consumed from kafka - user = {} and product = {}", record.key(), record.value());
                }
            }
        } catch (WakeupException wakeupException) {
            log.warn("Consumer blocking polling operation preempted");
        } finally {
            consumer.close();
        }
    }

    public void destroy() {
        log.info("Destroying kafka consumer");
        consumer.wakeup();
    }
}