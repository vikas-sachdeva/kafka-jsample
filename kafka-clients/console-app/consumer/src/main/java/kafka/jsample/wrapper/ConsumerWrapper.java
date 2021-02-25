package kafka.jsample.wrapper;

import kafka.jsample.model.Product;
import kafka.jsample.model.User;
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

    private Consumer<String, String> consumer;

    public void init() {
        log.info("Initializing kafka consumer");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_ADDRESS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void receive() {
        log.info("Connecting with kafka broker");
        consumer.subscribe(Collections.singletonList(Constants.TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                if (records.isEmpty()) {
                    log.info("No data received from kafka broker. Retrying");
                }
                for (ConsumerRecord<String, String> record : records) {
                    Product product = new Product().setProductId(record.value());
                    User user = new User().setUserId(record.key());
                    log.info("Data consumed from kafka - user = {} and product = {}", user, product);
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