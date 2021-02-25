package kafka.jsample.wrapper;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.jsample.models.Product;
import kafka.jsample.models.User;
import kafka.jsample.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class StreamWrapper {

    private KafkaStreams kafkaStreams;

    public void init() {
        log.info("Initializing kafka streamer");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_ADDRESS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constants.SCHEMA_REGISTRY_ADDRESS);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<User, Product> kStream = streamsBuilder.stream(Constants.SOURCE_TOPIC_NAME);

        kStream.peek(this::printOnEntry)
               .map(this::calculateShippingFee)
               .map(this::calculateDiscountOffer)
               .peek(this::printOnExit)
               .to(Constants.DEST_TOPIC_NAME);
        Topology topology = streamsBuilder.build();
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }

    private KeyValue<User, Product> calculateDiscountOffer(User user, Product product) {
        if (product.getPrice() > 1000) {
            log.info("product cost is high so adding discount offer");
            product.setPrice(product.getPrice() + (product.getPrice() / 10));
        }
        return new KeyValue<>(user, product);
    }

    private KeyValue<User, Product> calculateShippingFee(User user, Product product) {
        if (product.getPrice() < 200) {
            log.info("product cost is less so adding shipping charges");
            product.setPrice(product.getPrice() + 40);
        }
        return new KeyValue<>(user, product);
    }

    private void printOnExit(User user, Product product) {
        log.info("Data passed on exit - user = {} and product = {}", user, product);
    }

    private void printOnEntry(User user, Product product) {
        log.info("Data received on entry - user = {} and product = {}", user, product);
    }

    public void destroy() {
        log.info("Destroying kafka streamer");
        kafkaStreams.close();
    }
}