package kafka.jsample.util;

public class Constants {

    public static final String BROKER_ADDRESS = "192.168.99.102:9092";

    public static final String SCHEMA_REGISTRY_ADDRESS = "http://192.168.99.102:8081";

    public static final String SOURCE_TOPIC_NAME = "user-shopping-cart";

    public static final String DEST_TOPIC_NAME = "user-shopping-cart-checkout";

    public static final String APPLICATION_ID = "total-calculator";

    private Constants() {
    }
}