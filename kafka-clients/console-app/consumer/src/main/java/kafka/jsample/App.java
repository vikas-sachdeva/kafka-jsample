package kafka.jsample;

import kafka.jsample.wrapper.ConsumerWrapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
    
    public static void main(String[] args) {
        ConsumerWrapper consumerWrapper = new ConsumerWrapper();
        consumerWrapper.init();
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Exiting from the application");
            consumerWrapper.destroy();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Exception occurred while exiting application", e);
            }
        }));

        consumerWrapper.receive();
    }
}