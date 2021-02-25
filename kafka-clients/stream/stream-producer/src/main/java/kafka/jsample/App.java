package kafka.jsample;

import kafka.jsample.data.ProductDataGenerator;
import kafka.jsample.data.UserDataGenerator;
import kafka.jsample.wrapper.ProducerWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class App {

    public static void main(String[] args) {
        ProducerWrapper producerWrapper = new ProducerWrapper();
        producerWrapper.init();
        Thread mainThread = Thread.currentThread();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                producerWrapper.send(UserDataGenerator.getRandomUser(), ProductDataGenerator.getRandomProduct());
            }
        }, 0, 1000 * 2);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Exiting from the application");
            timer.cancel();
            producerWrapper.destroy();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Exception occurred while exiting application", e);
            }
        }));
    }
}