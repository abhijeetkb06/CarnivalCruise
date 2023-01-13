package ProducerConsumer;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This initiates the process of producer and consumer.
 * 
 * @author abhijeetbehera
 */
public class LaunchCruiseSearch {

	public static void main(String[] args) {

		BlockingQueue<String> sharedKeysQueue = new LinkedBlockingQueue<String>();
		AtomicInteger offset = new AtomicInteger(0);

		// Create number of producer threads
		Thread[] producer = new Thread[10];
		Arrays.stream(producer).forEach(p -> {
			offset.addAndGet(200);
			p = new Thread(new Producer(sharedKeysQueue,offset));
			p.setName("PRODUCER THREAD " + p);
			p.start();
        });

		// Create number of consumer threads
		Thread[] consumer = new Thread[10];// amount of threads
		Arrays.stream(consumer).forEach(c -> {
			c = new Thread(new Consumer(sharedKeysQueue));
			c.setName("CONSUMER THREAD " + c);
			c.start();
        });
	}
}
