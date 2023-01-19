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
		BlockingQueue<String> sharedValuesQueue = new LinkedBlockingQueue<String>();
		AtomicInteger offset = new AtomicInteger(0);

		// Create number of producer threads
		Thread[] producer = new Thread[10];
		Arrays.stream(producer).forEach(p -> {
			offset.addAndGet(95);
			p = new Thread(new KeysProducer(sharedKeysQueue,offset));
			p.setName("PRODUCER THREAD " + p);
			p.start();
        });

		// Create number of consumer threads
		Thread[] consumer = new Thread[25];// amount of threads
		Arrays.stream(consumer).forEach(c -> {
			c = new Thread(new KeysConsumer(sharedKeysQueue,sharedValuesQueue));
			c.setName("CONSUMER THREAD " + c);
			c.start();
        });

		try {
			// Give sometime to fetch keys and get documents so that value processor
			// thread doesn't get terminated because of nothing to process
			Thread.sleep(500);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		Thread[] valueProcessor = new Thread[5];// amount of threads
		Arrays.stream(valueProcessor).forEach(vp -> {
			vp = new Thread(new ValueProcessor(sharedKeysQueue,sharedValuesQueue));
			vp.setName("VALUE PROCESSOR THREAD " + vp);
			vp.start();
		});
	}
}
