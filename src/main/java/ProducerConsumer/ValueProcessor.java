package ProducerConsumer;

import com.couchbase.client.java.Cluster;

import java.util.concurrent.BlockingQueue;

public class ValueProcessor extends Thread {

    // Read data to consume once data is loaded in queue
    private BlockingQueue<String> keysQueue;
    private BlockingQueue<String> valuesQueue;
    private static final Cluster cluster = CouchbaseConfiguration.getInstance().getCluster();

    public ValueProcessor(BlockingQueue<String> keysQueue,BlockingQueue<String> valuesQueue) {
        super("VALUE PROCESSOR THREAD");
        this.keysQueue = keysQueue;
        this.valuesQueue=valuesQueue;
    }

    public void run() {
        try {
            // Capture time before process starts
            long startTime = System.currentTimeMillis();
            boolean firstRun = false;
            while (true) {

                if (valuesQueue.size()>=1) {
                    System.out.println("***************VALUES QUEUE SIZE NOT EMPTY************** " + valuesQueue.size());
                }

                // Remove the key from shared key queue and process
                String values = valuesQueue.take();

                // TODO: Implement filter logic to process documents

                if (keysQueue.size() < 1 && valuesQueue.size() < 1 && firstRun) {
                    System.out.println("***********TERMINATE*************VALUES QUEUE SIZE************** " + valuesQueue.size());
                    // Stop thread execution once the queue is exhausted
                    break;
                }
                firstRun =true;
            }
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("&&&&&&&&&&&&&&&& ------------- VALUE PROCESSOR THREAD EXECUTION COMPLETE ------------- &&&&&&&&&&&&&&&&&"+ Thread.currentThread().getName()
                    + "TIME TAKEN: " +totalTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
