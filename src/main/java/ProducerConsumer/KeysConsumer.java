package ProducerConsumer;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;

import java.util.concurrent.BlockingQueue;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class KeysConsumer extends Thread {

    // Read data to consume once data is loaded in queue
    private BlockingQueue<String> keysQueue;
    private BlockingQueue<String> valuesQueue;
    private static final Cluster cluster = CouchbaseConfiguration.getInstance().getCluster();

    public KeysConsumer(BlockingQueue<String> keysQueue, BlockingQueue<String> valuesQueue) {
        super("KEYS CONSUMER THREAD");
        this.keysQueue = keysQueue;
        this.valuesQueue=valuesQueue;
    }

    public void run() {
        try {
            // Capture time before query execution
            long startTime = System.currentTimeMillis();
            while (true) {

                System.out.println("***************KEYS QUEUE SIZE************** " + keysQueue.size());

                // Remove the key from shared key queue and process
                String key = keysQueue.take();
                bulkReadCBCCatalogUseKeys(cluster, key);

                if (keysQueue.size() < 1) {
                    System.out.println("***********TERMINATE*************KEYS QUEUE SIZE************** " + keysQueue.size());
                    // Stop thread execution once the queue is exhausted
                    break;
                }
            }
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("&&&&&&&&&&&&&&&& ------------- KEYS CONSUMER THREAD EXECUTION COMPLETE ------------- &&&&&&&&&&&&&&&&&"+ Thread.currentThread().getName()
                    + "TIME TAKEN: " +totalTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void bulkReadCBCCatalogUseKeys(Cluster cluster, String commaSeparatedKeys) {
        try {

            // Query to get documents based on the IDs constructed/fetched above
            var queryToFetchDoc = "SELECT *\n" +
                    "FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog\n" +
                    "USE KEYS [\"" +
                    commaSeparatedKeys +
                    "\"];";

            System.out.println("PRINT SQL constructed: " + queryToFetchDoc);

            // Capture time before query execution
            long startTime = System.currentTimeMillis();

            // Fetch all documents based on key
            QueryResult resultSetToFilter = cluster.query(queryToFetchDoc,
                    queryOptions().metrics(true));

            // TODO add filter logic after fetching the documents
            valuesQueue.add(commaSeparatedKeys);
         /* long networkLatency = System.currentTimeMillis() - startTime;
            System.out.println("Total TIME including Network latency in ms: " + networkLatency);
            System.out.println("Total Network latency TIME in ms: " + (networkLatency - resultSetToFilter.metaData().metrics().get().executionTime().toMillis()));*/
            System.out.println("CONSUMER Retrieving Docs TIME in ms: " + resultSetToFilter.metaData().metrics().get().executionTime().toMillis());
            System.out.println("Total Docs: " + resultSetToFilter.metaData().metrics().get().resultCount());

            System.out.println("Process completed" + resultSetToFilter.rowsAsObject().get(0));
        } catch (DocumentNotFoundException ex) {
            System.out.println("Document not found!");
        }
    }
}
