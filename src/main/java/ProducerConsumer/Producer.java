package ProducerConsumer;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class Producer extends Thread {

    // Load data in queue
    private BlockingQueue<String> keysQueue;
    private AtomicInteger offset = new AtomicInteger(0);

    private static final Cluster cluster = CouchbaseConfiguration.getInstance().getCluster();

    public Producer(BlockingQueue<String> keysQueue, AtomicInteger offset) {
        super("PRODUCER");
        this.keysQueue = keysQueue;
        this.offset = offset;
    }

    public void run() {

        while (offset.get() < 11050) {
            System.out.println("********* OFFSET **********" + offset);
            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' OFFSET " + offset + " LIMIT 1000";
            offset.addAndGet(200);
            QueryResult result = cluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(true));
            System.out.println("Retrieving Keys TIME in ms: " + result.metaData().metrics().get().executionTime().toMillis());

            // Extract IDs from result set and construct comma separated keys to pass in USE KEYS parameter
            List<String> docsToFetch = result.rowsAsObject().stream().map(s -> s.getString("id")).collect(Collectors.toList());

            String commaSeparatedKeys = String.join("\",\"", docsToFetch);
            try {
                keysQueue.put(commaSeparatedKeys);
                System.out.println(getName() + " Key added to queue " + commaSeparatedKeys);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.out.println("@@@@@@@@@ QUEUE SIZE PRODUCED @@@@@@@@ " + keysQueue.size());
        }
    }
}
