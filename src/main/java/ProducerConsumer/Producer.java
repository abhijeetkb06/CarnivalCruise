package ProducerConsumer;

import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class Producer extends Thread {

    // Load data in queue
    private BlockingQueue<String> keysQueue;
    private int offset = 0;

    public Producer(BlockingQueue<String> keysQueue, int offset) {
        super("PRODUCER");
        this.keysQueue = keysQueue;
        this.offset = offset;
    }

    public void run() {

        while (offset < 10001) {
            System.out.println("********* OFFSET **********" + offset);
            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' OFFSET " + offset + " LIMIT 1000";

            QueryResult result = CouchbaseConfiguration.getInstance().getCluster().query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(false));

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
