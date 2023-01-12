import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class CruiseSearch {
    static String connectionString = "couchbases://cb.n-nrhqi-iwnoilok.cloud.couchbase.com";
    static String username = "Abhijeet";
    static String password = "Password@P1";
    static String bucketName = "CruiseSearch-magma";

    public static void main(String[] args) {

        // Custom environment connection.
        Cluster cluster = Cluster.connect(connectionString, username, password);

        // Get a bucket reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Scope scope = bucket.scope("CruiseSearch");
        Collection collection = scope.collection("cbcatalog");

        bulkReadCatalogUseCCLQuery(cluster);

//        bulkReadCatalogUseCCLReactiveQuery(cluster);

//        bulkReadCBCatalogReactive(cluster, bucket, scope, collection);

//        bulkReadCBCCatalogUseKeys(cluster, bucket, scope, collection);

    }



    private static void bulkReadCBCCatalogUseKeys(Cluster cluster, Bucket bucket, Scope scope, Collection collection) {
        try {

            ReactiveCluster reactiveCluster = cluster.reactive();
            ReactiveBucket reactiveBucket = bucket.reactive();
            ReactiveScope reactiveScope = scope.reactive();
            ReactiveCollection reactiveCollection = collection.reactive();

            // Query to get random keys based on limit set
            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' limit 5000";

            QueryResult result = cluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(false));

            // Extract IDs from result set and construct comma separated keys to pass in USE KEYS parameter
            List<String> docsToFetch = result.rowsAsObject().stream().map(s -> s.getString("id")).collect(Collectors.toList());
            String commaSeparatedKeys = String.join("\",\"", docsToFetch);

            // Query to get documents based on the IDs constructed/fetched above
            var queryToFetchDoc = "SELECT *\n" +
                    "FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog\n" +
                    "USE KEYS [\"" +
                    commaSeparatedKeys +
                    "\"];";

            System.out.println("PRINT SQL constructed: " + queryToFetchDoc);

            // Capture time before query execution
            long startTime = System.currentTimeMillis();

            QueryResult resultSetToFilter = cluster.query(queryToFetchDoc,
                    queryOptions().metrics(true));

            long networkLatency = System.currentTimeMillis() - startTime;
            System.out.println("Total TIME including Network latency in ms: " + networkLatency);
            System.out.println("Total Network latency TIME in ms: " + (networkLatency - resultSetToFilter.metaData().metrics().get().executionTime().toMillis()));
            System.out.println("Retrieving Docs TIME in ms: " + resultSetToFilter.metaData().metrics().get().executionTime().toMillis());
            System.out.println("Total Docs: " + resultSetToFilter.metaData().metrics().get().resultCount());

            System.out.println("Process completed" + resultSetToFilter.rowsAsObject().get(0));
        } catch (DocumentNotFoundException ex) {
            System.out.println("Document not found!");
        }

/*            GetResult getResult = cbCatalogCol.get("CAD::ORE     ::25714::RS::~::2022-12-03");
            String sailingId = getResult.contentAsObject().getString("SAILING_ID");
            System.out.println(sailingId);*/
    }

    private static void bulkReadCBCatalogReactive(Cluster cluster, Bucket bucket, Scope scope, Collection collection) {
        try {

            ReactiveCluster reactiveCluster = cluster.reactive();
            ReactiveBucket reactiveBucket = bucket.reactive();
            ReactiveScope reactiveScope = scope.reactive();
            ReactiveCollection reactiveCollection = collection.reactive();

            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' limit 5000";

            QueryResult result = cluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(false));
            List<String> docsToFetch = result.rowsAsObject().stream().map(s -> s.getString("id")).collect(Collectors.toList());

            long startTime = System.currentTimeMillis();
            System.out.println("START TIME: " + startTime);
            List<GetResult> results = Flux.fromIterable(docsToFetch)
                    .flatMap(key -> reactiveCollection.get(key, GetOptions.getOptions().transcoder(RawStringTranscoder.INSTANCE)).onErrorResume(e -> Mono.empty())).collectList().block();

            long networkLatency = System.currentTimeMillis() - startTime;
            System.out.println("Total TIME including Network latency in ms: " + networkLatency);

            System.out.println("Total Docs: " + results.size());

            String returned = results.get(0).contentAs(String.class);
            System.out.println("Done" + returned);
        } catch (DocumentNotFoundException ex) {
            System.out.println("Document not found!");
        }
    }

    private static void bulkReadCatalogUseCCLQuery(Cluster cluster) {
        try {
            var query =
                    "SELECT MIN([t.DBL_CABIN_TOTAL,META(t).id])[1]\n" +
                            "FROM `CruiseSearch-magma`.`CruiseSearch`.catalog AS t --USE INDEX (ix23 USING GSI)\n" +
                            "WHERE QUALIFICATION_CODE = \"~\"\n" +
                            "    AND COUPON_CODE = \"~\"\n" +
                            "    AND LIST_REQUIRED_ENTITY_TYPE = \"RTE\"\n" +
                            "    AND DBL_LOWEST_FLAG = 1\n" +
                            "    AND SAIL_DATE BETWEEN \"2021-01-29\" AND \"2025-12-30\"\n" +
                            "     AND t.SAILING_ID IS NOT NULL\n" +
                            "     AND t.META_CODE IS NOT NULL\n" +
                            "GROUP BY t.SAILING_ID,\n" +
                            "         t.META_CODE;";
            long startTime = System.currentTimeMillis();
            System.out.println("START TIME: " + startTime);

            QueryResult result = cluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(true));

            long networkLatency = System.currentTimeMillis() - startTime;
            System.out.println("Total TIME including Network latency in ms: " + networkLatency);
            System.out.println("Total Network latency TIME in ms: " + (networkLatency - result.metaData().metrics().get().executionTime().toMillis()));
            System.out.println("Retrieving Keys TIME in ms: " + result.metaData().metrics().get().executionTime().toMillis());
            System.out.println("Total Docs: " + result.metaData().metrics().get().resultCount());
/*            System.out.println("***Query Executed***" );
            result.rowsAsObject().stream().forEach(
                    e-> System.out.println(
                            "SAILING_ID: "+e.getString("SAILING_ID")+", "+e.getString("META_CODE"))
            );*/

        } catch (CouchbaseException ex) {
            System.out.println("Exception: " + ex.toString());
        }
    }

    private static void bulkReadCatalogUseCCLReactiveQuery(Cluster cluster) {

        try {
            ReactiveCluster reactiveCluster = cluster.reactive();

            var query =
                    "SELECT RAW MIN([t.DBL_CABIN_TOTAL,META(t).id])[1]\n" +
                            "FROM `CruiseSearch-magma`.`CruiseSearch`.catalog AS t --USE INDEX (ix23 USING GSI)\n" +
                            "WHERE QUALIFICATION_CODE = \"~\"\n" +
                            "    AND COUPON_CODE = \"~\"\n" +
                            "    AND LIST_REQUIRED_ENTITY_TYPE = \"RTE\"\n" +
                            "    AND DBL_LOWEST_FLAG = 1\n" +
                            "    AND SAIL_DATE BETWEEN \"2021-01-29\" AND \"2025-12-30\"\n" +
                            "     AND t.SAILING_ID IS NOT NULL\n" +
                            "     AND t.META_CODE IS NOT NULL\n" +
                            "GROUP BY t.SAILING_ID,\n" +
                            "         t.META_CODE;";

            Mono<ReactiveQueryResult> result = reactiveCluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(true));

            System.out.println("Retrieving Keys TIME in ms: " +   result.metrics().block().metaData().metrics().block().metrics().get().executionTime().toMillis());
            System.out.println("Total Docs: " + result.metrics().block().metaData().metrics().block().metrics().get().resultCount());

//            result.flatMapMany(ReactiveQueryResult::rowsAsObject).subscribe(row -> System.out.println("Found row: " + row.size()));

/*            System.out.println("***Query Executed***" );
            result.rowsAsObject().stream().forEach(
                    e-> System.out.println(
                            "SAILING_ID: "+e.getString("SAILING_ID")+", "+e.getString("META_CODE"))
            );*/

        } catch (CouchbaseException ex) {
            System.out.println("Exception: " + ex.toString());
        }
    }
}
