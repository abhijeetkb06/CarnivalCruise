import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer;
import io.opentelemetry.api.OpenTelemetry;

import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;


import java.time.Duration;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class TracingCruiseSearch {

    private static final String OTEL_COLLECTOR_ENDPOINT = "http://localhost:14250";
    static String connectionString = "couchbases://cb.n-nrhqi-iwnoilok.cloud.couchbase.com";
    static String username = "Abhijeet";
    static String password = "Password@P1";
    static String bucketName = "CruiseSearch-magma";

    public static void main(String[] args) {

        enableLogging();

//        Cluster cluster = getJaegerTrace();
//        getPrometheusTrace();

        // Custom environment connection.
        Cluster cluster = Cluster.connect(connectionString, username, password);

        // Get a bucket reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Scope scope = bucket.scope("CruiseSearch");
        Collection collection = scope.collection("cbcatalog");

//        bulkReadCatalogUseCCLQuery(cluster);

//        bulkReadCatalogUseCCLReactiveQuery(cluster);

        bulkReadCBCatalogReactive(cluster, bucket, scope, collection);

//        bulkReadCBCCatalogUseKeys(cluster);

    }

    private static void enableLogging() {
        Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.FINEST);
        for(Handler h : logger.getParent().getHandlers()) {
            if(h instanceof ConsoleHandler){
                h.setLevel(Level.FINEST);
            }
        }

    }

/*    private static void getPrometheusTrace() {

        // Build the OpenTelemetry Meter
        MeterSdkProvider meterSdkProvider = OpenTelemetrySdk.getGlobalMeterProvider();
        Meter meter = meterSdkProvider.get("OpenTelemetryMetricsSample");

       // Start the Prometheus HTTP Server
        try {
            HTTPServer server = server = new HTTPServer(19090);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Register the Prometheus Collector
        PrometheusCollector.builder().setMetricProducer(meterSdkProvider.getMetricProducer()).buildAndRegister();
    }*/

    private static Cluster getJaegerTrace() {

        OpenTelemetry openTelemetry = openTelemetryDirect();
        Cluster cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password)
                .environment(env -> {
                    // Provide the OpenTelemetry object to the Couchbase SDK
                    env.requestTracer(OpenTelemetryRequestTracer.wrap(openTelemetry));
                }));

        return cluster;
    }

    public static OpenTelemetry openTelemetryDirect() {

        // Set the OpenTelemetry SDK's SdkTracerProvider
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .setResource(Resource.getDefault()
                        .merge(Resource.builder()
                                // An OpenTelemetry service name generally reflects the name of your app,
                                // e.g. "shopping-cart-service"
                                .put("service.name", "CCL_METRICS")
                                .build()))
                .addSpanProcessor(BatchSpanProcessor.builder(OtlpHttpSpanExporter.builder()
                        .setEndpoint(OTEL_COLLECTOR_ENDPOINT)
                        .build()).build())
                .setSampler(Sampler.alwaysOn())
                .build();

        // Set the OpenTelemetry SDK's OpenTelemetry
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .buildAndRegisterGlobal();
        return openTelemetry;
    }

    private static void bulkReadCBCCatalogUseKeys(Cluster cluster) {
        try {

            // Query to get random keys based on limit set
            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' limit 11022";

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

            // Fetch all documents based on key
            QueryResult resultSetToFilter = cluster.query(queryToFetchDoc,
                    queryOptions().metrics(true));

            long networkLatency = System.currentTimeMillis() - startTime;
           /* System.out.println("Total TIME including Network latency in ms: " + networkLatency);
            System.out.println("Total Network latency TIME in ms: " + (networkLatency - resultSetToFilter.metaData().metrics().get().executionTime().toMillis()));*/
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

            var query = "SELECT meta(c).id FROM `CruiseSearch-magma`.`CruiseSearch`.cbcatalog c WHERE meta(c).id like '%0%' limit 11022";

            QueryResult result = cluster.query(query,
                    queryOptions().adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(false));
            List<String> docsToFetch = result.rowsAsObject().stream().map(s -> s.getString("id")).collect(Collectors.toList());

            long startTime = System.currentTimeMillis();

          /*  List<GetResult> results = Flux.fromIterable(docsToFetch)
                    .flatMap(key -> reactiveCollection.get(key, GetOptions.getOptions().transcoder(RawStringTranscoder.INSTANCE)).onErrorResume(e -> Mono.empty())).collectList().block();
*/
           /* List<GetResult> results = Flux.fromIterable(docsToFetch)
                    .flatMap(key -> reactiveCollection.get(key, GetOptions.getOptions().transcoder(RawStringTranscoder.INSTANCE)).onErrorResume(e -> Mono.empty())).collectList().block();
*/

            //If you want to set a parent for a SDK request, you can do it in the respective *Options:
            //getOptions().parentSpan(OpenTelemetryRequestSpan.wrap(parentSpan))

            /*Span parentSpan = getTracer(openTelemetry).spanBuilder("parentSpan").setNoParent().startSpan();
            System.out.println("In parent method. TraceID : {}"+ parentSpan.getSpanContext().getTraceId());*/

            // Perform bulk read by controlling number of threads in parallel function
            List<GetResult> results = Flux.fromIterable(docsToFetch)
                    .parallel(10)
                    .runOn(Schedulers.boundedElastic())
                    .flatMap(key -> reactiveCollection.get(key, GetOptions.getOptions().transcoder(RawStringTranscoder.INSTANCE))
                            .onErrorResume(e -> Mono.empty()))
                    .sequential()
                    .collectList()
                    .block();


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
         /*   System.out.println("Total TIME including Network latency in ms: " + networkLatency);
            System.out.println("Total Network latency TIME in ms: " + (networkLatency - result.metaData().metrics().get().executionTime().toMillis()));*/
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
                    queryOptions().pipelineBatch(10).adhoc(false).maxParallelism(4).scanConsistency(QueryScanConsistency.NOT_BOUNDED).metrics(true));

            System.out.println("Retrieving Keys TIME in ms: " + result.metrics().block().metaData().metrics().block().metrics().get().executionTime().toMillis());
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
