package ProducerConsumer;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

import java.time.Duration;

public class CouchbaseConfiguration {

	static String connectionString = "couchbases://cb.n-nrhqi-iwnoilok.cloud.couchbase.com";
	static String username = "Abhijeet";
	static String password = "Password@P1";
	static String bucketName = "CruiseSearch-magma";

	static String scopeName = "CruiseSearch";

	static String collectionName = "cbcatalog";

	static Cluster cluster = null;
	static Bucket bucket = null;
	static Scope scope = null;
	static Collection collection = null;
	private static final CouchbaseConfiguration couchbaseInstance = null;

    private CouchbaseConfiguration() {
		// Custom environment connection.
		cluster = Cluster.connect(connectionString, username, password);

		// Get a bucket reference
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(Duration.ofSeconds(10));
		scope = bucket.scope(scopeName);
		collection = scope.collection(collectionName);
	}

    public static CouchbaseConfiguration getInstance() {
        if (couchbaseInstance == null) {
			return new CouchbaseConfiguration();
		}
		return couchbaseInstance;
    }
    
    public static Cluster getCluster() {
		return cluster;
	}

	public static Bucket getBucket() {
		return bucket;
	}

	public static Scope getScope() {
		return scope;
	}

	public static Collection getCollection() {
		return collection;
	}
}
