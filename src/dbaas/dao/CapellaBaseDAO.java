package dbaas.dao;



import com.couchbase.client.core.env.CertificateAuthenticator;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.manager.query.BuildQueryIndexOptions;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.json.*;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.security.KeyStore;


import java.time.Duration;
import java.util.Hashtable;
import java.util.Optional;

public class CapellaBaseDAO {

	private static Cluster cluster = null;

	//TODO - Change them before running the main method
	protected static String endpoint = "34.222.138.192";
	protected static String bucketName = "demo";
	private static String username = "testdb";
	private static String password = "password";

	private static String certPath = "/Users/anuj.sahni/couchbase/certs/demo-ca.pem";
	private static String keystorePath = "<keystore-path>";
	private static String keystorePass = "<keystore-pass>";

	private static Hashtable<String, Bucket> buckets = new Hashtable<String, Bucket>();

	/**
	 * This method authenticate using RBAC user credetials and uses root certificate
	 * ca.pem stored under a file path to securily connect to the remote 
	 * Couchbase Cluster.
	 * @param endpoint - couchbase cluster endpoint which should start with couchbases://
	 * @param username - db user name with R/W access granted to the bucket
	 * @param password - db user password
	 * @return Cluster object
	 */
	public static synchronized Cluster getClusterFromRootCert(String endpoint, 
			String username, String password, String certPath) {
		//singleton pattern on Cluster object
		if(cluster == null){
			System.out.println("Inside getClusterFromRootCert() method, where it "
					+ "will use username/password and certPath will be used to "
					+ "connect securely.");
			cluster = Cluster.connect(
					endpoint,
					ClusterOptions.clusterOptions(username, password)
					.environment(env -> env
							.securityConfig(security -> security
									.trustCertificate(Paths.get(certPath))
									)
							)
					);
		}
		return cluster;
	}//getClusterFromRootCert()


	/**
	 * This method authenticate using RBAC user credetials and uses root certificate
	 * ca.pem stored in the kestore to securily connect to the remote 
	 * Couchbase Cluster.
	 * @param endpoint - couchbase cluster endpoint which should start with couchbases://
	 * @param username - db user name with R/W access granted to the bucket
	 * @param password - db user password
	 * @return Cluster object
	 */
	public static synchronized Cluster getClusterFromKeyStore(String endpoint, 
			String username, String password, String keyStorePath, String keystorePass) 
	{
		//singleton pattern on Cluster object
		if(cluster == null){
			System.out.println("Inside keystore method, where it will use username/password and keystore to connect securely.");
			try {
				KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
				keyStore.load(new FileInputStream(keyStorePath), keystorePass.toCharArray());
				System.out.println("Keystore loaded successfully and found " + keyStore.size() + " certificates.");

				cluster = Cluster.connect(
						endpoint,
						ClusterOptions.clusterOptions(username, password)
						.environment(env -> env
								.securityConfig(security -> security
										.trustStore(Paths.get(keyStorePath), keystorePass, Optional.empty())
										)
								)
						);
			}catch(Exception e) {
				System.err.println(e.getMessage());
			}
		}
		return cluster;
	}//getClusterFromKeyStore()


	/**
	 * This method takes only the cluster endpoint as the input and hard code the 
	 * keystore location that should have the cleintCertificate in it.
	 * @param endpoint - couchbase cluster endpoint which should start with couchbases://
	 * @return Cluster object
	 */
	public static synchronized Cluster getClusterFromClientCert(String endpoint,
			String keyStorePath, String keystorePass) 
	{

		//singleton pattern on Cluster object
		if(cluster == null){
			System.out.println("Inside client-certificate method.");
			try {
				KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
				keyStore.load(new FileInputStream(keyStorePath), keystorePass.toCharArray());

				System.out.println("Keystore loaded successfully and found " + keyStore.size() + " certificates.");
				//				System.out.println( keyStore.getCertificate("selfsigned"));

				CertificateAuthenticator authenticator = CertificateAuthenticator.fromKeyStore(keyStore, keystorePass);
				cluster = Cluster.connect(endpoint, ClusterOptions.clusterOptions(authenticator)
						.environment(env -> env
								.securityConfig(security -> security
										.trustStore(Paths.get(keyStorePath), keystorePass, Optional.empty())
										)
								)
						);

			}catch(Exception e) {
				System.err.println(e.getMessage());
			}
		}
		return cluster;
	}//getClusterFromClientCert()



	/**
	 * This method writes the JsonObject object into the collection
	 * @param collection - Couchbase bucket.scope.collection
	 * @param jsonObj - Json Object representing the document to be persisted 
	 * into the collection.
	 * @param key - Primary key of the document that would be used to persist
	 * the document
	 */
	public static void upsertDoc(Collection collection, JsonObject jsonObj, String key) {
		if(collection!=null) {
			try {
				//Store the Document
				collection.upsert(key, jsonObj);
				System.out.println("UPSERT: \tSuccessful (Added doc by key: `" + key + "`)" );
			}catch(Exception e) {
				System.out.println("UPSERT: \tFailed");
				throw e;
			}
		}
	}//upsertDoc

	/**
	 * This method fetches document when key is passed
	 * @param collection - bucket.scope.collection object to be used to fetch
	 * @param key - Document primary key
	 * @return JsonObject
	 */
	public static JsonObject getDoc(Collection collection, String key) {
		GetResult getResult = null;
		if(collection!=null) {
			try {
				//get Document
				getResult = collection.get(key);
				System.out.println("GET: \t\tSuccessful (" + getResult.contentAsObject().toString() + ")");
			}catch(DocumentNotFoundException dnfe) {
				System.out.println("GET: \t\tFailed (no document exist by the key: `" + key + "`)");
				throw dnfe;
			}catch(Exception e) {
				System.out.println("GET: \t\tFailed");
				throw e;
			}
		}//EOF if
		return getResult.contentAsObject();
	}//getDoc

	public static void deleteDoc(Collection collection, String key) {
		if(collection!=null) {
			try {
				//delete Document by primary key
				collection.remove(key);
				System.out.println("DELETE: \tSuccessful (Removed doc by key: `" + key + "`)");
			}catch(DocumentNotFoundException dnfe) {
				System.out.println("DELETE: \tFailed (no document exist by the key: `" + key + "`)");
				throw dnfe;
			}catch(Exception e) {
				System.out.println("DELETE: \tFailed");
				throw e;
			}
		}//EOF if
	}

	

	public static Bucket getBucket(Cluster cluster, String bucketName) {

		Bucket bucket = null;
		if(buckets.containsKey(bucketName)) {
			bucket = buckets.get(bucketName);
		}else {
			bucket = cluster.bucket(bucketName);
			bucket.waitUntilReady(Duration.parse("PT5S"));
		}
		return bucket;
	}//getBucket
	
	/**
	 *  This method returns Collection object 
	 * @param cluster - Cluster object
	 * @param bucketName - bucketName
	 * @param scopeName - scopeName
	 * @param collectionName - collectionName
	 * @return Collection
	 */
	public static Collection getCollection(Cluster cluster, String bucketName, String scopeName, String collectionName) {		
		Collection collection = getBucket(cluster, bucketName).scope(scopeName).collection(collectionName);
		return collection;
	}
	
	
	public void runCRUD(Cluster cluster, String bucketName) {
		if(cluster!=null) {

			Bucket bucket = getBucket(cluster, bucketName);

			Collection collection = bucket.defaultCollection();

			// Create a JSON Document
			JsonObject jsonObj = JsonObject.create()
					.put("name", "Arthur")
					.put("email", "kingarthur@couchbase.com")
					.put("interests", JsonArray.from("Holy Grail", "African Swallows"));


			String key = "u:king_arthur";

			try {
				upsertDoc(collection, jsonObj, key);
				getDoc(collection, key);
				deleteDoc(collection, key);

			}catch(Exception e) {
				//TODO - Add a operation on error
				//System.err.println(e.getMessage());
			}

		}//EOF if
	}//runCRUD

	/**
	 *  Helper method to show how indexes can be created in deffered mode
	 *  and later all build as a single go.
	 * @param cluster - Couchbase cluster object 
	 * @param collection - Colleciton object where defferred indexes need to be 
	 * created.
	 */
	public void createIndex(Cluster cluster, Collection collection) {
		try {
			QueryResult queryResult = null;
			// First index DDL
			String indexDDL = "CREATE INDEX `idx_profile_females` "
					+ "ON `demo`.`usa`.`profile`(UPPER(`gender`), `firstName`) "
					+ "WHERE UPPER(`gender`)='F' "
					+ "	WITH { 'defer_build':true}";
			//Create a deferred index
			queryResult = cluster.query(indexDDL);
			
			// Second index DDL
			indexDDL = "CREATE INDEX `idx_profile_males` "
					+ "ON `demo`.`usa`.`profile`(UPPER(`gender`), `firstName`) "
					+ "WHERE UPPER(`gender`)='M' "
					+ "	WITH { 'defer_build':true}";
			
			//Create second deferred index
			queryResult = cluster.query(indexDDL);
			
			if(queryResult!=null) {
				System.out.print("Index created: ");
				// Build any deferred indexes within `bucketName`.scopeName.collectionName
				collection.queryIndexes().buildDeferredIndexes();
			}

		} catch (IndexExistsException e) {
			System.out.println("Index already exists: " + e.getMessage());
		}
	}//createIndex
	
	
	public static void main(String[] args) {

		//		Cluster cluster = CapellaBaseDAO.getClusterFromKeyStore(endpoint, username, password, keystorePath, keystorePass);
		Cluster cluster = CapellaBaseDAO.getClusterFromRootCert(endpoint, username, password, certPath);
		//		Cluster cluster = CapellaBaseDAO.getClusterFromClientCert(endpoint, keystorePath, keystorePass);
		
		
		CapellaBaseDAO baseDAO = new CapellaBaseDAO();
		baseDAO.runCRUD(cluster, bucketName);
		
		//Collection where index need to be defined
		Collection collection = CapellaBaseDAO.getCollection(cluster, "demo", "usa", "profile");
		baseDAO.createIndex(cluster, collection);


	}//main()

}//CapellaBaseDAO


