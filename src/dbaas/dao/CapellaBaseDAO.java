package dbaas.dao;



import com.couchbase.client.core.env.CertificateAuthenticator;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.security.KeyStore;


import java.time.Duration;
import java.util.Optional;

public class CapellaBaseDAO {

	private static Cluster cluster = null;

	//TODO - Change them before running the main method
	protected static String endpoint = "ec2.us-west-2.compute.amazonaws.com";
	protected static String bucketName = "<bucket-name>";
	private static String username = "<db-username>";
	private static String password = "<password>";

	private static String certStorePath = "<ca.pem location>";
	private static String keystorePath = "<keystore-path>";
	private static String keystorePass = "<keystore-pass>";


	/**
	 * This method authenticate using RBAC user credetials and uses root certificate
	 * ca.pem stored under a file path to securily connect to the remote 
	 * Couchbase Cluster.
	 * @param endpoint - couchbase cluster endpoint which should start with couchbases://
	 * @param username - db user name with R/W access granted to the bucket
	 * @param password - db user password
	 * @return Cluster object
	 */
	public static synchronized Cluster getClusterFromRootCert(String endpoint, String username, String password) {
		//singleton pattern on Cluster object
		if(cluster == null){
			cluster = Cluster.connect(
					endpoint,
					ClusterOptions.clusterOptions(username, password)
					.environment(env -> env
							.securityConfig(security -> security
									.trustCertificate(Paths.get(certStorePath))
									)
							)
					);
		}
		return cluster;
	}//getClusterFromPem()


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

	public void runCRUD(Cluster cluster, String bucketName) {
		if(cluster!=null) {

			Bucket bucket = cluster.bucket(bucketName);
			bucket.waitUntilReady(Duration.parse("PT5S"));	
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

	public static void main(String[] args) {

		//		Cluster cluster = CapellaBaseDAO.getClusterFromKeyStore(endpoint, username, password, keystorePath, keystorePass);
		//		Cluster cluster = CapellaBaseDAO.getClusterFromRootCert(endpoint, username, password);
		Cluster cluster = CapellaBaseDAO.getClusterFromClientCert(endpoint, keystorePath, keystorePass);
		CapellaBaseDAO baseDAO = new CapellaBaseDAO();
		baseDAO.runCRUD(cluster, bucketName);


	}//main()

}//CapellaBaseDAO


