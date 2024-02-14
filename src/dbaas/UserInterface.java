package dbaas;

import java.util.Hashtable;

import com.couchbase.client.core.deps.io.netty.util.internal.StringUtil;
import com.couchbase.client.java.Cluster;

import dbaas.dao.CapellaBaseDAO;

public class UserInterface {


	public static void help() {

		System.out.println("\n$java -jar bootstrap.jar [OPTION] ...\n\n"
				+ " This program will connect to Couchbase cluster via cluster \n"
				+ "(public/private) endpoint and other required argument to \n"
				+ "perform single document UPSERT followed by READ operation. \n"
				+ "If you see an output that means you can reach the cluster\n "
				+ "and perform CRUD operation on it.\n\n"

				+ " Options: \n"
				+ "-e, --endpoint			Couchbase cluster (IP or public or private endpoint)\n"
				+ "-c, --certpath			Path where the Capella certificate is downloaded (/path/to/ca.pem)\n"
				+ "-b, --bucket			Bucket name to use, where Database user can perform R/W operation\n"		 
				+ "-u, --username			Database username with bucket R/W access \n"
				+ "-p, --password			Database username password \n"
				+ "-s, --storepath			(optional) Keystore path where the certificates are imported (/path/to/my.keystore)\n"
				+ "-k, --storepass			(optional) Keystore password \n"
				+ "-h, --help			Usage help\n"

				+ "");
	}

	/**
	 * Assert that required options are there in the arguments
	 * @param ht - Hashtable with key/value inputs
	 */
	public static void validate(Hashtable<String, String> ht) {

		//display usage if -h or --help is present
		if((ht.containsKey("-h") || ht.containsKey("--help"))) {
			help();
			System.exit(0);
		}

		//make sure endpoint is mentioned
		if(!(ht.containsKey("-e") || ht.containsKey("--endpoint"))) {
			System.out.println("Couchbase cluster endpoint is a required field (-e, --endpoint). Try again.\n");
			System.exit(0);
		}
		//make sure either certPat or keystore path is mentioned
		if((!(ht.containsKey("-c") || ht.containsKey("--certpath"))) && 
				(!(ht.containsKey("-s") || ht.containsKey("--storepath")))) {
			System.out.println("Either use -c or -s option to securly connect to Capella");
			System.exit(0);
		}


		//make sure keystore password is mentioned when -s or --storepath option is used
		if(((ht.containsKey("-s") || ht.containsKey("--storepath"))) && 
				!(ht.containsKey("-k") || ht.containsKey("--storepass"))) {
			System.out.println("Keystore password is a required when keystore is used (-k, --storepass). Try again.\n");
			System.exit(0);
		}

		//make sure bucket name is mentioned
		if(!(ht.containsKey("-b") || ht.containsKey("--bucket"))) {
			System.out.println("Bucket name is required field (-b, --bucket). Try again.\n");
			System.exit(0);
		}


	}
	public static void main(String[] args) {

		Hashtable<String, String> ht = new Hashtable<String, String>();

		try {
			int size = args.length;
			String key = null;
			String value = null;

			if(size==0) {
				help();
				System.exit(0);
			}

			//get all input arguments
			for(int i = 0; i < size; i++) { 
				if(i % 2 == 0) {
					key = args[i].trim();
					//this will make sure keys without values are stored as well
					ht.put(key, "");
				}else {
					value =  args[i].trim();
					ht.put(key, value);
				}

			}//EOF for

			//Validate input arguments
			validate(ht);

			String endpoint = ht.get("-e");
			endpoint = StringUtil.isNullOrEmpty(endpoint)?ht.get("--endpoint"):endpoint;
			//check if couchbases:// exist as part of the endpoint
			int index = endpoint.indexOf("couchbases://");
			if(index<0) {
				endpoint="couchbases://"+endpoint;
			}
			
			String certPath = ht.get("-c");
			certPath = StringUtil.isNullOrEmpty(certPath)?ht.get("--certpath"):certPath;

			String keystorePath = ht.get("-s");
			keystorePath = StringUtil.isNullOrEmpty(keystorePath)?ht.get("--storepath"):keystorePath;

			String keystorePassword = ht.get("-k");
			keystorePassword = StringUtil.isNullOrEmpty(keystorePassword)?ht.get("--storepass"):keystorePassword;

			String username = ht.get("-u");
			username = StringUtil.isNullOrEmpty(username)?ht.get("--username"):username;

			String password = ht.get("-p");
			password = StringUtil.isNullOrEmpty(password)?ht.get("--password"):password;

			String bucket = ht.get("-b");
			bucket = StringUtil.isNullOrEmpty(bucket)?ht.get("--bucket"):bucket;

			System.out.println("endpoint: " + endpoint + " bucket: " + bucket + 
					" keystorePath: " + keystorePath + " keystorePassword: " + 
					keystorePassword 
					+ " username: " + username + " password: " + password + " certpath: " + certPath);

			Cluster cluster = null;
			//Now based on input arguments decide which method to use
			if(!StringUtil.isNullOrEmpty(certPath)) {
				cluster = CapellaBaseDAO.getClusterFromRootCert(endpoint, username, password, certPath);
			}else if(StringUtil.isNullOrEmpty(username) || StringUtil.isNullOrEmpty(password)) {
				cluster = CapellaBaseDAO.getClusterFromClientCert(endpoint, keystorePath, keystorePassword);
			}else {
				cluster = CapellaBaseDAO.getClusterFromKeyStore(endpoint, username, password, keystorePath, keystorePassword);
			}
			//run CRUD using cluster object
			CapellaBaseDAO baseDAO = new CapellaBaseDAO();
			baseDAO.runCRUD(cluster, bucket);

		}catch (Exception e) {
			e.printStackTrace();
		}



	}//main

}
