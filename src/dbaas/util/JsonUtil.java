package dbaas.util;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

public class JsonUtil {

	/**
	 * Convert a JSON string to pretty print version
	 * @param jsonString
	 * @return
	 */
	public static String toPrettyFormat(String jsonString) 
	{
	    JsonParser parser = new JsonParser();
	    com.google.gson.JsonObject json = parser.parse(jsonString).getAsJsonObject();

	    Gson gson = new GsonBuilder().setPrettyPrinting().create();
	    String prettyJson = gson.toJson(json);

	    return prettyJson;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
