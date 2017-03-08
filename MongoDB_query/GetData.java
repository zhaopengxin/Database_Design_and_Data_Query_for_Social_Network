import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Vector;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{
	
    static String prefix = "tajik.";
	
    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;
	
    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray

	
    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }
	
	
	
	
    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{ 
		
	// Your implementation goes here....
    JSONArray users_info = new JSONArray();
    try(Statement stat =oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
    		ResultSet.CONCUR_READ_ONLY)){
    	String query = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME, U.GENDER, U.YEAR_OF_BIRTH, "
    		+"U.MONTH_OF_BIRTH, U.DAY_OF_BIRTH, C.CITY_NAME, C.STATE_NAME, C.COUNTRY_NAME "
    			+"FROM "+userTableName+" U, "+ hometownCityTableName+" H, "+ cityTableName + " C WHERE "
    			+ "U.USER_ID=H.USER_ID AND H.HOMETOWN_CITY_ID=C.CITY_ID";
    	System.out.println(query);
    	ResultSet result = stat.executeQuery(query);
    	result = stat.executeQuery(query);
    	while(result.next()){
    		long user_id = result.getLong(1);
    		JSONObject user = new JSONObject();
    		JSONObject hometown = new JSONObject();
    		ArrayList<Long> friends = new ArrayList<Long>();
    		try(Statement stat2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
    				ResultSet.CONCUR_READ_ONLY)){
    			String friendQuery = "SELECT USER2_ID FROM (SELECT DISTINCT USER1_ID, USER2_ID FROM"
    				+" (SELECT USER1_ID, USER2_ID FROM "+friendsTableName+" UNION SELECT USER2_ID, "
    					+"USER1_ID FROM "+friendsTableName+")) WHERE USER1_ID = "+user_id+" and USER1_ID<USER2_ID";
    			ResultSet frResult = stat2.executeQuery(friendQuery);
    			while(frResult.next()){
    				friends.add(frResult.getLong(1));
    			}
    		}
    		Long[] friendList = new Long[friends.size()];
    		friendList = friends.toArray(friendList);
    		user.put("user_id", result.getLong(1));
    		user.put("first_name", result.getString(2));
    		user.put("last_name", result.getString(3));
    		user.put("gender", result.getString(4));
    		user.put("YOB", result.getInt(5));
    		user.put("MOB", result.getInt(6));
    		user.put("DOB", result.getInt(7));
    		hometown.put("city", result.getString(8));
    		hometown.put("state", result.getString(9));
    		hometown.put("country", result.getString(10));
    		user.put("hometown", hometown);
    		user.put("friends", friends);
    		users_info.put(user);
    	}//end query iteration
    } catch (SQLException err) {
        System.err.println(err.getMessage());
    }
		
		
//	// This is an example usage of JSONArray and JSONObject
//	// The array contains a list of objects
//	// All user information should be stored in the JSONArray object: users_info
//	// You will need to DELETE this stuff. This is just an example.
//
//	// A JSONObject is an unordered collection of name/value pairs. Add a few name/value pairs.
//	JSONObject test = new JSONObject();	// declare a new JSONObject
//	// A JSONArray consists of multiple JSONObjects. 
//	JSONArray users_info = new JSONArray();
//
//	test.put("user_id", "testid");		// populate the JSONObject
//	test.put("first_name", "testname");
//
//	JSONObject test2 = new JSONObject();
//	test2.put("user_id", "test2id");
//	test2.put("first_name", "test2name");
//
//	// users_info.add(test);			// add the JSONObject to JSONArray	
//	// users_info.add(test2);			// add the JSONObject to JSONArray	
//
//	// Use put method if using the alternate JSON modules.
//	users_info.put(test);		// add the JSONObject to JSONArray     
//	users_info.put(test2);		// add the JSONObject to JSONArray	
	return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
		
    }
}

