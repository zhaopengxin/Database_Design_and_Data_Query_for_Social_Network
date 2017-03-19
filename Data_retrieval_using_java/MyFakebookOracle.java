package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import project2.FakebookOracle.UserInfo;
import project2.FakebookOracle.UsersPair;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "tajik.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
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


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //159
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
    	ArrayList<String> longestFirstNameString=new ArrayList<String>();
    	ArrayList<String> shortestFirstNameString=new ArrayList<String>();
    	ArrayList<String> commonFirstNameString=new ArrayList<String>();    	
    	try(Statement stat=oracleConnection.createStatement()){
    		String query=new String();
    		query="select FIRST_NAME from "+userTableName;
    		ResultSet resultSet=stat.executeQuery(query);
    		ArrayList<String> first_name_list=new ArrayList<String>();
    		while(resultSet.next()){
    			first_name_list.add(resultSet.getString(1));//resultSet.getString("first_name");
    		}
    		longestFirstNameString=findLongestFirstName(first_name_list);
    		shortestFirstNameString=findShortestFirstName(first_name_list);
    		commonFirstNameString=findCommonFirstName(first_name_list);
    		resultSet.close();
    		stat.close();
    		
    		
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
    		System.err.println(e.getMessage());
		}
        for(String longName:longestFirstNameString){
        	this.longestFirstNames.add(longName);
        }
        for(String shortName:shortestFirstNameString){
        	this.shortestFirstNames.add(shortName);
        }
        for(String commonName:commonFirstNameString){
        this.mostCommonFirstNames.add(commonName);
        }
        //this.mostCommonFirstNamesCount = 10;
    }

    private ArrayList<String> findCommonFirstName(ArrayList<String> first_name_list) {
		// TODO Auto-generated method stub
    	ArrayList<String> commonName=new ArrayList<String>();
    	Map<String,Integer> nameList=new HashMap<String,Integer>();
    	for(String name:first_name_list){
    		int frequency=Collections.frequency(first_name_list, name);
    		nameList.put(name, frequency);
    	}
    	int maxNum=0;
    	for(String key:nameList.keySet()){
    		if(nameList.get(key)>=maxNum){
    			maxNum=nameList.get(key);
    		}   		
    	}
    	this.mostCommonFirstNamesCount = maxNum;
    	for(String key:nameList.keySet()){
    		if(nameList.get(key)==maxNum){
    			commonName.add(key);
    		}
    	}
		return commonName;
	}


	private ArrayList<String> findShortestFirstName(ArrayList<String> name_list) {
		// TODO Auto-generated method stub
    	ArrayList<String> minLengthName=new ArrayList<String>();
    	int minLength=name_list.get(0).length();
    	for(String name:name_list){								//find the min length of name
    		if(name.length()<=minLength){
    			minLength=name.length();
    		}
    	}
    	for(String name:name_list){								//when the length of name equals minLength, then add that
    		if(name.length()==minLength){
    			minLengthName.add(name);
    		}
    	}
		return minLengthName;
	}


	private  ArrayList<String> findLongestFirstName(ArrayList<String> name_list) {
		// TODO Auto-generated method stub
    	int maxLength=0;
    	ArrayList<String> maxLengthName=new ArrayList<String>();
    	for(String name:name_list){
    		if(name.length()>=maxLength){
    			maxLength=name.length();
    		}
    	}
    	for(String name:name_list){								//when the length of name equals maxLength, then add that
    		if(name.length()==maxLength){
    			maxLengthName.add(name);
    		}
    	}
		return maxLengthName;
	}
    


	@Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
		try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
			String query="select distinct U.USER_ID, U.FIRST_NAME, U.LAST_NAME from "+userTableName+" U "
					+ "where U.USER_ID =(select USER_ID from "+userTableName+" minus (select USER1_ID from "+friendsTableName+" UNION select USER2_ID from "+friendsTableName+"))";
			ResultSet resultSet=stat.executeQuery(query);
			while(resultSet.next()){
				
				this.lonelyUsers.add(new UserInfo(resultSet.getLong(1),resultSet.getString(2),resultSet.getString(3)));
				
			}
			resultSet.close();
			stat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
		}
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
    	try(Statement stat=oracleConnection.createStatement()){
    		String query="SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM "+userTableName+" U WHERE "
    				+ "U.USER_ID=ANY(SELECT CT.USER_ID FROM "+currentCityTableName+" CT, "+hometownCityTableName+" HC "
    				+ "WHERE CT.USER_ID=HC.USER_ID AND CT.CURRENT_CITY_ID<>HC.HOMETOWN_CITY_ID)";
    		ResultSet resultSet=stat.executeQuery(query); 
    		while(resultSet.next()){
    			this.liveAwayFromHome.add(new UserInfo(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3)));
    		}
    		resultSet.close();
    		stat.close();
    	}
        
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
    	try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    		String query="SELECT TAG_PHOTO_ID FROM "+tagTableName+" GROUP BY TAG_PHOTO_ID ORDER BY COUNT(*) DESC, TAG_PHOTO_ID";
    		ResultSet resultSet=stat.executeQuery(query);
    		while(resultSet.next() && n>0){
    			try(Statement stat2=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    				String query2="SELECT P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM "+photoTableName+" P, "+albumTableName+" A WHERE P.ALBUM_ID=A.ALBUM_ID AND PHOTO_ID="+resultSet.getString(1);
    				ResultSet resultSet2=stat2.executeQuery(query2);
    				resultSet2.next();
    				PhotoInfo p=new PhotoInfo(resultSet2.getString(1), resultSet2.getString(2), resultSet2.getString(3),resultSet2.getString(4),resultSet2.getString(5));
    				TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
    				String query3="SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM "+userTableName+" U, "+tagTableName+" T WHERE T.TAG_SUBJECT_ID=U.USER_ID AND T.TAG_PHOTO_ID="+resultSet.getString(1);
        			resultSet2=stat2.executeQuery(query3);
        			while(resultSet2.next()){
        				tp.addTaggedUser(new UserInfo(resultSet2.getLong(1), resultSet2.getString(2), resultSet2.getString(3)));
        				}
        			this.photosWithMostTags.add(tp);
        			n--;
    				}
    			
    		}
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) One of the users is female, and the other is male
    // (2) Their age difference is within "yearDiff"
    // (3) They are not friends with one another
    // (4) They should be tagged together in at least one photo
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user_id for the female
    // (iii) If there are still ties, choose the pair with the smaller user_id for the male
    //
    public void matchMaker(int n, int yearDiff) {
    	try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    		String query="SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH FROM "+userTableName+" U1, "+userTableName+" U2, "+tagTableName+" T1, "+tagTableName+" T2 WHERE U1.USER_ID<U2.USER_ID AND U1.GENDER<>U2.GENDER AND (U1.YEAR_OF_BIRTH-U2.YEAR_OF_BIRTH)<"+Integer.toString(yearDiff)+" AND (U2.YEAR_OF_BIRTH-U1.YEAR_OF_BIRTH)<"+Integer.toString(yearDiff)+" AND U1.USER_ID=T1.TAG_SUBJECT_ID AND U2.USER_ID=T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID=T2.TAG_PHOTO_ID AND (U1.USER_ID, U2.USER_ID) NOT IN (SELECT USER1_ID, USER2_ID FROM "+friendsTableName+" UNION SELECT USER2_ID, USER1_ID FROM "+friendsTableName+") GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH ORDER BY COUNT(*), U1.USER_ID, U2.USER_ID";
    		ResultSet resultSet=stat.executeQuery(query);
    		while(resultSet.next() && n>0){
    			MatchPair mp=new MatchPair(resultSet.getLong(1), resultSet.getString(2),resultSet.getString(3),resultSet.getInt(4),resultSet.getLong(5),resultSet.getString(6),resultSet.getString(7),resultSet.getInt(8));
    			try(Statement stat2=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    				String query2="SELECT P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM "+tagTableName+" T1, "+tagTableName+" T2, "+photoTableName+" P, "+albumTableName+" A WHERE T1.TAG_PHOTO_ID=T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID="+String.valueOf(resultSet.getLong(1))+" AND T2.TAG_SUBJECT_ID="+String.valueOf(resultSet.getLong(5))+" AND P.PHOTO_ID=T1.TAG_PHOTO_ID AND A.ALBUM_ID=P.ALBUM_ID";
    				ResultSet resultSet2=stat2.executeQuery(query2);
    				while(resultSet2.next()){
    					mp.addSharedPhoto(new PhotoInfo(resultSet2.getString(1), resultSet2.getString(2), resultSet2.getString(3), resultSet2.getString(4), resultSet2.getString(5)));
    				}
    				this.bestMatches.add(mp);
    				n--;
    				resultSet2.close();
    			}
    		}
    		resultSet.close();
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    public class Pair{
    	public String user1_id;
    	public String user2_id;
    	public int appTime;
    	public Pair(String user1_id, String user2_id){
    		this.user1_id=user1_id;
    		this.user2_id=user2_id;
    		this.appTime=0;
    	}
    }
    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //

    @Override
    public void suggestFriendsByMutualFriends(int n) {
    	try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    		String queryView="create view FRIENDUNION as select distinct user1_id, user2_id from (select user1_id, user2_id from "+friendsTableName+" UNION select user2_id, user1_id from "+friendsTableName+" )";
    		String query="SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME FROM FRIENDUNION F1 JOIN FRIENDUNION F2 ON F1.USER1_ID<>F2.USER1_ID AND F1.USER2_ID=F2.USER2_ID JOIN "+userTableName+" U1 ON F1.USER1_ID=U1.USER_ID JOIN "+userTableName+" U2 ON U2.USER_ID=F2.USER1_ID WHERE U1.USER_ID<U2.USER_ID AND (U1.USER_ID, U2.USER_ID) NOT IN (SELECT * FROM tajik.PUBLIC_FRIENDS) GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME ORDER BY COUNT(*) DESC, U1.USER_ID, U2.USER_ID";
    		ResultSet resultSet=stat.executeQuery(queryView);
    		resultSet=stat.executeQuery(query);		
    		while(resultSet.next() && n>0){
    			UsersPair p=new UsersPair(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3), resultSet.getLong(4), resultSet.getString(5), resultSet.getString(6));
    			try(Statement stat2=oracleConnection.createStatement()){
    				String query3="SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM tajik.PUBLIC_USERS U JOIN FRIENDUNION F1 ON F1.USER1_ID="+String.valueOf(resultSet.getLong(1))+" AND F1.USER2_ID=U.USER_ID JOIN FRIENDUNION F2 ON F2.USER2_ID=F1.USER2_ID AND F2.USER1_ID="+String.valueOf(resultSet.getLong(4));
    				ResultSet resultSet3=stat2.executeQuery(query3);
    				while(resultSet3.next()){
    					p.addSharedFriend(resultSet3.getLong(1), resultSet3.getString(2), resultSet3.getString(3));
    					this.suggestedUsersPairs.add(p);
					
    				}
    				n--;
    				resultSet3.close();
    			} catch (SQLException e){
    				System.err.println(e.getMessage());
    				}
    		}
    		resultSet=stat.executeQuery("drop view FRIENDUNION");
    		resultSet.close();
    	} catch (SQLException e) {
		// TODO Auto-generated catch block
    		System.err.println(e.getMessage());
		}

	}

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        this.eventCount = 12;
        this.popularStateNames.add("Michigan");
        this.popularStateNames.add("California");
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
    	try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    		String query="SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM "+userTableName+" U WHERE U.USER_ID IN (SELECT USER1_ID FROM "+friendsTableName+" F1 WHERE USER2_ID="+user_id+" UNION SELECT USER2_ID FROM "+friendsTableName+" F2 WHERE F2.USER1_ID="+user_id+" ) ORDER BY YEAR_OF_BIRTH, MONTH_OF_BIRTH, USER_ID DESC";
    		ResultSet resultSet=stat.executeQuery(query);
    		if(resultSet.first()){
    			this.oldestFriend=new UserInfo(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3));
    		}
    		if(resultSet.last()){
    			this.youngestFriend=new UserInfo(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3));
    		}
    		resultSet.close();
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());;
		}
//        this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
//        this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
    }
    
    
    class OldUsersInfo implements Comparable<UserInfo>{
		Long userId;
		String firstName;
		String lastName;
		public OldUsersInfo(Long uid, String fname, String lname){
			userId = uid;
			firstName = fname;
			lastName = lname;
		}
		public String toString(){
			return firstName+" "+lastName+"("+userId+")";
		}

		public int compareTo(UserInfo arg0) {
			return userId.compareTo(arg0.userId);
		}
	}

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {
    	try(Statement stat=oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
    		String query="SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME FROM "+userTableName+" U1, "+userTableName+" U2, "+friendsTableName+" F, "+hometownCityTableName+" HC1, "+hometownCityTableName+" HC2 WHERE U1.USER_ID=F.USER1_ID AND U2.USER_ID=F.USER2_ID AND U1.LAST_NAME=U2.LAST_NAME AND HC1.HOMETOWN_CITY_ID=HC2.HOMETOWN_CITY_ID AND U1.USER_ID=HC1.USER_ID AND U2.USER_ID=HC2.USER_ID AND (U1.YEAR_OF_BIRTH-U2.YEAR_OF_BIRTH)<10 AND (U2.YEAR_OF_BIRTH-U1.YEAR_OF_BIRTH)<10 ORDER BY U1.USER_ID, U2.USER_ID";
    		ResultSet resultSet=stat.executeQuery(query);
    		while(resultSet.next()){
    			 SiblingInfo s = new SiblingInfo(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3), resultSet.getLong(4), resultSet.getString(5), resultSet.getString(6));
    			 this.siblings.add(s);
    		}
    		resultSet.close();
    	} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Info(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

    }

}
