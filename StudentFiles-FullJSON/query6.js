// query6 : Find the Average friend count per user for the Users
// collection.  We define the `friend count` as the number of friends
// of a user. The average friend count per user is the average `friend
// count` towards a collection of users. In this function we ask you
// to find the `average friend count per user` for the users in the
// Users collection.
//
// Return a decimal variable as the average user friend count of all users
// in the Users collection.



function find_average_friendcount(dbname){
  	db = db.getSiblingDB(dbname)
  	// Implementation goes here
  	var totalNum = 0;
  	var totalUser = db.users.find().count();
 	var friend1 = db.users.aggregate([
  		{$project:{ user_id : 1, friends : 1}}
  		])
 	while(friend1.hasNext()){
 		var user = friend1.next();
 		var friends = user.friends;
 		var friend_num = friends.length;
 		totalNum = totalNum + friend_num;
 	}
 	var avarage_friend = totalNum/totalUser;
 	return avarage_friend;
}
