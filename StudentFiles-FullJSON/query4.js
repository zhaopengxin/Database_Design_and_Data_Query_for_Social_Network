
// query 4: find user pairs such that, one is male, second is female,
// their year difference is less than year_diff, and they live in same
// city and they are not friends with each other. Store each user_id
// pair as arrays and return an array of all the pairs. The order of
// pairs does not matter. Your answer will look something like the following:
// [
//      [userid1, userid2],
//      [userid1, userid3],
//      [userid4, userid2],
//      ...
//  ]
// In the above, userid1 and userid4 are males. userid2 and userid3 are females.
// Besides that, the above constraints are satisifed.
// userid is the field from the userinfo table. Do not use the _id field in that table.

  
function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname)
    //implementation goes here
    var userPair = [];
    var city = db.users.aggregate([
    	{$group:{_id: "$hometown.city", users: {$push: "$user_id"}}}
    	])
    while(city.hasNext()){
    	var user_list = city.next().users;
    	for(i = 0; i < user_list.length; i++){
    		for(j = i; j < user_list.length; j++){
    			var user1 = db.users.findOne({'user_id': user_list[i]});
    			var user2 = db.users.findOne({'user_id': user_list[j]});
    			var age1 = user1.YOB;
    			var age2 = user2.YOB;
    			var gender1 = user1.gender;
    			var gender2 = user2.gender;
    			var friend1_list = user1.friends;
    			var friend2_list = user2.friends;
    			var isFriend1 = friend1_list.indexOf(user2.user_id);
    			var isFriend2 = friend2_list.indexOf(user1.user_id);
    			if(user_list[i] != user_list[j] && gender1 != gender2 && Math.abs(age1-age2) < year_diff && isFriend1 == -1 && isFriend2 == -1){
    				if(gender1 == 'male'){
    					userPair.push([user1.user_id, user2.user_id]);
    				}else{
    					userPair.push([user2.user_id, user1.user_id]);
    				}
    			}
    		}
    	}
    }
    return userPair;
    // Return an array of arrays.
}
