//find the oldest friend for each user who has a friend. For simplicity, use only year of birth to determine age, if there is a tie, use the one with smallest user_id
//return a javascript object : key is the user_id and the value is the oldest_friend id
//You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify users collection.
//
//You should return something like this:(order does not matter)
//{user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname){
	db = db.getSiblingDB(dbname)
	var list = new Object();
	var friend1 = db.users.aggregate([
	  	{$project:{ user_id : 1, friends : 1}}
	  	// { $unwind: "$friends"},
	  	// {$group: {_id: "$user_id", friends: {$push: "$friends"}}}
	  	])
	while(friend1.hasNext()){
	  	var user = friend1.next();
	  	var userid = user.user_id;
	  	var userFriend = user.friends;
	  	var smalllist = [];
	  	var oldest = 1000;
	  	var friendid = 1000;
	  	var flag = true;
	  	var friend2 = db.users.aggregate([
		  	{ $unwind: "$friends"},
		  	{$group: {_id: "$friends", friends: {$push: "$user_id"}}}
	  	])
	  	while(friend2.hasNext() && flag){
	  		var user2 = friend2.next();

	  		var user2id = user2._id;
	  		if(userid == user2id){
	  			userFriend.push.apply(userFriend,user2.friends);
				flag = false;
	  		}
	  	}
	  	if(userFriend.length == 0){
	  				// print('without friends');
	  				// print(userid);
	  				continue;
	  			}
	  	for(var i = 0; i < userFriend.length; i++){
			var friend = db.users.findOne({'user_id':userFriend[i]});
			if(friend.YOB <= oldest){
				
				oldest = friend.YOB;
			}
	  	}
	  	for(var i = 0; i < userFriend.length; i++){
			var friend = db.users.findOne({'user_id':userFriend[i]});
			if(friend.YOB == oldest){				
				oldest = friend.YOB;
				smalllist.push(friend.user_id);
			}
	  	}
	  	friendid = Math.min.apply(null,smalllist);
	  	list[userid] = friendid;
	}
	return list;
	  //implementation goes here
	  //return an javascript object described above

}
