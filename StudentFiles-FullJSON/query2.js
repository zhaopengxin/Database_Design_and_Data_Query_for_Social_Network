//query2
//unwind friends and create a new collection called flat_users where each document has the following schema:
/*
{
  user_id:xxx
  friends:xxx
}
*/


function unwind_friends(dbname){
    db = db.getSiblingDB(dbname)
    //implementation goes here
    db.users.aggregate([
    	{ $unwind: "$friends"},
    	{ $project: {_id: 0, user_id: 1, friends: 1}},
    	{ $out: "flat_users"}


    	])
    
    // returns nothing. It creates a collection instead as specified above.
}
