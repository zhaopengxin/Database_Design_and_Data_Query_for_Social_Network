//query1 : find users who live in the specified city. 
// Returns an array of user_ids.

function find_user(city, dbname){
    db = db.getSiblingDB(dbname)
    //implementation goes here
    var id_list = [];
    var result = db.users.find({'hometown.city': city});
    while(result.hasNext()){
    	var user = result.next().user_id;
    	id_list.push(user);
    }
    return id_list;


    // returns a Javascript array. See test.js for a partial correctness check.  
    // This will be  an array of integers. The order does not matter.                                                               

}
