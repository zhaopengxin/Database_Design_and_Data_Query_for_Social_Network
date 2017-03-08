// query 7: For each city, find the average friend count per user in that city using
// MapReduce. Using the same terminology as in query6, we are asking
// you to write the mapper, reducer and finalizer to find the average
// friend count for each city.


// >var map = function(){ emit(this.location, this.age); }
// >var reduce = function( key, values ){ return Array.sum(values); }
// >var options = { out: "age_totals" }
// >db.mythings.mapReduce( map, reduce, options )

var city_average_friendcount_mapper = function() {
  	// implement the Map function of average friend count
  	// var allUser = this.find();
  	// while(allUser.hasNext()){
  	// 	var user = allUser.next();
  	// 	var cityKey = user.hometown.city;
  	// 	var friendNum = user.friends.length;
  	// 	print(cityKey+'\n');
  	// 	print(friendNum);
  	// 	emit(cityKey, friendNum);
  	// }
    var value = {
      count: 1,
      quantity : this.friends.length
    };
  	emit(this.hometown.city, value);
  	// emit(cityKey, friendNum);
  	//emit(this.user_id, this.DOB);
};

var city_average_friendcount_reducer = function(key, values) {
  	//implement the reduce function of average friend count
    reduceVal = {count: 0, quantity: 0 };
  	for(var i = 0; i < values.length; i++){
      reduceVal.count += values[i].count;
  		reduceVal.quantity += values[i].quantity;
  	}
  	return reduceVal;

};

var city_average_friendcount_finalizer = function(key, reduceVal) {
  // We've implemented a simple forwarding finalize function. This implementation 
  // is naive: it just forwards the reduceVal to the output collection.
  // Feel free to change it if needed. However, please keep this unchanged:
  // the var ret should be the average friend count per user of each city.

  var ret = reduceVal.quantity/reduceVal.count;
  return ret;
}
