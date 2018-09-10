var http = require("http");
var fs = require("fs");
var table = "Movies";

http.createServer(function (request, response) {

   // Send the HTTP header 
   // HTTP Status: 200 : OK
   // Content Type: text/plain
   response.writeHead(200, {'Content-Type': 'text/plain'});
   
   // Send the response body as "Hello World"
  
	var data = fs.readFileSync('temp.txt');
	var messageBody = JSON.parse(data.toString());

	messageBody.forEach(message => {
		console.log("iterations ===========")
		

		let schemaTableNameString = message.schemaName + message.tableName;


		console.log(schemaTableNameString);

		get(schemaTableNameString, function(err, data){
   			if(err) {
   				create(message, function(err, data){
   					console.log(data);
   				});
   			} else {
   				// update existing data.

   				var isValidUpdate = false;
   				if(message.processStatus !== data.Status) {
   				// check data to update.
   				 switch(message.processStatus) {
   				 	case "SUCCESS" : 
   				 			if(message.processStatus === "DELAY" || message.processStatus === "FAILURE") {
   				 				isValidUpdate = true;
   				 			} 
   				 		break;
   				 	case "DELAY" : 
   				 			if(message.processStatus === "SUCCESS") {
   				 				//isValidUpdate = true;
   				 			} else if(message.processStatus === "FAILURE") {
   				 				isValidUpdate = true;
   				 			}

   				 		break;
   				 	case "FAILURE" :
   				 			if( message.processStatus === "SUCCESS") {
   				 				//isValidUpdate = true;
   				 			} 
   				 		break;
   				 }
   				}

   				if(isValidUpdate) {
   					update(message, function(err, data){
   						console.log(data);
   				});
   				}
   				
   			}
		});
	});
	
 	response.end("Hello");

}).listen(8081);

function get(schemaTableNameString, callback) {
	var params = {
    TableName: table,
    Key:{
        "year": 1,
        "title": "title"
    }};

    callback(null, params);
};


function update(message, callback) {
	



var params = {
    TableName:table,
    Key:{
        "year": year,
        "title": title
    },
    UpdateExpression: "set info.rating = info.rating + :val",
    ExpressionAttributeValues:{
        ":val": 1
    },
    ReturnValues:"UPDATED_NEW"
};
	console.log("update data");
	callback(null, "Updated");
}

function create(message, callback) {
	var params = {
    TableName:table,
	    Item:{
	    	"serverName" : "ip-10-204-94-212",
			"processName" : "s3_TO_SNOWFLAKE",
			"fileName" : "201801170000",
			"odate" : "20180117",
			"schemaName" : "PCDW_TS2-TB",
			"tableName" : "T2_ACCT",
			"processStarttime" : "17123",
			"processEndtime" : "17123",
			"processStatus" : "FAILURE",
			"validated" : false,
			"currenttime" : "20180119161609",
			"recordCount" : 1,
			"rejectCount" : 1
	    }
	};

	docClient.put(params, function(err, data) {
    if (err) {
        console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        callback(err);
    } else {
        console.log("Added item:", JSON.stringify(data, null, 2));
        callback(null, data);
    }
});

	console.log("create data");
	callback(null, "Created");
}

// Console will print the message
console.log('Server running at http://127.0.0.1:8081/');




