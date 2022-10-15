/**
 * 
 */

var port = 8081;

const express = require('express');
const app = new express();
const path = require("path");
const stemmer = require("stemmer");

const AWS = require("aws-sdk");

const {DynamoDB, QueryCommand } = require('@aws-sdk/client-dynamodb-v2-node');

//AWS.config.loadFromPath('./config.json');
AWS.config.update({region: 'us-east-1'});

const client = new AWS.DynamoDB();

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"))

app.get('/', function(request, response) {
    response.sendFile('html/index.html', { root: __dirname });
});

app.get('/bear.jpg', function(request, response) {
    response.sendFile('html/bear.jpg', { root: __dirname });
});

app.get('/talks', function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();
  console.log(request.query.keyword);
  
  // TODO look up the word (or, in the case of EC2, words) in 'terms' in DynamoDB and hand them over as the variable 'results' below
  var word = stemmer(request.query.keyword) //stem the word and store it in word
  var params = { //specify parameters
	TableName: "inverted", //table name is inverted
	ExpressionAttributeValues: { //sort by keyword
		":keyword": word
	},
	KeyConditionExpression: "keyword = :keyword",
	ProjectionExpression: "#dynamodb_url", //workaround since url is reserved keyword - we want to project the urls
	ExpressionAttributeNames: {
		"#dynamodb_url" : "url" //url attribute name
	},
	Limit: 15 //limit of 15
  }
  const results = []; //instantiate empty array, we'll push into this
  var output = docClient.query(params, function(err, data) { //output function
	if (err) { //check to see if throws error
		console.log("Error", err);
	} else if (word == "a" || word == "all" || word == "any" || word == "but" || word == "the") { //check for stop words
		response.render("results", { "search": word, "results": results}); //render all results (this is empty if it is a stopword)
	} else {
		data.Items.forEach(function(element) { //for every single element, take the url segment and add the string to the result
			results.push(element['url']);s array
			//console.log(element['url']);
			//console.log(results.length)
		});
		response.render("results", { "search": word, "results": results}); //finally render the array of results
	}
  });
})

app.listen(port, () => {
  console.log(`HW1 app listening at http://localhost:${port}`)
})
