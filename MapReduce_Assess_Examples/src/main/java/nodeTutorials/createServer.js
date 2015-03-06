/*
 * This is a sample example on how to create Server
 * using Node.js 
 */

var http = require("http");
var url = require("url");
var fs = require("fs");

var server = http.createServer(function(req, res) {

	var method = req.method;
	var path = url.parse(req.url)['path'];
	
	console.log("path is :"+path);
	if(method == "GET" && ( path == "/" || path == "/index.html")){
		fs.createReadStream("index.html").pipe(res);
	}
	console.log("In Create Server");
	
});

var ip = "localhost";
var port = 1337;

server.listen(port,ip,function(){
	console.log("Running Server at "+ip+":"+port);
});