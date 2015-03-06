/**
 * Used for working on File uploads into Node.js
 * 
 */

/* Define dependencies. */

var http = require("http");
var express = require('express');
var logger = require('logger');
var multer = require('multer');
var app = express();

var port = 3000;
var serverUrl = "192.168.208.201";

/* Configure the multer. 
 * 
 * Used for the purpose of File uploading and all
 * 
 * */

app.use(multer({
	dest : './uploads/',
	rename : function(fieldname, filename) {
		return filename + Date.now();
	},
	onFileUploadStart : function(file) {
		console.log(file.originalname + ' is starting ...')
	},
	onFileUploadComplete : function(file, req, res) {
		console.log(file.fieldname + ' uploaded to  ' + file.path)
		isUploadCompleted(file, req, res);
	}
}));

/**
 * It calls after the completion of file upload
 * @param file
 * @param req
 * @param res
 */
function isUploadCompleted(file, req, res) {
	console.log(req.files);
	console.log("File uploaded -- " + file.originalname);
	res.end("Files uploaded " + file.originalname);
}

/* Handling routes. */
app.get('/index.html', function(req, res) {
	res.sendfile("index.html");
})

app.get('/', function(req, res) {
	res.sendfile("index.html");
});

/* Run the server. */
app.listen(port, serverUrl, function() {
	console.log("Server is Working on " + serverUrl + ":" + port);
});