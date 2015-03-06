/**
 * Sample program to connect between HDFS and Node.js
 */
var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient({
  user: 'cloudera',
  host: '192.168.110.142',
  port: 50070,

});

console.log(hdfs);
var remoteFileStream = hdfs.createReadStream('/user/cloudera/InputFiles/Sample.txt')
 
remoteFileStream.on('error', function onError (err) {
  // Do something with the error 
	if(err)
		console.log(err)
});
 
remoteFileStream.on('data', function onChunk (chunk) {
  // Do something with the data chunk 
	console.log(chunk.toString("utf-8"))
});
 
remoteFileStream.on('finish', function onFinish () {
  // Upload is done 
	console.log("Completed Reading file")
	console.log("Done")
});