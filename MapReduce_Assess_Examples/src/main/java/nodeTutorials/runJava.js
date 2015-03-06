exec = require("child_process").exec;

/*
 * D:/Git_Hub/Spark/MapReduce_Assess_Examples/target/classes/nodeTutorials
 * 
 * D:/Git_Hub/Spark/MapReduce_Assess_Examples
 */
var classPath = "D:/Git_Hub/Spark/MapReduce_Assess_Examples/target/classes/";

var command = "java -cp "+classPath+" nodeTutorials.Samples";

child = exec(command, function(err,
		stdout, stderr) {

	console.log("Std Out : " + stdout);

	if (err != null) {
		console.log("EXEC ERROR " + err);
	}
});