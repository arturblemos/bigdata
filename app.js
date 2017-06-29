const express = require('express');
const bodyParser = require('body-parser');
const http = require('http');
const path = require('path');
const app = express();
var request = require('request');
var sleep = require ('sleep');

app.set('port', 3000);
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static('public'));

app.get('/', function(req, res) {
      res.sendFile(path.join(__dirname + '/index.html'));
});

app.post('/', function(req, res) {
      	console.log(req.body.dtInicio);
      	console.log(req.body.dtFim);

      // Definir parametros do request
	var headers = {
	  'Content-Type':'application/json;charset=UTF-8'
	}
	var options = {
	    host: 'spark://bigdata-VirtualBox',
	    port: 6066,
	    method: 'POST',
	    headers: headers,
	    path:'/v1/submissions/create'}

	var data = '{"action" : "CreateSubmissionRequest","appArgs" : [ req.body.dtInicio, req.body.dtFim ], "appResource" : "file:/home/Documentos/server/dados/target/scala-2.11/driveclientesapp_2.11-1.0.jar", "clientSparkVersion" : "2.1.0", "environmentVariables" : {"SPARK_ENV_LOADED" : "1"},"mainClass" : "com.UFRJ.driveClientesApp","sparkProperties" : {"spark.jars" : "file:/home/Documentos/server/dados/target/scala-2.11/driveclientesapp_2.11-1.0.jar",   "spark.driver.supervise" : "false","spark.app.name" : "driveClientesApp","spark.eventLog.enabled": "true","spark.submit.deployMode" : "cluster",  "spark.master" : "spark://bigdata-VirtualBox:6066"}}'

	var req = http.request(options, function(error, res) {
	    res.setEncoding('utf8');
	    res.on('data', function (chunk) {
		console.log("body: " + chunk);
	    });
	    if (error) {
		debug.print("error:" + error)
		res.redirect('/');		
		}
	});

	//req.write(data)
	//req.end()
	// Start the request
	sleep.sleep(10);
	res.redirect('/');
})

app.listen(app.get('port'), () => {
  console.log('  App is running at http://localhost:%d in %s mode', app.get('port'), app.get('env'));
  console.log('  Press CTRL-C to stop\n');
});
