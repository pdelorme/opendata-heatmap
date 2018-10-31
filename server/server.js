
/**
 * Module dependencies.
 */

var config      = require('./config'),
	express     = require('express'),
	http        = require('http'),
	path        = require('path'),
	httpProxy   = require('http-proxy'),
	api         = require('./lib/api_service'),
	response    = require('./lib/response');

// prevent from crashing.
//process.on('uncaughtException', function (err) {
//	  console.error(err);
//	  console.log("Node NOT Exiting...");
//	});


var app = express();
var proxy = new httpProxy.RoutingProxy();

// all environments
app.set('port', process.env.PORT || config.server_port);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser({uploadDir: __dirname + '/uploads-tmp'}));
app.use(express.methodOverride());
app.use(express.cookieParser());
app.use(express.cookieSession({secret:'OpenDataHeatMap',cookie: { maxAge: 60 * 60 * 1000 }}));
app.use(app.router);
app.use(express.static(config.web_root));

function errorHandler (options) {
	var log = options.log || console.error
    	, stack = options.stack || false
    return function (err, req, res, next) {
		if (stack && err.stack) log(err.stack);
		var content = err.message;
		if (stack && err.stack) content += '\n' + err.stack;
		res.respond(content, err.code || 500);
	}
};

// development only
if ('development' == app.get('env')) {
  app.use(errorHandler({"stack": false}));
}

app.use(errorHandler({"stack": true}));
app.get('/*',function(req,res,next){
    res.header('Access-Control-Allow-Origin', '*' );
    next(); 
});

app.get('/api/geo-datasets',	api.get_geo_datasets);
app.get('/api/ajax-dataset',	api.get_ajax_dataset);
app.get('/api/area-geodata',	api.get_area_geodata);
app.get('/api/area-geodata-ds',  api.get_area_geodata_ds);
app.get('/api/geodata',         api.get_geodata);
app.post('/api/upload-dataset',	api.upload_dataset);
//app.get('/api/register_user',        api.register_user);
//app.get('/api/login',                api.login);
//app.get('/api/logout',               api.logout);
//app.get('/api/user_info/:login',     api.get_user_info);
//app.get('/api/user_info',            api.get_user_info);
app.get('/api/test',                 api.test);


// server startup.
http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});
