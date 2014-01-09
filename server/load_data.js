var config      = require('./config'),
	api         = require('./lib/api_service')

api.init(function(){
	console.log('starting');
	api.parse_directory(__dirname+'/data/',true);
});
