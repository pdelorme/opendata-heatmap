var config_prod = {
	server_port : 3001,
	db_host:"localhost",
	db_database:"data_heatmap",
	db_user:"vuparvous",
	db_password:"Oward2013",
	web_root:__dirname+"/../web/"
};

var config_dev = {
	server_port:3000,
	db_host:"localhost",
	db_database:"odhm",
	db_user:"root",
	db_password:"D3leurence",
	web_root:__dirname+"/../web/"
	
};

module.exports = config_dev;