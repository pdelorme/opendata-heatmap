var config_prod = {
    server_port : 80,
    db_host:"localhost",
    db_database:"opendata_heatmap",
    db_user:"lookal",
    db_password:"",
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