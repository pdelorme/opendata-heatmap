/**
 * USER BUISNESS
 */

/**
 * registering new user.
 */
exports.register_user = function f(req,res){
	var params = req.query;
	console.log("register_user params",params);
	if(!params.login || ! params.password){
		return res.respond('login and password must be provided',400);
	}
	var dbObject = {
		login      : params.login,
		password   : password(params.password), // password hash.
		first_name : params.first_name,
		last_name  : params.last_name,
		email      : params.email
	}
	
	db.select('select_user',dbObject, function(err, data){
		console.log(">>user:",err,data);
		if(err)
			return res.respond(err,400);
		if(data){
			if(data.password!=dbObject.password){
				// user already exists.
				return res.respond('invalid login or password',400);
			}
			// updates data values from params
			for (var key in dbObject) { 
				if(data[key]){
					data[key] = dbObject[key];
				}
			}
			if(params.newPassword){
				console.log("new PASSWORD.");
				dbObject.password = password(params.newPassword);
			}
			// reinjects data with updated values.
			db.update('update_user', dbObject, function(err,data){
				dbObject.password=undefined;
				return res.respond(dbObject,200);
			});
		} else {
			db.insert('insert_user',dbObject, function(err,data){
				dbObject.password=undefined;
				return res.respond(dbObject,200);
			});
		}
	});
}

/**
 * checking user/password.
 * @return user.
 */
exports.login = function f(req,res){
	var params = req.query;
	console.log("login params",params);
	if(!params.login || ! params.password){
		return res.respond('login and password must be provided',400);
	}

	params.password = password(params.password);
	db.select('select_user',params, function(err, data){
		// console.log("data",data);
		if(!err && data && data.password==params.password){
			data.password = undefined;
			req.session = {};
			req.session.login = params.login;
			return res.send(data);
		}
		return res.respond('invalid login or password',400);
	});
	
}

exports.logout = function f(req,res){
	req.session = null;
	return res.respond('OK',200);
}

function password(password){
	if(!password)
		return;
	var shasum = crypto.createHash('sha1');
	shasum.update(password);
	return shasum.digest('hex');
}
/**
 * gets user info
 */
exports.get_user_info = function f(req,res){
	if(!req.params.login){
		req.params.login = req.session.login;
	}
	db.select('select_user',req.params, function(err, data){
		var user_info = {
				login      : data.login,
				first_name : data.first_name,
				last_name  : data.last_name
		};
		
		return res.send(user_info);
	});
}