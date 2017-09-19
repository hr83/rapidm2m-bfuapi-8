/*
	rapidM2M API - interface driver for BACKEND access via rest API, for
		- UAPI frontend browser/javascript
		- FAPI frontend server/nodejs
		- BAPI backyard/nodejs (intermediate solution)

	TODO
		- als resource einweben? ... site.getSiblingSites = _getSiblingSites_factory( __allsites, site.customer_id, site.site_id);
				 all_sites ... global list of all sites recently seen
				 customer_id ... null (wildcard) or id of a customer
				 site_class .... null (wildcard) or class identifier
				 result ... list of sites mathing with customer_id and site_class
				 function _getSiblingSites_factory( all_sites, customer_id, omit_site_id){
					 return function( site_class){
						 var r=[];
						 for( var si in all_sites){
							 var site= all_sites[si];
							 if (omit_site_id == site.site_id) continue;
							 if (customer_id  != site.customer_id) continue;
							 if (site_class && (site_class != site.class)) continue;
							 r.push( site);
					 	}
				 return r;	};}

		 - subscription error handling
			-> mytask trotzdem aufrufen, aber mit fehlerkennung? task(err,data...) ?
			-> od. eigenständigen error-callback handler beim subscribe
		- vollständig data driven implementation - requires subscriber for c1+,h0+,pos,site

		- UAPI, FAPI use-case not tested yet

	--- in progress ---
		// return [ms] until next full multiple (relative time)
		global.nextFullMultipleSpan = function ( freq_ms){

		// return [Date] of next full multiple (absolute time)
		global.nextFullMultipleUTC = function( freq_ms){

	20150610 aa 0v008b
		- CHG rm.extend( userlevel, extpath, onrequest){
				uses a websock server at :8090 to redirect requests from /api/ext/xxx

				userlevel ... 1..9 - mdnUser
				extpath ... GET susi/<customer_id>/<my_var>/test -> appears as /api/ext/susi/....
				onrequest ...  function( reply_httpstatus, replybody_as_json)

	20150610 aa 0v008a
		- NEW rm.extend( userlevel, verb, resource, onrequest){
				starts an http server at localhost:8090 (__CONFIG.ext_port, __CONFIG.ext_ip) used to handle requests originally targeted to /api/ext/...

	20150421 aa 0v007a
		- CHG wenn kein socket verfügbar (externe installation), dann fallback auf 1s polling für config0
	    - DEL require http
		- CHG rm.subscribe
		- NEW rm.stats 0,1,2,3 // 0=none, 1=daily summary, 2=hourly summary, 3=verbose-each qjob end
		- DEL rm.cycleStats
		- NEW rm.setTimeoutQJob
		- NEW stamp()
		- NEW rm.setAuth
		- NEW rm.reportError

 	20150408 aa 0v006b
		- ADD sendNotifications supports now ',' and ';' as delimiter character

	20150403 aa 0v005
		- FIX added closure factory for subscriber tasks
		- ADD rm.exec

	STORAGE
		f. BAPI: ...MyDatanet\accessories\nodejs\node_modules\rapidm2m
		f. UAPI: ???todo public .js pfad
		f. FAPI: ???todo public installable npm

	USAGE - API LOW LEVEL ACCESS

		var rm = require('rapidm2m-bfuapi.js').bapi();	// BAPI
		var rm = require('rapidm2m-bfuapi.js').fapi('https://myserver.com/api/1');	// F/UAPI

		rm.get( path, onDone );
		rm.get( path, data, onDone );
		rm.put( path, data, onDone );
		rm.post( path, data, onDone );
		rm.del( path, onDone );


		path ... [] of url segments; URI encoding is applied automatically; e.g. ["customers","xyz","sites","abc","config0"]
				 or (not recommended):
				 string '\customers\xyz\sites\abc\config0' -> this will NOT encode '\' characters!
		data ... {} request payload (encoded as url param on GET or as body with PUT and POST)
		onDone ... function( err, replydata)

		rm.setAuth( username, password)

			use these auth credentials for further F/UAPI access

 		rm.reportError = function(err, msg)

			override this to install APP specific low-level error handler
			err ... http error code (e.g. 400, 404, 500...)
			msg ... detailed error msg


 USAGE - multi-query API

		rm.exec( jobs, onFinished)

		process given requests in parallel and call onFinished with all replies at once

		jobs = {
			jobA: {
				get|put|post: ['customers',customer_id,'sites',site_id,'config0'],
				params:{...} | undefined,
				// set by reply: -> see onFinished()
			},
			...,
			jobZ:{ ... }
		}
		onFinished ... function(err,jobs), where jobs is the same as on call, but with some information appended for each job
			jobs = {
				jobA: {
					err:null | <any error>,		// set by reply
					data:{...} | undefined		// set by reply
					// ... get/put/post/params remain the same as on call
				},
				...,
				jobZ:{ ... }
			}



	USAGE - LOGGING API

		rm.log.syserr( msg);
		rm.log.error( msg);
		rm.log.warning( msg);
		rm.log.useraction( msg);
		rm.log.sysaction( msg);
		rm.log.info( msg);
		rm.log.debug( msg);

		rm.log.syserr( msg, src);
		rm.log.error( msg, src);
		...

		msg ... string, message to log
		src ... string, optional; module and process information is always logged;
				optionally add your own identifier or __logsource (=__script + '#* + __line)

 USAGE - NOTIFICATIONS SHORTHAND

		rm.sendNotifications( customer_id, receivers, smssubject, mailsubject, mailbody, onError)

		receivers ... list of ',' (or ';') separated (unordered) email and sms tokens; e.g. "xyz@microtronics.at, +43 1 3722018, hhhhh@mydatanet.at"
		smssubject ... string, required to process sms numbers in <receivers>
		mailsubject ... string, required to process email receivers in <receivers>
		mailbody ... html string (w/o enclosing <HTML> tag), required to process email receivers in <receivers>
		onError ... function(err,data); optional; called in case of an error only!

	USAGE - LOCAL DB CACHE API
		var c = rm.openCache( cache_id);	// assumes {} for defaults
		var c = rm.openCache( cache_id, defaults);

		cache_id ... string used to build the full filename for persisting the cache
		defaults ... optional; default contents of cache after first initialisation; defaults to {};
		result ... cache object;

		example:
			var c = rm.openCache( 'mycache');
			c['abc']= 'anything you want';
			c.update(); // mark cache dirty for automatic persistance
			c.flush();  // persist now - optional

	USAGE - BAPI SUBSCRIPTIONS

		rm.subscribe( appid, resources, handler)

	 		appid     ... name of app template
	 		resources ... list of resources and their poll interval, e.g.
	 						{ site:5, config0:0, config1:1, config2:1, histdata0:10, alarm:0 }
	 						config0, alarm: are always set to 0 (data triggered)
	 						histdata0+: provides just the youngest stamp (w/o any app specific data)
	 						_site: compact site information - implicit resource (automatically generated);
	 								updated whenever possible (boot,site-list-refresh,site res polled, any data trgd event)

	 		handler ... function(cache, res, onTaskDone)
	 						cache ... {	customer_id, site_id, _site, 		// implicit resources, automatically generated
										config0, config1, ..., 				// resources explicitly subscribed
	 									config0_prev, ...					// prev value of above resource
	 									config0_fault, ... } 				// fault indication if resource can't be polled
	 						res ... '_site', 'config0',... resource triggering task execution
	 						onTaskDone(err) ...  callback for async processing


		rm.setTimeoutQJob( msec, site_uid, res, data)

			Inform all subscribers fitting to <site_uid/res> when timer expires.
			Any timer already started for this <site_uid/res> is canceled.

			msec ... timeout period, <0 = cancel pending timeout only, do not start a new one
	 		site_uid ... identifies the subscription/cache to address
	 		res  ... '!anytext-with-leading-exclamation-mark' - used by the subscription handler to identify the timer event
	 		data ... {...} optional; { stamp:stamp() } will be added automatically


	INTERNAL HELPERS
		__module
		__stack
		__line
		__script
		__logsource
		function assert( condition, message)
		function now()   ... formatted as Date
		function stamp() ... now() formatted as stamp
		function formatStamp( stamp, fmt, utc)
		function stampToDate( stamp)
		function dateToStamp( dt)
		function parseDate( str, utc)
		function formatDate( dt, fmt, utc)
		function formatAge( stamp_date, locales)
*/


var __CONFIG = {

    host: 	   'http://127.0.0.1:8083/api/1/',	// host for regular API calls
    username:null,								// credentials when not authorized as localhost
    password:null,

    host_bapi: 'http://localhost:8085',			// host for data triggered events

    // host for /ext handling
    ext_host: 'http://localhost:8090',
    ext_onError:function(err,msg,log){ rm.log[log]( err+':'+msg, 'BAPI/EXT'); }
}



var _       = require('underscore');
var fs    	= require('fs');
var path    = require('path');
var async   = require('async');
var request = require('request-json');
var http    = require('http');
var urlcoder= require('url');

// provide web-socket driver module
try{
    var __io_clt = require('socket.io/node_modules/socket.io-client');	// default installation on appliances
}
catch(e){
    var __io_clt = require('socket.io-client');  // fallback for local dbg installation - todo rem?
}



/***
 extend global space with getters for some debug info
 ***/

Object.defineProperty(global, '__module', {
    get: function () {
        var s = process.cwd();
        return  s.substring( s.lastIndexOf( path.sep) + 1);
    }
});

Object.defineProperty(global, '__stack', {
    get: function(){
        var orig = Error.prepareStackTrace;
        Error.prepareStackTrace = function(_, stack){ return stack; };
        var err = new Error;
        Error.captureStackTrace(err, arguments.callee);
        var stack = err.stack;
        Error.prepareStackTrace = orig;
        return stack;
    }
});

Object.defineProperty(global, '__line', {
    get: function(){
        return __stack[1].getLineNumber();
    }
});

var rootPathLen = process.cwd().length + 1;

Object.defineProperty(global, '__script', {
    get: function () {
        var script =  __stack[1].getFileName();
        return  script.substr( rootPathLen);
    }
});


Object.defineProperty(global, '__logsource', {
    get: function () {
        return  __script + '#' + __line;
    }
});

/* *********************************************************************************************************************
 	 extend nodejs.global / browser.window
********************************************************************************************************************* */
(function(global){

    global.assert = function( condition, msg){
        if( !condition) {
            if (rm && rm.log && rm.log.syserr)
                rm.log.syserr( '!!!ASSERTION!!! ' + msg);
            else
                console.log( '!!!ASSERTION!!!', msg);	// fallback to console only if rapid m2m logging not available yet
        }
    };

    global.now = function(){
        return new Date();
    };

    global.stamp = function(){
        return dateToStamp( now());
    };

    global.formatStamp = function ( stamp, fmt, utc){
        var dt= stampToDate( stamp);
        return formatDate( dt, fmt, utc);
    };


    global.stampToDate = function ( stamp){
        var s= stamp + '00000000000000';
        return new Date( Date.UTC(
            s.substr(0,4), s.substr(4,2)-1, s.substr(6,2),
            s.substr(8,2), s.substr(10,2), s.substr(12,2)));
    };

    global.dateToStamp = function ( dt){

        if (!dt) dt= now();
        /* todo rem - alternative solution
            function twodigits(n){
                if (n<10) return '0'+n;
                else      return n.toString();
            }

            var s = dt.getUTCFullYear() +
                    twodigits(dt.getUTCMonth()+1) +
                    twodigits(dt.getUTCDate()) +
                    twodigits(dt.getUTCHours()) +
                    twodigits(dt.getUTCMinutes()) +
                    twodigits(dt.getUTCSeconds()) +
                    ('000' + dt.getUTCMilliseconds()).substr(-3);
        */
        var s = formatDate( dt, 'yyyymmddhhnnss', true);
        return s.replace( /0*$/, '');	// remove trailing zeros
    };

    /*str ... yyyymmddhhnnss - rechtsseitige 0en u. trennzeichen sind optional
     result ... Date object */
    global.parseDate = function ( str, utc) {

        str = str.replace(/\D/g,'');	// remove all delimiters (non-digits)
        while( str.length < 14) str += '0';

        if (utc) {
            return( new Date( Date.UTC(
                str.substr(0,4),
                str.substr(4,2)-1,
                str.substr(6,2),
                str.substr(8,2),
                str.substr(10,2),
                str.substr(12,2)
            )));
        }
        else {
            return( new Date(
                str.substr(0,4),
                str.substr(4,2)-1,
                str.substr(6,2),
                str.substr(8,2),
                str.substr(10,2),
                str.substr(12,2)
            ));
        }
    };

    global.formatDate = function ( dt, fmt, utc) {

        var x = {};

        if (!dt) dt= now();

        if (utc) {
            x.y= dt.getUTCFullYear();
            x.m= dt.getUTCMonth()+1;
            x.d= dt.getUTCDate();
            x.h= dt.getUTCHours();
            x.n= dt.getUTCMinutes();
            x.s= dt.getUTCSeconds();
        }
        else {
            x.y= dt.getFullYear();
            x.m= dt.getMonth()+1;
            x.d= dt.getDate();
            x.h= dt.getHours();
            x.n= dt.getMinutes();
            x.s= dt.getSeconds();
        }

        fmt = fmt || 'yyyymmdd hhnnss';
        var res= '';
        for( var i=fmt.length-1; i >= 0; i--) {
            var c = fmt[i];	// get formatting char
            var v = x[c];	// get value acc. to formatting char
            if (typeof(v) !== "undefined") {
                res= (v % 10).toString() + res;	// adde newly formatted digit in front of result
                x[c]= Math.floor( v / 10);		// modify remaining value
            }
            else res= c + res;
        }

        return res;
    };

    /*  stamp:  mdn++ t_stamp YYYYMMDDHHNNSS
     locales: optional; default = 'seconds minutes hours days'
     result: user-friendly formatted age string */
    global.formatAge = function ( stamp_date, locales) {

        locales = locales || ['seconds minutes hours days'];
        var locs = locales.split(' ');
        var now = new Date();

        if (typeof stamp_date == 'string') stmp= stampToDate( stamp_date);
        else				  		  	   stmp= stamp_date;

        var age = now.valueOf() - stmp.valueOf();
        age /= 1000; // ms -> s
        if (age < 2*60) return Math.floor( age) + ' ' + locs[0]; //' s';
        if (age < 3*60*60) return Math.floor(age / 60) + ' ' + locs[1]; //' min';
        if (age < 72*60*60) return Math.floor(age / 60 / 60) + ' ' + locs[2]; //' hrs';
        return Math.floor(age / 24 / 60 / 60) + ' ' + locs[3]; // days
    };

    // return [ms] until next full multiple (relative time)
    global.nextFullMultipleSpan = function ( freq_ms){
        var ms = now().getTime();
        return freq_ms - ms % freq_ms;
    }

    // return [Date] of next full multiple (absolute time)
    global.nextFullMultipleUTC = function( freq_ms){
        var ms = now().getTime();
        ms -= ms % freq_ms;       // skip remainder
        ms += freq_ms;            // step to NEXT full multiple
        return new Date(ms);
    }

})(typeof global === 'undefined'? this['window'] : global);





/* ---------------------------------------------------------------------------------------------------------------------
	API LOW LEVEL ACCESS
--------------------------------------------------------------------------------------------------------------------- */
var rm = {};

rm.setAuth = function( username, password){
    rm.client.setBasicAuth( username, password);
};

rm.reportError = function(err, msg){

    rm.log.error( msg);
};

function encodePath_( path){

    var url;
    if (typeof path == 'object') {
        for( var i in path) path[i] = encodeURIComponent( path[i]);
        url= path.join('/');
    }
    else {
        url= encodeURI( path);
    }
    return url;
}

// parameters ... see file head comments
rm.get = function( path, _a, _b ){

    var data  = _a;
    var onDone= _b;
    if (_b === undefined) { data=null; onDone=_a; }

    var url = encodePath_( path);

    if (data) {
        url += '?json=' + encodeURIComponent( JSON.stringify(data));
    }
    rm.client.get(  url, function (err, result, body) {

        if (!err && (result.statusCode != 200)) err= result.statusCode;
        if (err) rm.reportError( err, 'GET '+ url + '...' + JSON.stringify({err:err, body:body, result:result}) );

        if(onDone) onDone( err, body);
    });
};

rm.post = function( path, data, onDone){

    var url = encodePath_( path);

    rm.client.post(  url, data, function (err, result, body) {

        // >=200 ... workaround for legacy results
        if (!err && ((result.statusCode < 200) || (result.statusCode > 204))) err= result.statusCode;
        if (err) rm.reportError( err, 'POST ' + url + '...' + JSON.stringify({err:err, body:body, result:result}) );

        if(onDone) onDone( err, body);
    });
};

rm.put = function( path, data, onDone){

    var url = encodePath_( path);

    rm.client.put(  url, data, function (err, result, body) {

        if (!err && (result.statusCode != 201) && (result.statusCode != 204)) err= result.statusCode;
        if (err) rm.reportError( err, 'PUT ' + url + '...' + JSON.stringify({err:err, body:body, result:result}) );

        if(onDone) onDone( err, body);
    });
};

rm.del = function( path, onDone){

    var url = encodePath_( path);

    rm.client.del(  url, function (err, result, body) {

        if (!err && (result.statusCode != 204)) err= result.statusCode;
        if (err) rm.reportError( err, 'DEL ' + url + '...' + JSON.stringify({err:err, body:body, result:result}) );

        if(onDone) onDone( err, body);
    });
};


/* ---------------------------------------------------------------------------------------------------------------------
	LOGGING API
    level ... string, predefined level at which to log the message
    msg ... -> rm.log
    src ... -> rm.log
--------------------------------------------------------------------------------------------------------------------- */
function log_( level, msg, src) {

    if (!msg) return;
    if (!level) return;
    if (src === undefined) src='';

    var o = {
        type: level,
        source: __module + "." + src + (src ? '.':'') + process.pid,
        message: msg
    };

    console.log( 'LOG | ', o.type, ' | ', o.source, ' | ', o.message);

    if (JSON.stringify(msg).indexOf( 'ECONNREFUSED') >= 0) {
        console.log( '... not logged due to API disability');
        return;
    }

    rm.post( ["system","log"], o, function( err,body){

        if (err) {
            console.log('rm.log: POST '+rm.client.host+'system/log ERROR #', err);
        }
    });
}

rm.log={};
rm.log.syserr 	 = function ( msg, src) { log_( "syserr", 	msg, src); };
rm.log.error 	 = function ( msg, src) { log_( "error", 	msg, src); };
rm.log.warning 	 = function ( msg, src) { log_( "warning", 	msg, src); };
rm.log.useraction= function ( msg, src) { log_( "useraction", msg, src); };
rm.log.sysaction = function ( msg, src) { log_( "sysaction", msg, src); };
rm.log.info 	 = function ( msg, src) { log_( "info", 		msg, src); };
rm.log.debug 	 = function ( msg, src) { log_( "debug", 	msg, src); };


/* ---------------------------------------------------------------------------------------------------------------------
	LOGGING API
    level ... string, predefined level at which to log the message
    msg ... -> rm.log
    src ... -> rm.log
	shorthand for SENDING NOTIFICATIONS
--------------------------------------------------------------------------------------------------------------------- */
rm.sendNotifications = function( customer_id, receivers, smssubject, mailsubject, mailbody, onError){

    if(!receivers) {
        console.log( 'SENDNOTIFICATIONS - no receivers defined!');
        return;
    }

    var recs= receivers.split(/;|,/);
    var sms = [];
    var mails=[];
    for( var i in recs) {
        var s = recs[i].trim();
        if (s){
            if (s.indexOf('@') >= 0) mails.push(s);
            else                     sms.push(s);
        }
    }
    console.log( 'SENDNOTIFICATIONS to sms:', sms, ', emails:', mails,', msg:',smssubject || mailsubject);

    if (mailsubject && mailbody && (mails.length > 0)){
        rm.post(
            ['customers',customer_id,'cn-sendmail'],
            { receivers: mails,                        // z.b. ['aa@microtronics.at, lfm@lfm.co.at'],
                subject  : mailsubject,
                body     : '<html>' + mailbody + '</html>'
            },
            function(err,data){
                if(err && onError) onError(err,data);
            }
        );
    }
    if (smssubject && (sms.length > 0)){
        rm.post(
            ['customers',customer_id,'cn-sendsms'],
            { receivers: sms,                        // z.b. ['+43 680 221457322, +43 1 123345'],
                subject  : smssubject
            },
            function(err,data){
                if(err && onError) onError(err,data);
            }
        );
    }
};


/* ---------------------------------------------------------------------------------------------------------------------
	LOCAL DB CACHE API
--------------------------------------------------------------------------------------------------------------------- */
rm.openCache = function( cache_id, defaults){

    defaults = defaults || {};

    var filename = './../../apps/data/__bapi__' + cache_id + '.json';
    var f;
    if (fs.existsSync( filename)) {						// load from file
        var txt = fs.readFileSync(filename);
        // todo: add try catch here if file was destroyed on last write and log event (user has to use bak file instead)
        f = JSON.parse( txt);
    }
    if (!f) f= defaults;

    f.__filename__= filename;
    f.__dirty__   = false;

    f.update = function(){
        f.__dirty__= true;
    };

    f.flush = function(){

        if (f.__dirty__) {

            f.__dirty__= false;

            fs.writeFile( f.__filename__, JSON.stringify( f), function (err) {

                if (err) rm.log.error( 'CACHE ' + f.__filename__ + ' saving error #' + JSON.stringify(err));
            });
        }
    };

    // isntall lazy saving of private storage - every 15s, if dirty
    setInterval( function(){ f.flush(); },	15000 );

    return f;
};




/* ---------------------------------------------------------------------------------------------------------------------
	 MULTI-QUERY API
 --------------------------------------------------------------------------------------------------------------------- */
rm.exec= function( jobs, onFinished){

    async.each( Object.keys(jobs),
        function(job_key,onJobDone){

            var job = jobs[job_key];

            var verb;
            if      (job.get)  verb='get';
            else if (job.put)  verb='put';
            else if (job.post) verb='post';
            else assert(false, 'unkown verb!');
            rm[verb]( job[verb], job.params||null, function(err,data){
                // return result-data only if no error occured (any err is already logged by the core function)
                jobs[job_key].err = err || null;
                if (!err) jobs[job_key].data= data;

                onJobDone(err);
            });
        },
        function(err){
            onFinished(err, jobs);
        }
    );
};

/* ---------------------------------------------------------------------------------------------------------------------
	SUBSCRIPTIONS API
--------------------------------------------------------------------------------------------------------------------- */

rm.stats = 3; // 0=none, 1=daily summary, 2=hourly summary, 3=verbose-each cycle end



var __statsLongterm = {
    callTime:0,
    callCount:0,
    qjobCount:0,
    pollCount:0,
    pollErr:0,
    pollResTime:0,
    pollSitesTime:0
};

var __poll_sitelist_initialized = false;
var __subcriber_socket_failed= false;

var __subs=[];
/* per sub: {
 appid:APP_ID,
 // customer_id, site_id
 handler:myTask,
 resources:{				// list of resources and their poll interval (TODO: 0=data triggered)
 config0:5,
 config1:5
 }

 // seconds 'till next polling
 timers:{ config0:0, config1:... }

 cache[_uid]:{
 // von site watcher aktualisiert
 _tag:xxx            markiert lebende sites - zum entfernen gelöschter sites aus dem cache
 customer_id
 site_id

 // von data watcher aktualisiert
 config0:null,		    // object, or null = not set yet
 config0_prev:null,  // object, or null = not set yet
 config1...
 }
 } */

/*
 appid,    ... name of app template
 resources ... list of resources and their poll interval
 these resources are always set to 0 (data triggered): config0, alarm

 implicit (hidden) resources (automatically generated):
 _site ... compact site information ... automatically updated whenever possible (boot,site-list-refresh,site res polled, any data trgd event)

 handler function(cache, res, onTaskDone) ...
 cache ... {	customer_id, site_id, _site, config0, config0_prev, config0_fault, ... }
 res ... '_site', 'config0',... resource recently changed (triggering task execution)
 onTaskDone(err) ...  callback for async processing
 */
rm.subscribe = function( appid, resources, handler){

    var o = {
        appid : 	appid,
        handler: 	handler,
        resources: 	_.clone( resources),
        timers: 	_.clone( resources),
        cache : 	{}
    };

    // these resources are always data-triggered
    if (o.resources.alarm)   o.resources.alarm  = 0;

    if (!__subcriber_socket_failed){
        if (o.resources.config0) o.resources.config0= 0;
    }

    // issue at least a single init fetch during startup sequence
    for( var tmr in o.timers) o.timers[tmr]= 1;

    // add hidden-resource "_site" which can not be explicitely polled!
    o.resources['_site']= 0;

    __subs.push( o);
};

/* reduce any verbose site information structure to it's compact form */
function _compactSite( site){
    var _site = {
        _uid: 			site._uid,
        site_id: 		site.site_id,
        name : 			site.name,
        class: 			site.class,
        state: 			site.state,
        device: 		null
    };

    if (site.device) _site.device = {
        device_id: 	site.device.device_id,
        stamp:		site.device.stamp,
        state:		site.device.state,
        msim_state:	site.device.msim_state,
        pos: {
            lat:	site.device.pos.lat,
            long:	site.device.pos.long
        },
        con: {
            path:	site.device.con.path,
            state:	site.device.con.state
        },
        gsm: {
            level:	site.device.gsm.level,
            mcc:	site.device.gsm.mcc,
            mnc:	site.device.gsm.mnc
        }
    };

    return _site;
}

function _pollSitelist(){

    rm.get( ['sites'], function(err,sites){

        if(err) {
            rm.log.error( err, 'subscription/polling sitelist');
            return;
        }

        var statsStart=now();
        for( var isub in __subs) {
            var sub = __subs[isub];

            var tag  = now();
            for( var isite in sites){

                var site= sites[isite];

                if (site.class == sub.appid){         // dynamically add elements to the cache

                    var cache = sub.cache[site._uid] || null;
                    if (!cache) {
                        console.log('CACHE add ', site.site_id);
                        sub.cache[site._uid]= {};      // add site to cache

                        cache= sub.cache[site._uid];

                        // dynamically add resource-elements to the cache
                        for (var res in sub.resources){
                            cache[res]        = null;
                            cache[res+'_prev']= null;
                        }
                    }

                    cache._tag        = tag;               // mark site as "alive"
                    cache.customer_id = site.customer_id;  // update site's identifier
                    cache.site_id     = site.site_id;

                    // low-frequency update of compact site information ... todo remove when running all data triggered
                    _addQJob( site._uid, '_site', _compactSite( site));
                }
            }

            for( var icache in sub.cache){

                if (sub.cache[icache]._tag != tag) {
                    console.log('CACHE remove ', sub.cache[icache].site_id);
                    sub.cache[icache]= null;     // remove deleted sites from cache
                }
            }
        }

        __poll_sitelist_initialized= true;

        __statsLongterm.pollSitesTime += now().getTime() - statsStart.getTime(); // msec
    });
}

setTimeout( _pollSitelist, 1000); // initial call immediately after boot
setInterval( _pollSitelist, 30000); // todo: data driven!



// refresh data for a  ***single site & resource*** and call handler if changed
function _pollSingleResource( sub, cache, res, site_uid, onData, onDone){

    if (!cache.customer_id) return; // may happen up on cloning an existing site!

    var uri=['customers',cache.customer_id,'sites',cache.site_id];
    switch (res){
        case "site"     : break;
        case "config0"  :
        case "config1"  :
        case "config2"  :
        case "config3"  :
        case "config4"  :
        case "config5"  :
        case "config6"  :
        case "config7"  :
        case "config8"  :
        case "config9"  : uri.push(res); break;
        case "histdata0":
        case "histdata1":
        case "histdata2":
        case "histdata3":
        case "histdata4":
        case "histdata5":
        case "histdata6":
        case "histdata7":
        case "histdata8":
        case "histdata9": uri.push(res); uri.push('youngest'); break;    // todo: liefert stamp only!!!
        default: assert( false, 'subscription for unknown resource "'+res+'"');
    }

    rm.get( uri, function(err,data){

        if (err) {
            cache[res+'_fault']= err;
            return onDone(err);          // TODO kann durch outdated site-list verursacht werden (site nicht mehr vorhanden) - was tun?
        }

        if (_.isEqual( data, cache[res])) {	// really changed?
            return onDone();
        }

        onData(site_uid,res,data);
    });
}

var __polling = 0;
function _pollAllResources(){

    if (!__poll_sitelist_initialized) return; // reject if full site list loaded yet

    if (__polling > 0) {
        console.log( 'ERR polling overflow');
        // todo add overall stats
        return;
    }
    __polling++;

    var statsStart = now();
    for ( var isub in __subs){
        var sub = __subs[ isub];

        for( var res in sub.resources){

            if (sub.timers[ res] > 1) {
                sub.timers[ res]--;
            }
            else if (sub.timers[ res] == 1) {
                sub.timers[ res] = sub.resources[res];	// reload timer interval

                for (var site_uid in sub.cache){      // cache implies site-list - iterate through site list
                    var cache= sub.cache[ site_uid];
                    // -> refresh data and call handler if changed
                    __statsLongterm.pollCount++;
                    _pollSingleResource( sub, cache, res, site_uid,
                        _addQJob,
                        function(err){	// todo für async vorbereitet... onDone(err);
                            if (err) __statsLongterm.pollErr++;
                        }
                    );
                }
            }
            // else <= 0 ... do not poll - it's data triggered
            else {
                // config0-fallback to 1s polling if socket not available
                if (__subcriber_socket_failed && (res=='config0')) {
                    sub.resources[res] = 1;
                    sub.timers[ res] = 1;
                }
            }
        }
    }
    __polling--;
    __statsLongterm.pollResTime += now().getTime() - statsStart.getTime(); // msec
}

setInterval( _pollAllResources, 1000);



/* qjob: { site_uid, res, data } */
function _processQJob( qjob, qjobDone){

    var stats_startTime = now();

    /* scan all subscribers...
     and generate todo's to process */
    var todos=[];
    for( var isub in __subs){

        var sub= __subs[ isub];

        var cache= sub.cache[ qjob.site_uid] || null;
        if (!cache) continue;                           		// site._uid part of this subscription?
        if ((qjob.res.substr(0,1) != '!') && 					// resource is app-specific?
            (sub.resources[ qjob.res] === undefined)) continue; // resource part of this subscription ?

        if (_.isEqual( qjob.data, cache[qjob.res])) continue; 	// cache is already up-to-date ?

        todos.push({
            res:	qjob.res,
            data:	qjob.data,
            cache:	cache,
            sub:	sub
        });
    }
    if (!todos.length) return qjobDone();

    async.each( todos,
        function(todo,onDone){

            var cache = todo.cache;
            var res   = todo.res;

            /*--- workaround --- polling to fix config0 is always empty issue --- todo rem */
            /*
            if (_.isEmpty( todo.data)) {
                console.log('!!! ERROR ',res,' empty - use polling workaround');

                _pollSingleResource( todo.sub, cache, res, qjob.site_uid,
                    function( site_uid, res, data){ // ondataavailable
                        cache[res+'_prev']= cache[res];
                        cache[res]	      = data;

                        for( var r in todo.sub.resources)
                            if (!cache[r]) return onDone();  // suppress handler as long as not all required resources have been collected

                        todo.sub.handler( cache, res, onDone);
                    },
                    onDone
                );
            }
            else { */
            /*--- /workaround */


            // workaround --- todo site_name sollte immer teil von site sein!
            if ((res == '_site') && (todo.data.name == '*')) todo.data.name = cache[res].name;

            if (res == 'site') {			// missuse "full site info" to update "compact site info", too
                cache['_site_prev']= cache['_site'];
                cache['_site']	   = _compactSite( todo.data);
            }

            cache[res+'_prev']= cache[res];
            cache[res]	      = todo.data;

            for( var r in todo.sub.resources)
                if (!cache[r]) return onDone();  // suppress handler as long as not all required resources have been collected

            todo.sub.handler( cache, res, onDone);
            // todo rem }
        },
        function(err){
            __statsLongterm.qjobCount++;
            __statsLongterm.callCount += todos.length;
            __statsLongterm.callTime  += now().getTime() - stats_startTime.getTime(); // msec

            if (rm.stats >= 3) _dumpStats();
            qjobDone(err);
        }
    );
}


// any bapi trigger event is queued to maintain order of events
var ___bapi_queue = async.queue( _processQJob, 1);

function _addQJob( site_uid, res, data){
    ___bapi_queue.push( {
        site_uid:site_uid,
        res:res,
        data:data
    });
}


var __qjobTimers={}; // list of "<site_uid>/<res>" : setTimeout-id pairs

// starts a new timeout for site_uid/res - and cancels any pending timer
// msec < 0 ... cancel pending timeout only, do not start a new one
// data ... optional; { stamp:stamp() } will be added automatically
rm.setTimeoutQJob = function( msec, site_uid, res, data){

    var tmrId = site_uid + '/' + res;

    clearTimeout( __qjobTimers[ tmrId]); // clear any pending timeout for this site/res
    if (msec < 0) return;

    data = data || {};				     // add current stamp to guarantee change of data
    data.stamp= stamp();				 // (otherwise the qjob scheduler wont' call the task handler)

    __qjobTimers[ tmrId]= setTimeout(
        function(){
            _addQJob( site_uid, res, data);
        },
        msec
    );
};




/* ---------------------------------------------------------------------------------------------------------------------
	API /EXT - DYNAMIC EXTENSIONS
--------------------------------------------------------------------------------------------------------------------- */

function initExtensions(){

    var __ext_clt = __io_clt( __CONFIG.ext_host);	// default installation on appliances

    var __ext_list={};
    var __ext_conerr= false;

    __ext_clt.on('connect', function(){
        rm.log.info( 'CONNECT', 'BAPI/EXT');
        if(__ext_conerr) {
            __ext_conerr= false;
            for( var extpath in __ext_list) ext_add(extpath); // re-register paths after conerror
        }
    });

    __ext_clt.on('connect_error', function(err){
        if (__ext_conerr) return; // ignore any follow-up error

        __ext_conerr= true;
        __CONFIG.ext_onError( 'conerr', err, 'error');
    });

    __ext_clt.on('disconnect', function(){
        __ext_conerr= true;
        __CONFIG.ext_onError( 'disc', '', 'warning');
    });


    __ext_clt.on('ext-do', function(data,cb){

        var ext= __ext_list[ data.path];
        if (!ext) {
            return cb( 500, 'E_INTERNAL unkown extpath (client) "'+ data.path +'"');
        }
        ext.handler( data.placeholders, data.data, cb);
    });


    function ext_add( extpath){

        __ext_clt.emit( 'ext-add', {path:extpath,ulevel:__ext_list[extpath].ulevel}, function(err){
            if(err) __CONFIG.ext_onError( 'extend', err, 'error');
        });
    }

    return(
        function( ulevel, extpath, onHandle){

            rm.log.debug( 'ADD '+extpath, 'BAPI/EXT');
            __ext_list[ extpath]={
                handler: onHandle,
                ulevel : ulevel
            };
            ext_add( extpath);
        }
    );
}

var __extensions;

rm.extend= function( ulevel, extpath, onHandle){

    if (!__extensions) __extensions= initExtensions();	// very late init - upon first use

    __extensions( ulevel, extpath, onHandle);
}





function _dumpStats( reftime){

    var msecpercall = __statsLongterm.callCount ? (__statsLongterm.callTime / __statsLongterm.callCount).toFixed(0) : '---';
    var cpuload     = reftime ? 'load='+(__statsLongterm.callTime/reftime*100).toFixed(1)+'%' : '';

    console.log(
        '===== STATS total exectime=', __statsLongterm.callTime+'ms',
        cpuload,
        msecpercall + 'ms/call',
        __statsLongterm.callCount+'calls',
        __statsLongterm.qjobCount+'jobs',
        __statsLongterm.pollErr, 'of',
        __statsLongterm.pollCount, 'errornous poll cycles',
        __statsLongterm.pollSitesTime + 'ms sites poll',
        __statsLongterm.pollResTime + 'ms res poll'
    );


    if (reftime){
        __statsLongterm.callCount = 0;
        __statsLongterm.qjobCount = 0;
        __statsLongterm.callTime  = 0;
        __statsLongterm.pollErr   = 0;
        __statsLongterm.pollCount = 0;
        __statsLongterm.pollResTime= 0;
        __statsLongterm.pollSitesTime= 0;
    }
}

setInterval(
    function(){ if (rm.stats == 2) _dumpStats( 3600*1000);	},
    3600*1000
);
setInterval(
    function(){ if (rm.stats == 1) _dumpStats( 24*3600*1000);	},
    24*3600*1000
);






/* *********************************************************************************************************************
 	export rm2m module functions (for nodejs and browser)
 ********************************************************************************************************************* */
(function(exports){

    exports.fapi = function(host){

        __CONFIG = _.extend( __CONFIG, {host:host});	// fallback to local BAPI host

        rm.client = request.newClient( __CONFIG.host);
        rm.log.sysaction( '**STARTED as FAPI**', 'init');	// create **STARTED** item
        return rm;
    };

    exports.init = exports.fapit;

    exports.bapi = function(DEV){

        __CONFIG = _.extend( __CONFIG, DEV);

        rm.client = request.newClient( __CONFIG.host);

        if (__CONFIG.username) {
            rm.client.setBasicAuth( __CONFIG.username, __CONFIG.password);
        }

        var ws_bapi= __io_clt( __CONFIG.host_bapi);

        ws_bapi.on('connect', function(){
            rm.log.info( 'connected', 'BAPI');
            if (__subcriber_socket_failed){
                rm.log.warning( 'ws_bapi services are back again -> config0:remains with 1s poll, alarm:now available', 'BAPI');
                __subcriber_socket_failed= false;
            }
        });
        ws_bapi.on('connect_error', function(err){
            if (!__subcriber_socket_failed) {
                rm.log.warning( 'ws_bapi services are not available for remote access -> config0:downgraded to 1s poll, alarm:not available', 'BAPI');
                __subcriber_socket_failed= true;
            }
        });
        ws_bapi.on('connect_timeout', function(err){
            rm.log.error( 'ws_bapi error: '+err, 'BAPI');
        });
        ws_bapi.on('disconnect', function(){
            rm.log.warning( 'disconnected','BAPI');
        });
        ws_bapi.on('event', function(event) {		// alarm logic for notifications

            // update cache - config0
            if (event.hasOwnProperty( 'config0')) _addQJob( event.site._uid, 'config0', event.config0);

            // update cache - alarm
            if (event.hasOwnProperty( 'alarm')) _addQJob( event.site._uid, 'alarm', event.alarm);

            // update cache - _site
            event.site.device= event.device;
            event.site.name  = '*';				// todo site_name sollte immer teil von site sein!
            _addQJob( event.site._uid, '_site', event.site);
        });

        rm.log.sysaction( '**STARTED as BAPI**', 'init');	// create **STARTED** item
        return rm;
    }

})(typeof exports === 'undefined'? this['rapidm2m-fuapi']={} : module.exports);
