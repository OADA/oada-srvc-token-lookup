/* Copyright 2017 Open Ag Data Alliance
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const kf = require('kafka-node');
const Database = require('arangojs').Database;

var Promise = require('bluebird');
var moment = require('moment');
var _ = require('lodash');
var bcrypt = require('bcryptjs');

var config = require('../../auth/oada-ref-auth-js/config');
var client = Promise.promisifyAll(new kf.Client("zookeeper:2181", "token_lookup"));
var offset = Promise.promisifyAll(new kf.Offset(client));
var consumer = Promise.promisifyAll(new kf.ConsumerGroup({
  host: 'zookeeper:2181', 
  groupId: 'token_lookups',
  fromOffset: 'latest'
}, [ 'token_request' ]));
var producer = Promise.promisifyAll(new kf.Producer(client, {
	partitionerType: 0
}));

var tokendocs = require('./tokens.json');
var userdocs = require('./users.json');
var clientdocs = require('./clients.json');
var codedocs = require('./codes.json');

var db;
var dbname;
var cols;
var colnames;
var frankid = null;

var jsonMsg;
var httpRespMsg;
var partition;
var connection_id;
var token;

// library under test:
var libs = {}; // pull this later after config sets the dbname it creates

producer = producer.onAsync('ready')
	.return(producer)
	.tap(function(prod) {
		return prod.createTopicsAsync(['http_request'], true);
	});

db = new Database(config.get('arango:connectionString'));
dbname = 'oada-ref-auth_arangotest-'+moment().format('YYYY-MM-DD-HH-mm-ss');
config.set('arango:database', dbname);
cols = config.get('arango:collections');
colnames = _.values(cols);

db.createDatabase(dbname).then(() => {
	db.useDatabase(dbname);
	// Create collections for users, clients, tokens, etc.
	return Promise.map(colnames, c => db.collection(c).create());
}).then(() => { 
	// Create the indexes on each collection:
	return Promise.all([
		db.collection(cols.users).createHashIndex('username', { unique: true, sparse: true }),
		db.collection(cols.clients).createHashIndex('clientId', { unique: true, sparse: true }),
		db.collection(cols.tokens).createHashIndex('token', { unique: true, sparse: true }),
		db.collection(cols.codes).createHashIndex('code', { unique: true, sparse: true }),
	]);
}).then(() => {
	// hash the password:
	const hashed = _.map(userdocs, u => {
		const r = _.cloneDeep(u);
		r.password = bcrypt.hashSync(r.password, config.get('server:passwordSalt'));
		return r;
	});
	// Save the demo documents in each collection:
	return Promise.props({
		 users: Promise.all(_.map(hashed, u => db.collection(cols.users).save(u))),
		 clients: Promise.all(_.map(clientdocs, c => db.collection(cols.clients).save(c))),
		 tokens: Promise.all(_.map(tokendocs, t => db.collection(cols.tokens).save(t))),
		 codes: Promise.all(_.map(codedocs, c => db.collection(cols.codes)  .save(c))),
	});
}).then(() => {
	// get Frank's id for test later:
	return db.collection('users').firstExample({username: 'frank'}).then(f => frankid = f._key);
	// Done!
}).then(() => {
	libs = {
		 tokens: require('../../auth/oada-ref-auth-js/db/arango/tokens'),
		 users: require('../../auth/oada-ref-auth-js/db/arango/users'),
		 clients: require('../../auth/oada-ref-auth-js/db/arango/clients'),
		 codes: require('../../auth/oada-ref-auth-js/db/arango/codes'),
	};

}).catch(err => {
	console.log('FAILED to initialize arango tests by creating database '+dbname);
	console.log('The error = ', err);
});

consumer.on('message', function(msg) {
	jsonMsg = JSON.parse(msg.value);
	console.log("parsed json message is: ", jsonMsg);

	//TODO: valid jsonMsg
	partition = jsonMsg.resp_partition;
	connection_id = jsonMsg.connection_id;
	token = jsonMsg.token; 

	//TODO: implement when token is not found
	libs.tokens.findByToken(token, (err, t) => {
		httpRespMsg = {
			"token": token,
			"partition": partition,
			"connection_id": connection_id,
			"doc": {
				"user_id": t.user._id,
				"scope": t.scopes,
			}
		};

		//TODO: implement when id is not found
		libs.users.findById(t.user._id, (err, u) => {
			httpRespMsg.doc.bookmark_id = u.bookmarks._id;
			producer.then(prod => {
				console.log("i will produce: ", httpRespMsg);
				return prod.sendAsync([{
					topic: 'http_response', messages: JSON.stringify(httpRespMsg)
				}]);
			});
		});
	});

	//offset.commit(msg);
});

