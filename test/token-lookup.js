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
 )* limitations under the License.
 */

'use strict';

const Database  = require('arangojs').Database;
const moment = require('moment');
const _ = require('lodash');
const expect = require('chai').expect;
const Promise = require('bluebird');
const bcrypt = require('bcryptjs');
const randomstring = require("randomstring");
const kf = require('kafka-node');
const oadaLib = require('oada-lib-arangodb');
const config = require('../config');

const userdocs = require('./users.json');
const clientdocs = require('./clients.json');
const tokendocs = require('./tokens.json');
const codedocs = require('./codes.json');

// To test the token lookup, have to make a test database and populate it
// with token and user 
let db = oadaLib.arango;
let cols = config.get('arango:collections');
let frankid = null;
let random_connection_id;
let random_token;

// kafka topics 
let consTopic;
let prodTopic;
let client;
let offset;
let consumer;
let producer;
let groupid;

describe('token lookup service', () => {
  before(() => {
    // Create the test database:
		
		random_connection_id = randomstring.generate();
		random_token = randomstring.generate();

		// get the kafka stuff for testing:
		consTopic = config.get('kafka:testConsumerTopic');
		prodTopic = config.get('kafka:testProducerTopic');
		
		client = Promise.promisifyAll(new kf.Client("zookeeper:2181", "token_lookup"));
		offset = Promise.promisifyAll(new kf.Offset(client));
		
		let consOptions = { autoCommit: true };
		consumer = Promise.promisifyAll(new kf.Consumer(client, [ {topic: consTopic} ], consOptions));

		producer = Promise.promisifyAll(new kf.Producer(client, {
			partitionerType: 0
		}));

		producer = producer.onAsync('ready')
			.return(producer)
			.tap(function(prod) {
				return prod.createTopicsAsync(['token_request'], true);
			});

		return oadaLib.init.run()
		.then(() => {
			return Promise.props({
				users: db.collection(cols.users).truncate(),
				clients: db.collection(cols.clients).truncate(),
				tokens: db.collection(cols.tokens).truncate(),
				codes: db.collection(cols.codes).truncate(),
			});
		})

		.then(() => {
      // hash the password:
      const hashed = _.map(userdocs, u => {
        const r = _.cloneDeep(u);
        r.password = bcrypt.hashSync(r.password, config.get('server:passwordSalt'));
        return r;
      });
      // Save the demo documents in each collection:
      return Promise.props({
          users: Promise.all(_.map(    hashed, u => db.collection(cols.users)  .save(u))),
        clients: Promise.all(_.map(clientdocs, c => db.collection(cols.clients).save(c))),
         tokens: Promise.all(_.map( tokendocs, t => db.collection(cols.tokens) .save(t))),
          codes: Promise.all(_.map(  codedocs, c => db.collection(cols.codes)  .save(c))),
      });
    }).then(() => {
      // get Frank's id for test later:
      return db.collection('users').firstExample({username: 'frank'}).then(f => frankid = f._key);
    // Done!
    }).catch(err => {
      console.log('The error = ', err);
    });
  });


  //--------------------------------------------------
  // The tests!
  //--------------------------------------------------

  describe('.tokens', () => {
    it('should be able to find the initial test token', done => {
      oadaLib.tokens.findByToken('xyz', (err,t) => {
        expect(err).to.be.a('null');
        expect(t.token).to.equal('xyz');
        done();
      });
    });

    it('should be able to successfully save a new token', done => {
      const newtoken = _.cloneDeep(tokendocs[0]);
      newtoken.token = random_token;
      newtoken.user = { _id: frankid};
      oadaLib.tokens.save(newtoken, (err,t) => {
        expect(err).to.be.a('null');
        expect(t.token).to.equal(newtoken.token);
        done();
      });
    });
  });

	describe('.token-lookup', () => {
		it('should be able to perform a token-lookup', done => {
			// make token_request message
			var t = {};
			t.resp_partition = 0;
			t.connection_id = random_connection_id;
			t.token = random_token;

			producer.then(prod => {
				// produce token_request message
				return prod.sendAsync([
					{ topic: prodTopic, messages: JSON.stringify(t) }
				]).then(() => {
					// token_lookup service should have produced an http_response message
					// at this point
					consumer.on('message', msg => Promise.try(() => {
						const httpMsg = JSON.parse(msg.value);
						expect(httpMsg.connection_id).to.equal(random_connection_id);
						expect(httpMsg.token).to.equal(random_token);
						done();
					}));
				});
			}).catch(done);
		});
	});

  //-------------------------------------------------------
  // After tests are done, get rid of our temp database
  //-------------------------------------------------------
  after(() => {
		return oadaLib.init.cleanup();
  });
});
