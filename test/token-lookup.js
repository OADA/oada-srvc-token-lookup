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
const config = require('../config');
const moment = require('moment');
const _ = require('lodash');
const expect = require('chai').expect;
const Promise = require('bluebird');
const bcrypt = require('bcryptjs');
const kf = require('kafka-node');

const userdocs = require('./users.json');
const clientdocs = require('./clients.json');
const tokendocs = require('./tokens.json');
const codedocs = require('./codes.json');

let libs = {};

// To test the token lookup, have to make a test database and populate it
// with token and user 
let db;
let dbname;
let cols;
let colnames;
let frankid = null;

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
    db = new Database(config.get('arango:connectionString'));
    dbname = 'oada-srvc-token-lookup-test';
    config.set('arango:database',dbname);
    cols = config.get('arango:collections');
    colnames = _.values(cols);

		// get the kafka stuff for testing:
		consTopic = config.get('kafka:consumerTopic');
		prodTopic = config.get('kafka:producerTopic');
		
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

    return db.createDatabase(dbname)
    .then(() => {
      db.useDatabase(dbname);

      // Create collections for users, clients, tokens, etc.
      return Promise.map(colnames, c => db.collection(c).create());
    }).then(() => { 

      // Create the indexes on each collection:
      return Promise.all([
        db.collection(cols.users)  .createHashIndex('username', { unique: true, sparse: true }),
        db.collection(cols.clients).createHashIndex('clientId', { unique: true, sparse: true }),
        db.collection(cols.tokens) .createHashIndex(   'token', { unique: true, sparse: true }),
        db.collection(cols.codes)  .createHashIndex(    'code', { unique: true, sparse: true }),
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
          users: Promise.all(_.map(    hashed, u => db.collection(cols.users)  .save(u))),
        clients: Promise.all(_.map(clientdocs, c => db.collection(cols.clients).save(c))),
         tokens: Promise.all(_.map( tokendocs, t => db.collection(cols.tokens) .save(t))),
          codes: Promise.all(_.map(  codedocs, c => db.collection(cols.codes)  .save(c))),
      });
    }).then(() => {
      // get Frank's id for test later:
      return db.collection('users').firstExample({username: 'frank'}).then(f => frankid = f._key);
    // Done!
    }).then(() => {
      libs = {
				users: require('../../../auth/oada-ref-auth-js/db/arango/users'),
        clients: require('../../../auth/oada-ref-auth-js/db/arango/clients'),
				tokens: require('../../../auth/oada-ref-auth-js/db/arango/tokens'),
				codes: require('../../../auth/oada-ref-auth-js/db/arango/codes'),
      };
    }).catch(err => {
      console.log('FAILED to initialize arango tests by creating database '+dbname);
      console.log('The error = ', err);
    });
  });


  //--------------------------------------------------
  // The tests!
  //--------------------------------------------------

  describe('.users', () => {
    it('should be able to find frank by his id', done => {
      libs.users.findById(frankid, (err,u) => {
        expect(err).to.be.a('null');
        expect(u.username).to.equal('frank');
        done();
      });
    });
    it('should be able to find frank with his password', done => {
      libs.users.findByUsernamePassword('frank', 'test', (err,u) => {
        expect(err).to.be.a('null');
        expect(u.username).to.equal('frank');
        done();
      });
    });
  });

  describe('.clients', () => {
    it('should be able to find the initial test client', done => {
      const clientId = '3klaxu838akahf38acucaix73@identity.oada-dev.com';
      libs.clients.findById(clientId, (err,c) => {
        expect(err).to.be.a('null');
        expect(c.clientId).to.equal(clientId);
        done();
      });
    });

    it('should be able to successfully save a new client', done => {
      const newclient = _.cloneDeep(clientdocs[0]);
      newclient.clientId = '12345abcd';
      libs.clients.save(newclient, (err,c) => {
        expect(err).to.be.a('null');
        expect(c.clientId).to.equal(newclient.clientId);
        done();
      });
    });
  });

  describe('.codes', () => {
    it('should be able to find the initial test code', done => {
      libs.codes.findByCode('xyz', (err,c) => {
        expect(err).to.be.a('null');
        expect(c.code).to.equal('xyz');
        done();
      });
    });

    it('should be able to successfully save a new code', done => {
      const newcode = _.cloneDeep(codedocs[0]);
      newcode.code = '012345abcd';
      newcode.user = { _id: frankid};
      libs.codes.save(newcode, (err,c) => {
        expect(err).to.be.a('null');
        expect(c.code).to.equal(newcode.code);
        done();
      });
    });
  });


  describe('.tokens', () => {
    it('should be able to find the initial test token', done => {
      libs.tokens.findByToken('xyz', (err,t) => {
        expect(err).to.be.a('null');
        expect(t.token).to.equal('xyz');
        done();
      });
    });

    it('should be able to successfully save a new token', done => {
      const newtoken = _.cloneDeep(tokendocs[0]);
      newtoken.token = '012345abcd';
      newtoken.user = { _id: frankid};
      libs.tokens.save(newtoken, (err,t) => {
        expect(err).to.be.a('null');
        expect(t.token).to.equal(newtoken.token);
        done();
      });
    });
  });

	describe('.token-lookup', () => {
		it('should be able to produce to token_request topic', () => {
			var t = {};
			t.resp_partition = 0;
			t.connection_id = 1234567890;
			t.token = "xyz";

			return producer.then(prod => {
				return prod.sendAsync([
					{ topic: prodTopic, messages: JSON.stringify(t) }
				]);
			});
		});

		// token-lookup should already published a http_response message by now

		it('should be able to consume from http_repsonse topic', done => {
			consumer.on('message', msg => Promise.try(() => {
				const httpMsg = JSON.parse(msg.value);
				expect(httpMsg.connection_id).to.equal(1234567890);
				done();
			}));
		});
	});

  //-------------------------------------------------------
  // After tests are done, get rid of our temp database
  //-------------------------------------------------------
  after(() => {
    db.useDatabase('_system'); // arango only lets you drop a database from the _system db
    return db.dropDatabase(dbname)
    .then(() => { console.log('Successfully cleaned up test database '+dbname); })
    .catch(err => console.log('Could not drop test database '+dbname+' after the tests! err = ', err));
  });
});
