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

const debug = require('debug');
const trace = debug('token-lookup:trace');
const info = debug('token-lookup:info');
const error = debug('token-lookup:error');

const Promise = require('bluebird');
const kf = require('kafka-node');
const oadaLib = require('oada-lib-arangodb');
const config = require('./config');

//---------------------------------------------------------
// Kafka intializations:
const client = new kf.Client(
  config.get('zookeeper:host'),
  config.get('zookeeper:tokenRequest')
);
const offset = new kf.Offset(client);
const consumer = new kf.ConsumerGroup({
  host: config.get('zookeeper:host'),
  groupId: 'token-lookup',
  fromOffset: 'latest'
}, [config.get('kafka:topics:tokenRequest')]);
let producer = new kf.Producer(client, {partitionerType: 0});

process.on('exit', () => {console.log('ere'); client.close()});
process.on('SIGINT', () => {console.log('ere'); client.close(); process.exit(2);});
process.on('uncaughtException', (a) => {console.log('ere', a); client.close(); process.exit(99);});


consumer.on('message', (msg) => {
  return Promise.try(() => {
      return JSON.parse(msg.value);
    })
    .then((req) => {
      if (!req ||
        typeof req.resp_partition === "undefined" ||
        typeof req.connection_id === "undefined" ||
        typeof req.token === "undefined") {
        throw new Error(`Invalid token_request ${JSON.stringify(req)}`);
      }

      const res = {
        type: 'http_response',
        token: req.token,
        token_exists: false,
        partition: req.resp_partition,
        connection_id: req.connection_id,
        doc: {
          user_id: null,
          scope: [],
          bookmarks_id: null,
          client_id: null,
        }
      };

      // Get token from db.  Later on, we should speed this up
      // by getting everything in one query.
      return oadaLib.tokens.findByToken(req.token.trim().replace(/^Bearer /,''))
        .then(t => {
          if(!t) {
            info('WARNING: token '+req.token+' does not exist.');
            return res;
          }

          if(!t.user) {
            info(`user for token ${t.token} not found`);
            t.user = {};
          }

          if(!t.user.bookmarks) {
            info(`No bookmarks for user from token ${t.token}`);
            t.user.bookmarks = {};
          }

          res.token_exists = true;
          res.doc.client_id = t.clientId;
          res.doc.user_id = t.user._id || res.doc.user_id;
          res.doc.bookmarks_id = t.user.bookmarks._id || res.doc.bookmarks_id;
          res.doc.scope = t.scope || res.doc.scope;

          return res;
      });
  })
  .then((res) => {
    return Promise.fromCallback((done) => {
      producer.send([{
        topic: config.get('kafka:topics:httpResponse'),
        messages: JSON.stringify(res)
      }], done);
    });
  })
  .catch(err => {
    error('%O', err);
  })
  .finally(() =>
    offset.commit('token-lookup', [{
      topic: config.get('kafka:topics:tokenRequest'),
      partition: msg.partition,
      offset: msg.offset
    }])
  );
});

