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

const debug = require('debug')('token-lookup');
const kf = require('kafka-node');
const Database = require('arangojs').Database;
const Promise = require('bluebird');

//---------------------------------------------------------
// Kafka intializations:
const client = Promise.promisifyAll(new kf.Client("zookeeper:2181", "token_lookup"));
const offset = Promise.promisifyAll(new kf.Offset(client));
const groupid = 'token_lookups';
const topic = 'token_request';
const consumer = Promise.promisifyAll(new kf.ConsumerGroup({
  host: 'zookeeper:2181', 
  groupId: groupid,
  fromOffset: 'latest'
}, [ topic ]));
let producer = Promise.promisifyAll(new kf.Producer(client, {
  partitionerType: 0
}));

const libs = {
   tokens: Promise.promisifyAll(require('oada-ref-auth/db/arango/tokens')),
    users: Promise.promisifyAll(require('oada-ref-auth/db/arango/users')),
};


//--------------------------------------------------
// Create topic if it doesn't exist:
producer = producer.onAsync('ready')
.then(() => prod.createTopicsAsync(['http_request'], true)
.then(() => debug('Producer topic http_request created');


//--------------------------------------------------
// Consume message for a token lookup:
consumer.on('message', msg => Promise.try(() => {
  const req = JSON.parse(msg.value);
  console.log("parsed json message is: ", jsonMsg);

  if (typeof req.partition     === 'undefined') debug('WARNING: request '+req+' does not have partition');
  if (typeof req.connection_id === 'undefined') debug('WARNING: request '+req+' does not have connection_id');
  if (typeof req.token         === 'undefined') debug('WARNING: request '+req+' does not have token');

  const res = {
    type: 'token_response',
    token: req.token,
    token_exists: false,
    partition: req.partition,
    connection_id: req.connection_id,
    doc: { 
      userid: null,
      scope: [],
      bookmarksid: null,
      clientid: null,
    }
  };

  // Get token from db.  Later on, we should speed this up 
  // by getting everything in one query.
  return libs.tokens.findByTokenAsync(token)
  .then(t => {
    if (t) { res.token_exists = true; }
    else { debug('WARNING: token '+token+' does not exist.'); }

    // Save the client in case we have a rate limiter service someday
    res.doc.clientId = t.clientId;

    // If there is no user, we can't lookup bookmarks
    if (!t.user || typeof t.user._id === 'undefined') {
      debug('WARNING: user for token '+token+' does not exist.');
      return res;
    }
    res.doc.userid = t.user._id;

    // If we have a user, lookup bookmarks:
    return libs.users.findByIdAsync(t.user._id);
    .then(u => {
      if (u && u.bookmarks && typeof u.bookmarks._id !== 'undefined') {
        res.doc.bookmarksid = u.bookmarks._id;
      } else {
        debug('WARNING: user '+res.doc.userid+' not found!');
      }
    });
  }).then(() => {

    // response is built now, post up to http_response
    return producer.then(prod => {
      debug('Producing message: ', res);
      return prod.sendAsync([
        { topic: 'http_response', messages: JSON.stringify(httpRespMsg) }
      ]);
    });

  }).catch(err => {
    debug('ERROR: failed to fetch token info for token '+token+'.  Error is: ', err);

  // Regardless of if something went wrong, we want to commit the message to prevent
  // errors from resulting in infinite re-processing of messages
  }).finally(() => 
    offset.commitAsync(groupid, [ { topic: topic, partition: msg.partition, offset: msg.offset } ]);
  );
  
});

