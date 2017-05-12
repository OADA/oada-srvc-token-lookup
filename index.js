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
const kf = require('kafka-node');
const Database = require('arangojs').Database;
const Promise = require('bluebird');
const oadaLib = require('oada-lib-arangodb');
const config = require('./config');

oadaLib.init.run();

//---------------------------------------------------------
// Kafka intializations:
const client = Promise.promisifyAll(new kf.Client("zookeeper:2181", "token_lookup"));
const offset = Promise.promisifyAll(new kf.Offset(client));
const groupid = 'token_lookups';
const prodTopic = config.get('kafka:producerTopic');
const consTopic = config.get('kafka:consumerTopic');
const consumer = Promise.promisifyAll(new kf.ConsumerGroup({
  host: 'zookeeper:2181',
  groupId: groupid,
  fromOffset: 'latest'
}, [ consTopic ]));
let producer = Promise.promisifyAll(new kf.Producer(client, {
  partitionerType: 0
}));

//--------------------------------------------------
// Create topic if it doesn't exist:
producer = producer.onAsync('ready')
	.return(producer)
	.tap(function(prod) {
		return prod.createTopicsAsync([ prodTopic ], true);
	});

//--------------------------------------------------
// Consume message for a token lookup:
consumer.on('message', msg => Promise.try(() => {
  const req = JSON.parse(msg.value);

  if (typeof req.resp_partition     === 'undefined') error('WARNING: request '+req+' does not have partition');
  if (typeof req.connection_id === 'undefined') error('WARNING: request '+req+' does not have connection_id');
  if (typeof req.token         === 'undefined') error('WARNING: request '+req+' does not have token');

  const res = {
    type: 'http_response',
    token: req.token,
    token_exists: false,
    partition: req.resp_partition,
    connection_id: req.connection_id,
    doc: {
      userid: null,
      scope: [],
      bookmarksid: null,
      clientid: null,
    }
  };

  // Strip off "Bearer" if necessary:
  req.token = req.token.trim().replace(/^Bearer /,'');
  // Get token from db.  Later on, we should speed this up
  // by getting everything in one query.
  return oadaLib.tokens.findByToken(req.token)
  .then(t => {
    if(!t) {
      info('WARNING: token '+req.token+' does not exist.');
      return;
    }

    res.token_exists = true;

    // Save the client in case we have a rate limiter service someday
    res.doc.clientid = t.clientId;

    // If there is no user, we can't lookup bookmarks
    if (!t.user || typeof t.user._id === 'undefined') {
      info('WARNING: user for token '+req.token+' does not exist.');
      return res;
		}
    res.doc.userid = t.user._id;

    // If we have a user, lookup bookmarks:
    return oadaLib.users.findById(t.user._id)
    .then(u => {
      if (u && u.bookmarks && typeof u.bookmarks._id !== 'undefined') {
        res.doc.bookmarksid = u.bookmarks._id;
      } else {
        info('WARNING: user '+res.doc.userid+' not found!');
      }
    });
  }).then(() => {

    // response is built now, post up to http_response
    return producer.then(prod => {
      trace('Producing message: ', res);
      return prod.sendAsync([
        { topic: prodTopic, messages: JSON.stringify(res) }
      ]);
    });

  }).catch(err => {
		error('ERROR: failed to fetch token info for token '
			+req.token+'.  Error is: ', err);

	// Regardless of if something went wrong, we want to commit the message to
	// prevent Verrors from resulting in infinite re-processing of messages
  }).finally(() =>
		offset.commitAsync(groupid,
			[ { topic: consTopic , partition: msg.partition, offset: msg.offset } ])
  );
}));

