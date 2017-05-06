'use strict';

var path = require('path');
var fs = require('fs');

module.exports = {
	init: '../../auth/oada-ref-auth-js/db/arango/init',                                                     
  server: {                                                                     
    sessionSecret: "2jp901p3#2#(!)kd9",                                         
    passwordSalt: "$2a$06$xbh/gQcEgAX5eapjlCgMYO",                              
    port: 80,                                                                   
    mode: "http",                                                               
    domain: "localhost", // in docker it's port 80 localhost                    
    publicUri: "https://localhost" // but to nginx proxy, it's https://localhost in dev
  },                                                                            
  wellknownPrefix: "/oadaauth",                                                 
  // Prefix should match nginx proxy's prefix for the auth service              
  //endpointsPrefix: "/oadaauth",                                               
  keys: {                                                                       
    signPems: path.join(__dirname,"sign/"),                                     
  },                     
  arango: {
    connectionString: "http://arangodb:8529",
    collections: {
      users: "users",
      clients: "clients",
      tokens: "tokens",
      codes: "codes",
    },
    defaultusers: [
      {   username: "frank",           password: "test",
              name: "Farmer Frank", family_name: "Frank",
        given_name: "Farmer",       middle_name: "",
          nickname: "Frankie",            email: "frank@openag.io",
      },
    ],
  },
  datastores: {
		users: './test/users',
		clients: './test/clients',
		tokens: './test/tokens',
		codes: './test/codes',
  },
  hint: {
    username: 'frank',
    password: 'pass'
  },
	kafka: {
		testConsumerTopic: "http_response",
		testProducerTopic: "token_request",
		consumerTopic: "token_request",
		producerTopic: "http_response"
	}
};
