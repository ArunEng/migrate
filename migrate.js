const { MongoClient, ObjectID } = require('mongodb');
const _ = require('lodash');
const { createLogger } = require('winston');
const { format, transports } = require('winston');
const { prettyPrint } = format

const url = 'mongodb://localhost:27017/test'

const client = new MongoClient(url);

const logger = createLogger({
    level: 'info',
    format: format.combine(
        format.colorize(),
        format.timestamp({
            format: 'YYYY-MM-DD HH:mm:ss'
        }),
        format.errors({ stack: true}),
        format.splat(),
        format.json(),
        prettyPrint(),
    ),
    transports: [
        new transports.File({filename: 'live-logs/not_updated.log', level: 'error'}),
        new transports.File({ filename: 'live-logs/updated.log', level: 'info'}),
        new transports.Console({ handleExceptions: true, format: format.colorize() })
    ],
    exceptionHandlers: [
        new transports.File({ filename: 'live-logs/exceptions.log'})
    ],
    exitOnError: false
});

let migrate = async () => {
    try{
        await client.connect();
        const db = client.db('mct_live');
        const collection = db.collection('historyPopulation');

        let aggregationPipeline = [
            {
              '$match': {
                'mainType': 'USERS', 
                'subType': 'USER_DEACTIVATION', 
                'createdAt': {
                  '$lt': new Date('Thu, 31 Dec 2020 00:00:00 GMT')
                }, 
                'user.scopes': {
                  '$nin': [
                    'ATHLET', 'COACH'
                  ]
                }
              }
            }, {
              '$project': {
                '_id': 1, 
                'miscellaneous': 1, 
                'user': {
                  '$arrayElemAt': [
                    '$user', 0
                  ]
                }, 
                'tId': 1, 
                'seasonId': 1, 
                'createdAt': 1
              }
            }, {
              '$sort': {
                'createdAt': 1
              }
            }, {
              '$group': {
                '_id': {
                  'tId': '$tId', 
                  'seasonId': '$seasonId', 
                  'userId': '$user._id'
                }, 
                'user': {
                  '$last': '$user'
                }, 
                'miscellaneous': {
                  '$last': '$miscellaneous'
                }, 
                'createdAt': {
                  '$last': '$createdAt'
                }, 
                'requestId': {
                  '$last': '$_id'
                }
              }
            }, {
              '$unionWith': {
                'coll': 'historyPopulation', 
                'pipeline': [
                  {
                    '$match': {
                      'subType': 'USER_ADD'
                    }
                  }, {
                    '$project': {
                      '_id': 1, 
                      'miscellaneous': 1, 
                      'user': {
                        '$arrayElemAt': [
                          '$user', 0
                        ]
                      }, 
                      'tId': 1, 
                      'seasonId': 1, 
                      'createdAt': 1
                    }
                  }, {
                    '$sort': {
                      'createdAt': 1
                    }
                  }, {
                    '$group': {
                      '_id': {
                        'tId': '$tId', 
                        'seasonId': '$seasonId', 
                        'userId': '$user._id'
                      }, 
                      'user': {
                        '$last': '$user'
                      }, 
                      'miscellaneous': {
                        '$last': '$miscellaneous'
                      }, 
                      'createdAt': {
                        '$last': '$createdAt'
                      }
                    }
                  }
                ]
              }
            }, {
              '$project': {
                '_id': 0, 
                'userId': '$_id.userId', 
                'tId': '$_id.tId', 
                'seasonId': '$_id.seasonId', 
                'createdAt': 1, 
                'scopes': '$user.scopes', 
                'miscellaneous': 1, 
                'requestId': 1
              }
            }, {
              '$sort': {
                'createdAt': 1
              }
            }, {
              '$group': {
                '_id': {
                  'tId': '$tId', 
                  'seasonId': '$seasonId', 
                  'userId': '$userId'
                }, 
                'count': {
                  '$sum': 1
                }, 
                'scopes': {
                  '$first': '$scopes'
                }, 
                'lastUpdated': {
                  '$first': '$createdAt'
                }, 
                'deactivatedAt': {
                  '$last': '$createdAt'
                }, 
                'requestId': {
                  '$last': '$requestId'
                }
              }
            }, {
              '$project': {
                'tId': '$_id.tId', 
                'seasonId': '$_id.seasonId', 
                'userId': '$_id.userId', 
                'scopes': 1, 
                'lastUpdated': 1, 
                'deactivatedAt': 1, 
                'count': 1, 
                '_id': 0, 
                'requestId': 1
              }
            }, {
              '$match': {
                'count': 2
              }
            }
        ];

        let deActivatedUser = await collection.aggregate(aggregationPipeline).toArray(); 
        let noOfDocUpdated = 0;
        for(let data of deActivatedUser){
          let user_deactivated = await collection.findOne({_id: data.requestId, mainType: 'USERS', subType: 'USER_DEACTIVATION', "user.scopes": {$nin: ['ATHLET', 'COACH']}});
          if(!_.isEmpty(user_deactivated)){
              let user_updated = await collection.findOneAndUpdate({_id: data.requestId, mainType: 'USERS', subType: 'USER_DEACTIVATION', "user.scopes": {$nin: ['ATHLET', 'COACH']}}, {$set: {"user.$[].scopes": data.scopes}}, {returnNewDocument: true});
              ++noOfDocUpdated;
              logger.info('USER UPDATED:::::\n', user_updated);
          }else{
              logger.error('USER NOT UPDATED::::::\n', user_deactivated);
          }
        }
        logger.info('No of documents update', { "No of document updated":noOfDocUpdated });
    } catch(e){
        logger.error(e)
        console.log(e);
    }finally{
        await client.close();
        logger.info('db connection closed');
    }
}

migrate();