import {EventEmitter} from 'events'
import {Worker, fork, setupMaster} from 'cluster'
import {join} from 'path'
import {format as formatUrl} from 'url'

import {without, find} from 'underscore'
import {auto, parallel, each, forever, doUntil} from 'async'
import {Config, Kinesis} from 'aws-sdk'
import {Logger, createLogger} from 'bunyan'
import {Queries} from 'vogels'

import {createKinesisClient} from './lib/aws/factory'
import config from './lib/config'
import {Lease} from './lib/models/Lease'
import {Cluster, Capacity as ClusterCapacity} from './lib/models/Cluster'
import {create as createServer} from './lib/server'
import streamProvider from './lib/stream-providers/kinesis-stream-provider'
import {ShardList, DescribeStreamData} from './lib/stream-providers/stream-provider'
import {Stream} from './lib/models/Stream'
import createStreamProvider from './lib/stream-providers/stream-provider-factory'


interface ClusterWorkerWithOpts extends Worker {
  opts: { shardId: string }
}

interface AWSEndpoints {
  kinesis: string
  dynamo: string
}

export interface ConsumerClusterOpts {
  streamName: string
  streamType: string,
  tableName: string
  awsConfig: Config
  dynamoEndpoint?: string
  dynamoStreamEndpoint?: string,
  localDynamo: Boolean
  kinesisEndpoint?: string
  localKinesis: Boolean
  localKinesisPort?: string
  capacity: ClusterCapacity
  startingIteratorType?: string
  logLevel?: string
  numRecords?: number
  timeBetweenReads?: number,
  customLogData?: string
}


// Cluster of consumers.
export class ConsumerCluster extends EventEmitter {
  public cluster: Cluster
  private opts: ConsumerClusterOpts
  private logger: Logger
  private isShuttingDownFromError = false
  private externalNetwork = {}
  private consumers = {}
  private consumerIds = []
  private lastGarbageCollectedAt = Date.now()
  private endpoints: AWSEndpoints
  private streamProvider: any

  constructor(pathToConsumer: string, opts: ConsumerClusterOpts) {
    super()
    this.opts = opts

    const DEFAULT_STREAM_TYPE = 'kinesis'

    if (!this.opts.streamType) {
      this.opts.streamType = DEFAULT_STREAM_TYPE
    }

    var customLogDataObject = this.opts.customLogData ? JSON.parse(this.opts.customLogData) : {}
    var loggerOptions = Object.assign({}, customLogDataObject, {
      name: 'KinesisCluster',
      level: opts.logLevel,
    })

    this.logger = createLogger(loggerOptions)

    setupMaster({
      exec: pathToConsumer,
      silent: true,
    })

    this.endpoints = {
      kinesis: this.getKinesisEndpoint(),
      dynamo: this.getDynamoEndpoint(),
    }

    this.streamProvider = createStreamProvider(this.opts)

    this.cluster = new Cluster(this.opts.tableName, this.opts.awsConfig, this.endpoints.dynamo)
    this.init()
  }

  private init() {
    auto({
      tableExists: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        this.logger.trace('Checking %s table exists', tableName)
        this.logger.trace('AWSConfig: %s', JSON.stringify(awsConfig))
        this.logger.trace('this.getDynamoEndpoint(): %s', this.getDynamoEndpoint())
        Cluster.tableExists(tableName, awsConfig, this.getDynamoEndpoint(), done)
      },

      createTable: ['tableExists', (done, data) => {
        if (data.tableExists) {
          this.logger.trace('not creating table')
          return done()
        }

        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        const capacity = this.opts.capacity || {}

        this.logger.trace({ table: tableName }, 'Creating DynamoDB table')
        Cluster.createTable(tableName, awsConfig, capacity, this.getDynamoEndpoint(), done)
      }],

      createStream: done => {
        this.logger.trace('running create stream')
        const streamName = this.opts.streamName
        this.logger.trace('stream name %s', streamName)
        const streamModel = new Stream(streamName, this.streamProvider)
        this.logger.trace('streamModel %s', streamModel)

        streamModel.exists((err, exists) => {
          this.logger.trace('streamModel exists result %s', exists)
          this.logger.trace('streamModel exists err %s', err)
          if (err) {
            return done(err)
          }

          if (exists) {
            return done()
          }

          this.logger.trace('attempting to create the stream')

          this.streamProvider.createStream({ StreamName: streamName, ShardCount: 1 }, err => {
            if (err) {
              return done(err)
            }

            streamModel.onActive(done)
          })
        })
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error ensuring Dynamo table exists')
      }

      this.loopReportClusterToNetwork()
      this.loopFetchExternalNetwork()
    })
  }

  private getKinesisEndpoint() {
    const isLocal = this.opts.localKinesis
    const port = this.opts.localKinesisPort
    const customEndpoint = this.opts.kinesisEndpoint
    let endpoint = null

    if (isLocal) {
      const endpointConfig = config.localKinesisEndpoint
      if (port) {
        endpointConfig.port = port
      }
      endpoint = formatUrl(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  private getDynamoEndpoint() {
    const isLocal = this.opts.localDynamo
    const customEndpoint = this.opts.dynamoEndpoint
    let endpoint = null

    if (isLocal) {
      var endpointConfig = config.localDynamoDBEndpoint
      endpoint = formatUrl(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  // Run an HTTP server. Useful as a health check.
  public serveHttp(port: string | number) {
    this.logger.trace('Starting HTTP server on port %s', port)
    createServer(port, () => this.consumerIds.length)
  }

  private consumeAvailableShard(shardId: string, leaseCounter: number) {
    // Stops accepting consumers, since the cluster will be reset based one an error
    if (this.isShuttingDownFromError) {
      return
    }

    this.spawn(shardId, leaseCounter)
  }

  private updateNetwork() {
    this.garbageCollectClusters()
    if (this.shouldTryToAcquireMoreShards()) {
      this.logger.trace('Should try to acquire more shards')
      this.fetchAvailableShard()
    } else if (this.hasTooManyShards()) {
      this.logger.trace({ consumerIds: this.consumerIds }, 'Have too many shards')
      this.killConsumer(err => {
        if (err) {
          this.logAndEmitError(err)
        }
      })
    }
  }

  // Compare cluster state to external network to figure out if we should try to change our shard allocation.
  private shouldTryToAcquireMoreShards() {
    if (this.consumerIds.length === 0) {
      return true
    }

    const externalNetwork = this.externalNetwork
    const networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return true
    }

    const lowestInOutterNetwork = networkKeys.reduce((memo, key) => {
      const count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length <= lowestInOutterNetwork
  }

  // Determine if we have too many shards compared to the rest of the network.
  private hasTooManyShards() {
    const externalNetwork = this.externalNetwork

    const networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return false
    }

    const lowestInOutterNetwork = networkKeys.reduce((memo, key) => {
      const count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length > (lowestInOutterNetwork + 1)
  }

  // Fetch data about unleased shards.
  private fetchAvailableShard() {
    // Hack around typescript
    var _asyncResults = <{
      shards: ShardList;
      leases: Queries.Query.Result;
    }>{}

    parallel({
      allShardIds: done => {
          let shards = []
          let foundAllShards = false
          var startShardId
          const next = done => {
            const params = {
              StreamName: this.opts.streamName,
              ExclusiveStartShardId: startShardId,
            }
            this.streamProvider.describeStream(params, (err, data: DescribeStreamData) => {
              if (err) {
                return done(err)
              }
              if (!data.StreamDescription.HasMoreShards) {
                foundAllShards = true
              }
              const lastShard = data.StreamDescription.Shards[data.StreamDescription.Shards.length - 1]
              startShardId = lastShard.ShardId
              shards = shards.concat(data.StreamDescription.Shards)
              done()
            })
          }
          const test = () => !!foundAllShards
          const finish = err => {
            if (err) {
              return done(err)
            }
            _asyncResults.shards = shards
            done()
          }
        doUntil(next, test, finish)
      },
      leases: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        Lease.fetchAll(tableName, awsConfig, this.getDynamoEndpoint(), (err, leases) => {
          if (err) {
            return done(err)
          }

          _asyncResults.leases = leases
          done()
        })
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error fetching available shards')
      }

      const {shards, leases} = _asyncResults
      const leaseItems = leases.Items

      const finishedShardIds = leaseItems.filter(lease => {
        return lease.get('isFinished')
      }).map(lease => {
        return <string>lease.get('id')
      })

      const allUnfinishedShards = shards.filter(shard => {
        return finishedShardIds.indexOf(shard.ShardId) === -1
      })

      const leasedShardIds: Array<string> = leaseItems.map(item => {
        return item.get('id')
      })

      const newShards = allUnfinishedShards.filter(shard => {

        // skip already leased shards
        if (leasedShardIds.indexOf(shard.ShardId) >= 0) {
          return false
        }

        // skip if parent shard is not finished (split case)
        if (shard.ParentShardId && !(finishedShardIds.indexOf(shard.ParentShardId) >= 0)) {
          this.logger.trace({ ParentShardId: shard.ParentShardId, ShardId: shard.ShardId },
            'Ignoring shard because ParentShardId is not finished')
          return false
        }

        // skip if adjacent parent shard is not finished (merge case)
        if (shard.AdjacentParentShardId && !(finishedShardIds.indexOf(shard.AdjacentParentShardId) >= 0)) {
          this.logger.trace({ AdjacentParentShardId: shard.AdjacentParentShardId, ShardId: shard.ShardId },
            'Ignoring shard because AdjacentParentShardId is not finished')
          return false
        }

        return true
      })

      // If there are shards theat have not been leased, pick one
      if (newShards.length > 0) {
        this.logger.trace({ newShards: newShards }, 'Unleased shards available')
        return this.consumeAvailableShard(newShards[0].ShardId, null)
      }

      // Try to find the first expired lease
      const firstExpiredLease = find(leaseItems, leaseItem => {
        if (leaseItem.get('expiresAt') > Date.now()) {
          return false
        }

        if (leaseItem.get('isFinished')) {
          return false
        }

        return true
      })

      if (firstExpiredLease) {
        let shardId = firstExpiredLease.get('id')
        let leaseCounter = firstExpiredLease.get('leaseCounter')
        this.logger.trace({ shardId: shardId, leaseCounter: leaseCounter }, 'Found available shard')
        this.consumeAvailableShard(shardId, leaseCounter);
      }
    })
  }

  // Create a new consumer processes.
  private spawn(shardId: string, leaseCounter: number) {
    this.logger.trace({ shardId: shardId, leaseCounter: leaseCounter }, 'Spawning consumer')
    const consumerOpts = {
      tableName: this.opts.tableName,
      awsConfig: this.opts.awsConfig,
      streamName: this.opts.streamName,
      streamType: this.opts.streamType,
      startingIteratorType: (this.opts.startingIteratorType || '').toUpperCase(),
      shardId: shardId,
      leaseCounter: leaseCounter,
      dynamoEndpoint: this.endpoints.dynamo,
      dynamoStreamEndpoint: this.opts.dynamoStreamEndpoint,
      kinesisEndpoint: this.endpoints.kinesis,
      numRecords: this.opts.numRecords,
      timeBetweenReads: this.opts.timeBetweenReads,
      logLevel: this.opts.logLevel,
      customLogData: this.opts.customLogData
    }

    const env = {
      CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
      CONSUMER_SUPER_CLASS_PATH: join(__dirname, 'AbstractConsumer.js'),
    }

    const consumer = <ClusterWorkerWithOpts>fork(env)
    consumer.opts = consumerOpts
    consumer.process.stdout.pipe(process.stdout)
    consumer.process.stderr.pipe(process.stderr)
    this.addConsumer(consumer)
  }

  // Add a consumer to the cluster.
  private addConsumer(consumer: ClusterWorkerWithOpts) {
    this.consumerIds.push(consumer.id)
    this.consumers[consumer.id] = consumer

    consumer.once('exit', code => {
      const logMethod = code === 0 ? 'info' : 'error'
      this.logger[logMethod]({ shardId: consumer.opts.shardId, exitCode: code }, 'Consumer exited')

      this.consumerIds = without(this.consumerIds, consumer.id)
      delete this.consumers[consumer.id]
    })
  }

  // Kill any consumer in the cluster.
  private killConsumer(callback: (err: any) => void) {
    const id = this.consumerIds[0]
    this.killConsumerById(id, callback)
  }

  // Kill a specific consumer in the cluster.
  private killConsumerById(id: number, callback: (err: any) => void) {
    this.logger.trace({ id: id }, 'Killing consumer')

    let callbackWasCalled = false
    const wrappedCallback = (err: any) => {
      if (callbackWasCalled) {
        return
      }
      callbackWasCalled = true
      callback(err)
    }

    // Force kill the consumer in 40 seconds, giving enough time for the consumer's shutdown
    // process to finish
    const timer = setTimeout(() => {
      if (this.consumers[id]) {
        this.consumers[id].kill()
      }
      wrappedCallback(new Error('Consumer did not exit in time'))
    }, 40000)

    this.consumers[id].once('exit', code => {
      clearTimeout(timer)
      let err = null
      if (code > 0) {
        err = new Error('Consumer process exited with code: ' + code)
      }
      wrappedCallback(err)
    })

    this.consumers[id].send(config.shutdownMessage)
  }

  private killAllConsumers(callback: (err: any) => void) {
    this.logger.trace('Killing all consumers')
    each(this.consumerIds, this.killConsumerById.bind(this), callback)
  }

  // Continuously fetch data about the rest of the network.
  private loopFetchExternalNetwork() {
    this.logger.trace('Starting external network fetch loop')

    const fetchThenWait = done => {
      this.fetchExternalNetwork(err => {
        if (err) {
          return done(err)
        }
        setTimeout(done, 5000)
      })
    }

    const handleError = err => {
      this.logAndEmitError(err, 'Error fetching external network data')
    }

    forever(fetchThenWait, handleError)
  }

  // Fetch data about the rest of the network.
  private fetchExternalNetwork(callback: (err?: any) => void) {
    this.cluster.fetchAll((err, clusters) => {
      if (err) {
        return callback(err)
      }

      this.externalNetwork = clusters.Items.filter(cluster => {
        return cluster.get('id') !== this.cluster.id
      }).reduce((memo, cluster) => {
        memo[cluster.get('id')] = cluster.get('activeConsumers')
        return memo
      }, {})

      this.logger.trace({ externalNetwork: this.externalNetwork }, 'Updated external network')
      this.updateNetwork()
      callback()
    })
  }

  // Continuously publish data about this cluster to the network.
  private loopReportClusterToNetwork() {
    this.logger.trace('Starting report cluster loop')
    const reportThenWait = done => {
      this.reportClusterToNetwork(err => {
        if (err) {
          return done(err)
        }

        setTimeout(done, 1000)
      })
    }

    const handleError = err => {
      this.logAndEmitError(err, 'Error reporting cluster to network')
    }

    forever(reportThenWait, handleError)
  }

  // Publish data about this cluster to the nework.
  private reportClusterToNetwork(callback: (err: any) => void) {
    this.logger.trace({ consumers: this.consumerIds.length }, 'Reporting cluster to network')
    this.cluster.reportActiveConsumers(this.consumerIds.length, callback)
  }

  // Garbage collect expired clusters from the network.
  private garbageCollectClusters() {
    if (Date.now() < (this.lastGarbageCollectedAt + (1000 * 60))) {
      return
    }

    this.lastGarbageCollectedAt = Date.now()
    this.cluster.garbageCollect((err, garbageCollectedClusters) => {
      if (err) {
        this.logger.error(err, 'Error garbage collecting clusters, continuing cluster execution anyway')
        return
      }

      if (garbageCollectedClusters.length) {
        this.logger.trace('Garbage collected %d clusters', garbageCollectedClusters.length)
      }
    })
  }

  private logAndEmitError(err: Error, desc?: string) {
    this.logger.error(desc)
    this.logger.error(err)

    // Only start the shutdown process once
    if (this.isShuttingDownFromError) {
      return
    }

    this.isShuttingDownFromError = true

    // Kill all consumers and then emit an error so that the cluster can be re-spawned
    this.killAllConsumers((killErr?: Error) => {
      if (killErr) {
        this.logger.error(killErr)
      }

      // Emit the original error that started the shutdown process
      this.emit('error', err)
    })
  }
}
