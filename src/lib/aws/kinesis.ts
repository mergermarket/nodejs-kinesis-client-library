import {doUntil} from 'async'
import {Kinesis} from 'aws-sdk'
import {DescribeStreamData, ShardList} from '../stream-providers/kinesis-stream-provider'

export interface ListShardsCallback {
  (err: any, data?: ShardList): void
}

export const listShards = (streamProvider, stream: string, callback: ListShardsCallback) => {
  let shards = []
  let foundAllShards = false
  var startShardId

  const next = done => {
    const params = {
      StreamName: stream,
      ExclusiveStartShardId: startShardId,
    }

    streamProvider.describeStream(params, (err, data: DescribeStreamData) => {
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
      return callback(err)
    }

    callback(null, shards)
  }

  doUntil(next, test, finish)
}
