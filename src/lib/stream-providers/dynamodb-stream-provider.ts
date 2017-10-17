import { DynamoDBStreams, AWSError } from 'aws-sdk'
import {
  StreamProvider,
  GetRecordsParams,
  GetRecordsData,
  GetShardIteratorParams,
  GetShardIteratorData,
  DescribeStreamParams,
  DescribeStreamData,
  StreamDescription,
  Record,
  Shard
} from './stream-provider'

export default function createDynamoDBStreamProvider(client: DynamoDBStreams, stream): StreamProvider {
  return {
    createStream(params, callback) {
      throw new Error('Unsupported operation')
    },

    getRecords(params, callback) {
      client.getRecords(toGetRecordsInput(params), toGetRecordsOutput(callback))
    },

    getShardIterator(params, callback) {
      client.getShardIterator(toGetShardIteratorInput(params), toGetShardIteratorOutput(callback))
    },

    describeStream(params, callback) {
      client.describeStream(toDescribeStreamInput(params), toDescribeStreamOutput(callback))
    }
  }
}

function toGetRecordsInput(params: GetRecordsParams): DynamoDBStreams.GetRecordsInput {
  return {
    ShardIterator: params.ShardIterator,
    Limit: params.Limit
  }
}

function toGetRecordsOutput(callback: (err: AWSError, data: GetRecordsData) => void) {
  return function (err: AWSError, data: DynamoDBStreams.GetRecordsOutput) {
    const callbackData: GetRecordsData = {
      NextShardIterator: data.NextShardIterator,
      Records: data.Records.map(toRecord)
    }
    callback(err, callbackData)
  }
}

function toGetShardIteratorInput(params: GetShardIteratorParams): DynamoDBStreams.GetShardIteratorInput {
  return {
    StreamArn: params.StreamName,
    SequenceNumber: params.StartingSequenceNumber,
    ShardId: params.ShardId,
    ShardIteratorType: params.ShardIteratorType
  }
}

function toGetShardIteratorOutput(callback: (err: AWSError, data: GetShardIteratorData) => void) {
  return function (err: AWSError, data: DynamoDBStreams.GetShardIteratorOutput) {
    const callbackData: GetShardIteratorData = {
      ShardIterator: data.ShardIterator
    }
    callback(err, callbackData)
  }
}

function toDescribeStreamInput(params: DescribeStreamParams): DynamoDBStreams.DescribeStreamInput {
  return {
    StreamArn: params.StreamName,
    ExclusiveStartShardId: params.ExclusiveStartShardId,
    Limit: params.Limit
  }
}

function toDescribeStreamOutput(callback: (err: AWSError, data: DescribeStreamData) => void) {
  return function (err: AWSError, data: DynamoDBStreams.DescribeStreamOutput) {
    const callbackData: DescribeStreamData = {
      StreamDescription: toStreamDescription(data.StreamDescription)
    }
    callback(err, callbackData)
  }
}

function toRecord(data: DynamoDBStreams.Record): Record {
  return {
    SequenceNumber: data.dynamodb.SequenceNumber,
    ApproximateArrivalTimestamp: data.dynamodb.ApproximateCreationDateTime,
    Data: new Buffer(JSON.stringify(data))
  }
}

function toStreamDescription(data: DynamoDBStreams.StreamDescription): StreamDescription {
  return {
    HasMoreShards: !!data.LastEvaluatedShardId,
    Shards: data.Shards.map(toShard)
  }
}

function toShard(data: DynamoDBStreams.Shard): Shard {
  return {
    ShardId: data.ShardId,
    ParentShardId: data.ParentShardId
  }
}
