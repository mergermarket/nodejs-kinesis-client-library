import {Kinesis} from 'aws-sdk'

export interface ListShardsCallback {
  (err: any, data?: Kinesis.Shard[]): void
}

export default function createKinesisStreamProvider(client: Kinesis, stream) {
  const createStream = (params, callback: (err: any) => void) => {
    client.createStream(params, callback)
  }

  const getRecords = (getRecordsParams, callback) => {
    client.getRecords(getRecordsParams, callback)
  }

  const getShardIterator = (params, callback) => {
    client.getShardIterator(params, callback)
  }

  const describeStream = (params, callback) => {
    client.describeStream(params, callback)
  }

  return {
    createStream: createStream,
    getRecords: getRecords,
    getShardIterator: getShardIterator,
    describeStream: describeStream
  }
}
