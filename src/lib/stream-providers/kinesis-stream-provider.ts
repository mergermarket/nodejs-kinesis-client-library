import { Kinesis } from 'aws-sdk'

export interface ListShardsCallback {
  (err: any, data?: Kinesis.Shard[]): void
}

export interface CreateStreamParams {
  StreamName: string,
  ShardCount: number
}

export interface GetRecordsParams {
  ShardIterator: string
  Limit?: number
}

export interface GetShardIteratorParams {
  StreamName: string,
  ShardId: string,
  ShardIteratorType: 'AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
  StartingSequenceNumber: string,
}

export interface DescribeStreamParams {
  StreamName: string;
  ExclusiveStartShardId?: string;
}

export default function createKinesisStreamProvider(client: Kinesis, stream) {
  return {
    createStream(params: CreateStreamParams, callback: (err: any) => void) {
      client.createStream(params, callback)
    },

    getRecords(params: GetRecordsParams, callback) {
      client.getRecords(params, callback)
    },

    getShardIterator(params: GetShardIteratorParams, callback) {
      client.getShardIterator(params, callback)
    },

    describeStream(params: DescribeStreamParams, callback) {
      client.describeStream(params, callback)
    }
  }
}
