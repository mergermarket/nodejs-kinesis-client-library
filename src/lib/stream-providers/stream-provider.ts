import { AWSError, Kinesis } from 'aws-sdk'

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
  ShardIteratorType: 'AT_SEQUENCE_NUMBER' | 'AFTER_SEQUENCE_NUMBER' | 'TRIM_HORIZON' | 'LATEST' | 'AT_TIMESTAMP',
  StartingSequenceNumber: string,
}

export interface Record {
  SequenceNumber: string;
  ApproximateArrivalTimestamp?: Date;
  Data: Data;
  PartitionKey?: string;
  EncryptionType?: EncryptionType;
}

export interface GetRecordsData {
  NextShardIterator?: string
  Records: RecordList
}

export interface DescribeStreamParams {
  StreamName: string;
  ExclusiveStartShardId?: string;
  Limit?: number
}

export interface GetShardIteratorData {
  ShardIterator: string
}

export interface DescribeStreamData {
  StreamDescription: StreamDescription
}

export interface StreamDescription {
  HasMoreShards: boolean,
  Shards: ShardList
}

export interface Shard {
  ShardId: string
  ParentShardId?: string
  AdjacentParentShardId?: string;
}

export type ShardList = Shard[]

export type RecordList = Record[]

export type EncryptionType = 'NONE' | 'KMS' | string

export type Data = Buffer | Uint8Array | Blob | string | Kinesis.Data

export interface StreamProvider {
  createStream(params: CreateStreamParams, callback: (err: Error) => void),
  getRecords(params: GetRecordsParams, callback: (err: AWSError, data: GetRecordsData) => void),
  getShardIterator(params: GetShardIteratorParams, callback: (err: AWSError, data: GetShardIteratorData) => void),
  describeStream(params: DescribeStreamParams, callback: (err: AWSError, data: DescribeStreamData) => void)
}
