import { Kinesis, DynamoDB, DynamoDBStreams } from 'aws-sdk'

export const createKinesisClient = (conf: Kinesis.ClientConfiguration, endpoint?: string): Kinesis => {
  const instance = new Kinesis({ ...conf, endpoint } || {})
  return instance
}

export const createDynamoClient = (conf: DynamoDB.ClientConfiguration, endpoint?: string): DynamoDB => {
  const instance = new DynamoDB({ ...conf, endpoint } || {})
  return instance
}

export const createDynamoDBStreamsClient = (conf: DynamoDBStreams.ClientConfiguration, endpoint?: string): DynamoDBStreams => {
  const instance = new DynamoDBStreams({ ...conf, endpoint } || {})
  return instance
}
