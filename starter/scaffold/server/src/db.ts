import { MongoClient, type Db } from 'mongodb';
import { config } from './config.js';

let clientPromise: Promise<MongoClient> | undefined;

export async function getDb(): Promise<Db> {
  if (!clientPromise) {
    const client = new MongoClient(config.mongoUrl);
    clientPromise = client.connect();
  }
  const connected = await clientPromise;
  return connected.db();
}