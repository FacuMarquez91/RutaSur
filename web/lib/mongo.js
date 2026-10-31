import {MongoClient} from 'mongodb';
const url=process.env.MONGO_URL||'mongodb://localhost:27017/?directConnection=true';
const client=globalThis.__rutasurMongo||new MongoClient(url);globalThis.__rutasurMongo=client;
export async function database(){await client.connect();return client.db(process.env.DB_NAME||'rutasur')}
export const plain=x=>JSON.parse(JSON.stringify(x));
