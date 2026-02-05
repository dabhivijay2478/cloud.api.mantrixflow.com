import { Redis } from 'ioredis';

const redis = new Redis({
  host: 'localhost',
  port: 6379,
  password: 'mantrixflow@Vijay@8737',
  db: 0,
});

redis.ping()
  .then(() => console.log('Connected successfully! Redis is working.'))
  .catch(err => console.error('Connection failed:', err.message))
  .finally(() => redis.quit());