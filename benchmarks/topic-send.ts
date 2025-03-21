import { createBenchContext, randomName } from './utils';

const { transit } = await createBenchContext();

const test1 = 'add 1k messages one by one without subscription';
const topic1 = transit.topic(randomName('topic'));
console.time(test1);
for (let i = 0; i < 1000; i++) {
  await topic1.send({ foo: 'bar' });
}
console.timeEnd(test1);

const test2 = 'add 1k messages one by one with 100 subscriptions';
const topic2 = transit.topic(randomName('topic'));
for (let i = 0; i < 100; i++) {
  const subscription = topic2.subscribe(randomName('subscription'));
  await subscription.waitInit();
}
console.time(test2);
for (let i = 0; i < 1000; i++) {
  await topic2.send({ foo: 'bar' });
}
console.timeEnd(test2);

const test3 = 'add 1k messages in bulk without subscription';
const topic3 = transit.topic(randomName('topic'));
console.time(test3);
const jobs = Array.from({ length: 1000 }, () => ({ foo: 'bar' }));
await topic3.sendBulk(jobs);
console.timeEnd(test3);

const test4 = 'add 1k messages in bulk with 100 subscriptions';
const topic4 = transit.topic(randomName('topic'));
for (let i = 0; i < 100; i++) {
  const subscription = topic4.subscribe(randomName('subscription'));
  await subscription.waitInit();
}
console.time(test4);
const jobs2 = Array.from({ length: 1000 }, () => ({ foo: 'bar' }));
await topic4.sendBulk(jobs2);
console.timeEnd(test4);

await transit.close();
