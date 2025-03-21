import { createBenchContext, randomName } from './utils';

const { transit } = await createBenchContext();

const test1 = 'process 1k messages from single consumer';
const topic1 = transit.topic(randomName('topic'));
await topic1.sendBulk(Array.from({ length: 1000 }, () => ({ foo: 'bar' })));
const subscription1 = topic1.subscribe(randomName('subscription'), {
  consumptionMode: 'sequential',
  startPosition: 'earliest',
});
const consumer1 = subscription1.consume(() => {}, { autostart: false });
await consumer1.waitInit();
console.time(test1);
await consumer1.consume();
console.timeEnd(test1);

const test2 = 'process 1k messages from 10 consumers';
const topic2 = transit.topic(randomName('topic'));

await topic2.sendBulk(Array.from({ length: 1000 }, () => ({ foo: 'bar' })));
const subscription2 = topic2.subscribe(randomName('subscription'), {
  consumptionMode: 'sequential',
  startPosition: 'earliest',
});
const consumers2 = [];
for (let i = 0; i < 10; i++) {
  consumers2.push(subscription2.consume(() => {}, { autostart: false }));
}
await Promise.all(consumers2.map((consumer) => consumer.waitInit()));
console.time(test2);
await Promise.all(consumers2.map((consumer) => consumer.consume()));
console.timeEnd(test2);

await transit.close();
