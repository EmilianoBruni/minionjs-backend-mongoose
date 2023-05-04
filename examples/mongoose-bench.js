import { performance } from 'perf_hooks';
import { MongooseBackend } from '../lib/mongoose-backend.js';
import Minion from '@minionjs/core';

const ENQUEUE = 10000;
const DEQUEUE = 1000;
const WORKERS = 4;
// const INFO = 100;
// const STATS = 100;

// const ENQUEUE = 1200;
// const DEQUEUE = 300;
// const WORKERS = 4;
const INFO = 0;
const STATS = 0;

const minion = new Minion(
    { uri: process.env.TEST_ONLINE },
    { backendClass: MongooseBackend }
);
minion.addTask('foo', async () => {
    // Do nothing
});
minion.addTask('bar', async () => {
    // Do nothing
});
await minion.reset({ all: true });

// Enqueue
console.log(`Clean start with ${ENQUEUE} jobs`);
const parents = [];
for (let i = 1; i <= 5; i++) {
    parents.push(await minion.enqueue('foo'));
}
console.time('Enqueue');
const enqueueStart = performance.now();
for (let i = 1; i <= ENQUEUE; i++) {
    await minion.enqueue(i % 1 === 1 ? 'foo' : 'bar', [], { parents });
}
const enqueueElapsed = ((performance.now() - enqueueStart) / 1000).toFixed(3);
const enqueueAvg = (ENQUEUE / enqueueElapsed).toFixed(2);
console.log(
    `Enqueued ${ENQUEUE} in ${enqueueElapsed} seconds (${enqueueAvg}/s)`
);

// Dequeue
async function dequeue(num) {
    console.log(`Worker ${num} will finish ${DEQUEUE} jobs`);
    const wStarts = performance.now();
    const worker = await minion.worker().register();
    for (let i = 1; i <= DEQUEUE; i++) {
        const job = await worker.dequeue(500);
        //console.log(`Worker ${num} dequeue job ${job.id}`);
        await job.finish();
    }
    await worker.unregister();
    const wElapsed = ((performance.now() - wStarts) / 1000).toFixed(3);
    const wAvg = (DEQUEUE / wElapsed).toFixed(2);
    console.log(
        `Worker ${num} finished ${DEQUEUE} jobs in ${wElapsed} seconds (${wAvg}/s)`
    );
}
const workers = [];
//minion.backend.mongoose.set('debug', true); // for debug
for (let i = 1; i <= WORKERS; i++) {
    workers.push(dequeue(i));
}
const dequeueStart = performance.now();
await Promise.all(workers);
const dequeueElapsed = ((performance.now() - dequeueStart) / 1000).toFixed(3);
const dequeueAvg = ((DEQUEUE * WORKERS) / dequeueElapsed).toFixed(2);
console.log(
    `${WORKERS} workers finished ${
        DEQUEUE * WORKERS
    } jobs in ${dequeueElapsed} seconds (${dequeueAvg}/s)`
);

// Job info
console.log(`Requesting job info ${INFO} times`);
const infoStart = performance.now();
for (let i = 1; i <= INFO; i++) {
    await minion.backend.listJobs(0, 1, { ids: [i] });
}
const infoElapsed = ((performance.now() - infoStart) / 1000).toFixed(3);
const infoAvg = (INFO / infoElapsed).toFixed(2);
console.log(
    `Received job info ${INFO} times in ${infoElapsed} seconds (${infoAvg}/s)`
);

// Stats
console.log(`Requesting stats ${STATS} times`);
const statsStart = performance.now();
for (let i = 1; i <= STATS; i++) {
    await minion.stats();
}
const statsElapsed = ((performance.now() - statsStart) / 1000).toFixed(3);
const statsAvg = (STATS / statsElapsed).toFixed(2);
console.log(
    `Received stats ${STATS} times in ${statsElapsed} seconds (${statsAvg}/s)`
);

minion.end();
