# Mongoose backend for minion.js

[![npm package](https://img.shields.io/npm/v/minionjs-backend-mongoose.svg)](http://npmjs.org/package/minionjs-backend-mongoose)
[![Build workflow](https://github.com/EmilianoBruni/minionjs-backend-mongoose/actions/workflows/build.yml/badge.svg)](https://github.com/EmilianoBruni/minionjs-backend-mongoose/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/github/EmilianoBruni/minionjs-backend-mongoose/badge.svg)](https://coveralls.io/github/EmilianoBruni/minionjs-backend-mongoose)
![Last Commit](https://img.shields.io/github/last-commit/EmilianoBruni/minionjs-backend-mongoose)
![Dependencies](https://img.shields.io/librariesio/github/EmilianoBruni/minionjs-backend-mongoose)
![Downloads](https://img.shields.io/npm/dt/minionjs-backend-mongoose)

_A Mongoose Backend written in Typescript/ES6 for [minion.js](https://github.com/mojolicious/minion.js), a high performance job queue for Node.js_


## Installation

```bash
npm i minionjs-backend-mongoose -s
```

## Usage

```js
import Minion from '@minionjs/core';
import MongooseBackend from 'minionjs-backend-mongoose';

// Use the high performance MongoDB backend
const uri = 'mongodb://user:password@localhost:27017/database?authSource=admin'
const minion = new Minion({uri: uri}, {backendClass: MongooseBackend}));
// or
// await mongoose.connect(uri);
// const minion = new Minion(mongoose, { backendClass: MongooseBackend });

// Add tasks
minion.addTask('somethingSlow', async (job, ...args) => {
  console.log('This is a background worker process.');
});

// Add hook before job started
minion.addJobHook('job:before', (minion, job) => {
  minion.app.log.trace(`Performing job "${job.id}" with task "${job.task}"`);
});

// Enqueue jobs
await minion.enqueue('somethingSlow', ['foo', 'bar']);
await minion.enqueue('somethingSlow', [1, 2, 3], {priority: 5});

// Perform jobs for testing
await minion.enqueue('somethingSlow', ['foo', 'bar']);
await minion.performJobs();

// Start a worker to perform up to 12 jobs concurrently
const worker = minion.worker();
worker.status.jobs = 12;
await worker.start();

console.log("Worker started and is waiting for other jobs");

process.on('SIGINT', async () => {
  console.log("Stopping worker...");
  await worker.stop();
  console.log("Shutdown minion");
  await minion.end();
})
```

Or using [mojo.js](https://github.com/mojolicious/mojo.js) framework and typescript

```typescript
/ node script.js minion-worker -j 12
// to start a worker to perform up to 12 jobs concurrently

import { minionPlugin } from '@minionjs/core';
import mojo, { MojoApp } from '@mojojs/core';
import MongooseBackend from 'minionjs-backend-mongoose';

const uri = 'mongodb://user:password@localhost:27017/database?authSource=admin'

export const app: MojoApp = mojo();

app.plugin(minionPlugin, { config: { uri: uri }, backendClass: MongooseBackend });

const minion = app.models.minion;

// Add tasks
minion.addTask('somethingSlow', async (job, ...args) => {
    console.log('This is a background worker process with id %s and args %s', job.id, args);
});

// Add hook before job started
minion.addJobHook('job:before', (minion, job) => {
    minion.app.log.trace(`Performing job "${job.id}" with task "${job.task}"`);
});

// Enqueue jobs
await minion.enqueue('somethingSlow', ['foo', 'bar']);
await minion.enqueue('somethingSlow', [1, 2, 3], { priority: 5 });

// Perform jobs for testing
await minion.enqueue('somethingSlow', ['foo', 'bar']);
await minion.performJobs();

app.start();

console.log("App is started and is waiting for other jobs");

process.on('SIGINT', async () => {
    console.log("Shutdown minion");
    await minion.end();
})
```

Now send some other jobs to these workers.

```typescript
import Minion from '@minionjs/core';
import MongooseBackend from 'minionjs-backend-mongoose';
import mongoose from 'mongoose';

const uri = 'mongodb://user:password@localhost:27017/database?authSource=admin'

await mongoose.connect(uri);
const minion = new Minion(mongoose, {backendClass: MongooseBackend});

const id = await minion.enqueue('somethingSlow', ['a', 'b', 'c']);
console.log("Enqueued job with id", id);

await minion.end();
```

and look to worker output to see that the job has been processed.

See [Minion.js documentation](https://github.com/mojolicious/minion.js) for other examples.

## Bugs / Help / Feature Requests / Contributing

* For feature requests or help, please visit [the discussions page on GitHub](https://github.com/EmilianoBruni/minionjs-backend-mongoose/discussions).

* For bug reports, please file an issue on [the issues page on GitHub](https://github.com/EmilianoBruni/minionjs-backend-mongoose/issues).

* Contributions welcome! Please open a [pull request on GitHub](https://github.com/EmilianoBruni/minionjs-backend-mongoose/pulls) with your changes. You can run them by me first on [the discussions page](https://github.com/EmilianoBruni/minionjs-backend-mongoose/discussions) if you'd like. Please add tests for any changes.

## Author

Emiliano Bruni - <info@ebruni.it>

## License

Licensed under [GNU GPLv3](./LICENSE)