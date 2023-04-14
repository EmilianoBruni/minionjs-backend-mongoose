import os from 'node:os';
import { MongooseBackend } from '../lib/mongoose-backend.js';
import Minion from '@minionjs/core';
import mojo, { util } from '@mojojs/core';
import mongoose from 'mongoose';
import t from 'tap';

const skip =
    process.env.TEST_ONLINE === undefined
        ? { skip: 'set TEST_ONLINE to enable this test' }
        : {};

t.test('Mongoose backend', skip, async t => {
    await mongoose.connect(process.env.TEST_ONLINE);
    const minion = new Minion(mongoose, { backendClass: MongooseBackend });
    await minion.update();

    await t.test('Check if correct Backend was loaded', async t => {
        t.ok(minion.backend !== undefined);
        t.equal(minion.backend.name, 'Mongoose');
    });

    await t.test('Nothing to repair', async t => {
        await minion.repair();
        t.ok(minion.app instanceof mojo().constructor);
    });

    await t.test('Register and unregister', async t => {
        const worker = minion.worker();
        await worker.register();
        t.same((await worker.info()).started instanceof Date, true);
        const notified = (await worker.info()).notified;
        t.same(notified instanceof Date, true);
        const id = worker.id;
        await worker.register();
        await util.sleep(500);
        await worker.register();
        t.same((await worker.info()).notified > notified, true);
        await worker.unregister();
        t.same(await worker.info(), null);
        await worker.register();
        t.not(worker.id, id);
        t.equal((await worker.info()).host, os.hostname());
        await worker.unregister();
        t.same(await worker.info(), null);
    });

    await minion.end();
});
