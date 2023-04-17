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
        await minion.reset({ all: true });
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

    await t.test('Job results', async t => {
        minion.addTask('test', async () => {
            return;
        });
        const worker = minion.worker();
        await worker.register();

        const id = await minion.enqueue('test');
        const promise = minion.result(id, { interval: 0 });
        const job = await worker.dequeue(0);
        t.equal(job.id, id);
        t.same(await job.note({ foo: 'bar' }), true);
        t.same(await job.finish({ just: 'works' }), true);
        const info = await promise;
        t.same(info.result, { just: 'works' });
        t.same(info.notes, { foo: 'bar' });

        let failed;
        const id2 = await minion.enqueue('test');
        t.not(id2, id);
        const promise2 = minion
            .result(id2, { interval: 0 })
            .catch(reason => (failed = reason));
        const job2 = await worker.dequeue();
        t.equal(job2.id, id2);
        t.not(job2.id, id);
        t.same(await job2.fail({ just: 'works too' }), true);
        await promise2;
        t.same(failed.result, { just: 'works too' });

        const promise3 = minion.result(id, { interval: 0 });
        const info2 = await promise3;
        t.same(info2.result, { just: 'works' });
        t.same(info2.notes, { foo: 'bar' });

        let finished;
        failed = undefined;
        const job3 = await minion.job(id);
        t.same(await job3.retry(), true);
        t.equal((await job3.info()).state, 'inactive');
        const ac = new AbortController();
        const signal = ac.signal;
        const promise4 = minion
            .result(id, { interval: 10, signal })
            .then(value => (failed = value))
            .catch(reason => (failed = reason));
        setTimeout(() => ac.abort(), 250);
        await promise4;
        t.same(finished, undefined);
        t.same(failed.name, 'AbortError');

        finished = undefined;
        failed = undefined;
        const job4 = await minion.job(id);
        t.same(await job4.remove(), true);
        const promise5 = minion
            .result(id, { interval: 10, signal })
            .then(value => (failed = value))
            .catch(reason => (failed = reason));
        await promise5;
        t.same(finished, null);
        t.same(failed, undefined);

        await worker.unregister();
    });
    // await t.test('Job dependencies', async t => {
    //     minion.removeAfter = 0;
    //     await minion.repair();
    //     // t.equal((await minion.stats()).finished_jobs, 0);
    //     const worker = await minion.worker().register();
    //     const id = await minion.enqueue('test');
    //     const id2 = await minion.enqueue('test');
    //     const id3 = await minion.enqueue('test', [], { parents: [id, id2] });
    //     const job = await worker.dequeue();
    //     t.equal(job.id, id);
    // });

    await minion.end();
});
