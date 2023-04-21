import os from 'node:os';
import { MongooseBackend } from '../lib/mongoose-backend.js';
import Minion from '@minionjs/core';
import mojo, { util } from '@mojojs/core';
import moment from 'moment';
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

    await t.test('Add task', async () => {
        minion.addTask('test', async () => {
            return;
        });
    });

    await t.test('Job results', async t => {
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

    await t.test('Wait for job', async t => {
        const worker = await minion.worker().register();
        setTimeout(() => minion.enqueue('test'), 500);
        const job = await worker.dequeue(10000);
        t.not(job, undefined);
        await job.finish({ one: ['two', ['three']] });
        t.same((await job.info()).result, { one: ['two', ['three']] });
        await worker.unregister();
    });

    await t.test('Repair missing worker', async t => {
        const worker = await minion.worker().register();
        const worker2 = await minion.worker().register();
        t.not(worker.id, worker2.id);

        const id = await minion.enqueue('test');
        const job = await worker2.dequeue();
        t.equal(job.id, id);
        t.equal((await job.info()).state, 'active');
        const workerId = worker2.id;
        const missingAfter = minion.missingAfter + 1;
        t.ok(await worker2.info());
        await minion.backend.mongoose.models.minionWorkers.updateOne(
            { _id: minion.backend._oid(workerId) },
            { notified: moment().subtract(missingAfter, 'milliseconds') }
        );

        await minion.repair();
        t.ok(!(await worker2.info()));
        const info = await job.info();
        t.equal(info.state, 'failed');
        t.equal(info.result, 'Worker went away');
        await worker.unregister();
    });

    await t.test('Repair abandoned job', async t => {
        const worker = await minion.worker().register();
        await minion.enqueue('test');
        const job = await worker.dequeue();
        await worker.unregister();
        await minion.repair();
        const info = await job.info();
        t.equal(info.state, 'failed');
        t.equal(info.result, 'Worker went away');
    });

    await t.test(
        'Repair abandoned job in minion_foreground queue (have to be handled manually)',
        async t => {
            const worker = await minion.worker().register();
            const id = await minion.enqueue('test', [], {
                queue: 'minion_foreground'
            });
            const job = await worker.dequeue(0, {
                queues: ['minion_foreground']
            });
            t.equal(job.id, id);
            await worker.unregister();
            await minion.repair();
            const info = await job.info();
            t.equal(info.state, 'active');
            t.equal(info.queue, 'minion_foreground');
            t.same(info.result, null);
        }
    );

    await t.test('Repair old jobs', async t => {
        t.equal(minion.removeAfter, 172800000);

        const worker = await minion.worker().register();
        const id = await minion.enqueue('test');
        const id2 = await minion.enqueue('test');
        const id3 = await minion.enqueue('test');

        await worker.dequeue().then(job => job.perform());
        await worker.dequeue().then(job => job.perform());
        await worker.dequeue().then(job => job.perform());

        const moo = minion.backend.mongoose;
        const moj = moo.models.minionJobs;
        for await (const i of [id2, id3]) {
            const finished = (await moj.findById(minion.backend._oid(id)))
                .finished;
            await moj.updateOne({ _id: minion.backend._oid(i) }, [
                {
                    $set: {
                        finished: moment(finished)
                            .subtract(minion.removeAfter + 1, 'milliseconds')
                            .toDate()
                    }
                }
            ]);
        }

        await worker.unregister();
        await minion.repair();
        t.ok(await minion.job(id));
        t.ok(!(await minion.job(id2)));
        t.ok(!(await minion.job(id3)));
    });

    await t.test('Repair stuck jobs', async t => {
        t.equal(minion.stuckAfter, 172800000);

        const worker = await minion.worker().register();
        const id = await minion.enqueue('test');
        const id2 = await minion.enqueue('test');
        const id3 = await minion.enqueue('test');
        const id4 = await minion.enqueue('test');

        const moo = minion.backend.mongoose;
        const moj = moo.models.minionJobs;
        const stuck = minion.stuckAfter + 1;
        for await (const i of [id, id2, id3, id4]) {
            await moj.updateOne({ _id: minion.backend._oid(i) }, [
                {
                    $set: {
                        delayed: moment()
                            .subtract(stuck, 'milliseconds')
                            .toDate()
                    }
                }
            ]);
        }

        const job = await worker.dequeue(0, { id: id4 });
        await job.finish('Works!');
        const job2 = await worker.dequeue(0, { id: id2 });
        await minion.repair();

        t.equal((await job2.info()).state, 'active');
        t.ok(await job2.finish());
        const job3 = await minion.job(id);
        t.equal((await job3.info()).state, 'failed');
        t.equal((await job3.info()).result, 'Job appears stuck in queue');
        const job4 = await minion.job(id3);
        t.equal((await job4.info()).state, 'failed');
        t.equal((await job4.info()).result, 'Job appears stuck in queue');
        const job5 = await minion.job(id4);
        t.equal((await job5.info()).state, 'finished');
        t.equal((await job5.info()).result, 'Works!');
        await worker.unregister();
    });

    await t.test('List workers', async t => {
        const worker = await minion.worker().register();
        const worker2 = minion.worker();
        worker2.status.whatever = 'works!';
        await worker2.register();
        const results = await minion.backend.listWorkers(0, 10);
        t.equal(results.total, 2);

        const host = os.hostname();
        const batch = results.workers;
        t.equal(batch[0].id, worker2.id);
        t.equal(batch[0].host, host);
        t.equal(batch[0].pid, process.pid);
        t.same(batch[0].started instanceof Date, true);
        t.equal(batch[1].id, worker.id);
        t.equal(batch[1].host, host);
        t.equal(batch[1].pid, process.pid);
        t.same(batch[1].started instanceof Date, true);
        t.notOk(batch[2]);

        const results2 = await minion.backend.listWorkers(0, 1);
        const batch2 = results2.workers;
        t.equal(results2.total, 2);
        t.equal(batch2[0].id, worker2.id);
        t.equal(batch2[0].status.whatever, 'works!');
        t.notOk(batch2[1]);
        worker2.status.whatever = 'works too!';
        await worker2.register();
        const batch3 = (await minion.backend.listWorkers(0, 1)).workers;
        t.equal(batch3[0].status.whatever, 'works too!');
        const batch4 = (await minion.backend.listWorkers(1, 1)).workers;
        t.equal(batch4[0].id, worker.id);
        t.notOk(batch4[1]);
        await worker.unregister();
        await worker2.unregister();

        await minion.reset({ all: true });

        const worker3 = await minion
            .worker({ status: { test: 'one' } })
            .register();
        const worker4 = await minion
            .worker({ status: { test: 'two' } })
            .register();
        const worker5 = await minion
            .worker({ status: { test: 'three' } })
            .register();
        const worker6 = await minion
            .worker({ status: { test: 'four' } })
            .register();
        const worker7 = await minion
            .worker({ status: { test: 'five' } })
            .register();
        const workers = minion.workers();
        workers.fetch = 2;
        t.notOk(workers.options.before);
        t.equal((await workers.next()).status.test, 'five');
        t.equal(workers.options.before, worker6.id);
        t.equal((await workers.next()).status.test, 'four');
        t.equal((await workers.next()).status.test, 'three');
        t.equal(workers.options.before, worker4.id);
        t.equal((await workers.next()).status.test, 'two');
        t.equal((await workers.next()).status.test, 'one');
        t.equal(workers.options.before, worker3.id);
        t.notOk(await workers.next());

        const workers1 = minion.workers({
            ids: [worker4.id, worker6.id, worker3.id]
        });
        const result = [];
        for await (const worker of workers1) {
            result.push(worker.status.test);
        }
        t.same(result, ['four', 'two', 'one']);

        const workers2 = minion.workers({
            ids: [worker4.id, worker6.id, worker3.id]
        });
        t.notOk(workers2.options.before);
        t.equal((await workers2.next()).status.test, 'four');
        t.equal(workers2.options.before, worker3.id);
        t.equal((await workers2.next()).status.test, 'two');
        t.equal((await workers2.next()).status.test, 'one');
        t.notOk(await workers2.next());

        const workers3 = minion.workers();
        workers3.fetch = 2;
        t.equal((await workers3.next()).status.test, 'five');
        t.equal((await workers3.next()).status.test, 'four');
        t.equal(await workers3.total(), 5);
        await worker7.unregister();
        await worker6.unregister();
        await worker5.unregister();
        t.equal((await workers3.next()).status.test, 'two');
        t.equal((await workers3.next()).status.test, 'one');
        t.notOk(await workers3.next());
        t.equal(await workers3.total(), 4);
        t.equal(await minion.workers().total(), 2);
        await worker4.unregister();
        await worker3.unregister();
    });

    await t.test('Exclusive lock', async t => {
        t.ok(await minion.lock('foo', 3600000));
        t.ok(!(await minion.lock('foo', 3600000)));
        t.ok(await minion.unlock('foo'));
        t.ok(!(await minion.unlock('foo')));
        t.ok(await minion.lock('foo', -3600000));
        t.ok(await minion.lock('foo', 0));
        t.ok(!(await minion.isLocked('foo')));
        t.ok(await minion.lock('foo', 3600000));
        t.ok(await minion.isLocked('foo'));
        t.ok(!(await minion.lock('foo', -3600000)));
        t.ok(!(await minion.lock('foo', 3600000)));
        t.ok(await minion.unlock('foo'));
        t.ok(!(await minion.unlock('foo')));
        t.ok(await minion.lock('foo', 3600000, { limit: 1 }));
        t.ok(!(await minion.lock('foo', 3600000, { limit: 1 })));
    });

    await t.test('Shared lock', async t => {
        t.ok(await minion.lock('bar', 3600000, { limit: 3 }));
        t.ok(await minion.lock('bar', 3600000, { limit: 3 }));
        t.ok(await minion.isLocked('bar'));
        t.ok(await minion.lock('bar', -3600000, { limit: 3 }));
        t.ok(await minion.lock('bar', 3600000, { limit: 3 }));
        t.ok(!(await minion.lock('bar', 3600000, { limit: 3 })));
        t.ok(await minion.lock('baz', 3600000, { limit: 3 }));
        t.ok(await minion.unlock('bar'));
        t.ok(await minion.lock('bar', 3600000, { limit: 3 }));
        t.ok(await minion.unlock('bar'));
        t.ok(await minion.unlock('bar'));
        t.ok(await minion.unlock('bar'));
        t.ok(!(await minion.unlock('bar')));
        t.ok(!(await minion.isLocked('bar')));
        t.ok(await minion.unlock('baz'));
        t.ok(!(await minion.unlock('baz')));
    });

    await t.test('List locks', async t => {
        t.equal((await minion.stats()).active_locks, 1);
        const results = await minion.backend.listLocks(0, 2);
        t.equal(results.locks[0].name, 'foo');
        t.same(results.locks[0].expires instanceof Date, true);
        t.notOk(results.locks[1]);
        t.equal(results.total, 1);
        await minion.unlock('foo');

        await minion.lock('yada', 3600000, { limit: 2 });
        await minion.lock('test', 3600000, { limit: 1 });
        await minion.lock('yada', 3600000, { limit: 2 });
        t.equal((await minion.stats()).active_locks, 3);
        const results2 = await minion.backend.listLocks(1, 1);
        t.equal(results2.locks[0].name, 'test');
        t.same(results2.locks[0].expires instanceof Date, true);
        t.notOk(results2.locks[1]);
        t.equal(results2.total, 3);

        const results3 = await minion.backend.listLocks(0, 10, {
            names: ['yada']
        });
        t.equal(results3.locks[0].name, 'yada');
        t.same(results3.locks[0].expires instanceof Date, true);
        t.equal(results3.locks[1].name, 'yada');
        t.same(results3.locks[1].expires instanceof Date, true);
        t.notOk(results3.locks[2]);
        t.equal(results3.total, 2);

        await minion.backend.mongoose.models.minionLocks.updateMany(
            {
                name: 'yada'
            },
            { expires: moment().subtract(1, 'seconds') }
        );
        await minion.unlock('test');
        t.equal((await minion.stats()).active_locks, 0);
        t.same((await minion.backend.listLocks(0, 10)).total, 0);
    });

    await t.test('Reset (locks)', async t => {
        await minion.enqueue('test');
        await minion.lock('test', 3600000);
        await minion.worker().register();
        t.equal((await minion.backend.listJobs(0, 1)).total, 1);
        t.equal((await minion.backend.listLocks(0, 1)).total, 1);
        t.equal((await minion.backend.listWorkers(0, 1)).total, 1);
        await minion.reset({ locks: true });
        t.equal((await minion.backend.listJobs(0, 1)).total, 1);
        t.equal((await minion.backend.listLocks(0, 1)).total, 0);
        t.equal((await minion.backend.listWorkers(0, 1)).total, 1);
    });

    await t.test('Reset (all)', async t => {
        await minion.lock('test', 3600000);
        t.equal((await minion.backend.listJobs(0, 1)).total, 1);
        t.equal((await minion.backend.listLocks(0, 1)).total, 1);
        t.equal((await minion.backend.listWorkers(0, 1)).total, 1);
        await minion.reset({ all: true });
        t.equal((await minion.backend.listJobs(0, 1)).total, 0);
        t.equal((await minion.backend.listLocks(0, 1)).total, 0);
        t.equal((await minion.backend.listWorkers(0, 1)).total, 0);
    });

    await minion.end();
});
