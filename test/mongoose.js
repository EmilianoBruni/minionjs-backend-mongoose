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

    await t.test('Stats', async t => {
        minion.addTask('add', async (job, first, second) => {
            await job.finish({ added: first + second });
        });
        minion.addTask('fail', async () => {
            throw new Error('Intentional failure!');
        });

        const stats = await minion.stats();
        t.equal(stats.workers, 0);
        t.equal(stats.active_workers, 0);
        t.equal(stats.inactive_workers, 0);
        t.equal(stats.enqueued_jobs, 0);
        t.equal(stats.active_jobs, 0);
        t.equal(stats.failed_jobs, 0);
        t.equal(stats.finished_jobs, 0);
        t.equal(stats.inactive_jobs, 0);
        t.equal(stats.delayed_jobs, 0);
        t.equal(stats.active_locks, 0);
        t.ok(stats.uptime);

        const worker = await minion.worker().register();
        t.equal((await minion.stats()).workers, 1);
        t.equal((await minion.stats()).inactive_workers, 1);
        await minion.enqueue('fail');
        t.equal((await minion.stats()).enqueued_jobs, 1);
        await minion.enqueue('fail');
        t.equal((await minion.stats()).enqueued_jobs, 2);
        t.equal((await minion.stats()).inactive_jobs, 2);

        const job = await worker.dequeue(0);
        const stats2 = await minion.stats();
        t.equal(stats2.workers, 1);
        t.equal(stats2.active_workers, 1);
        t.equal(stats2.active_jobs, 1);
        t.equal(stats2.inactive_jobs, 1);

        await minion.enqueue('fail');
        const job2 = await worker.dequeue();
        const stats3 = await minion.stats();
        t.equal(stats3.active_workers, 1);
        t.equal(stats3.active_jobs, 2);
        t.equal(stats3.inactive_jobs, 1);

        t.same(await job2.finish(), true);
        t.same(await job.finish(), true);
        t.equal((await minion.stats()).finished_jobs, 2);
        const job3 = await worker.dequeue();
        t.same(await job3.fail(), true);
        t.equal((await minion.stats()).failed_jobs, 1);
        t.same(await job3.retry(), true);
        t.equal((await minion.stats()).failed_jobs, 0);

        const job4 = await worker.dequeue();
        await job4.finish(['works']);
        await worker.unregister();
        const stats4 = await minion.stats();
        t.equal(stats4.workers, 0);
        t.equal(stats4.active_workers, 0);
        t.equal(stats4.inactive_workers, 0);
        t.equal(stats4.active_jobs, 0);
        t.equal(stats4.failed_jobs, 0);
        t.equal(stats4.finished_jobs, 3);
        t.equal(stats4.inactive_jobs, 0);

        await worker.unregister();
    });

    await t.test('History', async t => {
        await minion.enqueue('fail');
        const worker = await minion.worker().register();
        const job = await worker.dequeue();
        t.ok(await job.fail());
        await worker.unregister();
        const history = await minion.history();
        t.equal(history.daily.length, 24);
        t.equal(
            history.daily[23].finished_jobs + history.daily[22].finished_jobs,
            3
        );
        t.equal(
            history.daily[23].failed_jobs + history.daily[22].failed_jobs,
            1
        );
        t.equal(history.daily[0].finished_jobs, 0);
        t.equal(history.daily[0].failed_jobs, 0);
        t.ok(history.daily[0].epoch);
        t.ok(history.daily[1].epoch);
        t.ok(history.daily[12].epoch);
        t.ok(history.daily[23].epoch);
    });

    await t.test('List jobs', async t => {
        const id2 = await minion.enqueue('add');
        t.equal((await minion.backend.listJobs(1, 1)).total, 5);
        const results = await minion.backend.listJobs(0, 10);
        const batch = results.jobs;
        t.equal(results.total, 5);
        t.ok(batch[0].id);
        t.equal(batch[0].task, 'add');
        t.equal(batch[0].state, 'inactive');
        t.equal(batch[0].retries, 0);
        t.same(batch[0].created instanceof Date, true);
        t.equal(batch[2].task, 'fail');
        t.same(batch[2].args, []);
        t.same(batch[2].notes, {});
        t.same(batch[2].result, ['works']);
        t.equal(batch[2].state, 'finished');
        t.equal(batch[2].priority, 0);
        t.same(batch[2].parents, []);
        t.same(batch[2].children, []);
        t.equal(batch[2].retries, 1);
        t.same(batch[2].created instanceof Date, true);
        t.same(batch[2].delayed instanceof Date, true);
        t.same(batch[2].finished instanceof Date, true);
        t.same(batch[2].retried instanceof Date, true);
        t.same(batch[2].started instanceof Date, true);
        t.equal(batch[3].task, 'fail');
        t.equal(batch[3].state, 'finished');
        t.equal(batch[3].retries, 0);
        t.equal(batch[4].task, 'fail');
        t.equal(batch[4].state, 'finished');
        t.equal(batch[4].retries, 0);
        t.notOk(batch[5]);

        const batch2 = (
            await minion.backend.listJobs(0, 10, { states: ['inactive'] })
        ).jobs;
        t.equal(batch2[0].state, 'inactive');
        t.equal(batch2[0].retries, 0);
        t.notOk(batch2[1]);

        const batch3 = (
            await minion.backend.listJobs(0, 10, { tasks: ['add'] })
        ).jobs;
        t.equal(batch3[0].task, 'add');
        t.equal(batch3[0].retries, 0);
        t.notOk(batch3[1]);

        const batch4 = (
            await minion.backend.listJobs(0, 10, { tasks: ['add', 'fail'] })
        ).jobs;
        t.equal(batch4[0].task, 'add');
        t.equal(batch4[1].task, 'fail');
        t.equal(batch4[2].task, 'fail');
        t.equal(batch4[3].task, 'fail');
        t.equal(batch4[4].task, 'fail');
        t.notOk(batch4[5]);

        const batch5 = (
            await minion.backend.listJobs(0, 10, { queues: ['default'] })
        ).jobs;
        t.equal(batch5[0].queue, 'default');
        t.equal(batch5[1].queue, 'default');
        t.equal(batch5[2].queue, 'default');
        t.equal(batch5[3].queue, 'default');
        t.equal(batch5[4].queue, 'default');
        t.notOk(batch5[5]);

        const id = await minion.enqueue('test', [], {
            notes: { isTest: true }
        });
        const batch6 = (
            await minion.backend.listJobs(0, 10, { notes: ['isTest'] })
        ).jobs;
        t.equal(batch6[0].id, id);
        t.equal(batch6[0].task, 'test');
        t.same(batch6[0].notes, { isTest: true });
        t.notOk(batch6[1]);
        await (await minion.job(id)).remove();

        const batch7 = (
            await minion.backend.listJobs(0, 10, { queues: ['does_not_exist'] })
        ).jobs;
        t.notOk(batch7[0]);

        const results4 = await minion.backend.listJobs(0, 1);
        const batch8 = results4.jobs;
        t.equal(results4.total, 5);
        t.equal(batch8[0].state, 'inactive');
        t.equal(batch8[0].retries, 0);
        t.notOk(batch8[1]);

        const batch9 = (await minion.backend.listJobs(2, 1)).jobs;
        t.equal(batch9[0].state, 'finished');
        t.equal(batch9[0].retries, 1);
        t.notOk(batch9[1]);

        const jobs = minion.jobs();
        t.equal((await jobs.next()).task, 'add');
        // TODO: oid
        //t.equal(jobs.options.before, 1);
        t.equal((await jobs.next()).task, 'fail');
        t.equal((await jobs.next()).task, 'fail');
        t.equal((await jobs.next()).task, 'fail');
        t.equal((await jobs.next()).task, 'fail');
        t.notOk(await jobs.next());
        t.equal(await jobs.total(), 5);

        const jobs2 = minion.jobs({ states: ['inactive'] });
        t.equal(await jobs2.total(), 1);
        t.equal((await jobs2.next()).task, 'add');
        t.notOk(await jobs2.next());

        const jobs3 = minion.jobs({ states: ['active'] });
        t.notOk(await jobs3.next());

        const jobs4 = minion.jobs();
        t.notOk(jobs4.options.before);
        jobs4.fetch = 2;
        t.equal((await jobs4.next()).task, 'add');
        // TODO: oid
        //t.equal(jobs4.options.before, 4);
        t.equal((await jobs4.next()).task, 'fail');
        // TODO: oid
        //t.equal(jobs4.options.before, 4);
        t.equal((await jobs4.next()).task, 'fail');
        // TODO: oid
        //t.equal(jobs4.options.before, 2);
        t.equal((await jobs4.next()).task, 'fail');
        // TODO: oid
        //t.equal(jobs4.options.before, 2);
        t.equal((await jobs4.next()).task, 'fail');
        // TODO: oid
        //t.equal(jobs4.options.before, 1);
        t.notOk(await jobs4.next());
        t.equal(await jobs4.total(), 5);

        await minion.job(id2).then(job => job.remove());
    });

    await t.test('Enqueue, dequeue and perform', async t => {
        t.notOk(await minion.job(12345));
        const id = await minion.enqueue('add', [2, 2]);
        const info = await minion.job(id).then(job => job.info());
        t.same(info.args, [2, 2]);
        t.equal(info.priority, 0);
        t.equal(info.state, 'inactive');

        const worker = minion.worker();
        t.same(await worker.dequeue(), null);
        await worker.register();
        const job = await worker.dequeue();
        t.same((await worker.info()).jobs, [id]);
        t.same((await job.info()).created instanceof Date, true);
        t.same((await job.info()).started instanceof Date, true);
        t.same((await job.info()).time instanceof Date, true);
        t.equal((await job.info()).state, 'active');
        t.same(job.args, [2, 2]);
        t.equal(job.task, 'add');
        t.equal(job.retries, 0);
        console.log(await job.info());
        t.equal((await job.info()).worker.toString(), worker.id);
        t.notOk((await job.info()).finished);

        await job.perform();
        t.same((await worker.info()).jobs, []);
        t.same((await job.info()).finished instanceof Date, true);
        t.same((await job.info()).result, { added: 4 });
        t.equal((await job.info()).state, 'finished');
        await worker.unregister();

        const job2 = await minion.job(job.id);
        t.same(job2.task, 'add');
        t.same(job2.args, [2, 2]);
        t.equal((await job2.info()).state, 'finished');
    });

    await minion.end();
});
