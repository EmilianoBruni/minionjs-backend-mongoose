import type { IMinionJobs, IMinionWorkers } from './schemas/minion.js';
import type Minion from '@minionjs/core';
import type {
    DailyHistory,
    JobInfo,
    JobList,
    ListLocksOptions,
    LockInfo,
    LockOptions,
    LockList,
    MinionArgs,
    MinionHistory,
    MinionStats,
    RegisterWorkerOptions,
    ResetOptions,
    RetryOptions,
    WorkerInfo,
    WorkerList
} from '@minionjs/core/lib/types';
import type mongodb from 'mongodb';
import os from 'node:os';
import {
    minionJobsSchema,
    minionWorkersSchema,
    minionLocksSchema,
    IMinionLocks
} from './schemas/minion.js';
import Path from '@mojojs/path';
import moment from 'moment';
import { ObjectId, Types } from 'mongoose';
import { Mongoose } from 'mongoose';

export type MinionStates = 'inactive' | 'active' | 'failed' | 'finished';
export type MinionWorkerId = string;
export type MinionJobId = string | undefined;
export type MinionJobOid = Types.ObjectId;

export interface ListWorkersOptions {
    before?: string;
    ids?: MinionWorkerId[];
}

export interface ListJobsOptions {
    before?: string;
    ids?: MinionJobId[];
    notes?: string[];
    queues?: string[];
    states?: MinionStates[];
    tasks?: string[];
}

export interface EnqueueOptions {
    attempts?: number;
    delay?: number;
    expire?: number;
    lax?: boolean;
    notes?: Record<string, any>;
    parents?: MinionJobOid[];
    priority?: number;
    queue?: string;
}

export interface DequeuedJob {
    id: MinionJobId;
    args: MinionArgs;
    retries: number;
    task: string;
}

export interface DequeueOptions {
    id?: MinionJobId;
    minPriority?: number;
    queues?: string[];
}

//? If pg-backends should export these interface we could import

interface DequeueResult {
    id: MinionJobId;
    args: MinionArgs;
    retries: number;
    task: string;
}

interface EnqueueResult {
    id: MinionJobId;
}

interface JobWithMissingWorkerResult {
    id: MinionJobId;
    retries: number;
}

type ListJobsResult = JobInfo;

type ListLockResult = LockInfo;

type ListWorkersResult = WorkerInfo;

interface LockResult {
    minion_lock: boolean;
}

interface ReceiveResult {
    inbox: Array<[string, ...any[]]>;
}

interface RegisterWorkerResult {
    id: MinionWorkerId;
}

interface ServerVersionResult {
    server_version_num: number;
}

interface UpdateResult {
    attempts: number;
}

interface ConnectOptions extends mongodb.MongoClientOptions {
    /** Uri string  */
    uri: string;
    /** Set to false to [disable buffering](http://mongoosejs.com/docs/faq.html#callback_never_executes) on all models associated with this connection. */
    bufferCommands?: boolean;
    /** The name of the database you want to use. If not provided, Mongoose uses the database name from connection string. */
    dbName?: string;
    /** username for authentication, equivalent to `options.auth.user`. Maintained for backwards compatibility. */
    user?: string;
    /** password for authentication, equivalent to `options.auth.password`. Maintained for backwards compatibility. */
    pass?: string;
    /** Set to false to disable automatic index creation for all models associated with this connection. */
    autoIndex?: boolean;
    /** Set to `true` to make Mongoose automatically call `createCollection()` on every model created on this connection. */
    autoCreate?: boolean;
}

/**
 * Minion Mongoose backend class.
 */
export class MongooseBackend {
    /**
     * Backend name.
     */
    name = 'Mongoose';
    /**
     * Minion instance this backend belongs to.
     */
    minion: Minion;

    /**
     * `mongoose` object used to store mongoose connection
     */
    mongoose: Mongoose;

    _hostname = os.hostname();
    _isReplicaSet: boolean | undefined;

    /**
     * Return if MongoDB is a replicaset or stand-alone.
     */

    isReplicaSet(): boolean | undefined {
        if (this.mongoose === undefined) return undefined;
        if (this.mongoose.connection.readyState !== 1) return undefined;
        if (this._isReplicaSet !== undefined) return this._isReplicaSet;
        try {
            this.mongoose.connection.db
                .admin()
                .command({ replSetGetStatus: 1 });
            this._isReplicaSet = true;
        } catch (error) {
            this._isReplicaSet = false;
        }
        return this._isReplicaSet;
    }

    constructor(minion: Minion, config: ConnectOptions | Mongoose) {
        this.minion = minion;
        if ('uri' in config) {
            this.mongoose = new Mongoose();
            const { uri, ...mongooseConfig } = config;
            this.mongoose.connect(uri, mongooseConfig).catch(error => {
                throw new Error(error);
            });
        } else {
            this.mongoose = config;
        }
        this.mongoose.set({ debug: true, autoCreate: false, autoIndex: false });
        this._loadModels();
    }

    /**
     * Wait a given amount of time in milliseconds for a job, dequeue it and transition from `inactive` to `active`
     * state, or return `null` if queues were empty.
     */
    async dequeue(
        id: MinionWorkerId,
        wait: number,
        options: DequeueOptions
    ): Promise<DequeuedJob | null> {
        const job = await this._try(id, options);
        if (job !== null) return job;

        if (this.isReplicaSet()) {
            // TODO: reduce filter to match only new Job for this queue
            const newJob = this.mongoose.models.minionJobs.watch([]);
            const timeoutPromise = new Promise(resolve =>
                setTimeout(() => resolve(false), wait)
            );
            const doc = await Promise.race([newJob.next(), timeoutPromise]);
            console.log('PromiceRace: ' + JSON.stringify(doc));
        } else {
            // TODO: standalone
        }

        return await this._try(id, options);
    }

    /**
     * Stop using the queue.
     */
    async end(): Promise<void> {
        await this.mongoose.disconnect();
    }

    /**
     * Enqueue a new job with `inactive` state.
     */
    async enqueue(
        task: string,
        args: MinionArgs = [],
        options: EnqueueOptions = {}
    ): Promise<MinionJobId> {
        const mJobs = this.mongoose.models.minionJobs;

        const job = new mJobs<IMinionJobs>({
            args: args,
            attempts: options.attempts ?? 1,
            delayed: moment()
                .add(options.delay ?? 0, 'milliseconds')
                .toDate(),
            lax: options.lax ?? false,
            notes: options.notes ?? {},
            parents: options.parents ?? [],
            priority: options.priority ?? 0,
            queue: options.queue ?? 'default',
            task: task
        });
        if (options.expire !== undefined)
            job.expires = moment().add(options.expire, 'milliseconds').toDate();

        await job.save();

        return job._id?.toString();
    }

    /**
     * Transition from `active` to `failed` state with or without a result, and if there are attempts remaining,
     * transition back to `inactive` with a delay.
     */
    async failJob(
        id: MinionJobId,
        retries: number,
        result?: any
    ): Promise<boolean> {
        return await this._update('failed', id, retries, result);
    }

    /**
     * Transition from C<active> to `finished` state with or without a result.
     */
    async finishJob(
        id: MinionJobId,
        retries: number,
        result?: any
    ): Promise<boolean> {
        return await this._update('finished', id, retries, result);
    }

    /**
     * Returns the information about jobs in batches.
     */
    async listJobs(
        offset: number,
        limit: number,
        options: ListJobsOptions = {}
    ): Promise<JobList> {
        const results = this.mongoose.models.minionJobs.aggregate();
        if (options.ids !== undefined)
            results.match({
                _id: {
                    $in: options.ids.map(this._oid.bind(this))
                }
            });

        if (options.before !== undefined)
            results.match({
                $lt: this._oid(options.before)
            });
        if (options.notes !== undefined)
            options.notes.forEach(note => {
                results.match({
                    'notes.$`note`': { $exists: true }
                });
            });
        results.match({
            $or: [
                { state: { $ne: 'inactive' } },
                { expires: { $gt: moment() } },
                { expires: { $exists: false } }
            ]
        });

        results.sort({ _id: -1 }).skip(offset).limit(limit);

        results.lookup({
            from: 'minion_jobs',
            localField: 'parents',
            foreignField: 'children',
            as: 'children'
        });

        const jobs = await results.exec();

        return { total: jobs.length, jobs: jobs };
    }

    /**
     * Returns information about workers in batches.
     */
    async listWorkers(
        offset: number,
        limit: number,
        options: ListWorkersOptions = {}
    ): Promise<WorkerList> {
        const results = this.mongoose.models.minionWorkers.aggregate();
        if (options.ids !== undefined)
            results.match({
                _id: {
                    $in: options.ids.map(this._oid.bind(this))
                }
            });

        if (options.before !== undefined)
            results.match({
                $lt: this._oid(options.before)
            });

        results.sort({ _id: -1 }).skip(offset).limit(limit);

        results.lookup({
            from: 'minion_jobs',
            let: { worker_id: '$_id' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { state: 'active' },
                                { $eq: ['$_id', '$$worker_id'] }
                            ]
                        }
                    }
                },
                { $project: { _id: 1 } }
            ],
            as: 'jobs'
        });

        const workers = await results.exec();

        return { total: workers.length, workers: workers };
    }

    /**
     * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
     */
    async note(id: MinionJobId, merge: Record<string, any>): Promise<boolean> {
        console.log(merge);
        if (Object.keys(merge).length === 0) return false;
        type keyable = { [key: string]: any };
        type setOrNotSet = { toSet: keyable; toUnset: keyable };
        const key: setOrNotSet = { toSet: {}, toUnset: {} };
        Object.keys(merge).forEach(
            (k: string) =>
            (key[merge[k] === null ? 'toUnset' : 'toSet'][`notes.${k}`] =
                merge[k])
        );

        console.log(key);
        const result = await this.mongoose.models.minionJobs.updateOne(
            {
                _id: this._oid(id)
            },
            { $set: key.toSet, $unset: key.toUnset }
        );

        return result.modifiedCount > 0;
    }

    /**
     * Register worker or send heartbeat to show that this worker is still alive.
     */
    async registerWorker(
        id?: MinionWorkerId,
        options: RegisterWorkerOptions = {}
    ): Promise<MinionWorkerId> {
        const status = options.status ?? {};
        const mWorkers = this.mongoose.models.minionWorkers;

        this._initDB();

        const worker = await mWorkers.findOneAndUpdate<IMinionWorkers>(
            { _id: this._oid(id) || new Types.ObjectId() },
            {
                host: this._hostname,
                pid: process.pid,
                notified: moment(),
                status: status
            },
            {
                upsert: true,
                lean: true,
                returnDocument: 'after'
            }
        );
        return worker._id.toString();
    }

    /**
     * Repair worker registry and job queue if necessary.
     */
    async repair(): Promise<void> {
        const mongoose = this.mongoose;
        const minion = this.minion;

        const mWorkers = this.mongoose.models.minionWorkers;
        const mJobs = this.mongoose.models.minionJobs;

        // Workers without heartbeat
        await mWorkers.deleteMany({
            notified: {
                $lt: moment().subtract(minion.missingAfter, 'milliseconds')
            }
        });

        // Old jobs with no unresolved dependencies and expired jobs
        //   DELETE FROM minion_jobs WHERE id IN (
        //     SELECT j.id FROM minion_jobs AS j LEFT JOIN minion_jobs AS children
        //       ON children.state != 'finished' AND ARRAY_LENGTH(children.parents, 1) > 0 AND j.id = ANY(children.parents)
        const pipeline = mJobs
            .aggregate()
            .match({
                $expr: {
                    $and: [
                        { $in: ['$$parent', '$parent'] },
                        { $ne: ['$state', 'finished'] }
                    ]
                }
            })
            .pipeline();
        //     WHERE j.state = 'finished' AND j.finished <= NOW() - INTERVAL '1 millisecond' * ${minion.removeAfter}
        //       AND children.id IS NULL

        console.log(JSON.stringify(pipeline));

        const jobs = mJobs
            .aggregate()
            .match({
                state: 'finished',
                finished: {
                    $lte: moment().subtract(minion.removeAfter, 'milliseconds')
                }
            })
            .append({
                $lookup: {
                    from: 'minion_jobs',
                    let: { parent: '$_id' },
                    pipeline: [
                        {
                            $match: {
                                $expr: {
                                    $and: [
                                        { $in: ['$$parent', '$parent'] },
                                        { $ne: ['$state', 'finished'] }
                                    ]
                                }
                            }
                        }
                    ],
                    as: 'parents'
                }
            });

        const jobsToDelete: Types.ObjectId[] = [];
        for await (const job of jobs) {
            if (job.parents.length() == 0) jobsToDelete.push(job._id);
        }

        if (jobsToDelete.length > 0)
            await mJobs.deleteMany({ _id: { $in: jobsToDelete } });

        //     UNION ALL
        //     SELECT id FROM minion_jobs WHERE state = 'inactive' AND expires <= NOW()
        //   )
        await mJobs.deleteMany({
            state: 'inactive',
            expires: { $lte: moment() }
        });

        // Jobs with missing worker (can be retried)

        const jobsActive = mJobs.find({
            state: 'active',
            queue: { $ne: 'minion_foregroud' }
        });

        for await (const job of jobsActive) {
            if ((await mWorkers.countDocuments({ _id: job.worker })) == 0)
                await this.failJob(job.id, job.retries, 'Worker went away');
        }

        // Jobs in queue without workers or not enough workers (cannot be retried and requires admin attention)
        await mJobs.updateMany(
            {
                state: 'inactive',
                delayed: {
                    $lt: moment().subtract(minion.stuckAfter, 'milliseconds')
                }
            },
            { $set: { state: 'failed', result: 'Job appears stuck in queue' } }
        );
    }

    /**
     * Reset job queue.
     */
    async reset(options: ResetOptions): Promise<void> {
        if (options.all === true)
            for await (const coll of [
                this.mongoose.models.minionJobs,
                this.mongoose.models.minionLocks,
                this.mongoose.models.minionWorkers
            ]) {
                await coll.deleteMany({});
            }
        if (options.locks === true)
            await this.mongoose.models.minionLocks.deleteMany({});
    }

    /**
     * Transition job back to `inactive` state, already `inactive` jobs may also be retried to change options.
     */
    async retryJob(
        id: MinionJobId,
        retries: number,
        options: RetryOptions = {}
    ): Promise<boolean> {
        type keyable = { [key: string]: any };
        const filter = { _id: this._oid(id), retries: retries };
        const update: keyable = {
            delayed: moment()
                .add(options.delay ?? 0, 'milliseconds')
                .toDate(),
            retried: Date.now(),
            $inc: { retries: 1 }
        };

        if ('expire' in options)
            update.expires = moment()
                .add(options.expire, 'milliseconds')
                .toDate();
        /* @ts-ignore:enable */
        ['lax', 'parents', 'priority', 'queue'].forEach(k => {
            /* @ts-ignore:enable */
            if (k in options) update[k] = options[k];
        }
        );

        const res = await this.mongoose.models.minionJobs.updateOne(
            filter,
            update
        );
        return res.modifiedCount > 0;
    }

    /**
     * Unregister worker.
     */
    async unregisterWorker(id: MinionWorkerId): Promise<void> {
        await this.mongoose.models.minionWorkers.deleteOne({
            _id: this._oid(id)
        });
    }

    /**
     * Update database schema to latest version.
     */
    async update(): Promise<void> {
        /** Current do nothing. We don't support (and maybe Moongoose doesnt' need
         *  migration code
         */
        // const job = new this.mongoose.models.minionLocks({
        //     expires: Date.now(),
        //     name: 'Pippo'
        // });
        // await job.save();
        await this._initDB();
    }

    async _autoRetryJob(
        id: MinionJobId,
        retries: number,
        attempts: number
    ): Promise<boolean> {
        if (attempts <= 1) return true;
        const delay = this.minion.backoff(retries);
        return this.retryJob(id, retries, {
            attempts: attempts > 1 ? attempts - 1 : 1,
            delay
        });
    }

    _loadModels() {
        [minionJobsSchema, minionLocksSchema, minionWorkersSchema].forEach(
            model => {
                const schema = new this.mongoose.Schema(model.schema, {
                    collection: model.name,
                    ...model.options
                });
                this.mongoose.model(model.alias, schema);
            }
        );
    }

    async _initDB() {
        let coll = await this.mongoose.connection.db
            .listCollections({ name: 'minion_jobs' })
            .next();
        if (coll === null) {
            this.mongoose.models.minionJobs.schema.index(
                { finished: 1 },
                { background: true }
            );
            this.mongoose.models.minionJobs.schema.index(
                { state: 1, priority: -1, _id: 1 },
                { background: true }
            );
            this.mongoose.models.minionJobs.schema.index(
                { parents: 1 },
                { background: true }
            );
            this.mongoose.models.minionJobs.schema.index(
                { notes: 1 },
                { background: true }
            );
            this.mongoose.models.minionJobs.schema.index(
                { expires: 1 },
                { background: true }
            );
            await this.mongoose.models.minionJobs.ensureIndexes();
        }
        coll = await this.mongoose.connection.db
            .listCollections({ name: 'minion_locks' })
            .next();
        if (coll === null) {
            this.mongoose.models.minionLocks.schema.index(
                { name: 1, expires: -1 },
                { background: true }
            );
            await this.mongoose.models.minionLocks.ensureIndexes();
        }
    }

    _oid(oidString: string | undefined): Types.ObjectId | undefined {
        if (oidString == undefined) return undefined;
        return new this.mongoose.Types.ObjectId(oidString);
    }

    async _try(
        id: MinionWorkerId,
        options: DequeueOptions
    ): Promise<DequeuedJob | null> {
        const jobId = options.id;
        const minPriority = options.minPriority;
        const queues = options.queues ?? ['default'];
        const tasks = Object.keys(this.minion.tasks);

        const now = moment().toDate();

        //     SELECT id FROM minion_jobs AS j
        //     WHERE delayed <= NOW() AND id = COALESCE(${jobId}, id)
        // AND priority >= COALESCE(${minPriority}, priority) AND queue = ANY (${queues}) AND state = 'inactive'
        //       AND task = ANY (${tasks}) AND (EXPIRES IS NULL OR expires > NOW())
        const results = this.mongoose.models.minionJobs
            .aggregate<DequeueResult>()
            .match({
                delayed: { $lte: now },
                state: 'inactive',
                task: { $in: tasks },
                queue: { $in: queues },
                $or: [
                    { expires: { $gt: now } },
                    { expires: { $exists: false } }
                ]
            });

        if (jobId !== undefined) results.match({ _id: this._oid(jobId) });
        if (minPriority !== undefined)
            results.match({ priority: { $gte: minPriority } });

        // AND (parents = '{}' OR
        // NOT EXISTS (
        //       SELECT 1 FROM minion_jobs WHERE id = ANY (j.parents) AND (
        //         state = 'active' OR (state = 'failed' AND NOT j.lax)
        //         OR (state = 'inactive' AND (expires IS NULL OR expires > NOW())))
        //     ))

        results.lookup({
            from: 'minion_jobs',
            let: { p: '$parents', l: '$lax' },
            pipeline: [
                {
                    $match: {
                        $expr: { $in: ['$_id', '$$p'] },
                        $or: [
                            { state: 'active' },
                            { state: 'failed', $expr: { $eq: ['$$l', false] } },
                            {
                                state: 'inactive',
                                $or: [
                                    { $expr: { $gt: ['$expires', now] } },
                                    { expires: { $exists: false } }
                                ]
                            }
                        ]
                    }
                }
            ],
            as: 'parents_docs'
        });
        results.match({ $or: [{ parents_docs: [] }, { parents: [] }] });

        //     ORDER BY priority DESC, id
        //     LIMIT 1
        //     FOR UPDATE SKIP LOCKED
        //   )
        results.sort({ priority: -1, _id: 1 }).limit(1);
        //   RETURNING id, args, retries, task
        results.project({ id: '$_id', args: 1, retries: 1, task: 1 });

        const job = await results.exec();

        if (job.length > 0) {
            await this.mongoose.models.minionJobs.updateOne(
                { _id: job[0].id },
                {
                    started: now,
                    state: 'active',
                    worker: id
                }
            );
            job[0].id = job[0].id?.toString();
        }

        //   UPDATE minion_jobs SET started = NOW(), state = 'active', worker = ${id}
        return job[0];
    }

    async _update(
        state: 'finished' | 'failed',
        id: MinionJobId,
        retries: number,
        result?: any
    ): Promise<boolean> {
        const job: IMinionJobs | null =
            await this.mongoose.models.minionJobs.findOneAndUpdate(
                {
                    _id: this._oid(id),
                    retries: retries,
                    state: 'active'
                },
                { finished: Date.now(), result: result, state: state }
            );

        if (job === undefined) return false;
        const attempts = job?.attempts ?? 0;
        return state === 'failed'
            ? this._autoRetryJob(id, retries, attempts)
            : true;
    }
}
