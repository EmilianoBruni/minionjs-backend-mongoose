import type { IMinionJobs, IMinionWorkers } from './schemas/minion.js';
import type Minion from '@minionjs/core';
import type {
    DailyHistory,
    JobList,
    ListLocksOptions,
    LockOptions,
    LockList,
    MinionArgs,
    MinionHistory,
    MinionStats,
    RegisterWorkerOptions,
    ResetOptions,
    RetryOptions,
    WorkerList
} from '@minionjs/core/lib/types';
import os from 'node:os';
import {
    minionJobsSchema,
    minionWorkersSchema,
    minionLocksSchema
} from './schemas/minion.js';
import dayjs from 'dayjs';
import type { MongooseOptions, QueryWithHelpers, QuerySelector, FilterQuery } from 'mongoose';
import { Types, Mongoose } from 'mongoose';

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
    parents?: string[];
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

interface ConnectOptions extends MongooseOptions {
    /** Uri string  */
    uri: string;
    /** The name of the database you want to use. If not provided, Mongoose uses the database name from connection string. */
    dbName?: string;
    /** username for authentication, equivalent to `options.auth.user`. Maintained for backwards compatibility. */
    user?: string;
    /** password for authentication, equivalent to `options.auth.password`. Maintained for backwards compatibility. */
    pass?: string;
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
     * If you don't have admin access to the MongoDB server, 
     * you can't directly check the server status to determine if it's a 
     * replica set or standalone. However, you can infer this information from 
     * the connection string used to connect to the MongoDB server.
     * In a MongoDB connection string for a replica set, you'll typically see 
     * multiple hosts listed, separated by commas.
     */

    isReplicaSet(): boolean | undefined {
        if (this.mongoose === undefined) return undefined;
        //  if (this.mongoose.connection.readyState !== 1) return undefined;
        if (this._isReplicaSet !== undefined) return this._isReplicaSet;
        const hosts = this.mongoose.connection.getClient().options.hosts;
        this._isReplicaSet = hosts.length > 1;

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
        this.mongoose.set({ autoCreate: false, autoIndex: false });
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
            let timer;
            // TODO: reduce filter to match only new Job for this queue
            const newJob = this.mongoose.models.minionJobs.watch([]);
            const timeoutPromise = new Promise(
                resolve => (timer = setTimeout(resolve, wait))
            );
            await Promise.race([newJob.next(), timeoutPromise]);
            clearTimeout(timer);
        } else {
            // get capped notification collection
            const notification = await this.notificationCollection();
            // build an oid from current time
            const oid = new this.mongoose.Types.ObjectId(Math.floor(Date.now() / 1000).toString(16) + '0000000000000000');
            const cursor = notification.find({
                _id: {
                    $gt: this._oid(id)
                },
                queue: { $in: options.queues ?? ['default'] }
            }, {
                tailable: true,
                awaitData: true,
                maxAwaitTimeMS: wait * 1000
            });
            // TODO: look if stream is better. An example here: 
            // https://stackoverflow.com/questions/35805580/nodejs-mongodb-recurring-tailable-cursor
            await cursor.next();
        }

        return await this._try(id, options);
    }

    /**
     * Broadcast remote control command to one or more workers.
     */
    async broadcast(
        command: string,
        args: any[] = [],
        ids: MinionWorkerId[] = []
    ): Promise<boolean> {
        const inbox = [command, ...args];
        const match =
            ids.length === 0
                ? {}
                : { _id: { $in: ids.map(id => this._oid(id)) } };

        const res = await this.mongoose.models.minionWorkers.updateMany(match, {
            $push: { inbox: inbox }
        });

        return res.modifiedCount > 0;
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
        const mJ = this.mongoose.connection.db.collection('minion_jobs');
        const now = dayjs();

        const job: IMinionJobs = {
            args: args,
            attempts: options.attempts ?? 1,
            created: now.toDate(),
            retries: 0,
            state: 'inactive',
            delayed: now.add(options.delay ?? 0, 'milliseconds').toDate(),
            lax: options.lax ?? false,
            notes: options.notes ?? {},
            priority: options.priority ?? 0,
            queue: options.queue ?? 'default',
            task: task
        };
        if (options.expire !== undefined)
            job.expires = now.add(options.expire, 'milliseconds').toDate();
        if (options.parents != undefined && options.parents.length > 0)
            job.parents = options.parents?.map(e => this._oidNN(e));
        else job.parents = [];

        const ret = await mJ.insertOne(job);
        
        // notify if not in a replicaset
        if (!this.isReplicaSet()) {
            const notification = await this.notificationCollection();
            await notification.insertOne({
                c: 'created',
                queue: job.queue ?? 'default'
            });
        }

        return ret.insertedId.toString();
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
     * Get history information for job queue.
     */
    async history(): Promise<MinionHistory> {
        const mJ = this.mongoose.models.minionJobs;

        const now = dayjs();

        // we build an array with xx:00 for every hour from now to 23 hours ago
        let loop = dayjs(now)
            .minute(0)
            .second(0)
            .millisecond(0)
            .subtract(23, 'hours');
        const start = loop.clone();

        const boundaries = [];
        while (loop < now) {
            boundaries.push(loop.toDate());
            loop = loop.add(1, 'hours');
        }

        // add last range for last hour
        boundaries[24] = now.add(1, 'minutes').toDate();

        /** For all jobs in last 23 hours we bucket it for range
         *  [x, x+1 ] with _id: x
         */
        const result = mJ.aggregate<DailyHistory>();
        result.match({ finished: { $gt: start.toDate() } });
        result.append({
            $bucket: {
                groupBy: '$finished',
                boundaries: boundaries,
                default: null,
                output: {
                    finished_jobs: {
                        $sum: {
                            $cond: {
                                if: { $eq: ['$state', 'finished'] },
                                then: 1,
                                else: 0
                            }
                        }
                    },
                    failed_jobs: {
                        $sum: {
                            $cond: {
                                if: { $eq: ['$state', 'failed'] },
                                then: 1,
                                else: 0
                            }
                        }
                    }
                }
            }
        });
        result.project({
            finished_jobs: 1,
            failed_jobs: 1,
            epoch: { $divide: [{ $toDecimal: '$_id' }, 1000] },
            _id: 0
        });
        result.sort({ _id: 1 });

        const results = await result.exec();

        // we fill empty range [x,x+1]
        const history: DailyHistory[] = [];
        const emptyHistory: DailyHistory = {
            failed_jobs: 0,
            finished_jobs: 0,
            epoch: -1
        };

        boundaries.forEach(dta => {
            const epoch = dta.valueOf() / 1000;
            const historyItem = results.find(v => v.epoch == epoch);
            history.push(
                historyItem === undefined
                    ? { ...emptyHistory, epoch: epoch }
                    : historyItem
            );
        });

        // delete last which is out of bucket
        history.pop();
        return { daily: history };
    }

    /**
     * Returns the information about jobs in batches.
     */
    async listJobs(
        offset: number,
        limit: number,
        options: ListJobsOptions = {}
    ): Promise<JobList> {
        const mJ = this.mongoose.models.minionJobs;
        const results = mJ.aggregate<JobList>();
        if (options.ids !== undefined)
            results.match({
                _id: {
                    $in: options.ids.map(this._oid.bind(this))
                }
            });
        if (options.queues !== undefined)
            results.match({
                queue: {
                    $in: options.queues
                }
            });
        if (options.states !== undefined)
            results.match({
                state: {
                    $in: options.states
                }
            });
        if (options.tasks !== undefined)
            results.match({
                task: {
                    $in: options.tasks
                }
            });

        if (options.before !== undefined)
            results.match({
                _id: {
                    $lt: this._oid(options.before)
                }
            });
        if (options.notes !== undefined)
            options.notes.forEach(note => {
                results.match({
                    [`notes.${note}`]: { $exists: true }
                });
            });
        results.match({
            $or: [
                { state: { $ne: 'inactive' } },
                { expires: { $gt: dayjs().toDate() } },
                { expires: null }
            ]
        });

        const facetPipeLine = mJ.aggregate();
        facetPipeLine.addFields({ id: { $toString: '$_id' } });
        facetPipeLine.addFields({ time: new Date() });
        facetPipeLine.sort({ _id: -1 }).skip(offset).limit(limit);

        // TODO: check if this is correct
        facetPipeLine.lookup({
            from: 'minion_jobs',
            localField: '_id',
            foreignField: 'parents',
            as: 'children'
        });

        results.facet({
            total: [{ $count: 'count' }],
            /* @ts-ignore:enable */
            documents: facetPipeLine.pipeline()
        });

        results.project({
            total: {
                $cond: {
                    if: { $eq: [{ $size: '$total' }, 0] },
                    then: 0,
                    else: { $arrayElemAt: ['$total.count', 0] }
                }
            },
            jobs: '$documents'
        });

        const jobs = (await results.exec())[0];

        if (jobs.jobs.length > 0) {
            jobs.jobs.forEach(job => {
                // convert parents as an array of id
                if (job.parents.length > 0) {
                    const parents: number[] = [];
                    job.parents.forEach(parent => {
                        /* @ts-ignore:enable */
                        parents.push(parent._id.toString());
                    });
                    job.parents = parents;
                }
                // children is an array of id
                if (job.children.length > 0) {
                    const childrens: number[] = [];
                    job.children.forEach(children => {
                        /* @ts-ignore:enable */
                        childrens.push(children._id.toString());
                    });
                    job.children = childrens;
                }
            });
        }
        return jobs;
    }

    /**
     * Returns information about locks in batches.
     */
    async listLocks(
        offset: number,
        limit: number,
        options: ListLocksOptions = {}
    ): Promise<LockList> {
        const mL = this.mongoose.models.minionLocks;
        const results = mL.aggregate<LockList>();
        results.match({ expires: { $gt: dayjs().toDate() } });
        if (options.names !== undefined)
            results.match({
                name: { $in: options.names ?? [] }
            });
        results.facet({
            total: [{ $count: 'count' }],
            documents: [
                { $sort: { _id: -1 } },
                { $skip: offset },
                { $limit: limit },
                { $project: { _id: 0 } }
            ]
        });

        results.project({
            total: {
                $cond: {
                    if: { $eq: [{ $size: '$total' }, 0] },
                    then: 0,
                    else: { $arrayElemAt: ['$total.count', 0] }
                }
            },
            locks: '$documents'
        });

        const locks = (await results.exec())[0];
        return locks;
    }

    /**
     * Returns information about workers in batches.
     */
    async listWorkers(
        offset: number,
        limit: number,
        options: ListWorkersOptions = {}
    ): Promise<WorkerList> {
        const mW = this.mongoose.models.minionWorkers;
        const results = mW.aggregate<WorkerList>();
        if (options.ids !== undefined)
            results.match({
                _id: {
                    $in: options.ids.map(this._oid.bind(this))
                }
            });

        if (options.before !== undefined)
            results.match({
                _id: {
                    $lt: this._oid(options.before)
                }
            });

        const facetPipeLine = mW.aggregate();

        // minion expect an id field
        facetPipeLine.addFields({ id: { $toString: '$_id' } });
        facetPipeLine.sort({ _id: -1 }).skip(offset).limit(limit);
        facetPipeLine.lookup({
            from: 'minion_jobs',
            let: { worker_id: '$_id' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$state', 'active'] },
                                { $eq: ['$worker', '$$worker_id'] }
                            ]
                        }
                    }
                },
                { $project: { _id: 0, id: { $toString: '$_id' } } }
            ],
            as: 'jobs'
        });

        results.facet({
            total: [{ $count: 'count' }],
            /* @ts-ignore:enable */
            documents: facetPipeLine.pipeline()
        });

        results.project({
            total: {
                $cond: {
                    if: { $eq: [{ $size: '$total' }, 0] },
                    then: 0,
                    else: { $arrayElemAt: ['$total.count', 0] }
                }
            },
            workers: '$documents'
        });

        const workers = (await results.exec())[0];

        // convert worker.jobs from [ {id: ObjectIdString}] to [ObjectIdString]
        workers.workers.forEach(worker => {
            const jobsArray: number[] = [];
            worker.jobs.forEach(job => {
                /* @ts-ignore:enable */
                jobsArray.push(job.id);
            });
            worker.jobs = jobsArray;
        });

        return workers;
    }

    /**
     * Try to acquire a named lock that will expire automatically after the given amount of time in milliseconds. An
     * expiration time of `0` can be used to check if a named lock already exists without creating one.
     */
    async lock(
        name: string,
        duration: number,
        options: LockOptions = {}
    ): Promise<boolean> {
        const limit = options.limit ?? 1;
        const now = dayjs();

        const mL = this.mongoose.models.minionLocks;

        await mL.deleteMany({ expires: { $lt: now.toDate() } });

        if ((await mL.countDocuments({ name: name })) >= limit) return false;

        const new_expires = now.clone().add(duration / 1000, 'seconds');
        if (new_expires > now) {
            const new_lock = new mL({ name: name, expires: new_expires });
            await new_lock.save();
        }

        return true;
    }

    /**
     * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
     */
    async note(id: MinionJobId, merge: Record<string, any>): Promise<boolean> {
        if (Object.keys(merge).length === 0) return false;
        type keyable = { [key: string]: any };
        type setOrNotSet = { toSet: keyable; toUnset: keyable };
        const key: setOrNotSet = { toSet: {}, toUnset: {} };
        Object.keys(merge).forEach(
            (k: string) =>
            (key[merge[k] === null ? 'toUnset' : 'toSet'][`notes.${k}`] =
                merge[k])
        );

        const result = await this.mongoose.models.minionJobs.updateOne(
            {
                _id: this._oid(id)
            },
            { $set: key.toSet, $unset: key.toUnset }
        );

        return result.modifiedCount > 0;
    }

    /**
     * Return a notification capped collection
     */
    async notificationCollection(): Promise<QueryWithHelpers<any, any, any>> {
        const db = this.mongoose.connection.db;

        // check if minion_notifications exists in db
        const collections = await db.listCollections().toArray();
        const exists = collections.some(
            coll => coll.name === 'minion_notifications'
        );
        if (exists) {
            return db.collection('minion_notifications');
        }

        const collection = await db.createCollection('minion_notifications', {
            capped: true,
            size: 10240,
            max: 128
        });

        // insert a document to create the collection
        await collection.insertOne({});

        return collection;
    }

    /**
     * Receive remote control commands for worker.
     */
    async receive(id: MinionWorkerId): Promise<Array<[string, ...any[]]>> {
        const oldWorker =
            await this.mongoose.models.minionWorkers.findOneAndUpdate<IMinionWorkers>(
                { _id: this._oid(id), inbox: { $not: { $size: 0 } } },
                { inbox: [] }
            );
        return oldWorker ? oldWorker.inbox : [];
        // const results = await this.pg.query<ReceiveResult>`
        //   UPDATE minion_workers AS new SET inbox = '[]'
        //   FROM (SELECT id, inbox FROM minion_workers WHERE id = ${id} FOR UPDATE) AS old
        //   WHERE new.id = old.id AND old.inbox != '[]'
        //   RETURNING old.inbox AS inbox
        // `;
        //    return results.first?.inbox ?? [];
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
                notified: dayjs().toDate(),
                status: status
            },
            {
                upsert: true,
                returnDocument: 'after'
            }
        );
        return worker._id.toString();
    }

    /**
     * Remove `failed`, `finished` or `inactive` job from queue.
     */
    async removeJob(id: MinionJobId): Promise<boolean> {
        const res = await this.mongoose.models.minionJobs.deleteOne({
            _id: this._oid(id),
            state: { $in: ['inactive', 'failed', 'finished'] }
        });

        return res.deletedCount === 1;
    }

    /**
     * Repair worker registry and job queue if necessary.
     */
    async repair(): Promise<void> {
        const minion = this.minion;

        const mWorkers = this.mongoose.models.minionWorkers;
        const mJobs = this.mongoose.models.minionJobs;

        // Workers without heartbeat
        await mWorkers.deleteMany({
            notified: {
                $lt: dayjs()
                    .subtract(minion.missingAfter, 'milliseconds')
                    .toDate()
            }
        });

        const jobs = mJobs
            .aggregate()
            .match({
                state: 'finished',
                finished: {
                    $lte: dayjs()
                        .subtract(minion.removeAfter, 'milliseconds')
                        .toDate()
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
                                        { $in: ['$$parent', '$parents'] },
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
            if (job.parents.length == 0) jobsToDelete.push(job._id);
        }
        if (jobsToDelete.length > 0)
            await mJobs.deleteMany({ _id: { $in: jobsToDelete } });

        //     UNION ALL
        //     SELECT id FROM minion_jobs WHERE state = 'inactive' AND expires <= NOW()
        //   )
        await mJobs.deleteMany({
            state: 'inactive',
            expires: { $lte: dayjs().toDate() }
        });

        // Jobs with missing worker (can be retried)

        const jobsActive = mJobs.find({
            state: 'active',
            queue: { $ne: 'minion_foreground' }
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
                    $lt: dayjs()
                        .subtract(minion.stuckAfter, 'milliseconds')
                        .toDate()
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
        const now = dayjs();
        const update: keyable = {
            delayed: now.add(options.delay ?? 0, 'milliseconds').toDate(),
            retried: now.toDate(),
            $inc: { retries: 1 },
            state: 'inactive'
        };

        if ('expire' in options)
            update.expires = now
                .add(options.expire ?? 0, 'milliseconds')
                .toDate();
        /* @ts-ignore:enable */
        ['lax', 'parents', 'priority', 'queue', 'attempts'].forEach(k => {
            /* @ts-ignore:enable */
            if (k in options) update[k] = options[k];
        });

        const res = await this.mongoose.models.minionJobs.updateOne(
            filter,
            update
        );
        return res.modifiedCount > 0;
    }

    /**
     * Get statistics for the job queue.
     */
    async stats(): Promise<MinionStats> {
        const moo = this.mongoose;
        const mood = moo.models;
        const now = dayjs().toDate();
        const statsJobs = (
            await mood.minionJobs
                .aggregate<MinionStats>()
                .facet({
                    inactive_jobs: [
                        {
                            $match: {
                                state: 'inactive',
                                $or: [
                                    { expires: null },
                                    { expires: { $gt: now } }
                                ]
                            }
                        },
                        { $count: 'count' }
                    ],
                    active_jobs: [
                        { $match: { state: 'active' } },
                        { $count: 'count' }
                    ],
                    failed_jobs: [
                        { $match: { state: 'failed' } },
                        { $count: 'count' }
                    ],
                    finished_jobs: [
                        { $match: { state: 'finished' } },
                        { $count: 'count' }
                    ],
                    delayed_jobs: [
                        {
                            $match: { state: 'inactive', delayed: { $gt: now } }
                        },
                        { $count: 'count' }
                    ],
                    active_workers: [
                        { $match: { state: 'active' } },
                        { $group: { _id: '$worker' } },
                        { $count: 'count' }
                    ]
                })
                .project({
                    inactive_jobs: {
                        $cond: {
                            if: { $eq: [{ $size: '$inactive_jobs' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$inactive_jobs.count', 0] }
                        }
                    },
                    active_jobs: {
                        $cond: {
                            if: { $eq: [{ $size: '$active_jobs' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$active_jobs.count', 0] }
                        }
                    },
                    failed_jobs: {
                        $cond: {
                            if: { $eq: [{ $size: '$failed_jobs' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$failed_jobs.count', 0] }
                        }
                    },
                    finished_jobs: {
                        $cond: {
                            if: { $eq: [{ $size: '$finished_jobs' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$finished_jobs.count', 0] }
                        }
                    },
                    delayed_jobs: {
                        $cond: {
                            if: { $eq: [{ $size: '$delayed_jobs' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$delayed_jobs.count', 0] }
                        }
                    },
                    active_workers: {
                        $cond: {
                            if: { $eq: [{ $size: '$active_workers' }, 0] },
                            then: 0,
                            else: { $arrayElemAt: ['$active_workers.count', 0] }
                        }
                    }
                })
        )[0];

        const statsWorkers = {
            workers: await mood.minionWorkers.countDocuments({}),
            inactive_workers: -1
        };

        // TODO: enqueued_jobs
        //     (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM minion_jobs_id_seq) AS enqueued_jobs,
        //     EXTRACT(EPOCH FROM NOW() - PG_POSTMASTER_START_TIME()) AS uptime
        const enqueued_jobs =
            statsJobs.active_jobs +
            statsJobs.failed_jobs +
            statsJobs.finished_jobs +
            statsJobs.inactive_jobs;

        // if user doesn't have admin authorization. Server uptime missing
        let uptime;
        try {
            const ss = await moo.connection.db.command({
                iserverStatus: 1,
                asserts: 0,
                connections: 0,
                repl: 0,
                metrics: 0,
                locks: 0,
                electionMetrics: 0,
                extra_info: 0,
                flowControl: 0,
                freeMonitoring: 0,
                globalLock: 0,
                logicalSessionRecordCache: 0,
                network: 0,
                indexBulkBuilder: 0,
                opLatencies: 0,
                opReadConcernCounters: 0,
                opcounters: 0,
                opcountersRepl: 0,
                oplogTruncation: 0,
                scramCache: 0,
                storageEngine: 0,
                tcmalloc: 0,
                trafficRecording: 0,
                transactions: 0,
                transportSecurity: 0,
                twoPhaseCommitCoordinator: 0,
                wiredTiger: 0
            });
            uptime = ss.uptime;
        } catch {
            uptime = -1;
        }

        // I don't know why here required to works toLocaleString()
        const statsLocks = await mood.minionLocks.countDocuments({
            expires: { $gt: now }
        });

        statsWorkers.inactive_workers =
            statsWorkers.workers - statsJobs.active_workers;

        return {
            ...statsJobs,
            ...statsWorkers,
            active_locks: statsLocks,
            uptime: uptime,
            enqueued_jobs: enqueued_jobs
        };
    }

    /**
     * Release a named lock.
     */
    async unlock(name: string): Promise<boolean> {
        const mL = this.mongoose.models.minionLocks;
        const result = await mL
            .aggregate()
            .match({
                expires: { $gt: dayjs().toDate() },
                name: name
            })
            .sort({ expires: 1 })
            .limit(1);
        if (result.length === 0) {
            return false;
        } else {
            await mL.deleteOne({ _id: result[0]._id });
            return true;
        }
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
                // add virtual id to match id request of minion module
                // as number
                schema
                    .virtual('id')
                    .get(function () {
                        /* @ts-ignore:enable */
                        return this._id.toString();
                    })
                    .set(function (v) {
                        this._id = new Types.ObjectId(v);
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
    _oidNN(oidString: string): Types.ObjectId {
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

        const now = dayjs().toDate();

        const mJ = this.mongoose.models.minionJobs;

        const match: FilterQuery<IMinionJobs> = {
            __lock: undefined, // select a not locked document
            delayed: { $lte: now },
            state: 'inactive',
            task: { $in: tasks },
            queue: { $in: queues },
            $or: [{ expires: { $gt: now } }, { expires: undefined }]
        };

        if (jobId !== undefined) match._id = this._oid(jobId);
        if (minPriority !== undefined) match.priority = { $gte: minPriority };

        // track of locked jobs
        const lockedJobs: Types.ObjectId[] = [];

        const job: any | null = null;
        let retJob: DequeuedJob | null = null;
        while (retJob === null) {
            // find candidate document and optimistic locking
            const job = await mJ.findOneAndUpdate<IMinionJobs>(
                match,
                { $set: { __lock: id } },
                { sort: { priority: -1, _id: 1 } }
            );
            if (job === undefined || job === null) break; // not found a candidate to dequeue
            lockedJobs.push(job._id!);
            if (job.parents?.length == 0) {
                // just good if parents is empty
                retJob = await this._activateJob(id, job, lockedJobs);
            } else {
                const count = await mJ.countDocuments({
                    _id: { $in: job.parents },
                    $or: [
                        { state: 'active' },
                        { state: 'failed', $expr: { $eq: [false, job.lax] } },
                        {
                            state: 'inactive',
                            $or: [{ expires: { $gt: now } }, { expires: null }]
                        }
                    ]
                });
                if (count === 0)
                    retJob = await this._activateJob(id, job, lockedJobs);
            }
        }

        if (retJob === null) {
            // didn't find a valid candidate, remove locks
            await mJ.updateMany(
                { _id: { $in: lockedJobs } },
                { $unset: { __lock: 1 } }
            );
        }

        return retJob;
    }

    /* activate this job, unlock locked jobs and return DequeuedJob */
    async _activateJob(
        id: MinionWorkerId,
        job: IMinionJobs,
        lockedJobs: Types.ObjectId[]
    ): Promise<DequeuedJob> {
        const now = dayjs().toDate();
        const mJ =
            this.mongoose.connection.db.collection<IMinionJobs>('minion_jobs');

        const res = await mJ.updateOne(
            { _id: job._id },
            {
                $set: {
                    started: now,
                    state: 'active',
                    worker: this._oidNN(id)
                }
            }
        );
        console.assert(
            res.modifiedCount === 1,
            'Problem in activate locked job'
        );
        await mJ.updateMany(
            { _id: { $in: lockedJobs } },
            { $unset: { __lock: 1 } }
        );
        return {
            id: job._id?.toString(),
            args: job.args,
            retries: job.retries ?? 0,
            task: job.task
        };
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
        if (job === null) return false;
        const attempts = job?.attempts ?? 0;
        return state === 'failed'
            ? this._autoRetryJob(id, retries, attempts)
            : true;
    }
}
