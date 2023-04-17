import type Minion from '@minionjs/core';
import type {
    DailyHistory,
    DequeueOptions,
    DequeuedJob,
    EnqueueOptions,
    JobInfo,
    JobList,
    ListJobsOptions,
    ListLocksOptions,
    LockInfo,
    LockOptions,
    LockList,
    MinionArgs,
    MinionHistory,
    MinionJobId,
    MinionStats,
    RegisterWorkerOptions,
    ResetOptions,
    RetryOptions,
    WorkerInfo,
    WorkerList
} from '@minionjs/core/lib/types';
import type mongodb from 'mongodb';
import { Types } from 'mongoose';
import os from 'node:os';
import {
    minionJobsSchema,
    minionWorkersSchema,
    minionLocksSchema,
    IMinionJobs,
    IMinionLocks,
    IMinionWorkers
} from './schemas/minion.js';
import Path from '@mojojs/path';
import moment from 'moment';
import { Mongoose } from 'mongoose';

export type MinionWorkerId = string;
export interface ListWorkersOptions {
    before?: string;
    ids?: MinionWorkerId[];
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

interface ListJobsResult extends JobInfo {
}

interface ListLockResult extends LockInfo {
}

interface ListWorkersResult extends WorkerInfo {
}

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
     * Stop using the queue.
     */
    async end(): Promise<void> {
        await this.mongoose.disconnect();
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
        //return await this._update('failed', id, retries, result); // TODO:
        return false;
    }

    /**
   * Returns information about workers in batches.
   */
    async listWorkers(offset: number, limit: number, options: ListWorkersOptions = {}): Promise<WorkerList> {

        const results = this.mongoose.models.minionWorkers.aggregate();
        if (options.ids !== undefined) results.match({
            _id: {
                '$in':
                    options.ids.map(this._oid.bind(this))
            }
        });

        if (options.before !== undefined) results.match({
            '$lt': this._oid(options.before)
        })

        results.sort({ _id: -1 }).skip(offset).limit(limit)

        results.lookup({
            from: 'minion_jobs',
            let: { worker_id: "$_id" },
            pipeline: [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                { state: 'active' },
                                { '$eq': ["$_id", "$$worker_id"] }
                            ]
                        }
                    }
                },
                { '$project': { _id: 1 } }
            ],
            as: 'jobs',
        });

        const workers = await results.exec();

        return { total: (workers.length), workers: workers };
    }

    /**
   * Register worker or send heartbeat to show that this worker is still alive.
   */
    async registerWorker(id?: MinionWorkerId, options: RegisterWorkerOptions = {}): Promise<MinionWorkerId> {
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
            }, {
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
   * Unregister worker.
   */
    async unregisterWorker(id: MinionWorkerId): Promise<void> {
        await this.mongoose.models.minionWorkers.deleteOne({ _id: this._oid(id) })
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
}
