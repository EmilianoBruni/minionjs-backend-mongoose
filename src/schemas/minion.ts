import type { MinionStates } from '@minionjs/core/lib/types';
import type { Types } from 'mongoose';
import { Schema } from 'mongoose';

export interface MongooseSchema {
    name: string;
    alias: string;
    options: object;
    schema: object;
}

export interface IMinionJobs {
    _id?: Types.ObjectId;
    id?: Types.ObjectId;
    args: any[];
    attempts?: number;
    created?: Date;
    delayed: Date;
    expires?: Date;
    finished?: Date;
    notes?: any;
    parents?: Types.ObjectId[];
    priority: number;
    queue?: string;
    result?: any;
    retried?: Date;
    retries?: number;
    started?: Date;
    state?: MinionStates;
    task: string;
    worker?: [Schema.Types.ObjectId];
    lax?: boolean;
}

export const minionJobsSchema: MongooseSchema = {
    name: 'minion_jobs',
    alias: 'minionJobs',
    options: {
        timestamps: false
    },
    schema: {
        _id: { type: Schema.Types.ObjectId, auto: true },
        args: { type: [], required: true },
        attempts: { type: Number, default: 1 },
        created: { type: Date, default: Date.now },
        delayed: { type: Date, required: true },
        expires: { type: Date },
        finished: Date,
        notes: { type: Schema.Types.Mixed },
        parents: { type: [Schema.Types.ObjectId] },
        priority: { type: Number, require: true },
        queue: { type: String, default: 'default' },
        result: Schema.Types.Mixed,
        retried: Date,
        retries: { type: Number, default: 1 },
        started: Date,
        state: {
            type: String,
            default: 'inactive',
            enum: ['inactive', 'active', 'failed', 'finished']
        },
        task: { type: String, required: true },
        worker: { type: Schema.Types.ObjectId, ref: 'minion_workers' },
        lax: { type: Boolean, default: false }
    }
};

export interface IMinionWorkers {
    _id: Types.ObjectId;
    host: string;
    inbox: [];
    notified: Date;
    pid: number;
    started: Date;
    status: Schema.Types.Mixed;
}

export const minionWorkersSchema: MongooseSchema = {
    name: 'minon_workers',
    alias: 'minionWorkers',
    options: {
        timestamps: false
    },
    schema: {
        _id: Schema.Types.ObjectId,
        host: { type: String, required: true },
        inbox: [],
        notified: { type: Date, default: Date.now },
        pid: { type: Number, required: true },
        started: { type: Date, default: Date.now },
        status: Schema.Types.Mixed
    }
};

export interface IMinionLocks {
    _id: Types.ObjectId;
    name: string;
    expires: Date;
}

export const minionLocksSchema: MongooseSchema = {
    name: 'minion_locks',
    alias: 'minionLocks',
    options: {
        timestamps: false
    },
    schema: {
        name: { type: String, required: true },
        expires: { type: Date, required: true }
    }
};
