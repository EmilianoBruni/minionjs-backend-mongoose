import MongooseBackend from '../lib/mongoose-backend.js';
import Minion from '@minionjs/core';
import mongoose from 'mongoose';
import { test } from 'tap';

const skip =
    process.env.TEST_ONLINE === undefined
        ? { skip: 'set TEST_ONLINE to enable this test' }
        : {};

test('Connect via Mongoose connection', skip, async t => {
    await mongoose.connect(process.env.TEST_ONLINE);
    const minion = new Minion(mongoose, { backendClass: MongooseBackend });
    await minion.update();
    await t.test('Check if correct Backend was loaded', async t => {
        t.ok(minion.backend !== undefined);
        t.equal(minion.backend.name, 'Mongoose');
    });
    await minion.end();
});

test('Connect via Backend connection', skip, async t => {
    const minion = new Minion(
        { uri: process.env.TEST_ONLINE },
        { backendClass: MongooseBackend }
    );
    await t.test('Check if correct Backend was loaded', async t => {
        t.ok(minion.backend !== undefined);
        t.equal(minion.backend.name, 'Mongoose');
    });
    await minion.end();
});
