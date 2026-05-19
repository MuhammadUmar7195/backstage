/*
 * Copyright 2026 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestDatabases, mockServices } from '@backstage/backend-test-utils';
import { Duration } from 'luxon';
import { migrateBackendTasks } from '../database/migrateBackendTasks';
import { DB_TASKS_TABLE, DbTasksRow } from '../database/tables';
import { TaskStatePoller } from './TaskStatePoller';
import { nowPlus } from './util';
import { Knex } from 'knex';

jest.setTimeout(60_000);

const databases = TestDatabases.create({
  ids: ['POSTGRES_18', 'POSTGRES_14', 'SQLITE_3'],
});

describe.each(databases.eachSupportedId())(
  'TaskStatePoller, %p',
  databaseId => {
    let knex: Knex;

    beforeAll(async () => {
      await databases.init(databaseId);
      jest.useFakeTimers();
    }, 60_000);

    afterAll(() => {
      jest.useRealTimers();
    });

    beforeEach(async () => {
      knex = await databases.init(databaseId);
      await migrateBackendTasks(knex);
    });

    afterEach(async () => {
      await knex.destroy();
    });

    const defaultSettings = {
      version: 2,
      cadence: 'PT5S',
      timeoutAfterDuration: 'PT30S',
    };

    async function insertTask(id: string, opts: { ready?: boolean } = {}) {
      await knex<DbTasksRow>(DB_TASKS_TABLE).insert({
        id,
        settings_json: JSON.stringify(defaultSettings),
        next_run_start_at: opts.ready
          ? nowPlus(Duration.fromObject({ minutes: -1 }), knex)
          : nowPlus(Duration.fromObject({ hours: 1 }), knex),
      });
    }

    function createPoller(signal: AbortSignal) {
      const poller = new TaskStatePoller(
        knex,
        Duration.fromMillis(100),
        mockServices.logger.mock(),
      );
      poller.start(signal);
      return poller;
    }

    it('resolves with ready when a task is ready to run', async () => {
      await insertTask('task-a', { ready: true });

      const ac = new AbortController();
      const poller = createPoller(ac.signal);

      const promise = poller.waitForReady('task-a', ac.signal);
      await jest.advanceTimersByTimeAsync(200);
      const result = await promise;

      expect(result).toMatchObject({
        result: 'ready',
        settings: expect.objectContaining({ version: 2 }),
      });

      ac.abort();
    });

    it('waits until the task becomes ready', async () => {
      await insertTask('task-b', { ready: false });

      const ac = new AbortController();
      const poller = createPoller(ac.signal);

      let resolved = false;
      const promise = poller.waitForReady('task-b', ac.signal).then(r => {
        resolved = true;
        return r;
      });

      await jest.advanceTimersByTimeAsync(300);
      expect(resolved).toBe(false);

      // Make the task ready
      await knex<DbTasksRow>(DB_TASKS_TABLE)
        .where('id', 'task-b')
        .update({
          next_run_start_at: nowPlus(
            Duration.fromObject({ minutes: -1 }),
            knex,
          ),
        });

      await jest.advanceTimersByTimeAsync(200);
      const result = await promise;

      expect(result.result).toBe('ready');
      ac.abort();
    });

    it('resolves with abort when a task disappears from the database', async () => {
      await insertTask('task-c', { ready: false });

      const ac = new AbortController();
      const poller = createPoller(ac.signal);

      const promise = poller.waitForReady('task-c', ac.signal);

      await jest.advanceTimersByTimeAsync(50);
      await knex<DbTasksRow>(DB_TASKS_TABLE).where('id', 'task-c').delete();
      await jest.advanceTimersByTimeAsync(200);

      const result = await promise;
      expect(result.result).toBe('abort');
      ac.abort();
    });

    it('resolves with not-ready-yet when abort signal fires', async () => {
      await insertTask('task-d', { ready: false });

      const pollerAc = new AbortController();
      const poller = createPoller(pollerAc.signal);

      const waiterAc = new AbortController();
      const promise = poller.waitForReady('task-d', waiterAc.signal);

      waiterAc.abort();
      const result = await promise;

      expect(result.result).toBe('not-ready-yet');
      pollerAc.abort();
    });

    it('batches multiple tasks into a single poll cycle', async () => {
      await insertTask('task-e1', { ready: true });
      await insertTask('task-e2', { ready: true });
      await insertTask('task-e3', { ready: false });

      const ac = new AbortController();
      const poller = createPoller(ac.signal);

      const p1 = poller.waitForReady('task-e1', ac.signal);
      const p2 = poller.waitForReady('task-e2', ac.signal);

      await jest.advanceTimersByTimeAsync(200);

      const [r1, r2] = await Promise.all([p1, p2]);
      expect(r1.result).toBe('ready');
      expect(r2.result).toBe('ready');

      ac.abort();
    });

    it('does not resolve for tasks that are currently running', async () => {
      await insertTask('task-f', { ready: true });
      await knex<DbTasksRow>(DB_TASKS_TABLE)
        .where('id', 'task-f')
        .update({ current_run_ticket: 'some-ticket' });

      const ac = new AbortController();
      const poller = createPoller(ac.signal);

      let resolved = false;
      poller.waitForReady('task-f', ac.signal).then(() => {
        resolved = true;
      });

      await jest.advanceTimersByTimeAsync(500);
      expect(resolved).toBe(false);

      ac.abort();
    });
  },
);
