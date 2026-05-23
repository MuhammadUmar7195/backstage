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

import { LoggerService } from '@backstage/backend-plugin-api';
import { createDeferred, DeferredPromise } from '@backstage/types';
import { Knex } from 'knex';
import { Duration } from 'luxon';
import { DB_TASKS_TABLE, DbTasksRow } from '../database/tables';
import { TaskSettingsV2, taskSettingsV2Schema } from './types';

export type TaskPollResult =
  | { result: 'abort' }
  | { result: 'ready'; settings: TaskSettingsV2 };

export interface TaskListener {
  waitForReady(): Promise<TaskPollResult>;
}

interface PendingWaiter {
  taskId: string;
  deferred: DeferredPromise<TaskPollResult>;
}

/**
 * Shared poller that batches per-task readiness checks into a single query.
 *
 * Instead of each TaskWorker independently querying the database every few
 * seconds, workers call `setupListener` once at startup, then call
 * `waitForReady()` on the returned listener in a loop. The poller runs one
 * query per cycle covering all waiting tasks, and resolves the relevant
 * promises when tasks become ready.
 */
export class TaskStatePoller {
  readonly #knex: Knex;
  readonly #pollInterval: Duration;
  readonly #logger: LoggerService;
  readonly #waiters = new Map<string, PendingWaiter[]>();
  #pollTimer: ReturnType<typeof setTimeout> | undefined;
  #pollCycleRunning = false;
  #signal?: AbortSignal;

  constructor(options: {
    knex: Knex;
    pollInterval: Duration;
    logger: LoggerService;
  }) {
    this.#knex = options.knex;
    this.#pollInterval = options.pollInterval;
    this.#logger = options.logger;
  }

  /**
   * Register a listener for a task. The returned object's `waitForReady()`
   * returns a Promise that resolves when the task is detected as ready to
   * run, when the task disappears from the database (abort), or rejects
   * when the poller is shut down.
   */
  setupListener(taskId: string): TaskListener {
    return {
      waitForReady: () => {
        if (this.#signal?.aborted) {
          return Promise.reject(new Error('Poller has been shut down'));
        }

        const deferred = createDeferred<TaskPollResult>();
        const waiter: PendingWaiter = { taskId, deferred };

        let list = this.#waiters.get(taskId);
        if (!list) {
          list = [];
          this.#waiters.set(taskId, list);
        }
        list.push(waiter);

        this.#signal?.addEventListener(
          'abort',
          () => {
            this.#removeWaiter(waiter);
            deferred.reject(new Error('Poller has been shut down'));
          },
          { once: true },
        );

        this.#ensurePolling();

        return deferred;
      },
    };
  }

  /**
   * Start the poller. Must be called before `setupListener`. The poller
   * only runs when there are active waiters; it idles otherwise.
   */
  start(signal: AbortSignal): void {
    this.#signal = signal;
    signal.addEventListener('abort', () => {
      if (this.#pollTimer) {
        clearTimeout(this.#pollTimer);
        this.#pollTimer = undefined;
      }
    });
  }

  #ensurePolling(): void {
    if (this.#pollCycleRunning || this.#pollTimer || this.#signal?.aborted) {
      return;
    }
    this.#pollCycleRunning = true;
    Promise.resolve().then(() => this.#runPollCycle());
  }

  async #runPollCycle(): Promise<void> {
    try {
      await this.#poll();
    } catch (e) {
      this.#logger.warn(`Task state poll failed: ${e}`);
    }
    this.#pollCycleRunning = false;

    if (this.#waiters.size > 0 && !this.#signal?.aborted) {
      this.#pollTimer = setTimeout(() => {
        this.#pollTimer = undefined;
        this.#pollCycleRunning = true;
        this.#runPollCycle();
      }, this.#pollInterval.as('milliseconds'));
    }
  }

  async #poll(): Promise<void> {
    const taskIds = [...this.#waiters.keys()];
    if (taskIds.length === 0) {
      return;
    }

    const rows = await this.#knex<DbTasksRow>(DB_TASKS_TABLE)
      .whereIn('id', taskIds)
      .select({
        id: 'id',
        settingsJson: 'settings_json',
        ready: this.#knex.raw(
          `CASE
            WHEN next_run_start_at <= ? AND current_run_ticket IS NULL THEN TRUE
            ELSE FALSE
          END`,
          [this.#knex.fn.now()],
        ),
      });

    const foundIds = new Set<string>();

    for (const row of rows) {
      foundIds.add(row.id);

      if (!row.ready) {
        continue;
      }

      const waiters = this.#waiters.get(row.id);
      if (!waiters || waiters.length === 0) {
        continue;
      }

      let settings: TaskSettingsV2;
      try {
        settings = taskSettingsV2Schema.parse(JSON.parse(row.settingsJson));
      } catch {
        this.#resolveAll(row.id, { result: 'abort' });
        continue;
      }

      this.#resolveAll(row.id, { result: 'ready', settings });
    }

    for (const taskId of taskIds) {
      if (!foundIds.has(taskId)) {
        this.#resolveAll(taskId, { result: 'abort' });
      }
    }
  }

  #resolveAll(taskId: string, result: TaskPollResult): void {
    const waiters = this.#waiters.get(taskId);
    if (!waiters) {
      return;
    }
    this.#waiters.delete(taskId);

    for (const waiter of waiters) {
      waiter.deferred.resolve(result);
    }
  }

  #removeWaiter(waiter: PendingWaiter): void {
    const list = this.#waiters.get(waiter.taskId);
    if (!list) {
      return;
    }
    const idx = list.indexOf(waiter);
    if (idx >= 0) {
      list.splice(idx, 1);
    }
    if (list.length === 0) {
      this.#waiters.delete(waiter.taskId);
    }
  }
}
