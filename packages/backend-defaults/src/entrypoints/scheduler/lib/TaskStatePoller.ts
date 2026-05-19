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
import { Knex } from 'knex';
import { Duration } from 'luxon';
import { DB_TASKS_TABLE, DbTasksRow } from '../database/tables';
import { TaskSettingsV2, taskSettingsV2Schema } from './types';

export type TaskPollResult =
  | { result: 'not-ready-yet' }
  | { result: 'abort' }
  | { result: 'ready'; settings: TaskSettingsV2 };

interface PendingWaiter {
  taskId: string;
  resolve: (result: TaskPollResult) => void;
  cleanup: () => void;
}

/**
 * Shared poller that batches per-task readiness checks into a single query.
 *
 * Instead of each TaskWorker independently querying the database every few
 * seconds, workers call `waitForReady` which returns a Promise. The poller
 * runs one query per cycle covering all waiting tasks, and resolves the
 * relevant promises when tasks become ready.
 */
export class TaskStatePoller {
  private readonly waiters = new Map<string, PendingWaiter[]>();
  private pollTimer: ReturnType<typeof setTimeout> | undefined;
  private pollCycleRunning = false;
  private signal?: AbortSignal;

  constructor(
    private readonly knex: Knex,
    private readonly pollInterval: Duration,
    private readonly logger: LoggerService,
  ) {}

  /**
   * Returns a Promise that resolves when the given task is detected as ready
   * to run, or when the task disappears from the database (abort), or when
   * the signal fires.
   */
  waitForReady(taskId: string, signal: AbortSignal): Promise<TaskPollResult> {
    if (signal.aborted) {
      return Promise.resolve({ result: 'not-ready-yet' });
    }

    return new Promise<TaskPollResult>(resolve => {
      const waiter: PendingWaiter = {
        taskId,
        resolve,
        cleanup: () => {},
      };

      const onAbort = () => {
        this.removeWaiter(waiter);
        resolve({ result: 'not-ready-yet' });
      };

      waiter.cleanup = () => {
        signal.removeEventListener('abort', onAbort);
      };

      signal.addEventListener('abort', onAbort, { once: true });

      let list = this.waiters.get(taskId);
      if (!list) {
        list = [];
        this.waiters.set(taskId, list);
      }
      list.push(waiter);

      this.ensurePolling();
    });
  }

  /**
   * Start the poller. Must be called before `waitForReady`. The poller only
   * runs when there are active waiters; it idles otherwise.
   */
  start(signal: AbortSignal): void {
    this.signal = signal;
    signal.addEventListener('abort', () => {
      if (this.pollTimer) {
        clearTimeout(this.pollTimer);
        this.pollTimer = undefined;
      }
    });
  }

  private ensurePolling(): void {
    if (this.pollCycleRunning || this.pollTimer || this.signal?.aborted) {
      return;
    }
    // Use a microtask for the initial poll so it fires immediately even
    // under fake timers (setTimeout(fn, 0) requires timer advancement).
    this.pollCycleRunning = true;
    Promise.resolve().then(() => this.runPollCycle());
  }

  private async runPollCycle(): Promise<void> {
    try {
      await this.poll();
    } catch (e) {
      this.logger.warn(`Task state poll failed: ${e}`);
    }
    this.pollCycleRunning = false;

    if (this.waiters.size > 0 && !this.signal?.aborted) {
      this.pollTimer = setTimeout(() => {
        this.pollTimer = undefined;
        this.pollCycleRunning = true;
        this.runPollCycle();
      }, this.pollInterval.as('milliseconds'));
    }
  }

  private async poll(): Promise<void> {
    const taskIds = [...this.waiters.keys()];
    if (taskIds.length === 0) {
      return;
    }

    const rows = await this.knex<DbTasksRow>(DB_TASKS_TABLE)
      .whereIn('id', taskIds)
      .select({
        id: 'id',
        settingsJson: 'settings_json',
        ready: this.knex.raw(
          `CASE
            WHEN next_run_start_at <= ? AND current_run_ticket IS NULL THEN TRUE
            ELSE FALSE
          END`,
          [this.knex.fn.now()],
        ),
      });

    const foundIds = new Set<string>();

    for (const row of rows) {
      foundIds.add(row.id);

      if (!row.ready) {
        continue;
      }

      const waiters = this.waiters.get(row.id);
      if (!waiters || waiters.length === 0) {
        continue;
      }

      let settings: TaskSettingsV2;
      try {
        settings = taskSettingsV2Schema.parse(JSON.parse(row.settingsJson));
      } catch {
        this.resolveAll(row.id, { result: 'abort' });
        continue;
      }

      this.resolveAll(row.id, { result: 'ready', settings });
    }

    for (const taskId of taskIds) {
      if (!foundIds.has(taskId)) {
        this.resolveAll(taskId, { result: 'abort' });
      }
    }
  }

  private resolveAll(taskId: string, result: TaskPollResult): void {
    const waiters = this.waiters.get(taskId);
    if (!waiters) {
      return;
    }
    this.waiters.delete(taskId);

    for (const waiter of waiters) {
      waiter.cleanup();
      waiter.resolve(result);
    }
  }

  private removeWaiter(waiter: PendingWaiter): void {
    const list = this.waiters.get(waiter.taskId);
    if (!list) {
      return;
    }
    const idx = list.indexOf(waiter);
    if (idx >= 0) {
      list.splice(idx, 1);
    }
    if (list.length === 0) {
      this.waiters.delete(waiter.taskId);
    }
  }
}
