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

import { mockServices } from '@backstage/backend-test-utils';
import express from 'express';
import { EventEmitter } from 'node:events';
import { createRequest, createResponse } from 'node-mocks-http';
import type { RequestMethod } from 'node-mocks-http';
import { createRouter } from './router';

describe('createRouter', () => {
  let app: express.Express;

  beforeAll(async () => {
    const router = await createRouter({
      connections: {
        find: jest.fn(),
      },
      httpAuth: mockServices.httpAuth.mock(),
      logger: mockServices.logger.mock(),
    });
    app = express().use(router);
  });

  async function request(method: RequestMethod, url: string) {
    const req = createRequest({ method, url });
    const res = createResponse({ eventEmitter: EventEmitter });
    const done = new Promise<void>(resolve => {
      res.on('end', resolve);
    });

    app(req, res);
    await done;

    return {
      status: res.statusCode,
      body: res._getJSONData(),
    };
  }

  describe('GET /schema/:type', () => {
    it('returns the JSON schema for a connection type', async () => {
      const response = await request('GET', '/schema/github');

      expect(response.status).toBe(200);
      expect(response.body).toMatchObject({
        $schema: 'http://json-schema.org/draft-07/schema#',
        type: 'object',
        properties: {
          type: {
            const: 'github',
          },
          host: {
            type: 'string',
          },
          auth: {
            type: 'array',
          },
        },
        required: ['host', 'type', 'auth'],
        additionalProperties: false,
      });
      expect(response.body).not.toHaveProperty('parse');
    });

    it('returns 404 for an unknown connection type', async () => {
      const response = await request('GET', '/schema/unknown');

      expect(response.status).toBe(404);
      expect(response.body).toBe('Cannot find connection type');
    });
  });
});
