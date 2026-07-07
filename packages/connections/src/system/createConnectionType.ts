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
import { z } from 'zod/v4';
import type { JsonObject } from '@backstage/types';
import type {
  ConnectionAuthMethod,
  ConnectionType,
  MatchAuth,
  WithoutReservedFields,
} from '../api/ConnectionType';

const matchSchema = z
  .object({ plugins: z.array(z.string()) })
  .strict()
  .optional();

const connectionTypeValidators = new WeakMap<ConnectionType, z.ZodType>();

export function parseConnectionTypeConfig(
  connectionType: ConnectionType,
  value: unknown,
): unknown {
  const validator = connectionTypeValidators.get(connectionType);
  if (!validator) {
    throw new Error(
      `No validator found for connection type "${connectionType.type}"`,
    );
  }
  return validator.parse(value);
}

export function createConnectionType<
  TType extends string,
  TConfigSchema extends z.ZodObject,
  const TAuthMethods extends readonly ConnectionAuthMethod[],
>({
  configSchema,
  type,
  title,
  authMethods,
  matchAuth,
}: {
  type: TType;
  title: string;
  configSchema: WithoutReservedFields<TConfigSchema>;
  authMethods: TAuthMethods;
  matchAuth?: MatchAuth<TAuthMethods>;
}): ConnectionType<TType, TConfigSchema, TAuthMethods> {
  const authOptions = authMethods.map(am =>
    am.configSchema
      .extend({
        method: z.literal(am.method),
        match: matchSchema,
      })
      .strict(),
  );
  const validated = configSchema as unknown as TConfigSchema;
  const schema = validated
    .extend({
      type: z.literal(type),
      title: z.string().min(1).optional(),
      match: matchSchema,
      auth: z.array(
        authOptions.length === 1
          ? authOptions[0]
          : z.discriminatedUnion(
              'method',
              authOptions as [(typeof authOptions)[0], ...typeof authOptions],
            ),
      ),
    })
    .strict();
  const connectionType = {
    type,
    title,
    configSchema: validated,
    authMethods,
    schema: {
      ...schema.toJSONSchema({ target: 'draft-07', io: 'input' }),
    } as JsonObject,
    matchAuth,
  };
  connectionTypeValidators.set(connectionType, schema);
  return connectionType;
}
