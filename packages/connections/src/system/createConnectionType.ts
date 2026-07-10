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
  WithoutReservedAuthMethods,
  WithoutReservedFields,
} from '../api/ConnectionType';

type ConnectionAuthMethodSchema<
  TMethod extends string = string,
  TConfigSchema extends z.ZodObject = z.ZodObject,
> = {
  method: TMethod;
  title: string;
  configSchema: TConfigSchema;
};

type ConnectionAuthMethodsFromSchemas<
  TAuthMethods extends readonly ConnectionAuthMethodSchema[],
> = {
  readonly [I in keyof TAuthMethods]: TAuthMethods[I] extends ConnectionAuthMethodSchema<
    infer TMethod,
    infer TConfigSchema
  >
    ? ConnectionAuthMethod<TMethod, z.infer<TConfigSchema>>
    : never;
};

const matchSchema = z
  .object({ plugins: z.array(z.string()) })
  .strict()
  .optional();

export function createConnectionType<
  TType extends string,
  TConfigSchema extends z.ZodObject,
  const TAuthMethods extends readonly ConnectionAuthMethodSchema[],
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
  authMethods: WithoutReservedAuthMethods<TAuthMethods>;
  matchAuth?: MatchAuth<ConnectionAuthMethodsFromSchemas<TAuthMethods>>;
}): ConnectionType<
  TType,
  z.infer<TConfigSchema>,
  ConnectionAuthMethodsFromSchemas<TAuthMethods>
> {
  const validatedAuthMethods = authMethods as TAuthMethods;
  const authOptions = validatedAuthMethods.map(am =>
    am.configSchema
      .extend({
        method: z.literal(am.method),
        title: z.string().min(1).optional(),
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
  const jsonSchema = {
    ...schema.toJSONSchema({ target: 'draft-07', io: 'input' }),
  } as JsonObject;
  const portableSchema = {
    parse(input: unknown) {
      return schema.parse(input);
    },
    schema() {
      return { schema: structuredClone(jsonSchema) };
    },
  };
  const connectionType = {
    type,
    title,
    authMethods: validatedAuthMethods.map(({ method, title: authTitle }) => ({
      method,
      title: authTitle,
    })),
    configSchema: portableSchema,
    matchAuth,
  } as unknown as ConnectionType<
    TType,
    z.infer<TConfigSchema>,
    ConnectionAuthMethodsFromSchemas<TAuthMethods>
  >;
  return connectionType;
}
