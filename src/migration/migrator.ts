import type { MigrationLockOptions } from '../dialect/dialect-adapter.js'
import type { Kysely } from '../kysely.js'
import type { KyselyPlugin } from '../plugin/kysely-plugin.js'
import { NoopPlugin } from '../plugin/noop-plugin.js'
import { WithSchemaPlugin } from '../plugin/with-schema/with-schema-plugin.js'
import type { CreateSchemaBuilder } from '../schema/create-schema-builder.js'
import type { CreateTableBuilder } from '../schema/create-table-builder.js'
import { freeze, getLast, isObject } from '../util/object-utils.js'
import { ParameterCountGuard } from './parameter-count-guard.js'

export const DEFAULT_MIGRATION_TABLE = 'kysely_migration'
export const DEFAULT_MIGRATION_LOCK_TABLE = 'kysely_migration_lock'
export const DEFAULT_ALLOW_UNORDERED_MIGRATIONS = false
export const MIGRATION_LOCK_ID = 'migration_lock'
export const NO_MIGRATIONS: NoMigrations = freeze({ __noMigrations__: true })

export interface Migration {
  up(db: Kysely<any>): Promise<void>

  /**
   * An optional down method.
   *
   * If you don't provide a down method, migrating down stops with an error
   * when it reaches this migration. The migration is not rolled back and its
   * record is left in the migration table.
   */
  down?(db: Kysely<any>): Promise<void>
}

/**
 * A class for running migrations.
 *
 * ### Example
 *
 * This example uses the {@link FileMigrationProvider} that reads migrations
 * files from a single folder. You can easily implement your own
 * {@link MigrationProvider} if you want to provide migrations some
 * other way.
 *
 * ```ts
 * import { promises as fs } from 'node:fs'
 * import path from 'node:path'
 * import * as Sqlite from 'better-sqlite3'
 * import { Kysely, SqliteDialect } from 'kysely'
 * import { FileMigrationProvider, Migrator } from 'kysely/migration'
 *
 * const db = new Kysely<any>({
 *   dialect: new SqliteDialect({
 *     database: Sqlite(':memory:')
 *   })
 * })
 *
 * const migrator = new Migrator({
 *   db,
 *   provider: new FileMigrationProvider({
 *     fs,
 *     // Path to the folder that contains all your migrations.
 *     migrationFolder: 'some/path/to/migrations',
 *     path,
 *   })
 * })
 * ```
 */
export class Migrator {
  readonly #props: MigratorProps

  constructor(props: MigratorProps) {
    this.#props = freeze(props)
  }

  /**
   * Returns a {@link MigrationInfo} object for each migration.
   *
   * The returned array is sorted by migration name.
   */
  async getMigrations(): Promise<ReadonlyArray<MigrationInfo>> {
    const tableExists = await this.#doesTableExist(this.#migrationTable)

    const executedMigrations = tableExists
      ? await this.#props.db
          .withPlugin(this.#schemaPlugin)
          .selectFrom(this.#migrationTable)
          .select(['name', 'timestamp'])
          .$narrowType<{ name: string; timestamp: string }>()
          .execute()
      : []

    const migrations = await this.#resolveMigrations()

    return migrations.map(({ name, ...migration }) => {
      const executed = executedMigrations.find((it) => it.name === name)

      return {
        name,
        migration,
        executedAt: executed ? new Date(executed.timestamp) : undefined,
      }
    })
  }

  /**
   * Runs all migrations that have not yet been run.
   *
   * This method returns a {@link MigrationResultSet} instance and _never_ throws.
   * {@link MigrationResultSet.error} holds the error if something went wrong.
   * {@link MigrationResultSet.results} contains information about which migrations
   * were executed and which failed. See the examples below.
   *
   * This method goes through all possible migrations provided by the provider and runs the
   * ones whose names come alphabetically after the last migration that has been run. If the
   * list of executed migrations doesn't match the beginning of the list of possible migrations
   * an error is returned.
   *
   * ### Examples
   *
   * ```ts
   * import { promises as fs } from 'node:fs'
   * import path from 'node:path'
   * import * as Sqlite from 'better-sqlite3'
   * import { FileMigrationProvider, Migrator } from 'kysely/migration'
   *
   * const migrator = new Migrator({
   *   db,
   *   provider: new FileMigrationProvider({
   *     fs,
   *     migrationFolder: 'some/path/to/migrations',
   *     path,
   *   })
   * })
   *
   * const { error, results } = await migrator.migrateToLatest()
   *
   * results?.forEach((it) => {
   *   if (it.status === 'Success') {
   *     console.log(`migration "${it.migrationName}" was executed successfully`)
   *   } else if (it.status === 'Error') {
   *     console.error(`failed to execute migration "${it.migrationName}"`)
   *   }
   * })
   *
   * if (error) {
   *   console.error('failed to run `migrateToLatest`')
   *   console.error(error)
   * }
   * ```
   */
  async migrateToLatest(options?: MigrateOptions): Promise<MigrationResultSet> {
    return this.#migrate(() => ({ direction: 'Up', step: Infinity }), options)
  }

  /**
   * Migrate up/down to a specific migration.
   *
   * This method returns a {@link MigrationResultSet} instance and _never_ throws.
   * {@link MigrationResultSet.error} holds the error if something went wrong.
   * {@link MigrationResultSet.results} contains information about which migrations
   * were executed and which failed.
   *
   * ### Examples
   *
   * ```ts
   * import { promises as fs } from 'node:fs'
   * import path from 'node:path'
   * import { FileMigrationProvider, Migrator } from 'kysely/migration'
   *
   * const migrator = new Migrator({
   *   db,
   *   provider: new FileMigrationProvider({
   *     fs,
   *     // Path to the folder that contains all your migrations.
   *     migrationFolder: 'some/path/to/migrations',
   *     path,
   *   })
   * })
   *
   * await migrator.migrateTo('some_migration')
   * ```
   *
   * If you specify the name of the first migration, this method migrates
   * down to the first migration, but doesn't run the `down` method of
   * the first migration. In case you want to migrate all the way down,
   * you can use a special constant `NO_MIGRATIONS`:
   *
   * ```ts
   * import { promises as fs } from 'node:fs'
   * import path from 'node:path'
   * import { FileMigrationProvider, Migrator, NO_MIGRATIONS } from 'kysely/migration'
   *
   * const migrator = new Migrator({
   *   db,
   *   provider: new FileMigrationProvider({
   *     fs,
   *     // Path to the folder that contains all your migrations.
   *     migrationFolder: 'some/path/to/migrations',
   *     path,
   *   })
   * })
   *
   * await migrator.migrateTo(NO_MIGRATIONS)
   * ```
   */
  async migrateTo(
    targetMigrationName: string | NoMigrations,
    options?: MigrateOptions,
  ): Promise<MigrationResultSet> {
    return this.#migrate(
      ({
        migrations,
        executedMigrations,
        pendingMigrations,
      }: MigrationState) => {
        if (
          isObject(targetMigrationName) &&
          targetMigrationName.__noMigrations__ === true
        ) {
          return { direction: 'Down', step: Infinity }
        }

        if (
          !migrations.find((m) => m.name === (targetMigrationName as string))
        ) {
          throw new Error(`migration "${targetMigrationName}" doesn't exist`)
        }

        const executedIndex = executedMigrations.indexOf(
          targetMigrationName as string,
        )

        const pendingIndex = pendingMigrations.findIndex(
          (m) => m.name === (targetMigrationName as string),
        )

        if (executedIndex !== -1) {
          return {
            direction: 'Down',
            step: executedMigrations.length - executedIndex - 1,
          }
        }

        if (pendingIndex !== -1) {
          return { direction: 'Up', step: pendingIndex + 1 }
        }

        throw new Error(
          `migration "${targetMigrationName}" isn't executed or pending`,
        )
      },
      options,
    )
  }

  /**
   * Migrate one step up.
   *
   * This method returns a {@link MigrationResultSet} instance and _never_ throws.
   * {@link MigrationResultSet.error} holds the error if something went wrong.
   * {@link MigrationResultSet.results} contains information about which migrations
   * were executed and which failed.
   *
   * ### Examples
   *
   * ```ts
   * import { promises as fs } from 'node:fs'
   * import path from 'node:path'
   * import { FileMigrationProvider, Migrator } from 'kysely/migration'
   *
   * const migrator = new Migrator({
   *   db,
   *   provider: new FileMigrationProvider({
   *     fs,
   *     // Path to the folder that contains all your migrations.
   *     migrationFolder: 'some/path/to/migrations',
   *     path,
   *   })
   * })
   *
   * await migrator.migrateUp()
   * ```
   */
  async migrateUp(options?: MigrateOptions): Promise<MigrationResultSet> {
    return this.#migrate(() => ({ direction: 'Up', step: 1 }), options)
  }

  /**
   * Migrate one step down.
   *
   * This method returns a {@link MigrationResultSet} instance and _never_ throws.
   * {@link MigrationResultSet.error} holds the error if something went wrong.
   * {@link MigrationResultSet.results} contains information about which migrations
   * were executed and which failed.
   *
   * ### Examples
   *
   * ```ts
   * import { promises as fs } from 'node:fs'
   * import path from 'node:path'
   * import { FileMigrationProvider, Migrator } from 'kysely/migration'
   *
   * const migrator = new Migrator({
   *   db,
   *   provider: new FileMigrationProvider({
   *     fs,
   *     // Path to the folder that contains all your migrations.
   *     migrationFolder: 'some/path/to/migrations',
   *     path,
   *   })
   * })
   *
   * await migrator.migrateDown()
   * ```
   */
  async migrateDown(options?: MigrateOptions): Promise<MigrationResultSet> {
    return this.#migrate(() => ({ direction: 'Down', step: 1 }), options)
  }

  async #migrate(
    getMigrationDirectionAndStep: (state: MigrationState) => {
      direction: MigrationDirection
      step: number
    },
    options: MigrateOptions | undefined,
  ): Promise<MigrationResultSet> {
    try {
      await this.#ensureMigrationTableSchemaExists()
      await this.#ensureMigrationTableExists()
      await this.#ensureMigrationLockTableExists()

      return await this.#runMigrations(getMigrationDirectionAndStep, options)
    } catch (error) {
      if (error instanceof MigrationResultSetError) {
        return error.resultSet
      }

      return { error }
    }
  }

  get #migrationTableSchema(): string | undefined {
    return this.#props.migrationTableSchema
  }

  get #migrationTable(): string {
    return this.#props.migrationTableName ?? DEFAULT_MIGRATION_TABLE
  }

  get #migrationLockTable(): string {
    return this.#props.migrationLockTableName ?? DEFAULT_MIGRATION_LOCK_TABLE
  }

  get #allowUnorderedMigrations(): boolean {
    return (
      this.#props.allowUnorderedMigrations ?? DEFAULT_ALLOW_UNORDERED_MIGRATIONS
    )
  }

  get #schemaPlugin(): KyselyPlugin {
    if (this.#migrationTableSchema) {
      return new WithSchemaPlugin(this.#migrationTableSchema)
    }

    return new NoopPlugin()
  }

  /**
   * A copy of the database without any user plugins.
   *
   * The migrator's internal tables are created through this handle so that
   * the bookkeeping schema always gets created exactly the same way, no
   * matter what plugins the user has installed. Everything executed during
   * the migration run itself goes through the user's plugins.
   */
  get #internalDb(): Kysely<any> {
    return this.#props.db.withoutPlugins()
  }

  async #ensureMigrationTableSchemaExists(): Promise<void> {
    if (!this.#migrationTableSchema) {
      // Use default schema. Nothing to do.
      return
    }

    const schemaExists = await this.#doesSchemaExist()

    if (schemaExists) {
      return
    }

    try {
      await this.#createIfNotExists(
        this.#internalDb.schema.createSchema(this.#migrationTableSchema),
      )
    } catch (error) {
      const schemaExists = await this.#doesSchemaExist()

      // At least on PostgreSQL, `if not exists` doesn't guarantee the `create schema`
      // query doesn't throw if the schema already exits. That's why we check if
      // the schema exist here and ignore the error if it does.
      if (!schemaExists) {
        throw error
      }
    }
  }

  async #ensureMigrationTableExists(): Promise<void> {
    const tableExists = await this.#doesTableExist(this.#migrationTable)

    if (tableExists) {
      return
    }

    try {
      await this.#createIfNotExists(
        this.#internalDb.schema
          .withPlugin(this.#schemaPlugin)
          .createTable(this.#migrationTable)
          .addColumn('name', 'varchar(255)', (col) =>
            col.notNull().primaryKey(),
          )
          // The migration run time as ISO string. This is not a real date type as we
          // can't know which data type is supported by all future dialects.
          .addColumn('timestamp', 'varchar(255)', (col) => col.notNull()),
      )
    } catch (error) {
      const tableExists = await this.#doesTableExist(this.#migrationTable)

      // At least on PostgreSQL, `if not exists` doesn't guarantee the `create table`
      // query doesn't throw if the table already exits. That's why we check if
      // the table exist here and ignore the error if it does.
      if (!tableExists) {
        throw error
      }
    }
  }

  async #ensureMigrationLockTableExists(): Promise<void> {
    const tableExists = await this.#doesTableExist(this.#migrationLockTable)

    if (tableExists) {
      return
    }

    try {
      await this.#createIfNotExists(
        this.#internalDb.schema
          .withPlugin(this.#schemaPlugin)
          .createTable(this.#migrationLockTable)
          .addColumn('id', 'varchar(255)', (col) => col.notNull().primaryKey())
          .addColumn('is_locked', 'integer', (col) =>
            col.notNull().defaultTo(0),
          ),
      )
    } catch (error) {
      const tableExists = await this.#doesTableExist(this.#migrationLockTable)

      // At least on PostgreSQL, `if not exists` doesn't guarantee the `create table`
      // query doesn't throw if the table already exits. That's why we check if
      // the table exist here and ignore the error if it does.
      if (!tableExists) {
        throw error
      }
    }
  }

  async #ensureLockRowExists(db: Kysely<any>): Promise<void> {
    const lockRowExists = await this.#doesLockRowExists(db)

    if (lockRowExists) {
      return
    }

    try {
      await db
        .withPlugin(this.#schemaPlugin)
        .insertInto(this.#migrationLockTable)
        .values({ id: MIGRATION_LOCK_ID, is_locked: 0 })
        .execute()
    } catch (error) {
      const lockRowExists = await this.#doesLockRowExists(db)

      if (!lockRowExists) {
        throw error
      }
    }
  }

  async #doesSchemaExist(): Promise<boolean> {
    const schemas = await this.#props.db.introspection.getSchemas()

    return schemas.some((it) => it.name === this.#migrationTableSchema)
  }

  async #doesTableExist(tableName: string): Promise<boolean> {
    const schema = this.#migrationTableSchema

    const tables = await this.#props.db.introspection.getTables({
      withInternalKyselyTables: true,
    })

    return tables.some(
      (it) => it.name === tableName && (!schema || it.schema === schema),
    )
  }

  async #doesLockRowExists(db: Kysely<any>): Promise<boolean> {
    const lockRow = await db
      .withPlugin(this.#schemaPlugin)
      .selectFrom(this.#migrationLockTable)
      .where('id', '=', MIGRATION_LOCK_ID)
      .select('id')
      .executeTakeFirst()

    return !!lockRow
  }

  async #runMigrations(
    getMigrationDirectionAndStep: (state: MigrationState) => {
      direction: MigrationDirection
      step: number
    },
    options: MigrateOptions | undefined,
  ): Promise<MigrationResultSet> {
    const adapter = this.#props.db.getExecutor().adapter

    const lockOptions: MigrationLockOptions = freeze({
      lockTable:
        this.#props.migrationLockTableName ?? DEFAULT_MIGRATION_LOCK_TABLE,
      lockRowId: MIGRATION_LOCK_ID,
      lockTableSchema: this.#props.migrationTableSchema,
    })

    const disableTransactions =
      options?.disableTransactions ?? this.#props.disableTransactions

    // Each migration runs in its own transaction when the dialect supports
    // transactional DDL and transactions haven't been disabled. A migration
    // is recorded only after it has fully finished, so a failed migration
    // is rolled back together with its record attempt while already
    // finished migrations stay recorded.
    const transactional =
      !disableTransactions && adapter.supportsTransactionalDdl === true

    const run = async (db: Kysely<any>): Promise<MigrationResultSet> => {
      // All queries of the migration run go through the user's plugins,
      // guarded so that no plugin can change a query's parameter count.
      const guardedDb = this.#withMigrationPlugins(db)

      await this.#ensureLockRowExists(guardedDb)

      const state = await this.#getState(guardedDb)

      if (state.migrations.length === 0) {
        return { results: [] }
      }

      const { direction, step } = getMigrationDirectionAndStep(state)

      if (step <= 0) {
        return { results: [] }
      }

      if (direction === 'Down') {
        return await this.#migrateDown(guardedDb, state, step, transactional)
      } else if (direction === 'Up') {
        return await this.#migrateUp(guardedDb, state, step, transactional)
      }

      return { results: [] }
    }

    const runWithLock = async (
      db: Kysely<any>,
      cb: (db: Kysely<any>) => Promise<MigrationResultSet>,
    ): Promise<MigrationResultSet> => {
      try {
        await adapter.acquireMigrationLock(db, lockOptions)
        return await cb(db)
      } finally {
        await adapter.releaseMigrationLock(db, lockOptions)
      }
    }

    if (this.#props.db.isTransaction) {
      if (!adapter.supportsTransactionalDdl) {
        throw new Error(
          'Transactional DDL is not supported in this dialect. Passing a transaction to this migrator would result in failure or unexpected behavior.',
        )
      }

      if (disableTransactions) {
        throw new Error(
          '`disableTransactions` is true but the migrator was given a transaction. Passing a transaction to this migrator would result in failure or unexpected behavior.',
        )
      }

      return runWithLock(this.#props.db, run)
    }

    if (!adapter.supportsTransactionalDdl && !disableTransactions) {
      // The dialect can't run schema changes in a transaction. Say so before
      // running anything so a half-finished migration never comes as a
      // surprise. A migration is still only recorded after it has fully
      // finished.
      console.log(
        'kysely: warning: this dialect does not support transactional DDL. ' +
          'Migrations are executed without a transaction. If a migration ' +
          'fails, the schema changes it already made cannot be rolled back ' +
          'automatically, but the migration is not recorded as executed.',
      )
    }

    // A single connection is reserved for the whole migration run. It is not
    // lent to any other query while migrations are running and is returned
    // to the pool once the run is over.
    return this.#props.db.connection().execute((db) => runWithLock(db, run))
  }

  /**
   * Installs the plugins that guard the queries executed by migrations.
   *
   * The guard's `before` plugin is placed at the front of the plugin chain
   * and its `after` plugin at the back. User plugins sit between them, see
   * each other's transformed operation nodes in registration order, and are
   * not allowed to change the query's parameter count.
   *
   * When no user plugins are installed, there's nothing to guard and the
   * database is returned as is. This way a migrator that was given a
   * transaction runs the migrations in exactly that transaction object.
   */
  #withMigrationPlugins(db: Kysely<any>): Kysely<any> {
    const plugins = db.getExecutor().plugins

    if (plugins.length === 0) {
      return db
    }

    const guard = new ParameterCountGuard()

    let guarded = db.withoutPlugins().withPlugin(guard.before)

    for (const plugin of plugins) {
      guarded = guarded.withPlugin(plugin)
    }

    return guarded.withPlugin(guard.after)
  }

  async #getState(db: Kysely<any>): Promise<MigrationState> {
    const migrations = await this.#resolveMigrations()
    const executedMigrations = await this.#getExecutedMigrations(db)

    this.#ensureNoMissingMigrations(migrations, executedMigrations)
    if (!this.#allowUnorderedMigrations) {
      this.#ensureMigrationsInOrder(migrations, executedMigrations)
    }

    const pendingMigrations = this.#getPendingMigrations(
      migrations,
      executedMigrations,
    )

    return freeze({
      migrations,
      executedMigrations,
      lastMigration: getLast(executedMigrations),
      pendingMigrations,
    })
  }

  #getPendingMigrations(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>,
  ): ReadonlyArray<NamedMigration> {
    return migrations.filter((migration) => {
      return !executedMigrations.includes(migration.name)
    })
  }

  async #resolveMigrations(): Promise<ReadonlyArray<NamedMigration>> {
    const allMigrations = await this.#props.provider.getMigrations()

    return Object.keys(allMigrations)
      .sort()
      .map((name) => ({
        ...allMigrations[name],
        name,
      }))
  }

  async #getExecutedMigrations(
    db: Kysely<any>,
  ): Promise<ReadonlyArray<string>> {
    const executedMigrations = await db
      .withPlugin(this.#schemaPlugin)
      .selectFrom(this.#migrationTable)
      .select(['name', 'timestamp'])
      .$narrowType<{ name: string; timestamp: string }>()
      .execute()

    const nameComparator =
      this.#props.nameComparator || ((a, b) => a.localeCompare(b))

    return (
      executedMigrations
        // https://github.com/kysely-org/kysely/issues/843
        .sort((a, b) => {
          if (a.timestamp === b.timestamp) {
            return nameComparator(a.name, b.name)
          }

          return (
            new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
          )
        })
        .map((it) => it.name)
    )
  }

  #ensureNoMissingMigrations(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>,
  ) {
    // Ensure all executed migrations exist in the `migrations` list.
    for (const executed of executedMigrations) {
      if (!migrations.some((it) => it.name === executed)) {
        throw new Error(
          `corrupted migrations: previously executed migration ${executed} is missing`,
        )
      }
    }
  }

  #ensureMigrationsInOrder(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>,
  ) {
    // Ensure the executed migrations are the first ones in the migration list.
    for (let i = 0; i < executedMigrations.length; ++i) {
      if (migrations[i].name !== executedMigrations[i]) {
        throw new Error(
          `corrupted migrations: expected previously executed migration ${executedMigrations[i]} to be at index ${i} but ${migrations[i].name} was found in its place. New migrations must always have a name that comes alphabetically after the last executed migration.`,
        )
      }
    }
  }

  async #migrateDown(
    db: Kysely<any>,
    state: MigrationState,
    step: number,
    transactional: boolean,
  ): Promise<MigrationResultSet> {
    const migrationsToRollback: ReadonlyArray<NamedMigration> =
      state.executedMigrations
        .toReversed()
        .slice(0, step)
        .map((name) => {
          return state.migrations.find((it) => it.name === name)!
        })

    const results: MigrationResult[] = migrationsToRollback.map((migration) => {
      return {
        migrationName: migration.name,
        direction: 'Down',
        status: 'NotExecuted',
      }
    })

    for (let i = 0; i < results.length; ++i) {
      const migration = migrationsToRollback[i]
      const { down } = migration

      if (!down) {
        // A migration without a down method can't be rolled back. Stop right
        // here instead of deleting its record or rolling back even earlier
        // migrations.
        results[i] = {
          migrationName: migration.name,
          direction: 'Down',
          status: 'Error',
        }

        throw new MigrationResultSetError({
          error: new Error(
            `migration "${migration.name}" doesn't have a "down" method. ` +
              'Cannot roll it back. Its record was left in the migration table.',
          ),
          results,
        })
      }

      try {
        // Each rollback runs in its own transaction (when the dialect
        // supports transactional DDL). The record is deleted only after the
        // rollback has fully finished. If anything fails, the whole step is
        // rolled back and the record is left in the migration table.
        await this.#runMigrationStep(db, transactional, async (db) => {
          await down(db)
          await db
            .withPlugin(this.#schemaPlugin)
            .deleteFrom(this.#migrationTable)
            .where('name', '=', migration.name)
            .execute()
        })

        results[i] = {
          migrationName: migration.name,
          direction: 'Down',
          status: 'Success',
        }
      } catch (error) {
        results[i] = {
          migrationName: migration.name,
          direction: 'Down',
          status: 'Error',
        }

        throw new MigrationResultSetError({
          error,
          results,
        })
      }
    }

    return { results }
  }

  async #migrateUp(
    db: Kysely<any>,
    state: MigrationState,
    step: number,
    transactional: boolean,
  ): Promise<MigrationResultSet> {
    const migrationsToRun: ReadonlyArray<NamedMigration> =
      state.pendingMigrations.slice(0, step)

    const results: MigrationResult[] = migrationsToRun.map((migration) => {
      return {
        migrationName: migration.name,
        direction: 'Up',
        status: 'NotExecuted',
      }
    })

    for (let i = 0; i < results.length; i++) {
      const migration = migrationsToRun[i]

      try {
        // Each migration runs in its own transaction (when the dialect
        // supports transactional DDL). The migration is recorded only after
        // it has fully finished. If anything fails, the whole step is rolled
        // back and nothing is recorded.
        await this.#runMigrationStep(db, transactional, async (db) => {
          await migration.up(db)
          await db
            .withPlugin(this.#schemaPlugin)
            .insertInto(this.#migrationTable)
            .values({
              name: migration.name,
              timestamp: new Date().toISOString(),
            })
            .execute()
        })

        results[i] = {
          migrationName: migration.name,
          direction: 'Up',
          status: 'Success',
        }
      } catch (error) {
        results[i] = {
          migrationName: migration.name,
          direction: 'Up',
          status: 'Error',
        }

        throw new MigrationResultSetError({
          error,
          results,
        })
      }
    }

    return { results }
  }

  /**
   * Runs a single migration step.
   *
   * When the dialect supports transactional DDL and transactions haven't been
   * disabled, the step is wrapped in its own transaction so a failure rolls
   * back everything the step did, including schema changes. When the
   * migrator was given a transaction, the user owns the transaction and the
   * step runs directly in it.
   */
  async #runMigrationStep(
    db: Kysely<any>,
    transactional: boolean,
    step: (db: Kysely<any>) => Promise<void>,
  ): Promise<void> {
    if (transactional && !db.isTransaction) {
      await db.transaction().execute(step)
      return
    }

    await step(db)
  }

  async #createIfNotExists(
    qb: CreateTableBuilder<any, any> | CreateSchemaBuilder,
  ): Promise<void> {
    if (this.#props.db.getExecutor().adapter.supportsCreateIfNotExists) {
      qb = qb.ifNotExists()
    }

    await qb.execute()
  }
}

export interface MigrateOptions {
  /**
   * When `true`, don't run migrations in transactions even if the dialect supports transactional DDL.
   *
   * Default is `false`.
   *
   * This is useful when some migrations include queries that would fail otherwise.
   */
  readonly disableTransactions?: boolean
}

export interface MigratorProps extends MigrateOptions {
  readonly db: Kysely<any>
  readonly provider: MigrationProvider

  /**
   * The name of the internal migration table. Defaults to `kysely_migration`.
   *
   * If you do specify this, you need to ALWAYS use the same value. Kysely doesn't
   * support changing the table on the fly. If you run the migrator even once with a
   * table name X and then change the table name to Y, kysely will create a new empty
   * migration table and attempt to run the migrations again, which will obviously
   * fail.
   *
   * If you do specify this, ALWAYS ALWAYS use the same value from the beginning of
   * the project, to the end of time or prepare to manually migrate the migration
   * tables.
   */
  readonly migrationTableName?: string

  /**
   * The name of the internal migration lock table. Defaults to `kysely_migration_lock`.
   *
   * If you do specify this, you need to ALWAYS use the same value. Kysely doesn't
   * support changing the table on the fly. If you run the migrator even once with a
   * table name X and then change the table name to Y, kysely will create a new empty
   * lock table.
   *
   * If you do specify this, ALWAYS ALWAYS use the same value from the beginning of
   * the project, to the end of time or prepare to manually migrate the migration
   * tables.
   */
  readonly migrationLockTableName?: string

  /**
   * The schema of the internal migration tables. Defaults to the default schema
   * on dialects that support schemas.
   *
   * If you do specify this, you need to ALWAYS use the same value. Kysely doesn't
   * support changing the schema on the fly. If you run the migrator even once with a
   * schema name X and then change the schema name to Y, kysely will create a new empty
   * migration tables in the new schema and attempt to run the migrations again, which
   * will obviously fail.
   *
   * If you do specify this, ALWAYS ALWAYS use the same value from the beginning of
   * the project, to the end of time or prepare to manually migrate the migration
   * tables.
   *
   * This only works on postgres and mssql.
   */
  readonly migrationTableSchema?: string

  /**
   * Enforces whether or not migrations must be run in alpha-numeric order.
   *
   * When false, migrations must be run in their exact alpha-numeric order.
   * This is checked against the migrations already run in the database
   * (`migrationTableName`). This ensures your migrations are always run in
   * the same order and is the safest option.
   *
   * When true, migrations are still run in alpha-numeric order, but
   * the order is not checked against already-run migrations in the database.
   * Kysely will simply run all migrations that haven't run yet, in alpha-numeric
   * order.
   */
  readonly allowUnorderedMigrations?: boolean

  /**
   * A function that compares migration names, used when sorting migrations in
   * ascending order.
   *
   * Default is `name0.localeCompare(name1)`.
   */
  readonly nameComparator?: (name0: string, name1: string) => number
}

/**
 * All migration methods ({@link Migrator.migrateTo | migrateTo},
 * {@link Migrator.migrateToLatest | migrateToLatest} etc.) never
 * throw but return this object instead.
 */
export interface MigrationResultSet {
  /**
   * This is defined if something went wrong.
   *
   * An error may have occurred in one of the migrations in which case the
   * {@link results} list contains an item with `status === 'Error'` to
   * indicate which migration failed.
   *
   * An error may also have occurred before Kysely was able to figure out
   * which migrations should be executed, in which case the {@link results}
   * list is undefined.
   */
  readonly error?: unknown

  /**
   * {@link MigrationResult} for each individual migration that was supposed
   * to be executed by the operation.
   *
   * If all went well, each result's `status` is `Success`. If some migration
   * failed, the failed migration's result's `status` is `Error` and all
   * results after that one have `status` ´NotExecuted`.
   *
   * This property can be undefined if an error occurred before Kysely was
   * able to figure out which migrations should be executed.
   *
   * If this list is empty, there were no migrations to execute.
   */
  readonly results?: MigrationResult[]
}

type MigrationDirection = 'Up' | 'Down'

export interface MigrationResult {
  readonly migrationName: string

  /**
   * The direction in which this migration was executed.
   */
  readonly direction: MigrationDirection

  /**
   * The execution status.
   *
   *  - `Success` means the migration was successfully executed. Note that
   *    each migration runs in its own transaction when the dialect supports
   *    transactional DDL, so a later migration's failure doesn't roll back
   *    this migration.
   *
   *  - `Error` means the migration failed. In this case the
   *    {@link MigrationResultSet.error} contains the error.
   *
   *  - `NotExecuted` means that the migration was supposed to be executed
   *    but wasn't because an earlier migration failed.
   */
  readonly status: 'Success' | 'Error' | 'NotExecuted'
}

export interface MigrationProvider {
  /**
   * Returns all migrations, old and new.
   *
   * For example if you have your migrations in a folder as separate files,
   * you can implement this method to return all migration in that folder
   * as {@link Migration} objects.
   *
   * Kysely already has a built-in {@link FileMigrationProvider} for node.js
   * that does exactly that.
   *
   * The keys of the returned object are migration names and values are the
   * migrations. The order of the migrations is determined by the alphabetical
   * order of the migration names. The items in the object don't need to be
   * sorted, they are sorted by Kysely.
   */
  getMigrations(): Promise<Record<string, Migration>>
}

/**
 * Type for the {@link NO_MIGRATIONS} constant. Never create one of these.
 */
export interface NoMigrations {
  readonly __noMigrations__: true
}

export interface MigrationInfo {
  /**
   * Name of the migration.
   */
  name: string

  /**
   * The actual migration.
   */
  migration: Migration

  /**
   * When was the migration executed.
   *
   * If this is undefined, the migration hasn't been executed yet.
   */
  executedAt?: Date
}

interface NamedMigration extends Migration {
  readonly name: string
}

interface MigrationState {
  // All migrations sorted by name.
  readonly migrations: ReadonlyArray<NamedMigration>

  // Names of executed migrations sorted by execution timestamp
  readonly executedMigrations: ReadonlyArray<string>

  // Name of the last executed migration.
  readonly lastMigration?: string

  // Migrations that have not yet ran
  readonly pendingMigrations: ReadonlyArray<NamedMigration>
}

class MigrationResultSetError extends Error {
  readonly #resultSet: MigrationResultSet

  constructor(result: MigrationResultSet) {
    super()
    this.#resultSet = result
  }

  get resultSet(): MigrationResultSet {
    return this.#resultSet
  }
}
