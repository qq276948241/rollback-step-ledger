import Database from 'better-sqlite3'
import { PGlite } from '@electric-sql/pglite'
import { expect } from 'chai'
import {
  ColumnNode,
  Kysely,
  OperationNodeTransformer,
  PGliteDialect,
  PrimitiveValueListNode,
  RawNode,
  SelectQueryNode,
  SqliteDialect,
  ValueNode,
  ValuesNode,
  WhereNode,
  type KyselyPlugin,
  type RootOperationNode,
  type ValueNode as ValueNodeType,
} from '../../../dist/index.js'
import {
  Migrator,
  ParameterCountMismatchError,
  type Migration,
} from '../../../dist/migration/index.js'

type TestDatabase = any

function runSuite(
  variant: string,
  createDb: () => Kysely<TestDatabase>,
  supportsTransactionalDdl: boolean,
): void {
  describe(`${variant}: migration plugin chain`, () => {
    let db: Kysely<TestDatabase>

    beforeEach(() => {
      db = createDb()
    })

    afterEach(async () => {
      await db.destroy()
    })

    function migrator(
      migrations: Record<string, Migration>,
      plugins: KyselyPlugin[] = [],
    ): Migrator {
      let kysely: Kysely<any> = db

      for (const pluginInstance of plugins) {
        kysely = kysely.withPlugin(pluginInstance)
      }

      return new Migrator({
        db: kysely,
        provider: {
          getMigrations: async () => migrations,
        },
      })
    }

    async function recordedMigrations(): Promise<string[]> {
      const rows = await db
        .withoutPlugins()
        .selectFrom('kysely_migration')
        .select('name' as any)
        .execute()

      return rows.map((row: any) => row.name as string)
    }

    async function tableExists(name: string): Promise<boolean> {
      const tables = await db.introspection.getTables()
      return tables.some((table) => table.name === name)
    }

    it('runs an empty migration forward, records it, and rolls it back', async () => {
      const m = migrator({
        '001-empty': { up: async () => {}, down: async () => {} },
      })

      const up = await m.migrateUp()
      expect(up.error).to.equal(undefined)
      expect(up.results).to.eql([
        { migrationName: '001-empty', direction: 'Up', status: 'Success' },
      ])
      expect(await recordedMigrations()).to.eql(['001-empty'])

      const down = await m.migrateDown()
      expect(down.error).to.equal(undefined)
      expect(down.results).to.eql([
        { migrationName: '001-empty', direction: 'Down', status: 'Success' },
      ])
      expect(await recordedMigrations()).to.eql([])
    })

    it('does not record a failed migration and rolls its schema change back', async () => {
      const m = migrator({
        '001-broken-ddl': {
          async up(innerDb) {
            await innerDb.schema
              .createTable('left_behind')
              .addColumn('id', 'integer', (col) => col.primaryKey())
              .execute()

            // A failing schema statement rolls back the earlier one in the step.
            await innerDb.schema
              .createTable('left_behind')
              .addColumn('id', 'integer', (col) => col.primaryKey())
              .execute()
          },
          async down() {},
        },
      })

      const { error, results } = await m.migrateUp()
      expect(error).to.be.an('error')
      expect(results).to.eql([
        { migrationName: '001-broken-ddl', direction: 'Up', status: 'Error' },
      ])
      expect(await recordedMigrations()).to.eql([])
      expect(await tableExists('left_behind')).to.equal(
        !supportsTransactionalDdl ? true : false,
      )
    })

    it('does not run an already executed migration again', async () => {
      let upCalls = 0
      const m = migrator({
        '001': {
          up: async () => {
            upCalls++
          },
          down: async () => {},
        },
      })

      await m.migrateToLatest()
      const secondRun = await m.migrateToLatest()

      expect(upCalls).to.equal(1)
      expect(secondRun.results).to.eql([])
    })

    it('stops and names the extra migration when the list is ahead of the database', async () => {
      const first = migrator({
        '001-first': { up: async () => {}, down: async () => {} },
        '003-third': { up: async () => {}, down: async () => {} },
      })
      await first.migrateToLatest()

      const second = migrator({
        '001-first': { up: async () => {}, down: async () => {} },
        '002-extra': { up: async () => {}, down: async () => {} },
        '003-third': { up: async () => {}, down: async () => {} },
      })

      const { error, results } = await second.migrateToLatest()
      expect(getMessage(error)).to.contain('003-third')
      expect(getMessage(error)).to.contain('002-extra')
      expect(results).to.equal(undefined)
    })

    it('stops when the database has a migration missing from the list', async () => {
      const first = migrator({
        '001-first': { up: async () => {}, down: async () => {} },
        '002-second': { up: async () => {}, down: async () => {} },
      })
      await first.migrateToLatest()

      const second = migrator({
        '001-first': { up: async () => {}, down: async () => {} },
      })

      const { error, results } = await second.migrateToLatest()
      expect(getMessage(error)).to.equal(
        'corrupted migrations: previously executed migration 002-second is missing',
      )
      expect(results).to.equal(undefined)
    })

    it('rolls back only the latest migration', async () => {
      const calls: string[] = []
      const m = migrator({
        '001': {
          up: async () => {
            calls.push('up 001')
          },
          down: async () => {
            calls.push('down 001')
          },
        },
        '002': {
          up: async () => {
            calls.push('up 002')
          },
          down: async () => {
            calls.push('down 002')
          },
        },
      })

      await m.migrateToLatest()
      const result = await m.migrateDown()

      expect(result.results).to.eql([
        { migrationName: '002', direction: 'Down', status: 'Success' },
      ])
      expect(calls).to.eql(['up 001', 'up 002', 'down 002'])
      expect(await recordedMigrations()).to.eql(['001'])
    })

    it('stops rollback when the down method is missing and keeps the record', async () => {
      const m = migrator({
        '001-no-down': {
          up: async () => {},
        },
      })

      await m.migrateUp()
      const { error, results } = await m.migrateDown()

      expect(getMessage(error)).to.equal(
        'migration "001-no-down" doesn\'t have a "down" method. Cannot roll it back. Its record was left in the migration table.',
      )
      expect(results).to.eql([
        {
          migrationName: '001-no-down',
          direction: 'Down',
          status: 'Error',
        },
      ])
      expect(await recordedMigrations()).to.eql(['001-no-down'])
    })

    it('runs plugins in registration order, each seeing the previous transformed node', async () => {
      const seenBySecond: RootOperationNode[] = []

      const firstPlugin = plugin((node) =>
        new AddLiteralWhereTransformer('first_plugin').transformNode(node),
      )
      const secondPlugin = plugin((node) => {
        seenBySecond.push(node)
        return new AddLiteralWhereTransformer('second_plugin').transformNode(
          node,
        )
      })

      const m = migrator(
        {
          '001': {
            async up(innerDb) {
              await createTestTable(innerDb)
              await innerDb
                .insertInto('test_table')
                .values({ id: 0, name: 'seed' })
                .execute()
            },
            async down(innerDb) {
              await innerDb.schema.dropTable('test_table').execute()
            },
          },
        },
        [firstPlugin, secondPlugin],
      )

      const { error } = await m.migrateToLatest()
      expect(error).to.equal(undefined)

      // The second plugin must see the `where` clause added by the first one.
      expect(
        seenBySecond.some((node) =>
          JSON.stringify(node).includes('first_plugin'),
        ),
      ).to.equal(true)
    })

    it('rejects a plugin that changes the parameter count and rolls the step back', async () => {
      let migrationBodyFinished = false
      const m = migrator(
        {
          '001': {
            async up(innerDb) {
              await createTestTable(innerDb)
              await innerDb
                .insertInto('test_table')
                .values({ id: 1, name: 'ok' })
                .execute()

              migrationBodyFinished = true
            },
            async down(innerDb) {
              await innerDb.schema.dropTable('test_table').execute()
            },
          },
        },
        [plugin((node) => new AddParameterTransformer().transformNode(node))],
      )

      const { error, results } = await m.migrateUp()
      expect(error).to.be.an.instanceOf(ParameterCountMismatchError)
      expect(results).to.eql([
        { migrationName: '001', direction: 'Up', status: 'Error' },
      ])
      // The guard throws before compilation, so the rejected query never
      // reaches the database on any dialect.
      expect(migrationBodyFinished).to.equal(false)
      expect(await recordedMigrations()).to.eql([])
      expect(await tableExists('test_table')).to.equal(
        !supportsTransactionalDdl,
      )
    })

    it('does not execute the query when a plugin throws and rolls back', async () => {
      const m = migrator(
        {
          '001': {
            async up(innerDb) {
              await createTestTable(innerDb)
              await innerDb
                .insertInto('test_table')
                .values({ id: 1, name: 'ok' })
                .execute()
            },
            async down(innerDb) {
              await innerDb.schema.dropTable('test_table').execute()
            },
          },
        },
        [
          plugin(() => {
            throw new Error('plugin exploded')
          }),
        ],
      )

      const { error, results } = await m.migrateUp()
      expect(getMessage(error)).to.equal('plugin exploded')
      // The plugin already rejects while the migrator sets up its internal
      // tables, so no migration step is reported and nothing is recorded.
      expect(results).to.equal(undefined)
    })

    it('rejects a plugin that inlines a value instead of keeping a placeholder', async () => {
      const m = migrator(
        {
          '001': {
            async up(innerDb) {
              await createTestTable(innerDb)
              await innerDb
                .insertInto('test_table')
                .values({ id: 7, name: 'kept' })
                .execute()
            },
            async down(innerDb) {
              await innerDb.schema.dropTable('test_table').execute()
            },
          },
        },
        [plugin((node) => new InlineValueTransformer().transformNode(node))],
      )

      const { error } = await m.migrateUp()
      expect(error).to.be.an.instanceOf(ParameterCountMismatchError)
    })

    it('transforms the queries of every migration step (no caching across steps)', async () => {
      const seenNodes: RootOperationNode[] = []
      const m = migrator(
        {
          '001': {
            async up(innerDb) {
              await createTestTable(innerDb)
            },
            async down(innerDb) {
              await innerDb.schema.dropTable('test_table').execute()
            },
          },
          '002': {
            async up(innerDb) {
              await innerDb
                .insertInto('test_table')
                .values({ id: 1, name: 'one' })
                .execute()
            },
            async down(innerDb) {
              await innerDb
                .deleteFrom('test_table')
                .where('id', '=', 1)
                .execute()
            },
          },
        },
        [
          plugin((node) => {
            seenNodes.push(node)
            return node
          }),
        ],
      )

      const up1 = await m.migrateUp()
      const afterFirstStep = seenNodes.length
      expect(up1.error).to.equal(undefined)
      expect(afterFirstStep).to.be.greaterThan(0)

      const up2 = await m.migrateUp()
      expect(up2.error).to.equal(undefined)
      expect(seenNodes.length).to.be.greaterThan(afterFirstStep)

      const afterUp = seenNodes.length
      const down = await m.migrateDown()
      expect(down.error).to.equal(undefined)
      expect(seenNodes.length).to.be.greaterThan(afterUp)
    })

    it('keeps single-query behavior unchanged when no plugin is installed', async () => {
      const m = migrator({
        '001': {
          async up(innerDb) {
            await createTestTable(innerDb)
          },
          async down(innerDb) {
            await innerDb.schema.dropTable('test_table').execute()
          },
        },
      })

      await m.migrateUp()
      await db
        .insertInto('test_table')
        .values({ id: 9, name: 'plain' })
        .execute()

      const row = await db
        .selectFrom('test_table')
        .select(['id', 'name'])
        .executeTakeFirstOrThrow()

      expect(row).to.eql({ id: 9, name: 'plain' })
    })
  })
}

function plugin(
  transformQuery: (node: RootOperationNode) => RootOperationNode,
): KyselyPlugin {
  return {
    transformQuery: ({ node }) => transformQuery(node),
    transformResult: ({ result }) => Promise.resolve(result),
  }
}

function getMessage(error: unknown): string | undefined {
  if (error instanceof Error) {
    return error.message
  }
}

async function createTestTable(innerDb: Kysely<any>): Promise<void> {
  await innerDb.schema
    .createTable('test_table')
    .addColumn('id', 'integer')
    .addColumn('name', 'varchar(255)')
    .execute()
}

// Adds a `where` clause with a literal SQL fragment to select queries. The
// fragment contains the marker string, so a later plugin can see that an
// earlier plugin already transformed the operation node.
class AddLiteralWhereTransformer extends OperationNodeTransformer {
  readonly #marker: string

  constructor(marker: string) {
    super()
    this.#marker = marker
  }

  protected override transformSelectQuery(node: any): any {
    node = super.transformSelectQuery(node)

    if (!SelectQueryNode.is(node) || !node.from) {
      return node
    }

    const filter = RawNode.createWithSql(
      `'${this.#marker}' = '${this.#marker}'`,
    )

    return {
      ...node,
      where: node.where
        ? WhereNode.cloneWithOperation(node.where, 'And', filter)
        : WhereNode.create(filter),
    }
  }
}

// Adds one extra bound value parameter to insert queries, changing the
// parameter count. The migration guard must reject this before execution.
class AddParameterTransformer extends OperationNodeTransformer {
  protected override transformInsertQuery(node: any): any {
    node = super.transformInsertQuery(node)

    if (
      node.into?.table?.identifier?.name !== 'test_table' ||
      !node.columns ||
      !node.values ||
      node.values.kind !== 'ValuesNode'
    ) {
      return node
    }

    const firstValues = node.values.values[0]

    if (!firstValues || !PrimitiveValueListNode.is(firstValues)) {
      return node
    }

    return {
      ...node,
      columns: [...node.columns, ColumnNode.create('extra')],
      values: ValuesNode.create([
        PrimitiveValueListNode.create([...firstValues.values, 'extra-param']),
        ...node.values.values.slice(1),
      ]),
    }
  }
}

// Turns the first bound value into an immediate (inlined) value. This keeps
// the node tree valid-looking but removes a placeholder, so the guard
// rejects it: values must stay behind placeholders.
class InlineValueTransformer extends OperationNodeTransformer {
  #inlined = false

  protected override transformInsertQuery(node: any): any {
    if (node.into?.table?.identifier?.name !== 'test_table') {
      return node
    }

    return super.transformInsertQuery(node)
  }

  protected override transformValue(node: ValueNodeType): ValueNodeType {
    if (!this.#inlined && !node.immediate) {
      this.#inlined = true
      return ValueNode.createImmediate(node.value)
    }

    return node
  }
}

runSuite(
  'sqlite',
  () =>
    new Kysely<TestDatabase>({
      dialect: new SqliteDialect({ database: new Database(':memory:') }),
    }),
  false,
)

runSuite(
  'pglite',
  () =>
    new Kysely<TestDatabase>({
      dialect: new PGliteDialect({ pglite: async () => new PGlite() }),
    }),
  true,
)
