import type { KyselyPlugin } from '../plugin/kysely-plugin.js'
import { OperationNodeTransformer } from '../operation-node/operation-node-transformer.js'
import type { PrimitiveValueListNode } from '../operation-node/primitive-value-list-node.js'
import type { QueryId } from '../util/query-id.js'
import type { ValueNode } from '../operation-node/value-node.js'
import type { RootOperationNode } from '../operation-node/root-operation-node.js'

/**
 * An error thrown when a plugin changes the length of a query's parameter
 * list while migrations are running.
 *
 * Plugins may rewrite a query before it is compiled, but the rewritten query
 * must still go through the dialect's query compiler with the same number of
 * parameters. Values must stay behind placeholders and must not be inlined
 * into the SQL text.
 */
export class ParameterCountMismatchError extends Error {
  constructor(expected: number, actual: number) {
    super(
      `a plugin changed the query's parameter count from ${expected} to ${actual}. ` +
        'Plugins are not allowed to change the length of the parameter list ' +
        'while migrations run. Keep values in placeholders instead of inlining ' +
        'them into the SQL text.',
    )
    this.name = 'ParameterCountMismatchError'
  }
}

/**
 * A pair of plugins that guards the length of the parameter list of every
 * query executed while migrations run.
 *
 * Register {@link before} at the front of the plugin chain and {@link after}
 * at the back, so that all user plugins sit between them. {@link before}
 * counts the parameters of the incoming query and {@link after} recounts
 * them once every user plugin has rewritten the query. If the counts differ,
 * a {@link ParameterCountMismatchError} is thrown and the query is never
 * compiled or executed.
 *
 * Counts are tracked per query id in a {@link WeakMap}, so nothing is cached
 * across queries or migrations.
 */
export class ParameterCountGuard {
  readonly #counts = new WeakMap<QueryId, number>()

  readonly before: KyselyPlugin = {
    transformQuery: ({ node, queryId }): RootOperationNode => {
      this.#counts.set(queryId, countParameters(node))
      return node
    },
    transformResult: ({ result }) => Promise.resolve(result),
  }

  readonly after: KyselyPlugin = {
    transformQuery: ({ node, queryId }): RootOperationNode => {
      const expected = this.#counts.get(queryId)

      if (expected !== undefined) {
        const actual = countParameters(node)

        if (actual !== expected) {
          throw new ParameterCountMismatchError(expected, actual)
        }
      }

      return node
    },
    transformResult: ({ result }) => Promise.resolve(result),
  }
}

/**
 * Counts the parameters the dialect's query compiler would produce for the
 * given operation node tree: one for each non-immediate value and one for
 * each item of a primitive value list.
 */
function countParameters(node: RootOperationNode): number {
  const counter = new ParameterCounter()
  counter.transformNode(node)
  return counter.count
}

class ParameterCounter extends OperationNodeTransformer {
  #count = 0

  get count(): number {
    return this.#count
  }

  protected override transformValue(node: ValueNode): ValueNode {
    if (!node.immediate) {
      this.#count += 1
    }

    return node
  }

  protected override transformPrimitiveValueList(
    node: PrimitiveValueListNode,
  ): PrimitiveValueListNode {
    this.#count += node.values.length
    return node
  }
}
