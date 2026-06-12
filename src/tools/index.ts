import type { Env, ToolArgs } from '../types.js';
import type { McpResult } from './shared.js';
import { memoryTools } from './memory.js';
import { graphTools } from './graph.js';
import { knowledgeTools } from './knowledge.js';
import { trustPolicyTools } from './trust-policy.js';
import { snapshotTools } from './snapshots.js';
import { objectiveTools } from './objectives.js';
import { observabilityTools } from './observability.js';

export type { McpResult } from './shared.js';
export { buildTagInferredLinks } from './shared.js';

const domains = [
  memoryTools,
  graphTools,
  knowledgeTools,
  trustPolicyTools,
  snapshotTools,
  objectiveTools,
  observabilityTools,
] as const;

export async function callTool(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult> {
  for (const domain of domains) {
    const result = await domain(name, args, env, brainId);
    if (result !== null) return result;
  }
  throw Object.assign(new Error(`Unknown tool: ${name}`), { code: -32601 });
}
