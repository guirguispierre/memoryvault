// Single source of truth for how an MCP client connects to this server. Shared
// by the /view first-run onboarding panel and the /docs connect guide so the two
// can never drift: if they ever show different commands, that is a bug, and the
// fix is to change it here. Verified against the real transport and auth flow:
// /mcp speaks JSON-RPC over HTTP POST (plus SSE), implements initialize /
// tools/list / tools/call, and advertises OAuth discovery so clients authorize
// without a pasted token (see src/routes.ts handleMcp and README "OAuth mode").

export const MCP_SERVER_LABEL = 'memoryvault';
export const MCP_TRANSPORT_LABEL = 'HTTP (streamable)';
export const MCP_AUTH_LABEL = 'OAuth 2.1, leave the token blank and sign in once';
export const CLAUDE_CODE_PREFIX = 'claude mcp add --transport http ' + MCP_SERVER_LABEL;

export function mcpUrlFor(origin: string): string {
  return origin + '/mcp';
}

export function claudeCodeCommand(mcpUrl: string): string {
  return CLAUDE_CODE_PREFIX + ' ' + mcpUrl;
}

export function mcpJsonConfig(mcpUrl: string): string {
  return '{\n  "mcpServers": {\n    "' + MCP_SERVER_LABEL + '": {\n      "url": "' + mcpUrl + '"\n    }\n  }\n}';
}
