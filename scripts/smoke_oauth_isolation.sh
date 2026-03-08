#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${BASE_URL:-https://ai-memory-mcp.guirguispierre.workers.dev}}"
REDIRECT_URI="${REDIRECT_URI:-http://127.0.0.1:8787/memoryvault/callback}"
PASSWORD="${SMOKE_PASSWORD:-MemoryVaultPass!2026}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

[[ -n "$ADMIN_TOKEN" ]] || {
  echo "ERROR: ADMIN_TOKEN is required for POST /register" >&2
  exit 1
}

for cmd in curl jq awk sed openssl mktemp date; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

log() {
  printf '[%s] %s\n' "$(date -u '+%H:%M:%S')" "$*"
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

header_value() {
  local file="$1"
  local key="$2"
  awk -v wanted="$(printf '%s' "$key" | tr '[:upper:]' '[:lower:]')" '
    {
      lower = tolower($0);
      if (index(lower, wanted ":") == 1) {
        sub(/^[^:]+:[[:space:]]*/, "", $0);
        sub(/\r$/, "", $0);
        print $0;
        exit 0;
      }
    }
  ' "$file"
}

urlencode() {
  jq -rn --arg value "$1" '$value|@uri'
}

pkce_s256_challenge() {
  printf '%s' "$1" \
    | openssl dgst -binary -sha256 \
    | openssl base64 -A \
    | tr '+/' '-_' \
    | tr -d '='
}

oauth_authorize_and_exchange() {
  local mode="$1"
  local email="$2"
  local brain_name="$3"
  local verifier code_challenge state headers_file status location code token_json access refresh

  verifier="v$(openssl rand -hex 24)"
  code_challenge="$(pkce_s256_challenge "$verifier")"
  state="st$(openssl rand -hex 8)"
  headers_file="$(mktemp)"

  status="$(curl -sS -o /dev/null -D "$headers_file" -w "%{http_code}" \
    -X POST "$BASE_URL/authorize" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "response_type=code" \
    --data-urlencode "client_id=$CLIENT_ID" \
    --data-urlencode "redirect_uri=$REDIRECT_URI" \
    --data-urlencode "scope=mcp:full" \
    --data-urlencode "resource=$BASE_URL/mcp" \
    --data-urlencode "state=$state" \
    --data-urlencode "code_challenge=$code_challenge" \
    --data-urlencode "code_challenge_method=S256" \
    --data-urlencode "auth_mode=$mode" \
    --data-urlencode "email=$email" \
    --data-urlencode "password=$PASSWORD" \
    --data-urlencode "brain_name=$brain_name")"

  if [[ "$status" != "302" ]]; then
    fail "Expected /authorize ($mode) to return 302, got $status"
  fi

  location="$(header_value "$headers_file" "Location")"
  [[ -n "$location" ]] || fail "Missing Location header from /authorize ($mode)"
  code="$(printf '%s' "$location" | sed -nE 's/.*[?&]code=([^&]+).*/\1/p')"
  [[ -n "$code" ]] || fail "Unable to extract authorization code from redirect location"

  token_json="$(curl -fsS -X POST "$BASE_URL/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "grant_type=authorization_code" \
    --data-urlencode "client_id=$CLIENT_ID" \
    --data-urlencode "code=$code" \
    --data-urlencode "redirect_uri=$REDIRECT_URI" \
    --data-urlencode "code_verifier=$verifier")"

  access="$(jq -r '.access_token // empty' <<<"$token_json")"
  refresh="$(jq -r '.refresh_token // empty' <<<"$token_json")"
  [[ -n "$access" ]] || fail "Token response missing access_token"
  [[ -n "$refresh" ]] || fail "Token response missing refresh_token"

  printf '%s\t%s\n' "$access" "$refresh"
}

assert_plain_pkce_rejected() {
  local email="$1"
  local verifier state body_file status error error_description

  verifier="v$(openssl rand -hex 24)"
  state="st$(openssl rand -hex 8)"
  body_file="$(mktemp)"

  status="$(curl -sS -o "$body_file" -w "%{http_code}" \
    -X POST "$BASE_URL/authorize" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "response_type=code" \
    --data-urlencode "client_id=$CLIENT_ID" \
    --data-urlencode "redirect_uri=$REDIRECT_URI" \
    --data-urlencode "scope=mcp:full" \
    --data-urlencode "resource=$BASE_URL/mcp" \
    --data-urlencode "state=$state" \
    --data-urlencode "code_challenge=$verifier" \
    --data-urlencode "code_challenge_method=pLaIn" \
    --data-urlencode "auth_mode=signup" \
    --data-urlencode "email=$email" \
    --data-urlencode "password=$PASSWORD" \
    --data-urlencode "brain_name=Rejected Plain Flow")"

  [[ "$status" == "400" ]] || fail "Expected /authorize plain PKCE to return 400, got $status"
  error="$(jq -r '.error // empty' <"$body_file")"
  error_description="$(jq -r '.error_description // empty' <"$body_file")"
  [[ "$error" == "invalid_request" ]] || fail "Expected invalid_request for plain PKCE, got: $error"
  [[ "$error_description" == "Only S256 code_challenge_method is supported" ]] \
    || fail "Unexpected plain PKCE error_description: $error_description"
}

mcp_save_memory() {
  local token="$1"
  local key="$2"
  local content="$3"
  local payload

  payload="$(jq -cn \
    --arg key "$key" \
    --arg content "$content" \
    '{jsonrpc:"2.0",id:$key,method:"tools/call",params:{name:"memory_save",arguments:{type:"fact",key:$key,content:$content,tags:"smoke_test"}}}')"
  curl -fsS -X POST "$BASE_URL/mcp" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -d "$payload" >/dev/null
}

log "Running smoke OAuth + isolation test against: $BASE_URL"

tmp_body="$(mktemp)"
tmp_headers="$(mktemp)"

log "Checking unauthenticated /mcp OAuth challenge"
status="$(curl -sS -o "$tmp_body" -D "$tmp_headers" -w "%{http_code}" "$BASE_URL/mcp")"
[[ "$status" == "401" ]] || fail "Expected unauthenticated /mcp status 401, got $status"
www_auth="$(header_value "$tmp_headers" "WWW-Authenticate")"
[[ "$www_auth" == *"Bearer"* ]] || fail "Missing Bearer challenge in WWW-Authenticate"
[[ "$www_auth" == *"resource_metadata="* ]] || fail "Missing resource_metadata in WWW-Authenticate"

log "Checking OAuth metadata endpoints"
protected_resource="$(curl -fsS "$BASE_URL/.well-known/oauth-protected-resource")"
auth_server="$(curl -fsS "$BASE_URL/.well-known/oauth-authorization-server")"
resource_url="$(jq -r '.resource // empty' <<<"$protected_resource")"
auth_endpoint="$(jq -r '.authorization_endpoint // empty' <<<"$auth_server")"
token_endpoint="$(jq -r '.token_endpoint // empty' <<<"$auth_server")"
registration_endpoint="$(jq -r '.registration_endpoint // empty' <<<"$auth_server")"
challenge_methods="$(jq -c '.code_challenge_methods_supported // []' <<<"$auth_server")"
[[ "$resource_url" == "$BASE_URL/mcp" ]] || fail "Unexpected protected resource URL: $resource_url"
[[ "$auth_endpoint" == "$BASE_URL/authorize" ]] || fail "Unexpected authorization endpoint: $auth_endpoint"
[[ "$token_endpoint" == "$BASE_URL/token" ]] || fail "Unexpected token endpoint: $token_endpoint"
[[ "$registration_endpoint" == "$BASE_URL/register" ]] || fail "Unexpected registration endpoint: $registration_endpoint"
[[ "$challenge_methods" == "[\"S256\"]" ]] || fail "Unexpected code challenge methods: $challenge_methods"

log "Registering OAuth client"
register_payload="$(jq -cn \
  --arg redirect "$REDIRECT_URI" \
  '{client_name:"MemoryVault Smoke",redirect_uris:[$redirect],token_endpoint_auth_method:"none"}')"
register_json="$(curl -fsS -X POST "$BASE_URL/register" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$register_payload")"
CLIENT_ID="$(jq -r '.client_id // empty' <<<"$register_json")"
[[ -n "$CLIENT_ID" ]] || fail "Client registration did not return client_id"

seed="$(date +%s)-$(openssl rand -hex 3)"
user1_email="smoke-u1-${seed}@example.com"
user2_email="smoke-u2-${seed}@example.com"
plain_reject_email="smoke-plain-${seed}@example.com"

log "Verifying /authorize rejects plain PKCE"
assert_plain_pkce_rejected "$plain_reject_email"

log "Running OAuth signup flow for user 1 and user 2"
read -r user1_access user1_refresh < <(oauth_authorize_and_exchange "signup" "$user1_email" "Smoke Brain U1")
read -r user2_access user2_refresh < <(oauth_authorize_and_exchange "signup" "$user2_email" "Smoke Brain U2")
[[ -n "$user1_refresh" ]] || fail "Missing refresh token for user1"
[[ -n "$user2_refresh" ]] || fail "Missing refresh token for user2"

log "Verifying MCP tools/list includes new tools"
tools_json="$(curl -fsS -X POST "$BASE_URL/mcp" \
  -H "Authorization: Bearer $user1_access" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"tools","method":"tools/list"}')"
tool_count="$(jq '.result.tools | length' <<<"$tools_json")"
[[ "$tool_count" -ge 20 ]] || fail "Expected at least 20 tools, got $tool_count"
for tool in memory_link memory_unlink memory_links memory_changelog memory_conflicts objective_set objective_list objective_next_actions tool_manifest tool_changelog memory_explain_score memory_link_suggest memory_path_find memory_conflict_resolve memory_entity_resolve memory_source_trust_set memory_source_trust_get brain_policy_set brain_policy_get brain_snapshot_create brain_snapshot_list brain_snapshot_restore memory_subgraph memory_tag_stats memory_graph_stats memory_neighbors memory_watch; do
  jq -e --arg t "$tool" '.result.tools | map(.name) | index($t) != null' <<<"$tools_json" >/dev/null \
    || fail "Expected tool '$tool' in tools/list"
done

key1="smoke-key-u1-${seed}"
key2="smoke-key-u2-${seed}"
content1="smoke-content-u1-${seed}"
content2="smoke-content-u2-${seed}"

log "Writing per-user memories via MCP"
mcp_save_memory "$user1_access" "$key1" "$content1"
mcp_save_memory "$user2_access" "$key2" "$content2"

log "Verifying tenant isolation on /api/memories"
u1_own_count="$(curl -fsS -H "Authorization: Bearer $user1_access" \
  "$BASE_URL/api/memories?search=$(urlencode "$key1")&limit=50" | jq '.memories | length')"
u1_other_count="$(curl -fsS -H "Authorization: Bearer $user1_access" \
  "$BASE_URL/api/memories?search=$(urlencode "$key2")&limit=50" | jq '.memories | length')"
u2_own_count="$(curl -fsS -H "Authorization: Bearer $user2_access" \
  "$BASE_URL/api/memories?search=$(urlencode "$key2")&limit=50" | jq '.memories | length')"
u2_other_count="$(curl -fsS -H "Authorization: Bearer $user2_access" \
  "$BASE_URL/api/memories?search=$(urlencode "$key1")&limit=50" | jq '.memories | length')"
[[ "$u1_own_count" -ge 1 ]] || fail "User1 could not read own memory"
[[ "$u2_own_count" -ge 1 ]] || fail "User2 could not read own memory"
[[ "$u1_other_count" -eq 0 ]] || fail "User1 can see user2 memory (isolation broken)"
[[ "$u2_other_count" -eq 0 ]] || fail "User2 can see user1 memory (isolation broken)"

log "Creating a second session for user1 to test session revocation"
read -r user1_access_2 user1_refresh_2 < <(oauth_authorize_and_exchange "login" "$user1_email" "")
[[ -n "$user1_refresh_2" ]] || fail "Missing second refresh token for user1"

sessions_before="$(curl -fsS -H "Authorization: Bearer $user1_access_2" "$BASE_URL/auth/sessions")"
sessions_before_count="$(jq '.count' <<<"$sessions_before")"
[[ "$sessions_before_count" -ge 2 ]] || fail "Expected at least 2 sessions for user1, got $sessions_before_count"

log "Revoking all other sessions while keeping current"
revoke_json="$(curl -fsS -X POST "$BASE_URL/auth/sessions/revoke" \
  -H "Authorization: Bearer $user1_access_2" \
  -H "Content-Type: application/json" \
  -d '{"all":true}')"
revoked_count="$(jq '.revoked_count' <<<"$revoke_json")"
[[ "$revoked_count" -ge 1 ]] || fail "Expected to revoke at least one session, got $revoked_count"

old_status="$(curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $user1_access" "$BASE_URL/auth/me")"
[[ "$old_status" == "401" ]] || fail "Expected old user1 token to be revoked, got status $old_status"
current_status="$(curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $user1_access_2" "$BASE_URL/auth/me")"
[[ "$current_status" == "200" ]] || fail "Expected current user1 token to remain active, got status $current_status"

log "Smoke test passed"
