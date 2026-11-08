#!/usr/bin/env bash
# Sets up a Microsoft 365 tenant for the read-only Microsoft Graph live smoke
# (:grpc-microsoft-service:liveSmokeTest). Uses the Azure CLI signed in as a
# tenant admin. Never prints secret values.
#
#   scripts/microsoft-live-setup.sh check   # what tenant / licences / SharePoint exist
#   scripts/microsoft-live-setup.sh app     # Entra app + read-only Graph perms + consent + secret
#   scripts/microsoft-live-setup.sh site    # test M365 group site + a few seed files
#   scripts/microsoft-live-setup.sh verify  # app-only token can list sites and drives
#
# Sign in first (device code, works over ssh):
#   az login --use-device-code --allow-no-subscriptions
#
# Credentials are written as exports into a marked block in ~/.bashrc
# (MICROSOFT_TENANT_ID, MICROSOFT_CLIENT_ID, MICROSOFT_CLIENT_SECRET,
# MICROSOFT_SITE_ID, MICROSOFT_DRIVE_IDS). Re-running replaces the block.
set -euo pipefail

APP_NAME="${APP_NAME:-pipestream-graph-live-smoke}"
GROUP_NAME="${GROUP_NAME:-Pipestream Connector Test}"
GROUP_NICK="${GROUP_NICK:-pipestream-connector-test}"
BASHRC="${BASHRC:-$HOME/.bashrc}"
GRAPH="https://graph.microsoft.com/v1.0"
GRAPH_APP_ID="00000003-0000-0000-c000-000000000000"
# Microsoft Graph application roles (app-only).
ROLE_SITES_READ_ALL="332a536c-c7ef-4017-ab91-336970924f0d"
ROLE_FILES_READ_ALL="01d4889c-1287-42c6-ac1f-5d1e02578ef6"
BEGIN_MARK="# >>> pipestream microsoft live smoke >>>"
END_MARK="# <<< pipestream microsoft live smoke <<<"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 2; }; }
need az
need jq

signed_in() {
  az account show >/dev/null 2>&1 || {
    echo "Not signed in. Run: az login --use-device-code --allow-no-subscriptions" >&2
    exit 3
  }
}

tenant_id() { az account show --query tenantId -o tsv; }

# Reads the current value of one exported variable from the marked block.
block_get() {
  [ -f "$BASHRC" ] || return 0
  awk -v b="$BEGIN_MARK" -v e="$END_MARK" -v k="$1" '
    $0==b {inb=1; next} $0==e {inb=0; next}
    inb && index($0, "export " k "=")==1 { v=substr($0, length("export " k "=")+1); gsub(/^'\''|'\''$/, "", v); print v }
  ' "$BASHRC"
}

# Sets one variable in the marked block, keeping the others. Value never echoed.
block_set() {
  local key="$1" value="$2" tmp
  touch "$BASHRC"
  tmp="$(mktemp)"
  chmod 600 "$tmp"
  local -A vals=()
  local k
  for k in MICROSOFT_TENANT_ID MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET MICROSOFT_SITE_ID MICROSOFT_DRIVE_IDS; do
    vals[$k]="$(block_get "$k")"
  done
  vals[$key]="$value"
  awk -v b="$BEGIN_MARK" -v e="$END_MARK" '$0==b {skip=1} !skip {print} $0==e {skip=0}' "$BASHRC" > "$tmp"
  {
    echo "$BEGIN_MARK"
    for k in MICROSOFT_TENANT_ID MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET MICROSOFT_SITE_ID MICROSOFT_DRIVE_IDS; do
      [ -n "${vals[$k]}" ] && printf "export %s='%s'\n" "$k" "${vals[$k]}"
    done
    echo "$END_MARK"
  } >> "$tmp"
  cat "$tmp" > "$BASHRC"
  rm -f "$tmp"
}

cmd_check() {
  signed_in
  echo "== Signed in"
  az account show --query '{user:user.name, tenantId:tenantId}' -o table
  echo
  echo "== Organisation"
  az rest --url "$GRAPH/organization?\$select=id,displayName,verifiedDomains" 2>/dev/null \
    | jq -r '.value[] | "name: \(.displayName)\ndomains: \([.verifiedDomains[].name]|join(", "))"' \
    || echo "could not read organization (not an admin?)"
  echo
  echo "== Licences (subscribedSkus) and whether they include SharePoint"
  az rest --url "$GRAPH/subscribedSkus" 2>/dev/null | jq -r '
    if (.value|length)==0 then "NO LICENCES: this is an Entra-only tenant (no Microsoft 365 / SharePoint)"
    else .value[] | "\(.skuPartNumber)  enabled=\(.prepaidUnits.enabled) assigned=\(.consumedUnits)  sharepoint=\([.servicePlans[] | select(.servicePlanName|test("SHAREPOINT")) | .provisioningStatus] | if length==0 then "none" else join(",") end)"
    end' || echo "could not read subscribedSkus"
  echo
  echo "== Your directory roles"
  az rest --url "$GRAPH/me/memberOf?\$select=displayName" 2>/dev/null \
    | jq -r '[.value[] | select(."@odata.type"=="#microsoft.graph.directoryRole") | .displayName] | if length==0 then "none" else join(", ") end' \
    || echo "could not read roles"
  echo
  echo "== Existing app registration '$APP_NAME'"
  az ad app list --display-name "$APP_NAME" --query '[].{appId:appId}' -o tsv | sed 's/^/appId: /' || true
  echo
  echo "== Credential block in $BASHRC"
  local k
  for k in MICROSOFT_TENANT_ID MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET MICROSOFT_SITE_ID MICROSOFT_DRIVE_IDS; do
    if [ -n "$(block_get "$k")" ]; then echo "$k: set"; else echo "$k: unset"; fi
  done
}

cmd_app() {
  signed_in
  local tid app_id graph_sp_id sp_id secret
  tid="$(tenant_id)"
  app_id="$(az ad app list --display-name "$APP_NAME" --query '[0].appId' -o tsv)"
  if [ -z "$app_id" ]; then
    echo "creating app registration $APP_NAME"
    app_id="$(az ad app create --display-name "$APP_NAME" --sign-in-audience AzureADMyOrg --query appId -o tsv)"
  else
    echo "reusing app registration $APP_NAME ($app_id)"
  fi
  az ad sp show --id "$app_id" >/dev/null 2>&1 || az ad sp create --id "$app_id" >/dev/null
  sp_id="$(az ad sp show --id "$app_id" --query id -o tsv)"
  graph_sp_id="$(az ad sp show --id "$GRAPH_APP_ID" --query id -o tsv)"

  echo "requesting application permissions Sites.Read.All + Files.Read.All"
  az ad app permission add --id "$app_id" --api "$GRAPH_APP_ID" \
    --api-permissions "${ROLE_SITES_READ_ALL}=Role" "${ROLE_FILES_READ_ALL}=Role" >/dev/null 2>&1 || true

  echo "granting admin consent (app role assignments)"
  local role have
  for role in "$ROLE_SITES_READ_ALL" "$ROLE_FILES_READ_ALL"; do
    have="$(az rest --url "$GRAPH/servicePrincipals/$sp_id/appRoleAssignments" \
      | jq -r --arg r "$role" '[.value[] | select(.appRoleId==$r)] | length')"
    if [ "$have" = "0" ]; then
      az rest --method post --url "$GRAPH/servicePrincipals/$sp_id/appRoleAssignments" \
        --headers 'Content-Type=application/json' \
        --body "{\"principalId\":\"$sp_id\",\"resourceId\":\"$graph_sp_id\",\"appRoleId\":\"$role\"}" >/dev/null
    fi
  done
  az rest --url "$GRAPH/servicePrincipals/$sp_id/appRoleAssignments" \
    | jq -r '.value[] | "consented role: \(.appRoleId)"'

  if [ -n "$(block_get MICROSOFT_CLIENT_SECRET)" ] && [ "$(block_get MICROSOFT_CLIENT_ID)" = "$app_id" ] && [ "${ROTATE:-0}" != "1" ]; then
    echo "client secret already stored; set ROTATE=1 to replace it"
  else
    echo "creating a 1-year client secret (value not shown)"
    secret="$(az ad app credential reset --id "$app_id" --append --display-name live-smoke --years 1 --query password -o tsv 2>/dev/null)"
    block_set MICROSOFT_CLIENT_SECRET "$secret"
    unset secret
  fi
  block_set MICROSOFT_TENANT_ID "$tid"
  block_set MICROSOFT_CLIENT_ID "$app_id"
  echo "stored MICROSOFT_TENANT_ID, MICROSOFT_CLIENT_ID, MICROSOFT_CLIENT_SECRET in $BASHRC"
  echo "portal: https://entra.microsoft.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/CallAnAPI/appId/$app_id"
}

cmd_site() {
  signed_in
  local group_id site_json site_id drive_id web_url
  group_id="$(az rest --url "$GRAPH/groups?\$filter=mailNickname%20eq%20'$GROUP_NICK'&\$select=id" | jq -r '.value[0].id // empty')"
  if [ -z "$group_id" ]; then
    echo "creating Microsoft 365 group '$GROUP_NAME' (this provisions a SharePoint team site)"
    group_id="$(az rest --method post --url "$GRAPH/groups" --headers 'Content-Type=application/json' --body "{
      \"displayName\": \"$GROUP_NAME\", \"mailNickname\": \"$GROUP_NICK\",
      \"description\": \"Read-only fixture for the Pipestream Microsoft Graph connector live smoke\",
      \"groupTypes\": [\"Unified\"], \"mailEnabled\": true, \"securityEnabled\": false,
      \"visibility\": \"Private\" }" | jq -r .id)"
    local me
    me="$(az ad signed-in-user show --query id -o tsv)"
    az rest --method post --url "$GRAPH/groups/$group_id/owners/\$ref" --headers 'Content-Type=application/json' \
      --body "{\"@odata.id\": \"$GRAPH/directoryObjects/$me\"}" >/dev/null 2>&1 || true
  else
    echo "reusing group $GROUP_NAME ($group_id)"
  fi

  echo "waiting for the team site to provision (can take a few minutes)"
  local i
  for i in $(seq 1 40); do
    if site_json="$(az rest --url "$GRAPH/groups/$group_id/sites/root?\$select=id,webUrl" 2>/dev/null)"; then
      break
    fi
    site_json=""
    sleep 15
  done
  [ -n "$site_json" ] || { echo "site not ready yet; re-run 'site' in a few minutes" >&2; exit 4; }
  site_id="$(jq -r .id <<<"$site_json")"
  web_url="$(jq -r .webUrl <<<"$site_json")"
  drive_id="$(az rest --url "$GRAPH/groups/$group_id/drive?\$select=id" | jq -r .id)"
  block_set MICROSOFT_SITE_ID "$site_id"
  block_set MICROSOFT_DRIVE_IDS "$drive_id"
  echo "site: $web_url"
  echo "stored MICROSOFT_SITE_ID and MICROSOFT_DRIVE_IDS in $BASHRC"

  echo "seeding files under /connector-test"
  local tmpd ok=0
  tmpd="$(mktemp -d)"
  printf 'Hello from the Pipestream connector live smoke.\n' > "$tmpd/hello.txt"
  printf '# Connector test\n\nA markdown fixture with **bold** text.\n' > "$tmpd/readme.md"
  printf 'id,name,score\n1,alpha,0.9\n2,beta,0.4\n' > "$tmpd/data.csv"
  printf 'nested file one level down\n' > "$tmpd/nested.txt"
  upload() {
    az rest --method put --url "$GRAPH/drives/$drive_id/root:/$1:/content" \
      --headers 'Content-Type=text/plain' --body "@$2" >/dev/null 2>&1
  }
  if upload connector-test/hello.txt "$tmpd/hello.txt" \
     && upload connector-test/readme.md "$tmpd/readme.md" \
     && upload connector-test/data.csv "$tmpd/data.csv" \
     && upload connector-test/sub/nested.txt "$tmpd/nested.txt"; then
    ok=1
  fi
  rm -rf "$tmpd"
  if [ "$ok" = "1" ]; then
    echo "uploaded 4 files"
  else
    echo "could not upload with the CLI token; drag a few files into Documents at:"
    echo "  $web_url/Shared%20Documents"
  fi
}

cmd_verify() {
  local tid cid sec site token
  tid="$(block_get MICROSOFT_TENANT_ID)"; cid="$(block_get MICROSOFT_CLIENT_ID)"; sec="$(block_get MICROSOFT_CLIENT_SECRET)"
  site="$(block_get MICROSOFT_SITE_ID)"
  [ -n "$tid" ] && [ -n "$cid" ] && [ -n "$sec" ] || { echo "run 'app' first" >&2; exit 5; }
  token="$(curl -sS -X POST "https://login.microsoftonline.com/$tid/oauth2/v2.0/token" \
    --data-urlencode "client_id=$cid" --data-urlencode "client_secret=$sec" \
    --data-urlencode "scope=https://graph.microsoft.com/.default" \
    --data-urlencode "grant_type=client_credentials" | jq -r '.access_token // empty')"
  unset sec
  [ -n "$token" ] || { echo "token request failed (new secrets can take a minute to work)" >&2; exit 6; }
  echo "app-only token: ok; roles: $(cut -d. -f2 <<<"$token" | tr '_-' '/+' | base64 -d 2>/dev/null | jq -r '.roles // [] | join(",")' 2>/dev/null)"
  echo "sites?search=*:"
  curl -sS -H "Authorization: Bearer $token" "$GRAPH/sites?search=*&\$select=displayName,webUrl&\$top=5" \
    | jq -r 'if .error then "  error: \(.error.code) \(.error.message)" else .value[] | "  \(.displayName)  \(.webUrl)" end'
  if [ -n "$site" ]; then
    echo "drives on the test site:"
    curl -sS -H "Authorization: Bearer $token" "$GRAPH/sites/$site/drives?\$select=name,id" \
      | jq -r 'if .error then "  error: \(.error.code) \(.error.message)" else .value[] | "  \(.name)" end'
  fi
  unset token
}

case "${1:-check}" in
  check) cmd_check ;;
  app) cmd_app ;;
  site) cmd_site ;;
  verify) cmd_verify ;;
  all) cmd_check; cmd_app; cmd_site; sleep 30; cmd_verify ;;
  *) echo "usage: $0 check|app|site|verify|all" >&2; exit 1 ;;
esac
