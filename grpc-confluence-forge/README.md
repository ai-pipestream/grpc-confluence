# grpc-confluence-forge

A Forge app that queues Confluence product events so `grpc-confluence` can
collect them, and forwards app property calls made as the app. It has no UI. Each event is written to
Forge storage (`@forge/kvs`, kept 7 days). The `event-queue` web trigger lets
the connector take the oldest events and acknowledge them once handled.
`ConfluenceChangeService.WatchChanges` drains it (see `ForgeEventQueue`).

## Queue protocol

`POST` to the web trigger URL with header `x-pipestream-queue-secret`:

* `{"action":"take","max":100}` answers `{"events":[{"key":"evt:...","event":{...}}]}`, oldest first, without removing anything.
* `{"action":"ack","keys":["evt:..."]}` removes those events.

* `{"action":"take","max":100}` also answers each event's `expireTime`.

## One collector per queue

`take` removes nothing, and `ack` removes for everyone. Two collectors that
take before either acknowledges both get an event; once one has acknowledged
it, the other never sees it. An `ack` of a key that is already gone is
accepted. Run one `WatchChanges` stream per queue; a second environment of
the app has its own queue.

## Expiry

An event nobody collects is dropped 7 days after it was stored. Forge removes
expired entries up to 48 hours late and may still answer them until then, so
an event can be delivered up to 9 days after it was stored. The live change
run fails when it finds an older event in the queue.

## App API forwarding

`POST` to the `app-api` web trigger URL with the same header and
`{"method":...,"path":...,"body":<json or null>}`. It calls Confluence as the app and
answers `{"status":<int>,"body":<json or null>}`. Allowed:

* `GET`, `PUT`, `DELETE` on `/wiki/api/v2/app/properties[/{key}][?query]`
* `GET`, `POST`, `PUT`, `DELETE` on `/wiki/api/v2/custom-content[/{id}][?query]`

Other methods and paths get a 400.

## Custom content type

The `pipestream-record` module defines a custom content type (storage body, in spaces and
pages). Its type id is `forge:<app id>:<environment id>:pipestream-record`.

## Deploy (once per site)

```sh
npm install
forge register                     # writes the real app id into manifest.yml
forge variables set --encrypt QUEUE_SECRET <secret>
forge deploy -e development
forge install -e development --product confluence --site <site>.atlassian.net
forge webtrigger                   # prints the queue and app-api URLs
```

Use `-e production` on each command for the production environment. Each
environment has its own queue, secret, web trigger URLs and custom content
type id. The CI job `live-confluence-changes` deploys the environment its
`forge_environment` input names.

Then start `grpc-confluence` with `CONFLUENCE_FORGE_QUEUE_URL` set to that URL
and `CONFLUENCE_FORGE_QUEUE_SECRET` set to the same secret.

## Scopes

`write:confluence-content` and `write:confluence-space` are required by Forge
to receive trashed/deleted events; the app makes no content or space writes.
`read:app-data:confluence` and `write:app-data:confluence` cover the app
property forwarding.

## Not subscribed

Label events and whiteboard, database, folder and smart link
create/update/delete are not in the event list this app uses. They are picked
up by the timer `Sync` pass.

## Test

```sh
npm test
```
