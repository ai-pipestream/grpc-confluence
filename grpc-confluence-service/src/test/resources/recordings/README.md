# Recorded Confluence exchanges

`confluence/` holds the HTTP exchanges of one live write run: every request the
service sent to Confluence Cloud and the response it got, in order. The files are
written by the live run and are never edited by hand.

One scenario suite, `WriteScenarios`, runs against two backends:

| Mode | Test | Backend |
|---|---|---|
| record | `ConfluenceLiveWriteIT` (`liveWriteTest`) | the real workspace, through `RecordingTape` |
| replay | `ConfluenceWriteReplayTest` (`test`) | this directory, through `ReplayTape`, no network |

## Layout

```
confluence/run.json                               the run: base URL, space key, run name, flags,
                                                  scenario names, the outcome of every step
confluence/<scenario>/NNN-<METHOD>-<route>.json   one exchange, NNN in request order
confluence/<scenario>/NNN-...body.bin             a binary response body the run did not upload itself
```

An exchange file holds the request (method, host, decoded path, query pairs sorted by
name, the `accept`, `content-type` and `x-atlassian-token` headers, whether an
Authorization header was sent, the body) and the response (status, the `content-type`,
`retry-after` and `location` headers, the body). A JSON body is stored as JSON, a text
body as text. A multipart request is stored as its parts, a file part as SHA-256 and
length. A binary response is stored as SHA-256 and length.

## Record

```
export CONFLUENCE_EMAIL=... CONFLUENCE_API_TOKEN=...   # the liveSmokeTest credentials
export CONFLUENCE_LIVE_WRITE=1
export CONFLUENCE_SPACE_KEY=CLAUDEGRPC                 # optional, this is the default
./gradlew :grpc-confluence-service:liveWriteTest -PintegrationDir=../grpc-integration
```

The run replaces `confluence/` and writes `build/live-write.md`. In CI, start the `ci`
workflow by hand with `run_live_write` set; the recording is uploaded as the
`confluence-recordings` artifact. Review the files, then commit them.

## Scrubbing

Nothing is written unscrubbed. The Authorization header is never stored. These values are
replaced with placeholders, the same real value always by the same placeholder, so an id in
a response still matches the requests that use it:

| Value | Placeholder |
|---|---|
| the site host, and any host of the Atlassian site domain | `site-N.example.invalid` |
| cloud id | `00000000-0000-0000-0000-00000000000N` |
| account id | `account-N` |
| email address | `user-N@example.invalid` |
| `displayName`, `publicName` | `User N` |
| `profilePicture`, `avatarUrls` values | `/avatars/avatar-N.png` |
| API token, JWT, Basic or Bearer credential | `token-N` |
| any other long opaque token (cursors included) | `opaque-N` |

When the run ends every file is scrubbed once more and checked; if a check still finds
something, the whole recording is deleted and the run fails. `RecordingsGuardTest` runs the
same checks on this directory in every build.

## Replay

`ConfluenceWriteReplayTest` runs in the default `test` task, one test per scenario. It reads
`run.json`, so it uses the recorded run name and ids. A request must equal the next recorded
request of the scenario: method, host, path, query, the kept headers, whether it is
authorized, and the body. Exchanges recorded inside an unordered group (the concurrent
operations of `Mutate`) may arrive in any order within the group. A request that does not
match, a recorded exchange that is never requested, and a FAIL row each fail the scenario. A
recorded `Retry-After` is served as `0`.

Without a recording the replay tests are skipped, not passed.

## The fake server tests

`ConfluenceWriteRpcTest` and the other `FakeConfluenceServer` tests stay until a recording
is committed here. After that the replay covers what they cover with real responses, and
they can be removed.
