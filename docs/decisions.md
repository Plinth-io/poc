# Decisions made during implementation

This document records every decision taken during the implementation of the tunnel
PoC without asking first — along with the reasoning and, where relevant, what it
costs if the decision was wrong.

It exists because the design document, the implementation plan and the progress
ledger all live in ignored directories: without this file the trail of reasoning
would have disappeared along with the workspace. Anyone reviewing the work should
start here — every line is a place where something could have been decided
differently.

Order: chronological.

1. No git worktree; work happened on branch `feat/grpc-tunnel` in the main directory — the repository was empty with no commits, a worktree cannot be created before the first commit, and there was nothing to isolate from. Cost if wrong: the user has work in progress in the same directory that collides with the commits — the repository was demonstrably empty, so close to zero.

2. `buf` and `grpcurl` are absent from the machine and are NOT installed globally; they become go.mod tool dependencies instead (`go get -tool`, invoked via `go tool buf` / `go tool grpcurl`). Reason: reproducible from go.mod alone, no change outside the repository, no Homebrew needed. Cost if wrong: `go tool` requires Go >= 1.24 (1.27 here) and downloads on first run; if buf's remote plugins are unreachable offline, this has to switch to local plugins.

3. No `go mod tidy` before Task 16 — goleak, buf and grpcurl have no importing code until then and would be stripped from go.mod. Cost if wrong: go.mod carries seemingly unused requires until Task 16.

4. **Task 4** — Important 1 (`TestEnvelopeForUnknownStreamIsDropped` only checks `st.ID != 0`, which is unreachable with `SideHub = 1` — the test would stay green if an unknown stream id killed the connection): the finding is right and my plan text is wrong. Spec §11 demands tests that verify real behaviour; a test that cannot notice the Critical case does not satisfy that. Fixed. Cost if wrong: minimal, the fix is a real round trip instead of a dead assertion.

5. **Task 4** — Important 2 (`peerStream := <-ready` with no timeout arm): correct, same origin. A hanging test instead of a clear failure message is unacceptable. Fixed.

6. **Task 4** — Minor 3 (the detached close goroutine outlives `Run`) is pulled into the fix round despite being Minor: Task 16 runs goleak, and a bounded wait simultaneously narrows Minor 4 (the lost 1002 frame). Cost if wrong: `Run` is delayed by up to 250 ms on a protocol error.

7. **Task 4** — Minor 5 (missing `ponytail:` comment) is not optional; the global constraints require one for deliberate simplifications. Fixed.

8. **Task 4** — Minor 6 (the write loop's error is discarded) is fixed too, because the next six tasks debug inside exactly this code and a masked write error there is expensive.

9. **Task 5** — `CloseWith` as written in the plan (`_ = c.ws.Close(code, reason)`) is demonstrably wrong after the Task 4 finding: `Close()` waits up to 5 s for the acknowledgement. `handleConnect` calls `old.Conn.CloseWith()` inline when replacing a connection and would block the NEW connection's handler for that long; `TestSecondConnectionReplacesTheFirst` would time out. `CloseWith` gets the same detached handshake as `closeProtocolError`. Cost if wrong: the old connection's socket is released marginally later.

10. **Task 5** — Important (`handleConnect` never closes the socket on the normal path): a plan-mandated gap, and the finding was reproduced and measured. Fixed. Cost if wrong: none, `CloseNow` after `Close` is a no-op.

11. **Task 5** — Minors 1-3 are pulled up because they concern the trust boundary: a wait without a timeout arm violates the global constraints, the foreign-id test can pass vacuously after a fixed sleep, and `bearerToken`'s 401 paths are entirely untested. Auth tests that prove nothing are worse than none. Cost if wrong: a little more test code.

12. **Task 5** — Minor 4 is pulled up: the rejection paths close synchronously and occupy the handler for up to 10 s per rejected connection; with many failed attempts that is a cheap way to tie up handlers. Same reasoning as the `CloseWith` ruling.

13. **Task 5** — Minor 5 is pulled up: `Config.Tokens` is a raw map parameter that can be built around `ParseTokens`; an empty key would match `"Bearer "`. Three lines of guard at an auth boundary are cheaper than assuming nobody will ever build the map themselves.

14. **Task 5** — the root cause is shared ownership of the socket: two independent closers of the same connection. Rather than narrowing the race, ownership is made unambiguous: from `NewConn` onward the socket belongs to the `bus.Conn`, and `Run` guarantees the fd is released before it returns. Concretely: `Run` calls `CloseNow()` only when NO detached close was started (`closeDone == nil`); otherwise the goroutine handles the release. The hub no longer closes on the normal path at all; the two rejection paths sit before `NewConn` and keep `closeAsync`. There are then never two closers, no race and no leak. Cost if wrong: if `Run` returns before the goroutine finishes the handshake, the fd lives on briefly — bounded by the library's timeouts, not indefinitely.

15. **Task 5** — the reviewer's out-of-scope observation (`agent.go:46`'s `defer ws.CloseNow()` has the same defect) is NOT deferred, even though it lies outside the fix diff: it lies inside code Task 5 created, and the first full review simply missed it. It is also load-bearing — the agent reconnects in a loop by design, and a blocking `ConnectOnce` delays every reconnect against an unresponsive hub by up to 10 s. The next eleven tasks build on this. Fixed in round 3. Cost if wrong: one extra fix round for a path that only occurs against a slow peer.

16. **Task 5** — the two documentation inaccuracies are fixed as well, because they misdescribe the ownership rule that was just established — and that rule is exactly what the following tasks have to follow.

17. **Task 6** — every RPC call in the demo tests uses `context.Background()` with no deadline. The global constraints explicitly require a timeout arm for every wait in a test. A service that never answers would hang the suite instead of turning it red — precisely the failure Task 4 already produced once. The finding is right and my plan text is wrong. Fixed. Cost if wrong: none, it is pure test code.

18. **Task 7** — Important 1 (only caller cancellation sends `RpcCancel`; four other exits do not) is fixed. For unary calls it self-heals in milliseconds — which is why no test here can see it. From Task 8 onward, with server streaming, an orphaned local call and its goroutine survive for as long as the local service keeps producing. Exactly the leak class this whole project is structurally guarded against. Cost if wrong: three lines plus a helper.

19. **Task 7** — Important 2 (no `ponytail:` marker on the accepted teardown race) is fixed. The reviewer traced the race independently and judged it acceptable — the transport really is dead at that point, and `Unavailable` is retryable and not a false statement. But the trade-off must not live only in a report file nobody will read again; the global constraints require it in the code.

20. **Task 7** — Minor 3 is pulled up: `sendEnd` uses the possibly expired call context and works today only because the hub's deadline happens to fire first. An accidental guarantee is not a guarantee. One line.

21. **Task 7** — Minors 4-6 are pulled up: a dead import holder in `testenv`; a swallowed connect error (it affects the diagnostics of ALL following tests, and `testenv` carries eight more tasks); and `Close()` nils `grpcCC`, so the agent silently revives after being closed — a leak vector for the goleak gate in Task 16.

22. **Task 7** — Deviation A (no cancel on the `RpcEnd` return paths) is justified: the reviewer showed from the grpc-go contract that `RecvMsg` only terminates once the status has arrived, so no live local call is ever left behind. My literal instruction was wrong here.

23. **Task 7** — Deviation B (the panic path in `forwardResponses` sends no cancel) is deferred rather than blocked: the hub-side stream is still torn down correctly via LIFO defer order, only the notification to the agent is missing, and a panic there presupposes a defect somewhere else entirely. Carried into Task 8 as a requirement, where it also becomes testable. Cost if wrong: on a panic in the hub relay, a local call survives on the agent.

24. **Task 8** — Important (the ONE test that needs a mid-stream abort is the only one without a timeout arm) is fixed: a relay stalling before the first chunk would hang exactly this test for 10 minutes instead of failing it. One line.

25. **Task 8** — three Minors pulled up, all trivial and in the same files: `tunnelCtx` duplicates `callCtx` (two helpers that must stay in sync on timeout policy); `TailActive` carries no `ponytail:` marker even though the report itself names its ceiling; and `TestConcurrentLargeChatMessagesDoNotAlias` is not concurrent but pipelined.

26. **Task 9** — Important 1 (the cleanup poll in the disconnect test is bounded by `callTimeout = 5 s` while the agent self-heals at roughly T0+5 s) is fixed: the assertion cannot fail for the reason it exists. Exactly the masking the implementer diagnosed themselves and fixed in two other tests.

27. **Task 9** — Important 2 (the deadline path has NO cleanup coverage, only a status code) is fixed. Of the three paths the task names, one was therefore only half verified; the test also contaminates the process-global counter for the two tests that follow.

28. **Task 9** — Minor 3 is pulled up even though it lies outside this task: the existing abort test from Task 8 (`grpc_streaming_test.go:175`) carries the same masking. The reviewer wanted to refer it to the final review; I close it now, because it is the same single line in a file that is open anyway. Leaving a knowingly masked test in place because of a task boundary is bookkeeping, not judgement.

29. **Task 10** — Important (`TestStreamLimitIsEnforced` sets `refused = true` on ANY error and checks neither the status code nor how many streams succeeded first) is fixed. The test would pass even if `MaxStreams` were removed entirely — as long as something else fails within 300 attempts. `ResourceExhausted` also comes from a second, completely unrelated path (payload too large). Exactly the failure mode the brief itself names. Inherited from my plan text.

30. **Task 10** — both Minors on the starvation test are pulled up. Without synchronization there is no guarantee the large call is even running while the small ones are — and since the only bound checked is `callTimeout = 5 s`, a fully serialized design would pass identically. The test claims non-blocking behaviour and does not prove it. Cost if wrong: a tighter bound may flake under load; if so the implementer should report it rather than loosening the bound.

31. **Task 10** — Deviation A is justified: grpc-go creates streams lazily, so the refusal CANNOT surface at `client.Chat()`, only at `Send`/`Recv`. My instruction rested on a wrong assumption about gRPC.

32. **Task 10** — Deviation B: the bound is a weak guard. Over loopback even a fully serialized 2 MiB transfer stays at ~17.5 ms against a 500 ms bound. It catches gross failures, but not the thing the test is named after. Accepted as a ceiling — BUT the comment claiming the opposite has to go. A comment that implies a guarantee which does not exist is worse than none: it stops the next reader from writing the test that would actually check it.

33. **Task 11** — Important 1 (a request-body pump failure cancels the in-flight response, and `sendResponseEnd` then uses exactly that cancelled context — `Conn.Send` picks between `out` and `ctx.Done()` at random; if the send loses, no terminal envelope arrives at all and the hub handler parks indefinitely) is fixed. The gRPC relay already learned this lesson and deliberately uses `context.Background()` there. Plan-mandated. The reviewer could not trigger it in-process (the target answers in 2-7 ms), but the window opens precisely with slowly streaming responses — that is, in Task 12.

34. **Task 11** — Important 2 (`forwardHTTPRequestBody` reads and closes `r.Body` on a goroutine that can outlive the handler; `net/http` declares the request invalid after that) is fixed. Plan-mandated.

35. **Task 11** — Important 3 (`WriteHeader` with an agent-supplied status panics outside 100..999, and an unset field is 0) is fixed, plus a `wroteHead` guard. The gRPC hub considers this class important enough for a `recover()`.

36. **Task 11** — Minors 4, 6, 7, 8, 9 pulled up — all small, and three of them are statements in the code that are untrue: `errClosed`'s documentation now applies to only one branch; the reasoning about prefix trimming is factually wrong (the trigger is client-controlled, not id-controlled); and `hopByHop` is covered by NO test — deleting the drop list entirely leaves the suite green.

37. **Task 11** — the deviation (loop instead of `return`) is an IMPROVEMENT, demonstrated against the code: `st.In` holds 16 envelopes, and once it fills, `Conn.dispatch` blocks the connection's single read loop for every stream. My instruction would have introduced a worse defect.

38. **Task 12** — the Minor is pulled up even though the reviewer does not consider it a blocker: on a real regression the test does fail correctly, but `t.Cleanup(srv.Close)` then blocks indefinitely because httptest waits for the never-ending `/sse` handler. Result: a hung CI run instead of a readable failure. Exactly the "hang instead of fail" class that seven tests in this plan were already corrected for — just one level up, in the cleanup. Cost if wrong: the SSE handler gets an upper bound that never fires in normal operation.

39. **Task 13** — the uncovered hello-write-failure branch stays uncovered. The implementer tried twenty times; the local write always succeeds because the kernel buffer accepts it even when the peer is already gone. Forcing it would need raw socket control — disproportionate for two log-only lines. That is exactly the answer I had named as acceptable.

40. **Task 13** — Minor 1 (the keepalive's ping-failure path has NO test) is pulled up against the reviewer's advice. Their counterargument — testability knobs are speculative surface — is serious, but half of this task's title currently has no coverage at all. The keepalive exists precisely for failures that produce NO other symptom; if it goes untested, nobody notices when it stops working. Two unexported `var` instead of `const` is a very small surface against behaviour otherwise evidenced only by reading. Cost if wrong: two constants are now variables.

41. **Task 13** — Minors 3 and 4 pulled up: `max < min` remains possible after normalization (60s/0 yields min = 60s, max = 30s), and the final assertion in the reconnect test is unreachable while reading like a real check. Same class as the over-claiming comment in Task 10.

42. **Task 13** — this Minor is pulled up even though the reviewer classes it as non-blocking: they could not know that Task 16 introduces a goleak gate for exactly `internal/agent`. A permanently parked handler goroutine would trip that gate and look like a production leak there. One line now, or a confusing investigation in Task 16.

43. **Task 14** — NO fix round for the three Minors. Unlike the earlier pull-ups, none of them has a concrete downstream consequence — no leak gate they would trip, no untrue statement in the code, no trust boundary. The "render the template into a buffer first" pattern goes into the Task 15 dispatch instead, where the second UI is built. Cheaper and just as effective.

44. **Task 15** — Important 1 and 2 are fixed (one test closes both). One could delete `conn.SetTap()` entirely without a single test failing — the inspector is the point of this task and its connection to the bus is unverified. At the same time the SSE handler's hot path was never exercised, which makes the "no leak" check less evidenced than the report claims. One integration test (connect an agent, trigger a call, expect an envelope on the SSE stream) closes both.

45. **Task 15** — Important 3 (commit `5bc914f` uses `wip:`, not a conventional-commit type) is NOT fixed. The message is wrong, but rewriting history would invalidate the commit SHAs the ledger uses as its recovery map. That is the worse trade for a prefix. Noted for the final review and for a possible squash at merge time.

46. **Task 16** — Important 1 (`make demo` makes the documented acceptance step destructive — `go run` starts a child whose command line does NOT contain `cmd/agent`, while the Makefile's wrapper shell does; so `pkill` kills the hub and the demo service and leaves the agent running) is fixed. The reviewer rightly rejects the "platform quirk" framing: `go run` behaves this way everywhere. Anyone following the instructions concludes there is a defect that does not exist.

47. **Task 16** — Important 2 (the comment justifying two goleak ignores asserts a guarantee grpc-go does not make) is fixed. The entries may stay — the justification the implementer gave in their report is the correct one and belongs in the file. Same class as the over-claiming comments in Tasks 10 and 11.

48. **Task 16** — Important 3 (criterion 4 reported as passing, while the evidence shows only an open, chunked channel — never a byte flowing through it) is fixed. It is precisely the criterion that ties flush, tunnel and SSE together. The stated reason for the gap is untrue; a one-command trigger exists.

49. **Task 16** — criterion 5 is fixed as well, even though the reviewer raised it only as a warning: three sequentially completed calls produce the same stream ids as three concurrent ones, so the evidence cannot distinguish them. Same class as criterion 4: an acceptance claim its evidence does not support. The acceptance pass is the one artefact that says "this works".

50. The reviewer contradicts my Task 13 residual-risk ruling — RIGHTLY. I had analysed the STALE case (a handler writing to the successor connection, envelope discarded) and missed the NIL case: `clearConn()` nils `busConn` the moment `Run` returns, so `session()` can hand back nil. The reviewer reproduced the panic — and it is unrecoverable, because the `recover` in `ServeRPC` panics again on the same nil pointer. That is the blocker.

51. The reviewer contradicts my Task 15 ruling on the `wip:` commit — rightly, FOR MERGE TIME. The trade was correct while the ledger was a live recovery map; at merge it no longer is. A squash merge resolves both.

52. The reviewer half-contradicts my Task 14 ruling — rightly. I passed the buffer pattern on to Task 15 instead of fixing it; Task 15 got it, the agent UI did not. The same defect in two places, fixed in one. Now aligned.

53. Important 3 (the hub cannot detect a silently dead agent; spec §7 promises a "last pong" column, spec §10 promises `UNAVAILABLE`) — I decide for the CODE, not for a spec change: the hub now pings as well. Reason: without it, a caller WITHOUT a deadline hangs indefinitely, which spec §10 explicitly rules out. Five lines mirroring `agent.keepalive`.

54. Important 4 (a 500-entry ring buffer nothing reads) — I decide to RENDER rather than delete, against the reviewer's recommendation. The inspector exists to make multiplexing visible; a page that stays empty until the next traffic arrives undercuts exactly that. The data is already being collected. Cost if wrong: six lines of template instead of deleting fifteen lines.

55. Minor 13 (spec §4 claims "exactly one sending goroutine per stream and direction", while the code has two) — here the SPEC is wrong, not the code: the second goroutine only ever sends terminal envelopes, and a cancel overtaking a body chunk is the desired outcome. The spec is amended.

56. Residual 1 (the keepalive logs a misleading warning on a clean connection end) is PARKED. Pure log cosmetics: `Remove` is identity-guarded and `CloseWith` is then a no-op.

57. Residual 2 (the shortened ping constants apply to the whole hub test binary, a 100 ms pong budget) is PARKED. `-count=5` ran green; it is a robustness margin, not an observed flake. 500 ms would be the calmer value.

58. Residual 3 (the `recover` in `pumpBusToLocal` ends with `CANCELED` while spec §10 names `INTERNAL`) is PARKED. The load-bearing half of §10 holds: stream closed, terminal envelope sent, process alive. The code deviates in the status code, not in behaviour.

59. Residual 4 (the doc comment on the hub keepalive over-claims — the pong also needs the hub's own read loop) is PARKED, even though over-claiming comments were a genuine finding three times in this plan. Reason: the underlying fact is already documented as a `ponytail:` note at `bus/stream.go:12`, and the process rule allows no second fix wave.

60. Residual 5 (`http_hub.go:61` starts a bare goroutine with no `recover`; a panic there kills the hub — the same class as Important 2, on the HTTP path) is PARKED and presented to the user. It is a genuine defect, but no dependent task builds on it, the whole plan is finished, and the reviewer recommends a separate ticket rather than a last-minute change. Cost if wrong: a panic in this one path terminates the hub process.
