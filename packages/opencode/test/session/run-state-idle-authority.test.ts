import { describe, expect, test } from "bun:test"
import { Deferred, Effect, Fiber, Scope, SynchronizedRef } from "effect"
import { Runner } from "@/effect/runner"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionID } from "@/session/schema"

const output = {} as SessionV1.WithParts
const sessionID = "ses_run_state_idle_authority" as SessionID

const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>, message: string) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: "2 seconds",
      orElse: () => Effect.fail(new Error(message)),
    }),
  )

describe("SessionRunState idle authority", () => {
  test("registry admission cannot cross an older idle publication", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope
          type Runners = Map<SessionID, Runner.Runner<SessionV1.WithParts>>
          const runners = SynchronizedRef.makeUnsafe<Runners>(new Map())
          let status: "idle" | "busy" = "idle"

          const idlePublicationStarted = yield* Deferred.make<void>()
          const allowIdlePublication = yield* Deferred.make<void>()
          const replacementStarted = yield* Deferred.make<void>()
          const replacementDone = yield* Deferred.make<void>()

          const makeRunner = (current: Runners) => {
            let next!: Runner.Runner<SessionV1.WithParts>
            next = Runner.make<SessionV1.WithParts>(scope, {
              onIdle: SynchronizedRef.modifyEffect(
                runners,
                Effect.fnUntraced(function* (owned) {
                  if (next.pending) return [undefined, owned] as const
                  yield* Deferred.succeed(idlePublicationStarted, undefined)
                  yield* Deferred.await(allowIdlePublication)
                  status = "idle"
                  if (next.pending) return [undefined, owned] as const
                  if (owned.get(sessionID) === next) owned.delete(sessionID)
                  return [undefined, owned] as const
                }),
              ),
              onInterrupt: Effect.succeed(output),
            })
            current.set(sessionID, next)
            return next
          }

          const reserve = (work: Effect.Effect<SessionV1.WithParts>) =>
            Effect.gen(function* () {
              const wait = yield* SynchronizedRef.modifyEffect(
                runners,
                Effect.fnUntraced(function* (current) {
                  const runner = current.get(sessionID) ?? makeRunner(current)
                  const reserved = yield* runner.ensureRunningHandle(work)
                  return [reserved, current] as const
                }),
              )
              return yield* wait
            })

          const first = yield* reserve(
            Effect.sync(() => {
              status = "busy"
              return output
            }),
          ).pipe(Effect.forkChild)

          yield* bounded(
            Deferred.await(idlePublicationStarted),
            "timed out waiting for the first idle publication",
          )

          const replacement = yield* reserve(
            Effect.gen(function* () {
              status = "busy"
              yield* Deferred.succeed(replacementStarted, undefined)
              yield* Deferred.await(replacementDone)
              return output
            }),
          ).pipe(Effect.forkChild)

          // Idle publication owns the synchronized registry, so replacement
          // admission cannot create or reserve a Runner until it commits.
          const startedBeforeIdle = yield* Effect.raceFirst(
            Deferred.await(replacementStarted).pipe(Effect.as(true)),
            Effect.sleep("100 millis").pipe(Effect.as(false)),
          )
          expect(startedBeforeIdle).toBe(false)

          yield* Deferred.succeed(allowIdlePublication, undefined)
          expect(yield* bounded(Fiber.join(first), "timed out joining the first run")).toBe(output)
          yield* bounded(
            Deferred.await(replacementStarted),
            "timed out waiting for replacement work",
          )

          expect(status).toBe("busy")
          const current = yield* SynchronizedRef.get(runners)
          expect(current.get(sessionID)?.busy).toBe(true)

          yield* Deferred.succeed(replacementDone, undefined)
          expect(
            yield* bounded(Fiber.join(replacement), "timed out joining replacement work"),
          ).toBe(output)
        }),
      ),
    )
  })
})
