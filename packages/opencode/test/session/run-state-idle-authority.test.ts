import { describe, expect, test } from "bun:test"
import { Deferred, Effect, Fiber, Scope, Semaphore } from "effect"
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
  test("replacement admission waits for the previous idle publication", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope
          const runners = new Map<SessionID, Runner.Runner<SessionV1.WithParts>>()
          const lock = Semaphore.makeUnsafe(1)
          let status: "idle" | "busy" = "idle"

          const idlePublicationStarted = yield* Deferred.make<void>()
          const allowIdlePublication = yield* Deferred.make<void>()
          const replacementStarted = yield* Deferred.make<void>()
          const replacementDone = yield* Deferred.make<void>()

          const getRunner = () => {
            const existing = runners.get(sessionID)
            if (existing) return existing

            let next!: Runner.Runner<SessionV1.WithParts>
            next = Runner.make<SessionV1.WithParts>(scope, {
              onIdle: lock.withPermits(1)(
                Effect.gen(function* () {
                  if (next.pending) return
                  if (runners.get(sessionID) !== next) return
                  yield* Deferred.succeed(idlePublicationStarted, undefined)
                  yield* Deferred.await(allowIdlePublication)
                  status = "idle"
                  if (runners.get(sessionID) === next) runners.delete(sessionID)
                }),
              ),
              onInterrupt: Effect.succeed(output),
            })
            runners.set(sessionID, next)
            return next
          }

          const ensureRunning = (work: Effect.Effect<SessionV1.WithParts>) =>
            Effect.gen(function* () {
              const wait = yield* lock.withPermits(1)(getRunner().ensureRunningHandle(work))
              return yield* wait
            })

          const first = yield* ensureRunning(
            Effect.sync(() => {
              status = "busy"
              return output
            }),
          ).pipe(Effect.forkChild)

          yield* bounded(
            Deferred.await(idlePublicationStarted),
            "timed out waiting for the first idle publication",
          )

          const replacement = yield* ensureRunning(
            Effect.gen(function* () {
              status = "busy"
              yield* Deferred.succeed(replacementStarted, undefined)
              yield* Deferred.await(replacementDone)
              return output
            }),
          ).pipe(Effect.forkChild)

          // The session semaphore keeps admission behind the old publication.
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

          yield* Deferred.succeed(replacementDone, undefined)
          expect(
            yield* bounded(Fiber.join(replacement), "timed out joining replacement work"),
          ).toBe(output)
        }),
      ),
    )
  })
})
