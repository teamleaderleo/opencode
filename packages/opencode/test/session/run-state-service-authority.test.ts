import { describe, expect, test } from "bun:test"
import { Deferred, Effect, Fiber, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionID } from "@/session/schema"

const output = {} as SessionV1.WithParts
const sessionID = "ses_run_state_authority" as SessionID

const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>, message: string) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: "2 seconds",
      orElse: () => Effect.fail(new Error(message)),
    }),
  )

describe("SessionRunState ownership ordering", () => {
  test("an older idle publication cannot overwrite replacement busy state", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope
          const runners = new Map<SessionID, Runner.Runner<SessionV1.WithParts>>()
          let status: "idle" | "busy" = "idle"

          const idlePublicationStarted = yield* Deferred.make<void>()
          const allowIdlePublication = yield* Deferred.make<void>()
          const replacementStarted = yield* Deferred.make<void>()
          const replacementDone = yield* Deferred.make<void>()

          const runner = (id: SessionID) => {
            const existing = runners.get(id)
            if (existing) return existing

            const next = Runner.make<SessionV1.WithParts>(scope, {
              onIdle: Effect.gen(function* () {
                // This is the current SessionRunState ordering: release the
                // registry owner before the externally visible idle write has
                // committed.
                runners.delete(id)
                yield* Deferred.succeed(idlePublicationStarted, undefined)
                yield* Deferred.await(allowIdlePublication)
                status = "idle"
              }),
              onInterrupt: Effect.succeed(output),
            })
            runners.set(id, next)
            return next
          }

          const ensureRunning = (work: Effect.Effect<SessionV1.WithParts>) =>
            runner(sessionID).ensureRunning(work)

          const first = yield* ensureRunning(
            Effect.sync(() => {
              status = "busy"
              return output
            }),
          ).pipe(Effect.forkChild)

          yield* bounded(
            Deferred.await(idlePublicationStarted),
            "timed out waiting for the first idle publication barrier",
          )

          const replacement = yield* ensureRunning(
            Effect.gen(function* () {
              status = "busy"
              yield* Deferred.succeed(replacementStarted, undefined)
              yield* Deferred.await(replacementDone)
              return output
            }),
          ).pipe(Effect.forkChild)

          // Current source starts replacement work while the previous idle
          // publication is still suspended. A serialized repair may keep it
          // pending until the old publication commits, so do not require one
          // specific admission strategy here.
          yield* Effect.raceFirst(
            Deferred.await(replacementStarted),
            Effect.sleep("100 millis"),
          )

          yield* Deferred.succeed(allowIdlePublication, undefined)
          expect(
            yield* bounded(Fiber.join(first), "timed out joining the first run"),
          ).toBe(output)
          yield* bounded(
            Deferred.await(replacementStarted),
            "timed out waiting for replacement work to start",
          )

          // Capture before replacement completion can publish its own idle.
          const statusAfterOldIdle = status

          yield* Deferred.succeed(replacementDone, undefined)
          expect(
            yield* bounded(
              Fiber.join(replacement),
              "timed out joining replacement work",
            ),
          ).toBe(output)

          expect(statusAfterOldIdle).toBe("busy")
        }),
      ),
    )
  })
})
