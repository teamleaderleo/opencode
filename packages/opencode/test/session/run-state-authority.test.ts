import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Ref, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { it } from "../lib/effect"

const waitForState = <A, E>(runner: Runner.Runner<A, E>, tag: Runner.State<A, E>["_tag"]) =>
  Effect.gen(function* () {
    while (runner.state._tag !== tag) yield* Effect.yieldNow
  }).pipe(Effect.timeout("1 second"))

describe("SessionRunState authority", () => {
  it.live(
    "an older idle publication cannot overwrite a replacement run's busy status",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const registry = new Map<string, Runner.Runner<string>>()
      const status = yield* Ref.make<"idle" | "busy">("idle")
      const oldIdlePublicationStarted = yield* Deferred.make<void>()
      const allowOldIdlePublication = yield* Deferred.make<void>()
      const replacementDone = yield* Deferred.make<void>()

      const makeRunner = () => {
        const runner = Runner.make<string>(scope, {
          onIdle: Effect.gen(function* () {
            // Mirrors SessionRunState.onIdle: release the session's runner
            // registration before the asynchronous status publication settles.
            registry.delete("session")
            yield* Deferred.succeed(oldIdlePublicationStarted, undefined)
            yield* Deferred.await(allowOldIdlePublication)
            yield* Ref.set(status, "idle")
          }),
        })
        registry.set("session", runner)
        return runner
      }

      const original = makeRunner()
      const originalRun = yield* original
        .ensureRunning(
          Ref.set(status, "busy").pipe(Effect.andThen(Effect.succeed("original"))),
        )
        .pipe(Effect.forkChild)

      // The original runner is no longer registered, but its idle publication
      // is intentionally held before it can mutate the status source.
      yield* Deferred.await(oldIdlePublicationStarted)
      expect(registry.has("session")).toBe(false)

      const replacement = makeRunner()
      const replacementRun = yield* replacement
        .ensureRunning(
          Effect.gen(function* () {
            yield* Ref.set(status, "busy")
            yield* Deferred.await(replacementDone)
            return "replacement"
          }),
        )
        .pipe(Effect.forkChild)

      yield* waitForState(replacement, "Running")
      expect(yield* Ref.get(status)).toBe("busy")
      expect(registry.get("session")).toBe(replacement)

      // Let the older completion finish publishing idle after replacement work
      // has already taken authority.
      yield* Deferred.succeed(allowOldIdlePublication, undefined)
      expect(yield* Fiber.join(originalRun)).toBe("original")

      // This is the required session-level invariant. Current behavior writes
      // "idle" here even though the replacement runner is still active.
      expect(registry.get("session")).toBe(replacement)
      expect(replacement.busy).toBe(true)
      expect(yield* Ref.get(status)).toBe("busy")

      yield* Deferred.succeed(replacementDone, undefined)
      expect(yield* Fiber.join(replacementRun)).toBe("replacement")
    }),
  )
})
