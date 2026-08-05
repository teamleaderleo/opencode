import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { afterEach, describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Layer, Ref } from "effect"
import { BackgroundJob } from "@/background/job"
import { SessionRunState } from "@/session/run-state"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

let idlePublicationStarted!: Deferred.Deferred<void>
let allowIdlePublication!: Deferred.Deferred<void>
let currentStatus!: Ref.Ref<SessionStatus.Info>

const statusLayer = Layer.effect(
  SessionStatus.Service,
  Effect.gen(function* () {
    idlePublicationStarted = yield* Deferred.make<void>()
    allowIdlePublication = yield* Deferred.make<void>()
    currentStatus = yield* Ref.make<SessionStatus.Info>({ type: "idle" })

    return SessionStatus.Service.of({
      get: () => Ref.get(currentStatus),
      list: () =>
        Ref.get(currentStatus).pipe(
          Effect.map((status) =>
            status.type === "idle"
              ? new Map<SessionID, SessionStatus.Info>()
              : new Map<SessionID, SessionStatus.Info>(),
          ),
        ),
      set: (_sessionID, status) =>
        Effect.gen(function* () {
          if (status.type === "idle") {
            yield* Deferred.succeed(idlePublicationStarted, undefined)
            yield* Deferred.await(allowIdlePublication)
          }
          yield* Ref.set(currentStatus, status)
        }),
    })
  }),
)

const layer = LayerNode.compile(
  LayerNode.group([BackgroundJob.node, SessionRunState.node, SessionStatus.node]),
  [[SessionStatus.node, statusLayer]],
)

const it = testEffect(layer)

const output = {} as SessionV1.WithParts
const sessionID = "ses_run_state_authority" as SessionID

afterEach(async () => {
  await disposeAllInstances()
})

describe("SessionRunState service authority", () => {
  it.live(
    "an older idle publication cannot overwrite replacement busy state",
    Effect.gen(function* () {
      const runState = yield* SessionRunState.Service
      const status = yield* SessionStatus.Service
      const replacementStarted = yield* Deferred.make<void>()
      const replacementDone = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const first = yield* runState
          .ensureRunning(
            sessionID,
            Effect.succeed(output),
            status.set(sessionID, { type: "busy" }).pipe(Effect.as(output)),
          )
          .pipe(Effect.forkChild)

        yield* Deferred.await(idlePublicationStarted)

        const replacement = yield* runState
          .ensureRunning(
            sessionID,
            Effect.succeed(output),
            Effect.gen(function* () {
              yield* status.set(sessionID, { type: "busy" })
              yield* Deferred.succeed(replacementStarted, undefined)
              yield* Deferred.await(replacementDone)
              return output
            }),
          )
          .pipe(Effect.forkChild)

        // Give current source enough time to start replacement work while the
        // older idle publication is suspended. A serialized repair leaves the
        // replacement pending until the older publication commits.
        yield* Effect.sleep("100 millis")

        yield* Deferred.succeed(allowIdlePublication, undefined)
        expect(yield* Fiber.join(first)).toBe(output)
        yield* Deferred.await(replacementStarted)

        // Capture the status before replacement completion publishes its own
        // idle transition. Current source records stale idle here; a repair
        // keeps the replacement generation authoritative and therefore busy.
        const statusAfterOldIdle = yield* status.get(sessionID)

        // Always let the replacement finish before asserting, so an expected
        // regression failure cannot strand a child fiber or test-layer scope.
        yield* Deferred.succeed(replacementDone, undefined)
        expect(yield* Fiber.join(replacement)).toBe(output)
        expect(statusAfterOldIdle).toEqual({ type: "busy" })
      }).pipe(
        Effect.ensuring(
          Effect.all(
            [
              Deferred.succeed(allowIdlePublication, undefined),
              Deferred.succeed(replacementDone, undefined),
            ],
            { discard: true },
          ),
        ),
      )
    }),
  )
})
