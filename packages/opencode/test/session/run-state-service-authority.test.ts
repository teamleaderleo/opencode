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

      const first = yield* runState
        .ensureRunning(
          sessionID,
          Effect.succeed(output),
          status.set(sessionID, { type: "busy" }).pipe(Effect.as(output)),
        )
        .pipe(Effect.forkChild)

      yield* Deferred.await(idlePublicationStarted)

      const replacementStarted = yield* Deferred.make<void>()
      const replacementDone = yield* Deferred.make<void>()
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

      // Current code creates a replacement runner while the previous runner's
      // idle publication is suspended. A serialized repair keeps this request
      // pending until the old publication commits.
      yield* Effect.raceFirst(
        Deferred.await(replacementStarted),
        Effect.sleep("100 millis"),
      )

      yield* Deferred.succeed(allowIdlePublication, undefined)
      expect(yield* Fiber.join(first)).toBe(output)
      yield* Deferred.await(replacementStarted)

      // Current behavior is idle here because the older publication resumes
      // after replacement work has already made busy state authoritative.
      expect(yield* status.get(sessionID)).toEqual({ type: "busy" })

      yield* Deferred.succeed(replacementDone, undefined)
      expect(yield* Fiber.join(replacement)).toBe(output)
    }),
  )
})
