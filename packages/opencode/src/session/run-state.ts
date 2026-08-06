import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Runner } from "@/effect/runner"
import { BackgroundJob } from "@/background/job"
import { Effect, Latch, Layer, Scope, Context, SynchronizedRef } from "effect"
import { Session } from "./session"
import { SessionID } from "./schema"
import { SessionStatus } from "./status"

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<void>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    ready?: Latch.Latch,
  ) => Effect.Effect<SessionV1.WithParts, Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service

    type Runners = Map<SessionID, Runner.Runner<SessionV1.WithParts>>
    type State = {
      runners: SynchronizedRef.SynchronizedRef<Runners>
      scope: Scope.Scope
    }

    const state = yield* InstanceState.make(
      Effect.fn("SessionRunState.state")(function* () {
        const scope = yield* Scope.Scope
        const runners = SynchronizedRef.makeUnsafe<Runners>(new Map())
        yield* Effect.addFinalizer(
          Effect.fnUntraced(function* () {
            const active = yield* SynchronizedRef.get(runners)
            yield* Effect.forEach(active.values(), (runner) => runner.cancel, {
              concurrency: "unbounded",
              discard: true,
            })
            yield* SynchronizedRef.set(runners, new Map())
          }),
        )
        return { runners, scope } satisfies State
      }),
    )

    const makeRunner = (
      data: State,
      runners: Runners,
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
    ) => {
      let next!: Runner.Runner<SessionV1.WithParts>
      next = Runner.make<SessionV1.WithParts>(data.scope, {
        onIdle: Effect.gen(function* () {
          // Snapshot publication authority under the registry lock, but do not
          // run the external status effect while holding it. A replacement may
          // reserve this Runner during publication; Runner keeps it pending and
          // cannot start it until this idle effect returns.
          const publish = yield* SynchronizedRef.modify(data.runners, (current) => [
            !next.pending && current.get(sessionID) === next,
            current,
          ])
          if (!publish) return

          yield* status.set(sessionID, { type: "idle" })

          // Delete only if no replacement was reserved while publication was
          // in flight. Otherwise the same Runner remains the registry owner and
          // starts its pending generation immediately after this effect ends.
          yield* SynchronizedRef.modify(data.runners, (current) => {
            if (!next.pending && current.get(sessionID) === next) current.delete(sessionID)
            return [undefined, current] as const
          })
        }),
        onBusy: status.set(sessionID, { type: "busy" }),
        onInterrupt,
      })
      runners.set(sessionID, next)
      return next
    }

    const reserveRunning = (
      data: State,
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
    ) =>
      SynchronizedRef.modifyEffect(
        data.runners,
        Effect.fnUntraced(function* (runners) {
          const runner =
            runners.get(sessionID) ?? makeRunner(data, runners, sessionID, onInterrupt)
          const wait = yield* runner.ensureRunningHandle(work)
          return [wait, runners] as const
        }),
      )

    const assertNotBusy = Effect.fn("SessionRunState.assertNotBusy")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const busy = yield* SynchronizedRef.modify(data.runners, (runners) => [
        runners.get(sessionID)?.busy ?? false,
        runners,
      ])
      if (busy) yield* busyError(sessionID)
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      yield* cancelBackgroundJobs(background, sessionID)
      const data = yield* InstanceState.get(state)
      const action = yield* SynchronizedRef.modifyEffect(
        data.runners,
        Effect.fnUntraced(function* (runners) {
          const existing = runners.get(sessionID)
          if (existing) return [existing.cancel, runners] as const
          yield* status.set(sessionID, { type: "idle" })
          return [Effect.void, runners] as const
        }),
      )
      yield* action
    })

    const ensureRunning = Effect.fn("SessionRunState.ensureRunning")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      const wait = yield* reserveRunning(data, sessionID, onInterrupt, work)
      return yield* wait
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      ready?: Latch.Latch,
    ) {
      const data = yield* InstanceState.get(state)
      const runner = yield* SynchronizedRef.modify(data.runners, (runners) => {
        const current =
          runners.get(sessionID) ?? makeRunner(data, runners, sessionID, onInterrupt)
        return [current, runners] as const
      })
      return yield* runner
        .startShell(work, ready)
        .pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID))))
    })

    return Service.of({ assertNotBusy, cancel, ensureRunning, startShell })
  }),
)

const cancelBackgroundJobs = Effect.fn("SessionRunState.cancelBackgroundJobs")(function* (
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  const jobs = yield* background.list()
  const pending = new Set<string>([sessionID])
  const cancelled = new Set<string>()
  const matches = (job: BackgroundJob.Info) => {
    if (job.status !== "running") return false
    if (cancelled.has(job.id)) return false
    if (pending.has(job.id)) return true
    if (typeof job.metadata?.sessionId === "string" && pending.has(job.metadata.sessionId)) return true
    return typeof job.metadata?.parentSessionId === "string" && pending.has(job.metadata.parentSessionId)
  }
  let batch = jobs.filter(matches)
  while (batch.length > 0) {
    yield* Effect.forEach(
      batch,
      (job) =>
        background.cancel(job.id).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              cancelled.add(job.id)
              pending.add(job.id)
              if (typeof job.metadata?.sessionId === "string") pending.add(job.metadata.sessionId)
            }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    )
    batch = jobs.filter(matches)
  }
})

function busyError(sessionID: SessionID) {
  return new Session.BusyError({ sessionID })
}

export const node = LayerNode.make({ service: Service, layer: layer, deps: [BackgroundJob.node, SessionStatus.node] })

export * as SessionRunState from "./run-state"
