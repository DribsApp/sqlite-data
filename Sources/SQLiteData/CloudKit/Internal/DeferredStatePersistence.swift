#if canImport(CloudKit)
  package import CloudKit

  /// Keeps the persisted `CKSyncEngine` state from advancing past changes the
  /// local database never applied.
  ///
  /// `CKSyncEngine` advances its in-memory change token as soon as it delivers
  /// a batch, whether or not the delegate's writes succeeded. When the host
  /// suspends the database (GRDB's `Database.suspendNotification`) partway
  /// through applying a batch, the writes fail, and saving the next
  /// `.stateUpdate` would skip that batch for good. So such an event is kept
  /// here for retry, and its scope's state serialization is held back (only
  /// the newest) until the retry succeeds. If the process dies first, the next
  /// launch starts from the older saved token and fetches the batch again.
  package struct DeferredStatePersistence<Event: Sendable, State: Sendable>: Sendable {
    private var unapplied: [CKDatabase.Scope: [Event]] = [:]
    private var heldState: [CKDatabase.Scope: State] = [:]

    package init() {}

    package var hasAnythingDeferred: Bool {
      unapplied.values.contains { !$0.isEmpty } || !heldState.isEmpty
    }

    package mutating func recordUnapplied(_ event: Event, scope: CKDatabase.Scope) {
      unapplied[scope, default: []].append(event)
    }

    package mutating func takeUnapplied(scope: CKDatabase.Scope) -> [Event] {
      unapplied.removeValue(forKey: scope) ?? []
    }

    /// Whether the scope has unapplied events. While it does, a newer event of
    /// the scope must queue behind them rather than apply out of order.
    package func isBlocked(scope: CKDatabase.Scope) -> Bool {
      !(unapplied[scope] ?? []).isEmpty
    }

    /// Holds `state` (replacing any older held one) and returns true when the
    /// scope has unapplied events; returns false, holding nothing, otherwise.
    package mutating func holdIfBlocked(_ state: State, scope: CKDatabase.Scope) -> Bool {
      guard !(unapplied[scope] ?? []).isEmpty else { return false }
      heldState[scope] = state
      return true
    }

    /// The held state, removed, once the scope has nothing unapplied.
    package mutating func takeHeldStateIfUnblocked(scope: CKDatabase.Scope) -> State? {
      guard (unapplied[scope] ?? []).isEmpty else { return nil }
      return heldState.removeValue(forKey: scope)
    }

    package mutating func reset() {
      unapplied = [:]
      heldState = [:]
    }
  }

  /// What `SyncEngine` knows about GRDB's suspension notifications, for
  /// deciding whether an event's writes may have been refused.
  package struct DatabaseSuspensionState: Sendable {
    /// Suspensions begun so far. A change across an event means a suspension
    /// started while it ran, even if it also ended before the event did.
    package private(set) var suspendCount = 0
    package private(set) var isSuspended = false

    package init() {}

    package mutating func didSuspend() {
      suspendCount += 1
      isSuspended = true
    }

    package mutating func didResume() {
      isSuspended = false
    }

    /// Whether work that began at `before` and ended at `self` may have run
    /// while the database refused writes: it started suspended, ended
    /// suspended, or a suspension began in between.
    package func overlapsSuspension(since before: Self) -> Bool {
      before.isSuspended || isSuspended || suspendCount != before.suspendCount
    }
  }
#endif
