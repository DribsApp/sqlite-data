#if canImport(CloudKit)
  /// Wraps the sync engine's database work so the host app can keep the
  /// process alive and the database resumed while it runs (for example a
  /// background-task assertion plus GRDB's `Database.resumeNotification`).
  ///
  /// The engine calls `run` around every `CKSyncEngine` event it handles, around
  /// building each batch of changes to send, and around its start task. It
  /// never posts GRDB's suspension notifications itself.
  public protocol SyncEngineActivity: Sendable {
    func run<T: Sendable>(_ name: String, _ work: @Sendable () async -> T) async -> T
  }
#endif
