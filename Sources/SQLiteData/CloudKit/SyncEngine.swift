#if canImport(CloudKit)
  public import CloudKit
  package import ConcurrencyExtras
  import Dependencies
  public import GRDB
  public import IssueReporting
  import OrderedCollections
  public import OSLog
  import Observation
  public import StructuredQueries
  import StructuredQueriesSQLite
  #if EXCLUDE_EXPORTS
    public import StructuredQueriesSQLiteCore
  #endif
  import SwiftData
  import TabularData

  #if canImport(UIKit)
    import UIKit
  #endif

  /// An object that manages the synchronization of local and remote SQLite data.
  ///
  /// See <doc:CloudKitSync> for more information.
  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  public final class SyncEngine: Observable, Sendable {
    package let userDatabase: UserDatabase
    package let logger: Logger
    package let metadatabase: any DatabaseWriter
    package let tables: [any SynchronizableTable]
    package let privateTables: [any SynchronizableTable]
    let tablesByName: [String: any SynchronizableTable]
    package let tablesByOrder: [String: Int]
    let foreignKeysByTableName: [String: [ForeignKey]]
    package let syncEngines = LockIsolated<SyncEngines>(SyncEngines())
    package let defaultZone: CKRecordZone
    let delegate: (any SyncEngineDelegate)?
    let activity: (any SyncEngineActivity)?
    let defaultSyncEngines:
      @Sendable (any DatabaseReader, SyncEngine)
        -> (private: any SyncEngineProtocol, shared: any SyncEngineProtocol)
    package let container: any CloudContainer
    let dataManager = Dependency(\.dataManager)
    private let observationRegistrar = ObservationRegistrar()
    private let notificationsObserver = LockIsolated<(any NSObjectProtocol)?>(nil)
    private let activityCounts = LockIsolated(ActivityCounts())
    // Server records parked by a foreign-key violation during apply (child
    // arrived before its parent — routine across fetch batches, since the
    // server delivers records in arbitrary order). The per-batch retry
    // replays from this cache instead of re-downloading the parked backlog
    // from CloudKit — previously that re-fetch ran after EVERY batch over a
    // growing set (quadratic server traffic; observed pacing a ~6k-record
    // initial sync to over an hour and drawing requestRateLimited). The
    // persisted `UnsyncedRecordID` table remains the crash-recovery net: a
    // relaunch mid-sync falls back to the server fetch for whatever this
    // in-memory cache lost.
    private let parkedServerRecords = LockIsolated<[CKRecord.ID: CKRecord]>([:])
    // GRDB suspension tracking (see `DeferredStatePersistence`). Lock order:
    // `syncEngines` before `deferredState`, never the reverse, so that
    // `stop()`/`start()` swap the engines and drop what the old ones
    // deferred in one step.
    // Starts out not suspended, so the host MUST build the engine before it
    // starts posting suspensions; this is required for correctness, not an
    // optimization. An engine built while the database is already suspended
    // never saw that suspension: until the next notification its counter
    // reports none, so each failed attempt counts only toward
    // `maxAttemptsWithoutSuspension`, and a single fetch (batch, then
    // stateUpdate, then didFetch…) burns the cap, gives the batch up and
    // lets the token move past it. GRDB exposes no public suspension state to
    // read here, and posting a notification to learn it would suspend or
    // resume every observing pool. Dribs satisfies this: it builds the engine
    // in `DatabaseLaunchRecovery` before `DatabaseSuspensionLifecycle.install()`.
    private let suspension = LockIsolated(DatabaseSuspensionState())
    private let deferredState = LockIsolated(
      DeferredStatePersistence<DeferredEvent, CKSyncEngine.State.Serialization>()
    )
    private let suspensionObservers = LockIsolated<[any NSObjectProtocol]>([])
    /// A queued event, with how many attempts in a row failed with a
    /// suspension-shaped error while no suspension activity was seen.
    private struct DeferredEvent: Sendable {
      let event: Event
      var attemptsWithoutSuspension = 0
    }
    /// After this many such attempts the event is given up on (see
    /// `applyDetectingSuspension`).
    private static let maxAttemptsWithoutSuspension = 3
    private let isRetryingAfterResume = LockIsolated(false)
    private let startTask = LockIsolated<Task<Void, Never>?>(nil)
    /// See `completedPrivateFetchCount`.
    private let completedPrivateFetches = LockIsolated(0)

    func withActivity<T: Sendable>(_ name: String, _ work: @Sendable () async -> T) async -> T {
      guard let activity else { return await work() }
      return await activity.run(name, work)
    }
    #if DEBUG && canImport(DeveloperToolsSupport)
      private let previewTimerTask = LockIsolated<Task<Void, Never>?>(nil)
    #endif

    /// The error message used when a write occurs to a record for which the current user does not
    /// have permission.
    ///
    /// This error is thrown from any database write to a row for which the current user does
    /// not have permissions to write, as determined by its `CKShare` (if applicable). To catch
    /// this error try casting it to `DatabaseError` and checking its message:
    ///
    /// ```swift
    /// do {
    ///   try await database.write { db in
    ///     Reminder.find(id)
    ///       .update { $0.title = "Personal" }
    ///       .execute(db)
    ///   }
    /// } catch let error as DatabaseError where error.message == SyncEngine.writePermissionError {
    ///   // User does not have permission to write to this record.
    /// }
    /// ```
    public static let writePermissionError =
      "co.pointfree.SQLiteData.CloudKit.write-permission-error"
    public static let invalidRecordNameError =
      "co.pointfree.SQLiteData.CloudKit.invalid-record-name-error"

    /// Initialize a sync engine.
    ///
    /// - Parameters:
    ///   - database: The database to synchronize to CloudKit.
    ///   - tables: A list of tables that you want to synchronize _and_ that you want to be
    ///     shareable with other users on CloudKit.
    ///   - privateTables: A list of tables that you want to synchronize to CloudKit but that
    ///     you do not want to be shareable with other users.
    ///   - containerIdentifier: The container identifier in CloudKit to synchronize to. If omitted
    ///     the container will be determined from the entitlements of your app.
    ///   - defaultZone: The zone for all records to be stored in.
    ///   - startImmediately: Determines if the sync engine starts right away or requires an
    ///     explicit call to ``start()``. By default this argument is `true`.
    ///   - delegate: A delegate object that can be notified of events and override default sync
    ///     engine behavior.
    ///   - logger: The logger used to log events in the sync engine. By default a `.disabled`
    ///     logger is used, which means logs are not printed.
    public convenience init<
      each T1: PrimaryKeyedTable & _SendableMetatype,
      each T2: PrimaryKeyedTable & _SendableMetatype
    >(
      for database: any DatabaseWriter,
      tables: repeat (each T1).Type,
      privateTables: repeat (each T2).Type,
      containerIdentifier: String? = nil,
      defaultZone: CKRecordZone = CKRecordZone(zoneName: "co.pointfree.SQLiteData.defaultZone"),
      startImmediately: Bool? = nil,
      delegate: (any SyncEngineDelegate)? = nil,
      logger: Logger = isTesting
        ? Logger(.disabled) : Logger(subsystem: "SQLiteData", category: "CloudKit"),
      activity: (any SyncEngineActivity)? = nil
    ) throws
    where
      repeat (each T1).PrimaryKey.QueryOutput: IdentifierStringConvertible,
      repeat (each T1).TableColumns.PrimaryColumn: WritableTableColumnExpression,
      repeat (each T2).PrimaryKey.QueryOutput: IdentifierStringConvertible,
      repeat (each T2).TableColumns.PrimaryColumn: WritableTableColumnExpression
    {
      @Dependency(\.context) var context
      let containerIdentifier =
        containerIdentifier
        ?? ModelConfiguration(groupContainer: .automatic).cloudKitContainerIdentifier
        ?? (context != .live ? "container" : nil)
      var allTables: [any SynchronizableTable] = []
      var allPrivateTables: [any SynchronizableTable] = []
      for table in repeat each tables {
        allTables.append(SynchronizedTable(for: table))
      }
      for privateTable in repeat each privateTables {
        allPrivateTables.append(SynchronizedTable(for: privateTable))
      }
      let userDatabase = UserDatabase(database: database)

      guard context == .live
      else {
        let privateDatabase = MockCloudDatabase(databaseScope: .private)
        let sharedDatabase = MockCloudDatabase(databaseScope: .shared)
        let container = MockCloudContainer(
          containerIdentifier: containerIdentifier ?? "iCloud.co.pointfree.SQLiteData.Tests",
          privateCloudDatabase: privateDatabase,
          sharedCloudDatabase: sharedDatabase
        )
        privateDatabase.set(container: container)
        sharedDatabase.set(container: container)
        try self.init(
          container: container,
          defaultZone: defaultZone,
          defaultSyncEngines: { _, syncEngine in
            (
              private: MockSyncEngine(
                database: privateDatabase,
                parentSyncEngine: syncEngine,
                state: MockSyncEngineState()
              ),
              shared: MockSyncEngine(
                database: sharedDatabase,
                parentSyncEngine: syncEngine,
                state: MockSyncEngineState()
              )
            )
          },
          userDatabase: userDatabase,
          logger: logger,
          delegate: delegate,
          tables: allTables,
          privateTables: allPrivateTables,
          activity: activity
        )
        try setUpSyncEngine()
        if startImmediately ?? !isTesting {
          _ = try start()
        }
        return
      }

      guard let containerIdentifier else {
        throw SchemaError.noCloudKitContainer
      }

      let container = CKContainer(identifier: containerIdentifier)
      try self.init(
        container: container,
        defaultZone: defaultZone,
        defaultSyncEngines: { metadatabase, syncEngine in
          (
            private: CKSyncEngine(
              CKSyncEngine.Configuration(
                database: container.privateCloudDatabase,
                stateSerialization: try? metadatabase.read { db in
                  try StateSerialization
                    .find(#bind(.private))
                    .select(\.data)
                    .fetchOne(db)
                },
                delegate: syncEngine
              )
            ),
            shared: CKSyncEngine(
              CKSyncEngine.Configuration(
                database: container.sharedCloudDatabase,
                stateSerialization: try? metadatabase.read { db in
                  try StateSerialization
                    .find(#bind(.shared))
                    .select(\.data)
                    .fetchOne(db)
                },
                delegate: syncEngine
              )
            )
          )
        },
        userDatabase: userDatabase,
        logger: logger,
        delegate: delegate,
        tables: allTables,
        privateTables: allPrivateTables,
        activity: activity
      )
      try setUpSyncEngine()
      if startImmediately ?? !isTesting {
        _ = try start()
      }
    }

    package init(
      container: any CloudContainer,
      defaultZone: CKRecordZone,
      defaultSyncEngines:
        @escaping @Sendable (
          any DatabaseReader,
          SyncEngine
        ) -> (private: any SyncEngineProtocol, shared: any SyncEngineProtocol),
      userDatabase: UserDatabase,
      logger: Logger,
      delegate: (any SyncEngineDelegate)?,
      tables: [any SynchronizableTable],
      privateTables: [any SynchronizableTable] = [],
      activity: (any SyncEngineActivity)? = nil
    ) throws {
      let allTables = OrderedSet((tables + privateTables).map(HashableSynchronizedTable.init))
        .map(\.type)
      self.tables = allTables
      self.privateTables = privateTables
      self.delegate = delegate
      self.activity = activity

      let foreignKeysByTableName = Dictionary(
        uniqueKeysWithValues: try userDatabase.read { db in
          try allTables.map { table -> (String, [ForeignKey]) in
            func open<T>(
              _: some SynchronizableTable<T>
            ) throws -> (String, [ForeignKey]) {
              (
                T.tableName,
                try PragmaForeignKeyList<T>
                  .join(PragmaTableInfo<T>.all) { $0.from.eq($1.name) }
                  .select {
                    ForeignKey.Columns(
                      table: $0.table,
                      from: $0.from,
                      to: $0.to,
                      onUpdate: $0.onUpdate,
                      onDelete: $0.onDelete,
                      isNotNull: $1.isNotNull
                    )
                  }
                  .fetchAll(db)
              )
            }
            return try open(table)
          }
        }
      )
      self.container = container
      self.defaultZone = defaultZone
      self.defaultSyncEngines = defaultSyncEngines
      self.userDatabase = userDatabase
      self.logger = logger
      self.metadatabase = try defaultMetadatabase(
        logger: logger,
        url: try URL.metadatabase(
          databasePath: userDatabase.path,
          containerIdentifier: container.containerIdentifier
        ),
        configuration: userDatabase.configuration
      )
      self.tablesByName = Dictionary(
        uniqueKeysWithValues: self.tables.map { ($0.base.tableName, $0) }
      )
      self.foreignKeysByTableName = foreignKeysByTableName
      tablesByOrder = try SQLiteData.tablesByOrder(
        userDatabase: userDatabase,
        tables: allTables,
        tablesByName: tablesByName
      )
      #if os(iOS)
        @Dependency(\.defaultNotificationCenter) var defaultNotificationCenter
        notificationsObserver.withValue {
          $0 = defaultNotificationCenter.addObserver(
            forName: UIApplication.willResignActiveNotification,
            object: nil,
            queue: nil
          ) { [syncEngines] _ in
            _ = Task { @MainActor in
              let (privateSyncEngine, sharedSyncEngine) = syncEngines.withValue {
                ($0.private, $0.shared)
              }
              let send = Task {
                try await privateSyncEngine?.sendChanges(CKSyncEngine.SendChangesOptions())
                try await sharedSyncEngine?.sendChanges(CKSyncEngine.SendChangesOptions())
              }
              // A send can outlast the background budget (a first sync, a
              // restore's reseed). UIKit kills an app whose task is still open
              // after its expiration handler returns, so the handler cancels
              // the send and ends the task itself; the normal path ends it
              // after the send. Ending is idempotent, whichever comes first.
              // Begin and end are `NS_SWIFT_NONISOLATED`, so the handler can
              // end the task from wherever UIKit calls it.
              let application = UIApplication.shared
              let taskIdentifier = LockIsolated(UIBackgroundTaskIdentifier.invalid)
              let endBackgroundTask: @Sendable () -> Void = {
                let identifier = taskIdentifier.withValue { identifier in
                  defer { identifier = .invalid }
                  return identifier
                }
                guard identifier != .invalid else { return }
                application.endBackgroundTask(identifier)
              }
              taskIdentifier.setValue(
                application.beginBackgroundTask(withName: "SQLiteData.sendChanges") {
                  send.cancel()
                  endBackgroundTask()
                }
              )
              _ = await send.result
              endBackgroundTask()
            }
          }
        }
      #endif
      if userDatabase.configuration.observesSuspensionNotifications {
        // GRDB's own notifications, on the center GRDB observes. An event
        // whose writes overlapped a suspension is retried (see
        // `DeferredStatePersistence`). The pools registered their observers
        // when they opened, before these. NotificationCenter calls observers
        // in registration order in practice (not a documented guarantee), so:
        // - on a suspend, GRDB interrupts the writer before `suspendCount`
        //   moves. A handler whose write was just aborted can finish in that
        //   gap and look clean to the counter, which is why
        //   `applyDetectingSuspension` also watches for reported interruption
        //   errors, a signal that does not depend on the order;
        // - on a resume, the pool is writable again before `isSuspended`
        //   clears.
        let center = NotificationCenter.default
        suspensionObservers.withValue {
          $0 = [
            center.addObserver(forName: Database.suspendNotification, object: nil, queue: nil) {
              [suspension] _ in
              suspension.withValue { $0.didSuspend() }
            },
            center.addObserver(forName: Database.resumeNotification, object: nil, queue: nil) {
              [weak self] _ in
              self?.databaseDidResume()
            },
          ]
        }
      }
      try validateSchema()
    }

    deinit {
      notificationsObserver.withValue {
        guard let observer = $0
        else { return }
        NotificationCenter.default.removeObserver(observer)
      }
      suspensionObservers.withValue {
        $0.forEach(NotificationCenter.default.removeObserver)
      }
    }

    nonisolated package func setUpSyncEngine() throws {
      try userDatabase.write { db in
        try setUpSyncEngine(writableDB: db)
      }
    }

    nonisolated package func setUpSyncEngine(writableDB db: Database) throws {
      let attachedMetadatabasePath: String? =
        try PragmaDatabaseList
        .where { $0.name.eq(String.sqliteDataCloudKitSchemaName) }
        .select(\.file)
        .fetchOne(db)
      if let attachedMetadatabasePath {
        let metadatabaseName =
          metadatabase.path.isEmpty
          ? try URL.metadatabase(
            databasePath: "",
            containerIdentifier: self.container.containerIdentifier
          )
          .lastPathComponent
          : URL(filePath: metadatabase.path).lastPathComponent
        let attachedMetadatabaseName =
          URL(string: attachedMetadatabasePath)?.lastPathComponent ?? ""
        @Dependency(\.context) var context
        if metadatabaseName != attachedMetadatabaseName
          && !(context == .preview && attachedMetadatabaseName.isEmpty)
        {
          throw SchemaError(
            reason: .metadatabaseMismatch(
              attachedPath: attachedMetadatabasePath,
              syncEngineConfiguredPath: metadatabase.path
            ),
            debugDescription: """
              Metadatabase attached in 'prepareDatabase' does not match metadatabase prepared in \
              'SyncEngine.init'. Are different CloudKit container identifiers being provided?
              """
          )
        }
      } else {
        try #sql(
          """
          ATTACH DATABASE \(bind: metadatabase.path) AS \(quote: .sqliteDataCloudKitSchemaName)
          """
        )
        .execute(db)
      }
      db.add(function: $currentTime)
      db.add(function: SyncEngine.$isSynchronizing)
      db.add(function: $didUpdate)
      db.add(function: $didDelete)
      db.add(function: $hasPermission)
      db.add(function: $currentZoneName)
      db.add(function: $currentOwnerName)

      for trigger in SyncMetadata.callbackTriggers(for: self) {
        try trigger.execute(db)
      }

      for table in tables {
        try table.base.createTriggers(
          foreignKeysByTableName: foreignKeysByTableName,
          tablesByName: tablesByName,
          defaultZone: defaultZone,
          privateTables: privateTables,
          db: db
        )
      }
    }

    /// Starts the sync engine if it is stopped.
    ///
    /// When a sync engine is started it will upload all data stored locally that has not yet
    /// been synchronized to CloudKit, and will download all changes from CloudKit since the
    /// last time it synchronized.
    ///
    /// > Note: By default, sync engines start syncing when initialized.
    public func start() async throws {
      try await start().value
    }

    /// Determines if the sync engine is currently sending local changes to the CloudKit server.
    ///
    /// It is an observable value, which means if it is accessed in a SwiftUI view, or some other
    /// observable context, then the view will automatically re-render when the value changes. As
    /// such, it can be useful for displaying a progress view to indicate that work is currently
    /// being done to synchronize changes.
    public var isSendingChanges: Bool {
      sendingChangesCount > 0
    }

    /// Determines if the sync engine is currently processing changes being sent to the device
    /// from CloudKit.
    ///
    /// It is an observable value, which means if it is accessed in a SwiftUI view, or some other
    /// observable context, then the view will automatically re-render when the value changes. As
    /// such, it can be useful for displaying a progress view to indicate that work is currently
    /// being done to synchronize changes.
    public var isFetchingChanges: Bool {
      fetchingChangesCount > 0
    }

    /// Determines if the sync engine is currently sending or receiving changes from CloudKit.
    ///
    /// This value is true if either of ``isSendingChanges`` or ``isFetchingChanges`` is true.
    /// It is an observable value, which means if it is accessed in a SwiftUI view, or some other
    /// observable context, then the view will automatically re-render when the value changes. As
    /// such, it can be useful for displaying a progress view to indicate that work is currently
    /// being done to synchronize changes.
    public var isSynchronizing: Bool {
      isSendingChanges || isFetchingChanges
    }

    /// Stops the sync engine if it is running.
    ///
    /// All edits made after stopping the sync engine will not be synchronized to CloudKit.
    /// You must start the sync engine again using ``start()`` to synchronize the changes.
    public func stop() {
      guard isRunning else { return }
      #if DEBUG && canImport(DeveloperToolsSupport)
        previewTimerTask.withValue {
          $0?.cancel()
          $0 = nil
        }
      #endif
      let stoppedEngines = observationRegistrar.withMutation(of: self, keyPath: \.isRunning) {
        syncEngines.withValue {
          let stoppedEngines = [$0.private, $0.shared].compactMap { $0 }
          $0 = SyncEngines()
          // The next engine starts from the saved token and fetches anything
          // queued here again; nothing deferred by this one may outlive it.
          deferredState.withValue { $0.reset() }
          return stoppedEngines
        }
      }
      // Discarding the engines does not end their work: a fetch already in
      // flight keeps its caller waiting, and keeps delivering events, until
      // CloudKit answers, which may be never. Cancel it. CKSyncEngine cancels
      // asynchronously and the next start builds new engines, so nothing
      // waits for this.
      Task { [stoppedEngines] in
        for engine in stoppedEngines {
          await engine.cancelOperations()
        }
      }
    }

    /// Determines if the sync engine is currently running or not.
    public var isRunning: Bool {
      observationRegistrar.access(self, keyPath: \.isRunning)
      return syncEngines.withValue {
        $0.isRunning
      }
    }

    /// The number of private-database fetches that finished with every
    /// fetched change applied to the local database.
    ///
    /// Counts the running private engine's `didFetchChanges` events, but
    /// only when no batch of the scope is still waiting to be applied (one a
    /// database suspension interrupted; see `DeferredStatePersistence`).
    /// Events from engines that ``stop()`` discarded never count. The count
    /// only grows: read it before an operation and again after it to learn
    /// whether a complete fetch finished in between. It is not observable.
    public var completedPrivateFetchCount: Int {
      completedPrivateFetches.value
    }

    private func start() throws -> Task<Void, Never> {
      guard !isRunning else { return Task {} }
      observationRegistrar.withMutation(of: self, keyPath: \.isRunning) {
        syncEngines.withValue {
          let (privateSyncEngine, sharedSyncEngine) = defaultSyncEngines(metadatabase, self)
          $0 = SyncEngines(
            private: privateSyncEngine,
            shared: sharedSyncEngine
          )
          deferredState.withValue { $0.reset() }
        }
      }

      let previousRecordTypes = try metadatabase.read { db in
        try RecordType.all.fetchAll(db)
      }
      let currentRecordTypes = try userDatabase.read { db in
        let namesAndSchemas =
          try SQLiteSchema
          .where {
            $0.type.eq(#bind(.table))
              && $0.tableName.in(tables.map { $0.base.tableName })
          }
          .fetchAll(db)
        return try namesAndSchemas.compactMap { schema -> RecordType? in
          guard let sql = schema.sql, let table = tablesByName[schema.name]
          else { return nil }
          func open<T>(_: some SynchronizableTable<T>) throws -> RecordType {
            try RecordType(
              tableName: schema.name,
              schema: sql,
              tableInfo: Set(
                PragmaTableInfo<T>
                  .select {
                    TableInfo.Columns(
                      defaultValue: $0.defaultValue,
                      isPrimaryKey: $0.isPrimaryKey,
                      name: $0.name,
                      isNotNull: $0.isNotNull,
                      type: $0.type
                    )
                  }
                  .fetchAll(db)
              )
            )
          }
          return try open(table)
        }
      }
      let previousRecordTypeByTableName = Dictionary(
        uniqueKeysWithValues: previousRecordTypes.map {
          ($0.tableName, $0)
        }
      )
      let currentRecordTypeByTableName = Dictionary(
        uniqueKeysWithValues: currentRecordTypes.map {
          ($0.tableName, $0)
        }
      )

      #if DEBUG && canImport(DeveloperToolsSupport)
        @Dependency(\.context) var context
        @Dependency(\.continuousClock) var clock
        if context == .preview {
          previewTimerTask.withValue {
            $0?.cancel()
            $0 = Task { @Sendable [weak self] in
              await withErrorReporting {
                while true {
                  guard let self else { break }
                  try await clock.sleep(for: .seconds(1))
                  try await self.syncChanges()
                }
              }
            }
          }
        }
      #endif
      let startTask = Task<Void, Never> {
        await self.withActivity("start") {
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            guard try await container.accountStatus() == .available
            else { return }
            syncEngines.withValue {
              $0.private?.state.add(pendingDatabaseChanges: [.saveZone(defaultZone)])
            }
            try await uploadRecordsToCloudKit(
              previousRecordTypeByTableName: previousRecordTypeByTableName,
              currentRecordTypeByTableName: currentRecordTypeByTableName
            )
            try await updateLocalFromSchemaChange(
              previousRecordTypeByTableName: previousRecordTypeByTableName,
              currentRecordTypeByTableName: currentRecordTypeByTableName
            )
            try await cacheUserTables(recordTypes: currentRecordTypes)
            // Reconcile rows whose pending upload was lost: metadata exists but
            // no server record was ever stamped and no pending change survives
            // (e.g. the process died before the not-running trigger path's
            // fire-and-forget persistence Task ran, or before live engine state
            // was serialized). Previously this ran only on account sign-IN, so
            // such rows strand forever on normal launches. Touching them here
            // re-fires the metadata triggers into the now-running engine; a
            // no-op when nothing is stranded, and legitimately-pending rows
            // coalesce by record ID.
            try await enqueueUnknownRecordsForCloudKit()
          }
        }
      }
      self.startTask.withValue {
        $0?.cancel()
        $0 = startTask
      }
      return startTask
    }

    /// Fetches pending remote changes from the server.
    ///
    /// Use this method to ensure the sync engine immediately fetches all pending remote changes
    /// before your app continues. This isn't necessary in normal use, as the engine automatically
    /// syncs your app's records. It is useful, however, in scenarios where you require more control
    /// over sync, such as pull-to-refresh.
    ///
    /// - Parameter options: The options to use when fetching changes.
    public func fetchChanges(
      _ options: CKSyncEngine.FetchChangesOptions = CKSyncEngine.FetchChangesOptions()
    ) async throws {
      await startTask.withValue(\.self)?.value
      let (privateSyncEngine, sharedSyncEngine) = syncEngines.withValue {
        ($0.private, $0.shared)
      }
      guard let privateSyncEngine, let sharedSyncEngine
      else { return }
      async let `private`: Void = privateSyncEngine.fetchChanges(options)
      async let shared: Void = sharedSyncEngine.fetchChanges(options)
      _ = try await (`private`, shared)
    }

    /// Sends pending local changes to the server.
    ///
    /// Use this method to ensure the sync engine sends all pending local changes to the server
    /// before your app continues. This isn't necessary in normal use, as the engine automatically
    /// syncs your app's records. It is useful, however, in scenarios where you require greater
    /// control over sync, such as a "Backup now" button.
    ///
    /// - Parameter options: The options to use when sending changes.
    public func sendChanges(
      _ options: CKSyncEngine.SendChangesOptions = CKSyncEngine.SendChangesOptions()
    ) async throws {
      await startTask.withValue(\.self)?.value
      let (privateSyncEngine, sharedSyncEngine) = syncEngines.withValue {
        ($0.private, $0.shared)
      }
      guard let privateSyncEngine, let sharedSyncEngine
      else { return }
      async let `private`: Void = privateSyncEngine.sendChanges(options)
      async let shared: Void = sharedSyncEngine.sendChanges(options)
      _ = try await (`private`, shared)
    }

    /// Synchronizes local and remote pending changes.
    ///
    /// Use this method to ensure the sync engine immediately fetches all pending remote changes
    /// _and_ sends all pending local changes to the server. This isn't necessary in normal use,
    /// as the engine automatically syncs your app's records. It is useful, however, in scenarios
    /// where you require greater control over sync.
    ///
    /// - Parameters:
    ///   - fetchOptions: The options to use when fetching changes.
    ///   - sendOptions: The options to use when sending changes.
    public func syncChanges(
      fetchOptions: CKSyncEngine.FetchChangesOptions = CKSyncEngine.FetchChangesOptions(),
      sendOptions: CKSyncEngine.SendChangesOptions = CKSyncEngine.SendChangesOptions()
    ) async throws {
      try await sendChanges(sendOptions)
      try await fetchChanges(fetchOptions)
    }

    private func cacheUserTables(recordTypes: [RecordType]) async throws {
      try await userDatabase.write { db in
        try RecordType
          .upsert { recordTypes.map { RecordType.Draft($0) } }
          .execute(db)
      }
    }

    private func uploadRecordsToCloudKit(
      previousRecordTypeByTableName: [String: RecordType],
      currentRecordTypeByTableName: [String: RecordType]
    ) async throws {
      try await enqueueLocallyPendingChanges()
      try await userDatabase.write { db in
        let newTableNames = currentRecordTypeByTableName.keys.filter { tableName in
          previousRecordTypeByTableName[tableName] == nil
        }

        try $_isSynchronizingChanges.withValue(false) {
          for tableName in newTableNames {
            try self.uploadRecordsToCloudKit(tableName: tableName, db: db)
          }
        }
      }
    }

    private func enqueueLocallyPendingChanges() async throws {
      // Read by rowid, enqueue into engine state, and only then delete the
      // rows that were read — in that order. The previous shape (read, then a
      // blanket 'PendingRecordZoneChange.delete()' in a later transaction)
      // could wipe a change the not-running trigger path persisted between
      // the two, losing it forever. With a scoped delete every interleaving
      // is safe: a change inserted concurrently is not in the read set, so it
      // survives for the next start; a crash after the enqueue but before the
      // delete merely re-enqueues the same change next start (the engine
      // coalesces by record ID).
      let pendingRows: [(rowID: Int64, change: CKSyncEngine.PendingRecordZoneChange)] =
        try await metadatabase.read { db in
          try Row.fetchAll(
            db,
            sql: """
              SELECT "rowid", "pendingRecordZoneChange"
              FROM "\(String.sqliteDataCloudKitSchemaName)_pendingRecordZoneChanges"
              """
          )
          .compactMap { row in
            guard
              let data = row["pendingRecordZoneChange"] as Data?,
              let representation = CKSyncEngine.PendingRecordZoneChange.DataRepresentation(
                queryBinding: .blob([UInt8](data))
              )
            else { return nil }
            return (row["rowid"] as Int64, representation.queryOutput)
          }
        }
      guard !pendingRows.isEmpty else { return }
      let changesByIsPrivate = Dictionary(grouping: pendingRows.map(\.change)) {
        switch $0 {
        case .deleteRecord(let recordID), .saveRecord(let recordID):
          recordID.zoneID.ownerName == CKCurrentUserDefaultName
        @unknown default:
          false
        }
      }
      syncEngines.withValue {
        $0.private?.state.add(pendingRecordZoneChanges: changesByIsPrivate[true] ?? [])
        $0.shared?.state.add(pendingRecordZoneChanges: changesByIsPrivate[false] ?? [])
      }
      let consumedRowIDs = pendingRows.map(\.rowID)
      try await userDatabase.write { db in
        try db.execute(
          sql: """
            DELETE FROM "\(String.sqliteDataCloudKitSchemaName)_pendingRecordZoneChanges"
            WHERE "rowid" IN (\(databaseQuestionMarks(count: consumedRowIDs.count)))
            """,
          arguments: StatementArguments(consumedRowIDs)
        )
      }
    }

    private func enqueueUnknownRecordsForCloudKit() async throws {
      try await userDatabase.write { db in
        try $_isSynchronizingChanges.withValue(false) {
          try SyncMetadata
            .where { !$0.hasLastKnownServerRecord }
            .update { $0.recordPrimaryKey = $0.recordPrimaryKey }
            .execute(db)
        }
      }
    }

    private func uploadRecordsToCloudKit<T>(
      table: some SynchronizableTable<T>,
      db: Database
    ) throws {
      // try T.update { $0.primaryKey = $0.primaryKey }.execute(db)
      try #sql(
        """
        UPDATE \(T.self) SET \(quote: T.primaryKey.name) = \(quote: T.primaryKey.name)
        """
      )
      .execute(db)
    }

    private func uploadRecordsToCloudKit(tableName: String, db: Database) throws {
      guard let table = self.tablesByName[tableName]
      else { return }
      func open<T>(_ table: some SynchronizableTable<T>) throws {
        try uploadRecordsToCloudKit(table: table, db: db)
      }
      try open(table)
    }

    private func updateLocalFromSchemaChange(
      previousRecordTypeByTableName: [String: RecordType],
      currentRecordTypeByTableName: [String: RecordType]
    ) async throws {
      let tablesWithChangedSchemas = currentRecordTypeByTableName.filter { tableName, recordType in
        previousRecordTypeByTableName[tableName]?.schema != recordType.schema
      }

      for (tableName, currentRecordType) in tablesWithChangedSchemas {
        guard let table = tablesByName[tableName]
        else { continue }
        func open<T>(_ table: some SynchronizableTable<T>) async throws {
          let previousRecordType = previousRecordTypeByTableName[tableName]
          let changedColumns = currentRecordType.tableInfo.subtracting(
            previousRecordType?.tableInfo ?? []
          )
          .map(\.name)
          let lastKnownServerRecords = try await metadatabase.read { db in
            try SyncMetadata
              .where { $0.recordType.eq(tableName) }
              .select(\._lastKnownServerRecordAllFields)
              .fetchAll(db)
          }
          for case .some(let lastKnownServerRecord) in lastKnownServerRecords {
            let query = try await updateQuery(
              for: table,
              record: lastKnownServerRecord,
              columnNames: T.TableColumns.writableColumns.map(\.name),
              changedColumnNames: changedColumns
            )
            try await userDatabase.write { db in
              try #sql(query).execute(db)
            }
          }
        }
        try await open(table)
      }
    }

    package func tearDownSyncEngine() throws {
      try userDatabase.write { db in
        for table in tables.reversed() {
          try table.base
            .dropTriggers(defaultZone: defaultZone, privateTables: privateTables, db: db)
        }
        for trigger in SyncMetadata.callbackTriggers(for: self).reversed() {
          try trigger.drop().execute(db)
        }
      }
      try metadatabase.erase()
      try migrate(metadatabase: metadatabase)
    }

    /// Deletes synchronized data locally on device and restarts the sync engine.
    ///
    /// This method is called automatically by the sync engine when it detects the device's iCloud
    /// account has logged out or changed. To customize this behavior, provide a
    /// ``SyncEngineDelegate`` to the sync engine and implement
    /// ``SyncEngineDelegate/syncEngine(_:accountChanged:)``.
    ///
    /// > Important: It is only appropriate to call this method when the device's iCloud account
    /// > logs out or changes.
    public func deleteLocalData() async throws {
      stop()
      try tearDownSyncEngine()
      await withErrorReporting(.sqliteDataCloudKitFailure) {
        try await userDatabase.write { db in
          for table in tables {
            func open<T>(_: some SynchronizableTable<T>) {
              withErrorReporting(.sqliteDataCloudKitFailure) {
                try T.delete().execute(db)
              }
            }
            open(table)
          }
          try setUpSyncEngine(writableDB: db)
        }
      }
      try await start()
    }

    @DatabaseFunction(
      "sqlitedata_icloud_didUpdate",
      as: ((
        String,
        String,
        String,
        String,
        String,
        [String]?.JSONRepresentation
      ) -> Void).self
    )
    func didUpdate(
      recordName: String,
      zoneName: String,
      ownerName: String,
      oldZoneName: String,
      oldOwnerName: String,
      descendantRecordNames: [String]?
    ) {
      var oldChanges: [CKSyncEngine.PendingRecordZoneChange] = []
      var newChanges: [CKSyncEngine.PendingRecordZoneChange] = []

      let oldZoneID = CKRecordZone.ID(zoneName: oldZoneName, ownerName: oldOwnerName)
      let zoneID = CKRecordZone.ID(zoneName: zoneName, ownerName: ownerName)

      if oldZoneID != zoneID {
        oldChanges.append(.deleteRecord(CKRecord.ID(recordName: recordName, zoneID: oldZoneID)))
        for descendantRecordName in descendantRecordNames ?? [] {
          oldChanges.append(
            .deleteRecord(CKRecord.ID(recordName: descendantRecordName, zoneID: oldZoneID))
          )
        }
        newChanges.append(.saveRecord(CKRecord.ID(recordName: recordName, zoneID: zoneID)))
        for descendantRecordName in descendantRecordNames ?? [] {
          newChanges.append(
            .saveRecord(CKRecord.ID(recordName: descendantRecordName, zoneID: zoneID))
          )
        }
      } else {
        newChanges.append(
          .saveRecord(CKRecord.ID(recordName: recordName, zoneID: zoneID))
        )
      }

      guard isRunning else {
        // TODO: Perform this work in a trigger instead of a task.
        Task { [changes = oldChanges + newChanges] in
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await userDatabase.write { db in
              try PendingRecordZoneChange
                .insert {
                  for change in changes {
                    PendingRecordZoneChange(change)
                  }
                }
                .execute(db)
            }
          }
        }
        return
      }
      let oldSyncEngine = self.syncEngines.withValue {
        oldZoneID.ownerName == CKCurrentUserDefaultName ? $0.private : $0.shared
      }
      let syncEngine = self.syncEngines.withValue {
        zoneID.ownerName == CKCurrentUserDefaultName ? $0.private : $0.shared
      }
      oldSyncEngine?.state.add(pendingRecordZoneChanges: oldChanges)
      syncEngine?.state.add(pendingRecordZoneChanges: newChanges)
    }

    @DatabaseFunction(
      "sqlitedata_icloud_didDelete",
      as: ((String, CKRecord?.SystemFieldsRepresentation, CKShare?.SystemFieldsRepresentation)
        -> Void).self
    )
    func didDelete(recordName: String, record: CKRecord?, share: CKShare?) {
      let zoneID = record?.recordID.zoneID ?? defaultZone.zoneID
      var changes: [CKSyncEngine.PendingRecordZoneChange] = [
        .deleteRecord(
          CKRecord.ID(
            recordName: recordName,
            zoneID: zoneID
          )
        )
      ]
      if let share {
        changes.append(.deleteRecord(share.recordID))
      }
      guard isRunning else {
        Task { [changes] in
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await userDatabase.write { db in
              try PendingRecordZoneChange
                .insert { changes.map { PendingRecordZoneChange($0) } }
                .execute(db)
            }
          }
        }
        return
      }

      let syncEngine = self.syncEngines.withValue {
        zoneID.ownerName == CKCurrentUserDefaultName ? $0.private : $0.shared
      }
      syncEngine?.state.add(pendingRecordZoneChanges: changes)
    }

    package func acceptShare(metadata: ShareMetadata) async throws {
      guard let rootRecordID = metadata.hierarchicalRootRecordID
      else {
        reportIssue("Attempting to share without root record information.")
        return
      }
      let container = type(of: container).createContainer(identifier: metadata.containerIdentifier)
      _ = try await container.accept(metadata)
      try await syncEngines.shared?.fetchChanges(
        CKSyncEngine.FetchChangesOptions(
          scope: .zoneIDs([rootRecordID.zoneID]),
          operationGroup: nil
        )
      )
    }

    /// Whether or not the ``SyncEngine`` is currently writing changes to the database.
    ///
    /// See <doc:CloudKitSync#Updating-triggers-to-be-compatible-with-synchronization> for more info.
    @DatabaseFunction("sqlitedata_icloud_syncEngineIsSynchronizingChanges")
    public static var isSynchronizing: Bool {
      if _isCreatingTemporaryTrigger {
        reportIssue(
          """
          Invoked 'SyncEngine.isSynchronizing' at trigger creation, which is unexpected. Use \
          'SyncEngine.$isSynchronizing' to invoke at trigger execution, instead.
          """
        )
      }
      return _isSynchronizingChanges
    }

    @available(*, deprecated, message: "Use 'SyncEngine.$isSynchronizing', instead.")
    public static func isSynchronizingChanges() -> some QueryExpression<Bool> {
      $isSynchronizing
    }

    private var sendingChangesCount: Int {
      get {
        observationRegistrar.access(self, keyPath: \.isSendingChanges)
        return activityCounts.withValue(\.sendingChangesCount)
      }
      set {
        observationRegistrar.withMutation(of: self, keyPath: \.isSendingChanges) {
          activityCounts.withValue { $0.sendingChangesCount = newValue }
        }
      }
    }
    private var fetchingChangesCount: Int {
      get {
        observationRegistrar.access(self, keyPath: \.isFetchingChanges)
        return activityCounts.withValue(\.fetchingChangesCount)
      }
      set {
        observationRegistrar.withMutation(of: self, keyPath: \.isFetchingChanges) {
          activityCounts.withValue { $0.fetchingChangesCount = newValue }
        }
      }
    }
  }

  extension PrimaryKeyedTable {
    @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
    fileprivate static func createTriggers(
      foreignKeysByTableName: [String: [ForeignKey]],
      tablesByName: [String: any SynchronizableTable],
      defaultZone: CKRecordZone,
      privateTables: [any SynchronizableTable],
      db: Database
    ) throws {
      let parentForeignKey =
        foreignKeysByTableName[tableName]?.count == 1
        ? foreignKeysByTableName[tableName]?.first
        : nil

      for trigger in metadataTriggers(
        parentForeignKey: parentForeignKey,
        defaultZone: defaultZone,
        privateTables: privateTables
      ) {
        try trigger.execute(db)
      }
    }

    @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
    fileprivate static func dropTriggers(
      defaultZone: CKRecordZone,
      privateTables: [any SynchronizableTable],
      db: Database
    ) throws {
      for trigger in metadataTriggers(
        parentForeignKey: nil,
        defaultZone: defaultZone,
        privateTables: privateTables
      )
      .reversed() {
        try trigger.drop().execute(db)
      }
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension SyncEngine: CKSyncEngineDelegate {
    public func handleEvent(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
      guard let event = Event(event)
      else {
        reportIssue("Unrecognized event received: \(event)")
        return
      }
      await handleEvent(event, syncEngine: syncEngine)
    }

    package func handleEvent(_ event: Event, syncEngine: any SyncEngineProtocol) async {
      await withActivity("handleEvent.\(event.description)") {
        await self.handleEventDeferringInterruptedWork(event, syncEngine: syncEngine)
      }
    }

    /// Whether `syncEngine` is the running engine for its scope.
    private func isCurrentEngine(_ syncEngine: any SyncEngineProtocol) -> Bool {
      syncEngines.withValue { engines in
        guard engines.isRunning else { return false }
        switch syncEngine.database.databaseScope {
        case .private:
          return engines.private === syncEngine
        case .shared:
          return engines.shared === syncEngine
        default:
          return false
        }
      }
    }

    /// Only an engine whose user database obeys GRDB's suspension
    /// notifications tracks them. Any other pool keeps writing through a
    /// posted suspension, so there is nothing to retry.
    private var tracksSuspension: Bool {
      userDatabase.configuration.observesSuspensionNotifications
    }

    /// Runs `body` on the deferred state, but only while `syncEngine` is one of
    /// the running engines: a delivery still in flight from an engine that
    /// `stop()` discarded must not queue events for, or hold or release state
    /// into, the engines that replaced it. Returns nil for such an engine.
    private typealias Deferred =
      DeferredStatePersistence<DeferredEvent, CKSyncEngine.State.Serialization>

    private func withDeferredState<R: Sendable>(
      of syncEngine: any SyncEngineProtocol,
      _ body: @Sendable (inout Deferred) -> R
    ) -> R? {
      syncEngines.withValue { _ in
        // Re-entrant: `syncEngines`' lock is recursive.
        guard isCurrentEngine(syncEngine) else { return nil }
        return deferredState.withValue { body(&$0) }
      }
    }

    private func handleEventDeferringInterruptedWork(
      _ event: Event,
      syncEngine: any SyncEngineProtocol
    ) async {
      guard tracksSuspension, withDeferredState(of: syncEngine, { _ in () }) != nil else {
        // `handleStateUpdate` drops a stopped engine's state either way.
        await handleEventNow(event, syncEngine: syncEngine)
        countCompletedFetchIfApplied(event, syncEngine: syncEngine)
        return
      }
      let scope = syncEngine.database.databaseScope
      // CKSyncEngine delivers one scope's events serially, so retrying
      // here, before the event itself, keeps the batches in order.
      await retryUnappliedEvents(scope: scope, syncEngine: syncEngine)
      switch event {
      case .stateUpdate(let stateSerialization):
        let held = withDeferredState(of: syncEngine) {
          $0.holdIfBlocked(stateSerialization, scope: scope)
        }
        if held == true {
          logger.log("sqlitedata-diag: holding back sync state behind unapplied changes")
          return
        }
        // This state is newer than any held one, which must not be written
        // over it afterwards.
        _ = withDeferredState(of: syncEngine) { $0.takeHeldStateIfUnblocked(scope: scope) }
        await handleEventNow(event, syncEngine: syncEngine)
        return
      case .fetchedDatabaseChanges, .fetchedRecordZoneChanges, .sentRecordZoneChanges:
        if withDeferredState(of: syncEngine, { $0.isBlocked(scope: scope) }) == true {
          // An older event of this scope is still unapplied (the retry above
          // failed again). Applying this one first could reorder the
          // batches, e.g. resurrect a record a later batch deletes.
          deferUnapplied([DeferredEvent(event: event)], scope: scope, syncEngine: syncEngine)
        } else {
          await applyDetectingSuspension(
            DeferredEvent(event: event),
            scope: scope,
            syncEngine: syncEngine
          )
        }
      default:
        await handleEventNow(event, syncEngine: syncEngine)
      }
      if let held = withDeferredState(of: syncEngine, { $0.takeHeldStateIfUnblocked(scope: scope) })
        ?? nil
      {
        await handleStateUpdate(stateSerialization: held, syncEngine: syncEngine)
      }
      countCompletedFetchIfApplied(event, syncEngine: syncEngine)
    }

    /// Counts a private `didFetchChanges` from the running engine when the
    /// scope has nothing left to apply (see `completedPrivateFetchCount`).
    /// Its unapplied events were retried before this event was handled, so a
    /// scope still blocked here means part of the fetch never landed.
    private func countCompletedFetchIfApplied(_ event: Event, syncEngine: any SyncEngineProtocol) {
      guard case .didFetchChanges = event, syncEngine.database.databaseScope == .private
      else { return }
      let isApplied = withDeferredState(of: syncEngine) { !$0.isBlocked(scope: .private) }
      guard isApplied == true else { return }
      completedPrivateFetches.withValue { $0 += 1 }
    }

    private func retryUnappliedEvents(
      scope: CKDatabase.Scope,
      syncEngine: any SyncEngineProtocol
    ) async {
      // The scope's queue is empty while these retry. That is safe only
      // because CKSyncEngine delivers one engine's events serially: no other
      // event of this scope can run (and find the queue empty) meanwhile.
      let events = withDeferredState(of: syncEngine) { $0.takeUnapplied(scope: scope) } ?? []
      guard !events.isEmpty else { return }
      logger.log("sqlitedata-diag: retrying \(events.count) suspension-interrupted events")
      for (index, event) in events.enumerated() {
        let isDone = await applyDetectingSuspension(event, scope: scope, syncEngine: syncEngine)
        guard isDone else {
          // Keep the rest queued, in order, behind the one that failed again.
          deferUnapplied(Array(events[(index + 1)...]), scope: scope, syncEngine: syncEngine)
          return
        }
      }
    }

    private func deferUnapplied(
      _ events: [DeferredEvent],
      scope: CKDatabase.Scope,
      syncEngine: any SyncEngineProtocol
    ) {
      guard !events.isEmpty else { return }
      _ = withDeferredState(of: syncEngine) { deferred in
        for event in events {
          deferred.recordUnapplied(event, scope: scope)
        }
      }
    }

    /// Every write in the handlers is wrapped in `withErrorReporting`, which
    /// reports a failure and moves on; the handlers do not return it. So this
    /// infers a suspension failure from two signals, either of which marks
    /// the event unapplied:
    /// - the suspension state moved (or was suspended) across the event;
    /// - one of its writes reported a suspension-shaped GRDB error (see
    ///   `DatabaseError.isSuspensionShaped`). This one does not depend on the
    ///   order in which NotificationCenter calls GRDB's observers and ours.
    ///   The recorder is added to the current reporters, so the host's
    ///   reporters still get every issue.
    ///
    /// Retrying an event that did apply is harmless (upserts are idempotent;
    /// the relaunch path re-delivers batches the same way).
    ///
    /// The error signal alone can also fire for a cause that is not a
    /// suspension and recurs on every attempt (a host `interrupt()`, or an app
    /// function raising the same error). Deferring such an event forever
    /// would block its scope for good: later events queue behind it and its
    /// state is never saved, on every launch. So after
    /// `maxAttemptsWithoutSuspension` attempts in a row that saw the error
    /// but no suspension activity, the event is given up on and treated as
    /// applied: its error was already reported, which is what upstream does
    /// with every failure. An attempt that does overlap a suspension resets
    /// the count, because a real suspension is transient.
    ///
    /// Returns whether the event is done with (applied, or given up on);
    /// false when it was queued again.
    @discardableResult
    private func applyDetectingSuspension(
      _ deferred: DeferredEvent,
      scope: CKDatabase.Scope,
      syncEngine: any SyncEngineProtocol
    ) async -> Bool {
      let event = deferred.event
      let interruptions = InterruptionRecorder()
      let before = suspension.value
      await withIssueReporters(IssueReporters.current + [interruptions]) {
        await handleEventNow(event, syncEngine: syncEngine)
      }
      let sawSuspension = suspension.value.overlapsSuspension(since: before)
      guard sawSuspension || interruptions.sawInterruption else { return true }
      var deferred = deferred
      deferred.attemptsWithoutSuspension =
        sawSuspension ? 0 : deferred.attemptsWithoutSuspension + 1
      if deferred.attemptsWithoutSuspension >= Self.maxAttemptsWithoutSuspension {
        logger.log(
          """
          sqlitedata-diag: gave up on \(event.description, privacy: .public) after \
          \(deferred.attemptsWithoutSuspension) interrupted attempts with no suspension; \
          treating it as applied
          """
        )
        return true
      }
      deferUnapplied([deferred], scope: scope, syncEngine: syncEngine)
      let reason =
        sawSuspension ? "overlapped a suspension" : "suspension-shaped error, no suspension seen"
      logger.log(
        """
        sqlitedata-diag: \(event.description, privacy: .public) deferred \
        (\(reason, privacy: .public)); attempts without suspension \
        \(deferred.attemptsWithoutSuspension)/\(Self.maxAttemptsWithoutSuspension)
        """
      )
      return false
    }

    private func databaseDidResume() {
      suspension.withValue { $0.didResume() }
      guard deferredState.withValue(\.hasAnythingDeferred) else { return }
      // One retry at a time. The retry's own activity can resume the database
      // (the host's hold does, from the background), which lands here again;
      // starting another fetch from that would chain fetches for as long as
      // the queue cannot drain. A genuine resume that arrives while a retry
      // runs is dropped too; that is safe, because the queue also drains on
      // the scope's next event.
      let isAlreadyRetrying = isRetryingAfterResume.withValue {
        defer { $0 = true }
        return $0
      }
      guard !isAlreadyRetrying else { return }
      // Retry inside CKSyncEngine's own serial event delivery: a fetch
      // brackets itself with will/didFetchChanges events, and every event
      // retries its scope's unapplied events first.
      Task { [weak self] in
        guard let self else { return }
        defer { self.isRetryingAfterResume.setValue(false) }
        await self.withActivity("retryAfterResume") {
          try? await self.fetchChanges()
        }
      }
    }

    private func handleEventNow(_ event: Event, syncEngine: any SyncEngineProtocol) async {
      #if DEBUG
        logger.log(event, syncEngine: syncEngine)
      #endif

      switch event {
      case .accountChange(let changeType):
        await handleAccountChange(changeType: changeType, syncEngine: syncEngine)
      case .stateUpdate(let stateSerialization):
        await handleStateUpdate(stateSerialization: stateSerialization, syncEngine: syncEngine)
      case .fetchedDatabaseChanges(let modifications, let deletions):
        await handleFetchedDatabaseChanges(
          modifications: modifications,
          deletions: deletions,
          syncEngine: syncEngine
        )
      case .sentDatabaseChanges:
        break
      case .fetchedRecordZoneChanges(let modifications, let deletions):
        await handleFetchedRecordZoneChanges(
          modifications: modifications,
          deletions: deletions,
          syncEngine: syncEngine
        )
      case .sentRecordZoneChanges(
        let savedRecords,
        let failedRecordSaves,
        let deletedRecordIDs,
        let failedRecordDeletes
      ):
        await handleSentRecordZoneChanges(
          savedRecords: savedRecords,
          failedRecordSaves: failedRecordSaves,
          deletedRecordIDs: deletedRecordIDs,
          failedRecordDeletes: failedRecordDeletes,
          syncEngine: syncEngine
        )

      case .willFetchRecordZoneChanges:
        await MainActor.run {
          fetchingChangesCount += 1
        }
      case .didFetchRecordZoneChanges:
        await MainActor.run {
          fetchingChangesCount -= 1
        }

      case .willFetchChanges:
        await MainActor.run {
          fetchingChangesCount += 1
        }
      case .didFetchChanges:
        await MainActor.run {
          fetchingChangesCount -= 1
        }

      case .willSendChanges:
        await MainActor.run {
          sendingChangesCount += 1
        }
      case .didSendChanges:
        await MainActor.run {
          sendingChangesCount -= 1
        }

      @unknown default:
        break
      }
    }

    public func nextRecordZoneChangeBatch(
      _ context: CKSyncEngine.SendChangesContext,
      syncEngine: CKSyncEngine
    ) async -> CKSyncEngine.RecordZoneChangeBatch? {
      await nextRecordZoneChangeBatch(
        reason: context.reason,
        options: context.options,
        syncEngine: syncEngine
      )
    }

    package func nextRecordZoneChangeBatch(
      reason: CKSyncEngine.SyncReason = .scheduled,
      options: CKSyncEngine.SendChangesOptions = CKSyncEngine.SendChangesOptions(scope: .all),
      syncEngine: any SyncEngineProtocol
    ) async -> CKSyncEngine.RecordZoneChangeBatch? {
      let batch = await withActivity("nextRecordZoneChangeBatch") {
        UncheckedSendable(
          await self.nextRecordZoneChangeBatchNow(
            reason: reason, options: options, syncEngine: syncEngine
          )
        )
      }
      return batch.value
    }

    private func nextRecordZoneChangeBatchNow(
      reason: CKSyncEngine.SyncReason,
      options: CKSyncEngine.SendChangesOptions,
      syncEngine: any SyncEngineProtocol
    ) async -> CKSyncEngine.RecordZoneChangeBatch? {
      var changes = await pendingRecordZoneChanges(options: options, syncEngine: syncEngine)
      guard !changes.isEmpty
      else { return nil }

      changes.sort { lhs, rhs in
        switch (lhs, rhs) {
        case (.saveRecord(let lhs), .saveRecord(let rhs)):
          guard
            let lhsRecordType = lhs.tableName,
            let lhsIndex = tablesByOrder[lhsRecordType],
            let rhsRecordType = rhs.tableName,
            let rhsIndex = tablesByOrder[rhsRecordType]
          else { return true }
          return lhsIndex < rhsIndex
        case (.deleteRecord(let lhs), .deleteRecord(let rhs)):
          guard
            let lhsRecordType = lhs.tableName,
            let lhsIndex = tablesByOrder[lhsRecordType],
            let rhsRecordType = rhs.tableName,
            let rhsIndex = tablesByOrder[rhsRecordType]
          else { return true }
          return lhsIndex > rhsIndex
        case (.saveRecord, .deleteRecord):
          return false
        case (.deleteRecord, .saveRecord):
          return true
        default:
          return true
        }
      }

      #if DEBUG
        let state = LockIsolated(NextRecordZoneChangeBatchLoggingState())
        defer {
          let state = state.withValue(\.self)
          if let tabularDescription = state.tabularDescription {
            logger.debug(
              """
              SQLiteData (\(syncEngine.database.databaseScope.label).db) \
              nextRecordZoneChangeBatch: \(reason)
                \(tabularDescription)
              """
            )
          }
        }
      #endif

      let batch = await syncEngine.recordZoneChangeBatch(pendingChanges: changes) { recordID in
        guard
          let (metadata, allFields) = await withErrorReporting(
            .sqliteDataCloudKitFailure,
            catching: {
              try await metadatabase.read { db in
                try SyncMetadata
                  .find(recordID)
                  .select { ($0, $0._lastKnownServerRecordAllFields) }
                  .fetchOne(db)
              }
            }
          )
            ?? nil
        else {
          syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
          return nil
        }

        var missingTable: CKRecord.ID?
        var missingRecord: CKRecord.ID?
        var sentRecord: CKRecord.ID?
        #if DEBUG
          defer {
            state.withValue { [missingTable, missingRecord, sentRecord] in
              if let missingTable {
                $0.events.append("⚠️ Missing table")
                $0.recordTypes.append(metadata.recordType)
                $0.recordNames.append(missingTable.recordName)
              }
              if let missingRecord {
                $0.events.append("⚠️ Missing record")
                $0.recordTypes.append(metadata.recordType)
                $0.recordNames.append(missingRecord.recordName)
              }
              if let sentRecord {
                $0.events.append("➡️ Sending")
                $0.recordTypes.append(metadata.recordType)
                $0.recordNames.append(sentRecord.recordName)
              }
            }
          }
        #endif

        guard let table = tablesByName[metadata.recordType]
        else {
          syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
          missingTable = recordID
          return nil
        }
        func open<T>(_: some SynchronizableTable<T>) async -> CKRecord? {
          let row =
            await withErrorReporting(.sqliteDataCloudKitFailure) {
              // NB: Fake 'sending' result.
              nonisolated(unsafe) var result: T.QueryOutput?
              try await userDatabase.read { db in
                result =
                  try T
                  .unscoped
                  .where {
                    #sql("\($0.primaryKey) = \(bind: metadata.recordPrimaryKey)")
                  }
                  .fetchOne(db)
              }
              return result
            }
            ?? nil
          guard let row
          else {
            syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
            missingRecord = recordID
            return nil
          }

          let record =
            allFields
            ?? CKRecord(
              recordType: metadata.recordType,
              recordID: recordID
            )
          if let parentRecordName = metadata.parentRecordName,
            !privateTables.contains(where: { $0.base.tableName == metadata.recordType })
          {
            record.parent = CKRecord.Reference(
              recordID: CKRecord.ID(
                recordName: parentRecordName,
                zoneID: recordID.zoneID
              ),
              action: .none
            )
          } else {
            record.parent = nil
          }

          record.update(
            with: T(queryOutput: row),
            userModificationTime: metadata.userModificationTime
          )
          await refreshLastKnownServerRecord(record)
          sentRecord = recordID
          return record
        }
        return await open(table)
      }
      return batch
    }

    private func pendingRecordZoneChanges(
      options: CKSyncEngine.SendChangesOptions,
      syncEngine: any SyncEngineProtocol
    ) async -> [CKSyncEngine.PendingRecordZoneChange] {
      var changes = syncEngine.state.pendingRecordZoneChanges.filter(options.scope.contains)
      guard !changes.isEmpty
      else { return [] }

      let deletedRecordIDs: [CKRecord.ID] = changes.compactMap {
        switch $0 {
        case .saveRecord(_):
          return nil
        case .deleteRecord(let recordID):
          return recordID
        @unknown default:
          return nil
        }
      }

      if syncEngine.database.databaseScope == .shared {
        let (sharesToDelete, recordsWithRoot):
          ([CKShare?], [(lastKnownServerRecord: CKRecord?, rootLastKnownServerRecord: CKRecord?)]) =
            await withErrorReporting(.sqliteDataCloudKitFailure) {
              guard !deletedRecordIDs.isEmpty
              else { return ([], []) }

              return try await metadatabase.read { db in
                let sharesToDelete =
                  try SyncMetadata
                  .findAll(deletedRecordIDs)
                  .where(\.isShared)
                  .select(\.share)
                  .fetchAll(db)

                let recordsWithRoot =
                  try With {
                    SyncMetadata
                      .findAll(deletedRecordIDs)
                      .where { $0.parentRecordName.is(nil) }
                      .select {
                        RecordWithRoot.Columns(
                          parentRecordName: $0.parentRecordName,
                          recordName: $0.recordName,
                          lastKnownServerRecord: $0.lastKnownServerRecord,
                          rootRecordName: $0.recordName,
                          rootLastKnownServerRecord: $0.lastKnownServerRecord
                        )
                      }
                      .union(
                        all: true,
                        SyncMetadata
                          .join(RecordWithRoot.all) { $1.recordName.is($0.parentRecordName) }
                          .select { metadata, tree in
                            RecordWithRoot.Columns(
                              parentRecordName: metadata.parentRecordName,
                              recordName: metadata.recordName,
                              lastKnownServerRecord: metadata.lastKnownServerRecord,
                              rootRecordName: tree.rootRecordName,
                              rootLastKnownServerRecord: tree.rootLastKnownServerRecord
                            )
                          }
                      )
                  } query: {
                    RecordWithRoot
                      .select { ($0.lastKnownServerRecord, $0.rootLastKnownServerRecord) }
                  }
                  .fetchAll(db)

                return (sharesToDelete, recordsWithRoot)
              }
            }
            ?? ([], [])

        let shareRecordIDsToDelete = sharesToDelete.compactMap(\.?.recordID)

        for recordWithRoot in recordsWithRoot {
          guard
            let lastKnownServerRecord = recordWithRoot.lastKnownServerRecord,
            let rootLastKnownServerRecord = recordWithRoot.rootLastKnownServerRecord
          else { continue }
          guard let rootShareRecordID = rootLastKnownServerRecord.share?.recordID
          else { continue }
          guard shareRecordIDsToDelete.contains(rootShareRecordID)
          else { continue }
          changes.removeAll(where: { $0 == .deleteRecord(lastKnownServerRecord.recordID) })
          syncEngine.state.remove(
            pendingRecordZoneChanges: [.deleteRecord(lastKnownServerRecord.recordID)]
          )
        }
      }

      await withErrorReporting(.sqliteDataCloudKitFailure) {
        guard !deletedRecordIDs.isEmpty
        else { return }
        try await userDatabase.write { db in
          try SyncMetadata
            .findAll(deletedRecordIDs)
            .delete()
            .execute(db)
        }
      }

      return changes
    }

    package func handleAccountChange(
      changeType: CKSyncEngine.Event.AccountChange.ChangeType,
      syncEngine: any SyncEngineProtocol
    ) async {
      guard syncEngine === syncEngines.private
      else { return }

      switch changeType {
      case .signIn:
        syncEngine.state.add(pendingDatabaseChanges: [.saveZone(defaultZone)])
        await withErrorReporting {
          try await enqueueUnknownRecordsForCloudKit()
        }
        await delegate?.syncEngine(self, accountChanged: changeType)
      case .signOut, .switchAccounts:
        guard let delegate
        else {
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await deleteLocalData()
          }
          return
        }
        await delegate.syncEngine(self, accountChanged: changeType)

      @unknown default:
        break
      }
    }

    package func handleStateUpdate(
      stateSerialization: CKSyncEngine.State.Serialization,
      syncEngine: any SyncEngineProtocol
    ) async {
      await withErrorReporting(.sqliteDataCloudKitFailure) {
        try await userDatabase.write { db in
          // Checked inside the write: a state from an engine `stop()`
          // discarded (still in flight, or released from the held state)
          // must not land after it. `deleteLocalData()` stops the engine and
          // then erases the metadatabase in later writes, which queue behind
          // this one; an old token written after the erase would make the
          // next launch resume past changes whose local rows were deleted.
          guard isCurrentEngine(syncEngine) else {
            logger.log("sqlitedata-diag: dropping sync state from a stopped engine")
            return
          }
          try StateSerialization.upsert {
            StateSerialization.Draft(
              scope: syncEngine.database.databaseScope,
              data: stateSerialization
            )
          }
          .execute(db)
        }
      }
    }

    package func handleFetchedDatabaseChanges(
      modifications: [CKRecordZone.ID],
      deletions: [(zoneID: CKRecordZone.ID, reason: CKDatabase.DatabaseChange.Deletion.Reason)],
      syncEngine: any SyncEngineProtocol
    ) async {
      let defaultZoneDeleted =
        await withErrorReporting(.sqliteDataCloudKitFailure) {
          try await userDatabase.write { db in
            var defaultZoneDeleted = false
            for (zoneID, reason) in deletions {
              switch reason {
              case .deleted, .purged:
                try deleteRecords(in: zoneID, db: db)
                if zoneID == self.defaultZone.zoneID {
                  defaultZoneDeleted = true
                }
              case .encryptedDataReset:
                try uploadRecords(in: zoneID, db: db)
              @unknown default:
                reportIssue("Unknown deletion reason: \(reason)")
              }
            }
            return defaultZoneDeleted
          }
        }
        ?? false
      if defaultZoneDeleted {
        syncEngine.state.add(pendingDatabaseChanges: [.saveZone(self.defaultZone)])
      }
      @Sendable
      func deleteRecords(in zoneID: CKRecordZone.ID, db: Database) throws {
        let recordTypes = Dictionary(
          grouping:
            try SyncMetadata
            .where { $0.zoneName.eq(zoneID.zoneName) && $0.ownerName.eq(zoneID.ownerName) }
            .select { ($0.recordType, $0.recordPrimaryKey) }
            .fetchAll(db),
          by: \.0
        )
        .mapValues {
          $0.map(\.1)
        }
        for (recordType, primaryKeys) in recordTypes {
          guard let table = tablesByName[recordType]
          else { continue }
          func open<T: PrimaryKeyedTable>(_: some SynchronizableTable<T>) {
            withErrorReporting(.sqliteDataCloudKitFailure) {
              try T.unscoped.where { #sql("\($0.primaryKey)").in(primaryKeys) }.delete().execute(db)
            }
          }
          open(table)
        }
      }
      @Sendable
      func uploadRecords(in zoneID: CKRecordZone.ID, db: Database) throws {
        let recordTypes = Set(
          try SyncMetadata
            .where(\.hasLastKnownServerRecord)
            .select(\.lastKnownServerRecord)
            .fetchAll(db)
            .compactMap { $0?.recordID.zoneID == zoneID ? $0?.recordType : nil }
        )
        var pendingRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange] = []
        for recordType in recordTypes {
          guard let table = tablesByName[recordType]
          else { continue }
          func open<T>(_: some SynchronizableTable<T>) {
            withErrorReporting(.sqliteDataCloudKitFailure) {
              pendingRecordZoneChanges.append(
                contentsOf: try T.unscoped.select(\._recordName).fetchAll(db).map {
                  .saveRecord(CKRecord.ID(recordName: $0, zoneID: zoneID))
                }
              )
            }
          }
          open(table)
        }
        syncEngine.state.add(pendingRecordZoneChanges: pendingRecordZoneChanges)
      }
    }

    package func handleFetchedRecordZoneChanges(
      modifications: [CKRecord] = [],
      deletions: [(recordID: CKRecord.ID, recordType: CKRecord.RecordType)] = [],
      syncEngine: any SyncEngineProtocol
    ) async {
      let deletedRecordIDsByRecordType = OrderedDictionary(
        grouping: deletions.sorted { lhs, rhs in
          topologicallyAscending(
            lhsTableName: lhs.recordType,
            rhsTableName: rhs.recordType,
            rootFirst: false
          )
        },
        by: \.recordType
      )
      .mapValues { $0.map(\.recordID) }
      for (recordType, recordIDs) in deletedRecordIDsByRecordType {
        if let table = tablesByName[recordType] {
          func open<T>(_: some SynchronizableTable<T>) async {
            await withErrorReporting(.sqliteDataCloudKitFailure) {
              try await userDatabase.write { db in
                try T
                  .unscoped
                  .where {
                    #sql("\($0.primaryKey)").in(
                      SyncMetadata.findAll(recordIDs)
                        .select(\.recordPrimaryKey)
                    )
                  }
                  .delete()
                  .execute(db)

                try UnsyncedRecordID
                  .findAll(recordIDs)
                  .delete()
                  .execute(db)
              }
            }
          }
          await open(table)
        } else if recordType == CKRecord.SystemType.share {
          for shareRecordID in recordIDs {
            await withErrorReporting(.sqliteDataCloudKitFailure) {
              try await deleteShare(shareRecordID: shareRecordID)
            }
          }
        } else {
          // NB: Deleting a record from a table we do not currently recognize.
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await userDatabase.write { db in
              try SyncMetadata
                .findAll(recordIDs)
                .delete()
                .execute(db)
            }
          }
        }
      }

      let unsyncedRecords =
        await withErrorReporting(.sqliteDataCloudKitFailure) {
          var unsyncedRecordIDs = try await userDatabase.write { db in
            Set(
              try UnsyncedRecordID.all
                .fetchAll(db)
                .map(CKRecord.ID.init(unsyncedRecordID:))
            )
          }
          let modificationRecordIDs = Set(modifications.map(\.recordID))
          let unsyncedRecordIDsToDelete = modificationRecordIDs.intersection(unsyncedRecordIDs)
          unsyncedRecordIDs.subtract(modificationRecordIDs)
          if !unsyncedRecordIDsToDelete.isEmpty {
            try await userDatabase.write { db in
              try UnsyncedRecordID
                .findAll(unsyncedRecordIDsToDelete)
                .delete()
                .execute(db)
            }
          }
          // Parked records (FK violation: child before parent) replay from
          // the in-memory cache via the post-apply fixpoint below — their
          // content was already delivered, so no server round trip. The
          // server fetch here recovers ONLY cache misses: a relaunch
          // mid-sync lost the in-memory records, leaving their persisted
          // `UnsyncedRecordID` rows without content. Before the cache, this
          // re-fetch ran after EVERY batch over the whole growing backlog —
          // quadratic server traffic that paced a ~6k-record initial sync to
          // over an hour and drew requestRateLimited.
          var unsyncedRecords: [CKRecord] = []
          let parked = parkedServerRecords.withValue { $0 }
          var missingRecordIDs: [CKRecord.ID] = []
          for recordID in unsyncedRecordIDs where parked[recordID] == nil {
            missingRecordIDs.append(recordID)
          }
          if !missingRecordIDs.isEmpty {
            logger.log(
              """
              sqlitedata-diag: recovering \(missingRecordIDs.count) relaunch-lost \
              parked records from server
              """
            )
          }
          let batchSize = 150
          let orderedMissingRecordIDs = missingRecordIDs.sorted {
            topologicallyAscending(
              lhsTableName: $0.tableName,
              rhsTableName: $1.tableName,
              rootFirst: true
            )
          }
          for start in stride(from: 0, to: orderedMissingRecordIDs.count, by: batchSize) {
            let recordIDsBatch =
              orderedMissingRecordIDs
              .dropFirst(start)
              .prefix(batchSize)
            let results = try await syncEngine.database.records(for: Array(recordIDsBatch))
            for (recordID, result) in results {
              switch result {
              case .success(let record):
                unsyncedRecords.append(record)
              case .failure(let error as CKError) where error.code == .unknownItem:
                try await userDatabase.write { db in
                  try UnsyncedRecordID.find(recordID).delete().execute(db)
                }
              case .failure:
                continue
              }
            }
          }
          return unsyncedRecords
        }
        ?? [CKRecord]()

      let modifications = (modifications + unsyncedRecords).sorted { lhs, rhs in
        topologicallyAscending(
          lhsTableName: lhs.recordType,
          rhsTableName: rhs.recordType,
          rootFirst: true
        )
      }

      enum ShareOrReference {
        case share(CKShare)
        case reference(CKShare.Reference)
      }
      let shares: [ShareOrReference] =
        await withErrorReporting(.sqliteDataCloudKitFailure) {
          try await userDatabase.write { db in
            // Every record in a shared hierarchy carries the same share
            // reference, so a large initial fetch would otherwise fetch the
            // same CKShare once per record ('CKFetchRecordsOperation' +
            // 'shareMetadata', two server calls each) — enough to draw
            // 'serviceUnavailable'/'requestRateLimited' from the container.
            // Deduplicate references per batch, and skip shares that are
            // already cached: a share the client has never seen needs the
            // bootstrap fetch, but updates to a known share arrive as CKShare
            // records in the change feed (the '.share' case) and refresh the
            // cache without this fetch.
            let cachedShareRecordIDs = Set(
              try SyncMetadata
                .where(\.isShared)
                .select(\.share)
                .fetchAll(db)
                .compactMap { $0?.recordID }
            )
            var shares: [ShareOrReference] = []
            var fetchedShareRecordIDs: Set<CKRecord.ID> = []
            var referencesByRecordID: [CKRecord.ID: CKShare.Reference] = [:]
            var appliedCount = 0
            for record in modifications {
              if let share = record as? CKShare {
                shares.append(.share(share))
                fetchedShareRecordIDs.insert(share.recordID)
              } else {
                if upsertFromServerRecord(record, db: db) {
                  appliedCount += 1
                }
                if let shareReference = record.share {
                  referencesByRecordID[shareReference.recordID] = shareReference
                }
              }
            }
            // Replay parked records ONLY when this batch applied something —
            // an applied record is the only event that can unblock a parked
            // child, so a batch where everything parked (the common shape
            // while the FK tree's root records haven't arrived yet) skips
            // the replay entirely. When there IS progress, replay to
            // fixpoint: each pass applies the children unblocked by the
            // previous one. A blind full replay after every batch was
            // quadratic local work (re-attempting thousands of failing
            // upserts per batch) once the network refetch was fixed.
            if appliedCount > 0 {
              var parkedCount = parkedServerRecords.withValue(\.count)
              while parkedCount > 0 {
                let parkedRecords = parkedServerRecords
                  .withValue { Array($0.values) }
                  .sorted { lhs, rhs in
                    topologicallyAscending(
                      lhsTableName: lhs.recordType,
                      rhsTableName: rhs.recordType,
                      rootFirst: true
                    )
                  }
                for record in parkedRecords {
                  upsertFromServerRecord(record, db: db)
                }
                let remaining = parkedServerRecords.withValue(\.count)
                guard remaining < parkedCount else { break }
                parkedCount = remaining
              }
            }
            for (recordID, shareReference) in referencesByRecordID
            where !fetchedShareRecordIDs.contains(recordID)
              && !cachedShareRecordIDs.contains(recordID)
            {
              shares.append(.reference(shareReference))
            }
            let parkedNow = parkedServerRecords.withValue(\.count)
            logger.log(
              """
              sqlitedata-diag: batch applied \(appliedCount)/\(modifications.count), \
              parked \(parkedNow), share fetches \(shares.count)
              """
            )
            if appliedCount == 0, parkedNow > 0 {
              // Composition of the stuck set, to make dependency-order
              // pathologies visible in a plain log collect.
              let byType = parkedServerRecords.withValue {
                Dictionary(grouping: $0.keys) { $0.tableName ?? "?" }
                  .map { "\($0.key)=\($0.value.count)" }
                  .sorted()
                  .joined(separator: " ")
              }
              logger.log("sqlitedata-diag: parked composition \(byType, privacy: .public)")
            }
            return shares
          }
        }
        ?? []

      await withTaskGroup(of: Void.self) { group in
        for share in shares {
          group.addTask {
            switch share {
            case .share(let share):
              await withErrorReporting(.sqliteDataCloudKitFailure) {
                try await self.cacheShare(share)
              }
            case .reference(let shareReference):
              self.logger.log(
                "sqlitedata-diag: share-reference bootstrap fetch"
              )
              guard
                let record = try? await syncEngine.database.record(for: shareReference.recordID),
                let share = record as? CKShare
              else { return }
              await withErrorReporting(.sqliteDataCloudKitFailure) {
                try await self.cacheShare(share)
              }
            }
          }
        }
      }
    }

    private func topologicallyAscending(
      lhsTableName: String?,
      rhsTableName: String?,
      rootFirst: Bool
    ) -> Bool {
      switch (lhsTableName, rhsTableName) {
      case (nil, nil), (nil, _):
        return false
      case (_, nil):
        return true
      case (.some(let lhs), .some(let rhs)):
        let lhsIndex = tablesByOrder[lhs] ?? (rootFirst ? .max : .min)
        let rhsIndex = tablesByOrder[rhs] ?? (rootFirst ? .max : .min)
        guard lhsIndex != rhsIndex
        else {
          return lhs < rhs
        }
        return rootFirst ? lhsIndex < rhsIndex : lhsIndex > rhsIndex
      }
    }

    package func handleSentRecordZoneChanges(
      savedRecords: [CKRecord] = [],
      failedRecordSaves: [(record: CKRecord, error: CKError)] = [],
      deletedRecordIDs: [CKRecord.ID] = [],
      failedRecordDeletes: [CKRecord.ID: CKError] = [:],
      syncEngine: any SyncEngineProtocol
    ) async {
      for savedRecord in savedRecords {
        await refreshLastKnownServerRecord(savedRecord)
      }

      var newPendingRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange] = []
      var newPendingDatabaseChanges: [CKSyncEngine.PendingDatabaseChange] = []
      defer {
        syncEngine.state.add(pendingDatabaseChanges: newPendingDatabaseChanges)
        syncEngine.state.add(pendingRecordZoneChanges: newPendingRecordZoneChanges)
      }
      // Saves given up on: CKSyncEngine drops a failed save from its pending
      // changes unless the error is transient, and nothing here re-enqueues
      // these, so the row stays unsynced until it is next edited. Grouped by
      // table and code, so a schema mismatch rejecting a whole table reports
      // once per batch rather than once per row.
      struct DroppedSave: Hashable {
        let recordType: CKRecord.RecordType
        let code: CKError.Code
      }
      var droppedSaves: [DroppedSave: (count: Int, message: String)] = [:]
      func recordDroppedSave(_ record: CKRecord, _ error: CKError) {
        droppedSaves[
          DroppedSave(recordType: record.recordType, code: error.code),
          default: (0, error.localizedDescription)
        ].count += 1
      }
      for (failedRecord, error) in failedRecordSaves {
        func clearServerRecord() async {
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await userDatabase.write { db in
              try SyncMetadata
                .find(failedRecord.recordID)
                .update { $0.setLastKnownServerRecord(nil) }
                .execute(db)
            }
          }
        }

        switch error.code {
        case .serverRecordChanged:
          guard let serverRecord = error.serverRecord else { continue }
          await upsertFromServerRecord(serverRecord)
          newPendingRecordZoneChanges.append(.saveRecord(failedRecord.recordID))

        case .zoneNotFound:
          let zone = CKRecordZone(zoneID: failedRecord.recordID.zoneID)
          newPendingDatabaseChanges.append(.saveZone(zone))
          newPendingRecordZoneChanges.append(.saveRecord(failedRecord.recordID))
          await clearServerRecord()

        case .unknownItem:
          newPendingRecordZoneChanges.append(.saveRecord(failedRecord.recordID))
          await clearServerRecord()

        case .serverRejectedRequest:
          await clearServerRecord()
          recordDroppedSave(failedRecord, error)

        case .referenceViolation:
          guard
            let recordPrimaryKey = failedRecord.recordID.recordPrimaryKey,
            let table = tablesByName[failedRecord.recordType],
            foreignKeysByTableName[table.base.tableName]?.count == 1,
            let foreignKey = foreignKeysByTableName[table.base.tableName]?.first
          else {
            continue
          }
          func open<T>(_: some SynchronizableTable<T>) async throws {
            try await userDatabase.write { db in
              try $_isSynchronizingChanges.withValue(false) {
                switch foreignKey.onDelete {
                case .cascade:
                  try T
                    .unscoped
                    .where { #sql("\($0.primaryKey) = \(bind: recordPrimaryKey)") }
                    .delete()
                    .execute(db)
                case .restrict:
                  preconditionFailure(
                    "'RESTRICT' foreign key actions not supported for parent relationships."
                  )
                case .setDefault:
                  guard
                    let recordType = try RecordType.find(T.tableName).fetchOne(db),
                    let columnInfo = recordType.tableInfo.first(where: {
                      $0.name == foreignKey.from
                    })
                  else { return }
                  let defaultValue = columnInfo.defaultValue ?? "NULL"
                  try #sql(
                    """
                    UPDATE \(T.self)
                    SET \(quote: foreignKey.from, delimiter: .identifier) = (\(raw: defaultValue))
                    WHERE (\(T.primaryKey)) = (\(bind: recordPrimaryKey))
                    """
                  )
                  .execute(db)
                  break
                case .setNull:
                  try #sql(
                    """
                    UPDATE \(T.self)
                    SET \(quote: foreignKey.from, delimiter: .identifier) = NULL
                    WHERE (\(T.primaryKey)) = (\(bind: recordPrimaryKey))
                    """
                  )
                  .execute(db)
                case .noAction:
                  preconditionFailure(
                    "'NO ACTION' foreign key actions not supported for parent relationships."
                  )
                }
              }
            }
          }
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await open(table)
          }

        case .permissionFailure:
          guard
            let recordPrimaryKey = failedRecord.recordID.recordPrimaryKey,
            let table = tablesByName[failedRecord.recordType]
          else { continue }
          func open<T>(_: some SynchronizableTable<T>) async throws {
            do {
              let serverRecord = try await container.sharedCloudDatabase.record(
                for: failedRecord.recordID
              )
              await upsertFromServerRecord(serverRecord, force: true)
            } catch let error as CKError where error.code == .unknownItem {
              try await userDatabase.write { db in
                try T
                  .unscoped
                  .where { #sql("\($0.primaryKey) = \(bind: recordPrimaryKey)") }
                  .delete()
                  .execute(db)
              }
            }
          }
          await withErrorReporting(.sqliteDataCloudKitFailure) {
            try await open(table)
          }

        case .batchRequestFailed:
          newPendingRecordZoneChanges.append(.saveRecord(failedRecord.recordID))
          break

        // Transient: the sync engine keeps these pending and retries them.
        case .networkFailure, .networkUnavailable, .zoneBusy, .serviceUnavailable,
          .notAuthenticated, .operationCancelled, .requestRateLimited, .serverResponseLost,
          .accountTemporarilyUnavailable:
          continue

        case .internalError, .partialFailure, .badContainer, .missingEntitlement,
          .invalidArguments, .resultsTruncated, .assetFileNotFound,
          .assetFileModified, .incompatibleVersion, .constraintViolation, .changeTokenExpired,
          .badDatabase, .quotaExceeded, .limitExceeded, .userDeletedZone, .tooManyParticipants,
          .alreadyShared, .managedAccountRestricted, .participantMayNeedVerification,
          .assetNotAvailable:
          recordDroppedSave(failedRecord, error)
        #if canImport(FoundationModels)
          case .participantAlreadyInvited:
            continue
        #endif
        @unknown default:
          recordDroppedSave(failedRecord, error)
        }
      }

      // A fresh error carries the code alone: the failed save's own error holds
      // the client record, whose fields are user data.
      for (droppedSave, dropped) in droppedSaves {
        reportIssue(
          CKError(droppedSave.code),
          """
          Dropped \(dropped.count) unsent '\(droppedSave.recordType)' \
          \(dropped.count == 1 ? "save" : "saves") after a non-retryable CloudKit error: \
          \(dropped.message)
          """
        )
      }

      let enqueuedUnsyncedRecordID =
        await withErrorReporting(.sqliteDataCloudKitFailure) {
          try await userDatabase.write { db in
            var enqueuedUnsyncedRecordID = false
            for (failedRecordID, error) in failedRecordDeletes {
              switch error.code {
              case .referenceViolation:
                enqueuedUnsyncedRecordID = true
                try UnsyncedRecordID.insert {
                  UnsyncedRecordID(recordID: failedRecordID)
                } onConflictDoUpdate: { _ in
                }
                .execute(db)
                syncEngine.state.remove(pendingRecordZoneChanges: [.deleteRecord(failedRecordID)])
                break
              case .batchRequestFailed:
                syncEngine.state.add(pendingRecordZoneChanges: [.deleteRecord(failedRecordID)])
                break
              case .networkFailure, .networkUnavailable, .zoneBusy, .serviceUnavailable,
                .notAuthenticated, .operationCancelled, .internalError, .partialFailure,
                .badContainer, .requestRateLimited, .missingEntitlement, .invalidArguments,
                .resultsTruncated, .assetFileNotFound, .assetFileModified, .incompatibleVersion,
                .constraintViolation, .changeTokenExpired, .badDatabase, .quotaExceeded,
                .limitExceeded, .userDeletedZone, .tooManyParticipants, .alreadyShared,
                .managedAccountRestricted, .participantMayNeedVerification, .serverResponseLost,
                .assetNotAvailable, .accountTemporarilyUnavailable, .permissionFailure,
                .unknownItem, .serverRecordChanged, .serverRejectedRequest, .zoneNotFound:
                break
              #if canImport(FoundationModels)
                case .participantAlreadyInvited:
                  break
              #endif
              @unknown default:
                break
              }
            }
            return enqueuedUnsyncedRecordID
          }
        }
        ?? false
      if enqueuedUnsyncedRecordID {
        await handleFetchedRecordZoneChanges(syncEngine: syncEngine)
      }
    }

    private func cacheShare(_ share: CKShare) async throws {
      let metadata = try await container.shareMetadata(for: share, shouldFetchRootRecord: false)
      guard let rootRecordID = metadata.hierarchicalRootRecordID
      else { return }
      try await userDatabase.write { db in
        try SyncMetadata
          .find(rootRecordID)
          .update { $0.share = #bind(share) }
          .execute(db)
      }
    }

    /// Clears a share that the owner stopped from the system sharing UI out of the record's sync
    /// metadata, so the record stops reading as shared without waiting for a fetch.
    /// `CloudSharingView`'s controller delegate calls this on iOS. Public for AppKit, where the
    /// app runs `NSSharingService(named: .cloudSharing)` itself and gets the stop from
    /// `NSCloudSharingServiceDelegate.sharingService(_:didStopSharing:)`: a `fetchChanges()`
    /// there returns without a network request until CloudKit notifies the change.
    public func deleteShare(shareRecordID: CKRecord.ID) async throws {
      let shareAndRecordNameAndZone = try await metadatabase.read { db in
        try SyncMetadata
          .where(\.isShared)
          .select { ($0.share, $0.recordName, $0.zoneName, $0.ownerName) }
          .fetchAll(db)
          .first(where: { share, _, _, _ in share?.recordID == shareRecordID }) ?? nil
      }
      guard let (_, recordName, zoneName, ownerName) = shareAndRecordNameAndZone
      else { return }
      let rootRecordID = CKRecord.ID(
        recordName: recordName,
        zoneID: CKRecordZone.ID(zoneName: zoneName, ownerName: ownerName)
      )
      let rootRecord = try await container.privateCloudDatabase.record(for: rootRecordID)
      try await userDatabase.write { db in
        try SyncMetadata
          .find(
            CKRecord.ID(
              recordName: recordName,
              zoneID: CKRecordZone.ID(zoneName: zoneName, ownerName: ownerName)
            )
          )
          .update {
            $0.setLastKnownServerRecord(rootRecord)
            $0.share = #bind(nil)
          }
          .execute(db)
      }
    }

    private func upsertFromServerRecord(
      _ serverRecord: CKRecord,
      force: Bool = false
    ) async {
      await withErrorReporting(.sqliteDataCloudKitFailure) {
        try await userDatabase.write { db in
          upsertFromServerRecord(serverRecord, force: force, db: db)
        }
      }
    }

    /// Returns `false` when the record was parked by a foreign-key violation
    /// (child before parent — retried by the replay fixpoint in
    /// `handleFetchedRecordZoneChanges`); `true` for every other outcome.
    @discardableResult
    private func upsertFromServerRecord(
      _ serverRecord: CKRecord,
      force: Bool = false,
      db: Database
    ) -> Bool {
      var parked = false
      withErrorReporting(.sqliteDataCloudKitFailure) {
        guard
          let recordPrimaryKey = serverRecord.recordID.recordPrimaryKey,
          serverRecord.encryptedValues[CKRecord.userModificationTimeKey] != nil
        else {
          return
        }

        try SyncMetadata.insert {
          SyncMetadata(
            recordPrimaryKey: recordPrimaryKey,
            recordType: serverRecord.recordType,
            zoneName: serverRecord.recordID.zoneID.zoneName,
            ownerName: serverRecord.recordID.zoneID.ownerName,
            parentRecordPrimaryKey: serverRecord.parent?.recordID.recordPrimaryKey,
            parentRecordType: serverRecord.parent?.recordID.tableName,
            lastKnownServerRecord: serverRecord,
            _lastKnownServerRecordAllFields: serverRecord,
            share: nil,
            userModificationTime: serverRecord.userModificationTime
          )
        } onConflict: {
          ($0.recordPrimaryKey, $0.recordType)
        } doUpdate: {
          if tablesByName[serverRecord.recordType] == nil {
            $0.setLastKnownServerRecord(serverRecord)
          } else {
            $0.zoneName = serverRecord.recordID.zoneID.zoneName
            $0.ownerName = serverRecord.recordID.zoneID.ownerName
          }
        }
        .execute(db)

        guard
          let metadata = try SyncMetadata.find(serverRecord.recordID).fetchOne(db),
          let table = tablesByName[serverRecord.recordType]
        else {
          return
        }

        serverRecord.userModificationTime = metadata.userModificationTime

        func open<T>(_ table: some SynchronizableTable<T>) throws {
          var columnNames: [String] = T.TableColumns.writableColumns.map(\.name)
          if !force,
            let allFields = metadata._lastKnownServerRecordAllFields,
            let row = try T.unscoped.find(#sql("\(bind: metadata.recordPrimaryKey)")).fetchOne(db)
          {
            serverRecord.update(
              with: allFields,
              row: T(queryOutput: row),
              columnNames: &columnNames,
              parentForeignKey: foreignKeysByTableName[T.tableName]?.count == 1
                ? foreignKeysByTableName[T.tableName]?.first
                : nil
            )
          }

          do {
            try $_currentZoneID.withValue(serverRecord.recordID.zoneID) {
              try #sql(upsert(table, record: serverRecord, columnNames: columnNames)).execute(db)
            }
            try UnsyncedRecordID.find(serverRecord.recordID).delete().execute(db)
            parkedServerRecords.withValue { $0[serverRecord.recordID] = nil }
            try SyncMetadata
              .find(serverRecord.recordID)
              .update { $0.setLastKnownServerRecord(serverRecord) }
              .execute(db)
          } catch {
            guard
              let error = error as? DatabaseError,
              error.resultCode == .SQLITE_CONSTRAINT,
              error.extendedResultCode == .SQLITE_CONSTRAINT_FOREIGNKEY
            else {
              throw error
            }
            try UnsyncedRecordID.insert {
              UnsyncedRecordID(recordID: serverRecord.recordID)
            } onConflictDoUpdate: { _ in
            }
            .execute(db)
            parkedServerRecords.withValue { $0[serverRecord.recordID] = serverRecord }
            parked = true
          }
        }
        try open(table)
      }
      return !parked
    }

    private func refreshLastKnownServerRecord(_ record: CKRecord) async {
      await withErrorReporting(.sqliteDataCloudKitFailure) {
        try await userDatabase.write { db in
          let metadata = try SyncMetadata.find(record.recordID).fetchOne(db)
          func updateLastKnownServerRecord() throws {
            try SyncMetadata
              .find(record.recordID)
              .update { $0.setLastKnownServerRecord(record) }
              .execute(db)
          }

          if let lastKnownDate = metadata?.lastKnownServerRecord?.modificationDate {
            if let recordDate = record.modificationDate, lastKnownDate < recordDate {
              try updateLastKnownServerRecord()
            }
          } else {
            try updateLastKnownServerRecord()
          }
        }
      }
    }

    private func updateQuery<T>(
      for _: some SynchronizableTable<T>,
      record: CKRecord,
      columnNames: some Collection<String>,
      changedColumnNames: some Collection<String>
    ) async throws -> QueryFragment {
      let nonPrimaryKeyChangedColumns =
        changedColumnNames
        .filter {
          $0 != T.primaryKey.name && record.hasSet(key: $0)
        }
      guard
        !nonPrimaryKeyChangedColumns.isEmpty
      else {
        return ""
      }
      var record = record
      let recordHasAsset = nonPrimaryKeyChangedColumns.contains { columnName in
        record[columnName] is CKAsset
      }
      if recordHasAsset {
        logger.log(
          "sqlitedata-diag: asset re-fetch for \(record.recordType, privacy: .public)"
        )
        record = try await container.database(for: record.recordID).record(for: record.recordID)
      }

      var query: QueryFragment = "INSERT INTO \(T.self) ("
      query.append(columnNames.map { "\(quote: $0)" }.joined(separator: ", "))
      query.append(") VALUES (")
      query.append(
        columnNames
          .map { columnName in
            if let asset = record[columnName] as? CKAsset {
              let data = try? asset.fileURL.map { try dataManager.wrappedValue.load($0) }
              if data == nil {
                reportIssue("Asset data not found on disk")
              }
              return data?.queryFragment ?? "NULL"
            } else {
              return record.encryptedValues[columnName]?.queryFragment ?? "NULL"
            }
          }
          .joined(separator: ", ")
      )
      query.append(") ON CONFLICT(\(quote: T.primaryKey.name)) DO UPDATE SET ")
      query.append(" ")
      query.append(
        nonPrimaryKeyChangedColumns
          .map { columnName in
            if let asset = record[columnName] as? CKAsset {
              let data = try? asset.fileURL.map { try dataManager.wrappedValue.load($0) }
              if data == nil {
                reportIssue("Asset data not found on disk")
              }
              return
                "\(quote: columnName) = \(data?.queryFragment ?? #""excluded".\#(quote: columnName)"#)"
            } else {
              return """
                \(quote: columnName) = \
                \(record.encryptedValues[columnName]?.queryFragment ?? #""excluded".\#(quote: columnName)"#)
                """
            }
          }
          .joined(separator: ",")
      )
      return query
    }
  }

  @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
  extension CKSyncEngine.PendingRecordZoneChange {
    var id: CKRecord.ID? {
      switch self {
      case .saveRecord(let id):
        return id
      case .deleteRecord(let id):
        return id
      @unknown default:
        return nil
      }
    }
  }

  extension CKRecord.ID {
    var tableName: String? {
      guard
        let i = recordName.utf8.lastIndex(of: UTF8.CodeUnit(ascii: ":")),
        let j = recordName.utf8.index(i, offsetBy: 1, limitedBy: recordName.utf8.endIndex)
      else { return nil }
      let recordTypeBytes = recordName.utf8[j...]
      guard !recordTypeBytes.isEmpty else { return nil }
      return String(Substring(recordTypeBytes))
    }

    var recordPrimaryKey: String? {
      guard
        let i = recordName.utf8.lastIndex(of: UTF8.CodeUnit(ascii: ":"))
      else { return nil }
      let recordPrimaryKeyBytes = recordName.utf8[..<i]
      guard
        !recordPrimaryKeyBytes.isEmpty
      else { return nil }
      return String(Substring(recordPrimaryKeyBytes))
    }
  }

  extension String {
    package static let sqliteDataCloudKitSchemaName = "sqlitedata_icloud"
    package static let sqliteDataCloudKitFailure = "SQLiteData CloudKit Failure"
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension URL {
    package static func metadatabase(
      databasePath: String,
      containerIdentifier: String?
    ) throws -> URL {
      let databasePath = databasePath.isEmpty ? ":memory:" : databasePath
      guard let databaseURL = URL(string: databasePath)
      else {
        struct InvalidDatabasePath: Error {}
        throw InvalidDatabasePath()
      }
      guard !databaseURL.isInMemory
      else {
        return URL(string: "file:\(String.sqliteDataCloudKitSchemaName)?mode=memory&cache=shared")!
      }
      return
        databaseURL.deletingLastPathComponent().appending(
          component: ".\(databaseURL.deletingPathExtension().lastPathComponent)"
        )
        .appendingPathExtension("metadata\(containerIdentifier.map { "-\($0)" } ?? "").sqlite")
    }

    package var isInMemory: Bool {
      path.isEmpty
        || path.hasPrefix(":memory:")
        || absoluteString.hasPrefix(":memory:")
        || URLComponents(url: self, resolvingAgainstBaseURL: false)?
          .queryItems?
          .contains(where: { $0.name == "mode" && $0.value == "memory" })
          == true
    }
  }

  /// Notes whether any reported error was suspension-shaped
  /// (`DatabaseError.isSuspensionShaped`). It reports nothing itself.
  private final class InterruptionRecorder: IssueReporter, Sendable {
    private let _sawInterruption = LockIsolated(false)
    var sawInterruption: Bool { _sawInterruption.value }

    func reportIssue(
      _ message: @autoclosure () -> String?,
      severity: IssueSeverity,
      fileID: StaticString,
      filePath: StaticString,
      line: UInt,
      column: UInt
    ) {}

    func reportIssue(
      _ error: any Error,
      _ message: @autoclosure () -> String?,
      fileID: StaticString,
      filePath: StaticString,
      line: UInt,
      column: UInt
    ) {
      record(error)
    }

    func expectIssue(
      _ error: any Error,
      _ message: @autoclosure () -> String?,
      fileID: StaticString,
      filePath: StaticString,
      line: UInt,
      column: UInt
    ) {
      record(error)
    }

    private func record(_ error: any Error) {
      guard (error as? DatabaseError)?.isSuspensionShaped == true else { return }
      _sawInterruption.setValue(true)
    }
  }

  extension DatabaseError {
    /// Whether this is an error GRDB's suspension produces (GRDB 7.11,
    /// `Database.swift`), as opposed to any `SQLITE_ABORT`/`SQLITE_INTERRUPT`
    /// (`isInterruptionError`):
    /// - `SQLITE_ABORT` "Database is suspended": `checkForSuspensionViolation`
    ///   refuses a statement that could take a write lock while suspended;
    /// - `SQLITE_ABORT` "Transaction was aborted": `checkForAbortedTransaction`,
    ///   after the interrupt that `suspend()` issues rolled the transaction
    ///   back under a transaction block;
    /// - `SQLITE_INTERRUPT`: that interrupt, caught by SQLite mid-statement
    ///   (`sqlite3_interrupt`; the message depends on where SQLite noticed).
    ///
    /// Matched on the exact (extended) code, so `SQLITE_ABORT_ROLLBACK` and
    /// other aborts with GRDB's or an app's own messages ("Snapshot is lost",
    /// a function raising `SQLITE_ABORT`) are not. A host's own `interrupt()`
    /// still matches and cannot be told apart; the retry cap in
    /// `SyncEngine.applyDetectingSuspension` bounds that case.
    package var isSuspensionShaped: Bool {
      switch extendedResultCode {
      case .SQLITE_INTERRUPT:
        return true
      case .SQLITE_ABORT:
        return message == "Database is suspended" || message == "Transaction was aborted"
      default:
        return false
      }
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  package struct SyncEngines {
    private let rawValue: (private: any SyncEngineProtocol, shared: any SyncEngineProtocol)?
    init() {
      rawValue = nil
    }
    init(private: any SyncEngineProtocol, shared: any SyncEngineProtocol) {
      rawValue = (`private`, shared)
    }
    var isRunning: Bool {
      rawValue != nil
    }
    package var `private`: (any SyncEngineProtocol)? {
      guard let `private` = rawValue?.private
      else {
        if isRunning {
          reportIssue("Private sync engine has not been set.")
        }
        return nil
      }
      return `private`
    }
    package var `shared`: (any SyncEngineProtocol)? {
      guard let `shared` = rawValue?.shared
      else {
        if isRunning {
          reportIssue("Shared sync engine has not been set.")
        }
        return nil
      }
      return `shared`
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension Database {
    /// Attaches the metadatabase to an existing database connection.
    ///
    /// Invoke this method when preparing your database connection in order to allow querying the
    /// ``SyncMetadata`` table (see <doc:CloudKitSync#Accessing-CloudKit-metadata> for more info):
    ///
    /// ```swift
    /// func appDatabase() -> any DatabaseWriter {
    ///   var configuration = Configuration()
    ///   configuration.prepareDatabase = { db in
    ///     db.attachMetadatabase()
    ///     …
    ///   }
    /// }
    /// ```
    ///
    /// By default this method will use the container identifier assigned in your app's
    /// entitlements. If you wish to use a different container identifier then you can provide
    /// the `containerIdentifier` argument.
    ///
    /// See <doc:PreparingDatabase> for more information on preparing your database.
    ///
    /// - Parameter containerIdentifier: The identifier of the CloudKit container used to
    /// synchronize data. Defaults to the value set in the app's entitlements.
    public func attachMetadatabase(containerIdentifier: String? = nil) throws {
      @Dependency(\.context) var context
      let containerIdentifier =
        containerIdentifier
        ?? ModelConfiguration(groupContainer: .automatic).cloudKitContainerIdentifier
        ?? (context != .live ? "container" : nil)

      guard let containerIdentifier else {
        throw SyncEngine.SchemaError.noCloudKitContainer
      }

      let databasePath = try PragmaDatabaseList.select(\.file).fetchOne(self)
      guard let databasePath else {
        struct PathError: Error {}
        throw SyncEngine.SchemaError(
          reason: .unknown,
          debugDescription: """
            Expected to load a database path from the connection, but failed to do so.
            """
        )
      }
      let url = try URL.metadatabase(
        databasePath: databasePath,
        containerIdentifier: containerIdentifier
      )
      let path = url.isInMemory ? url.absoluteString : url.path(percentEncoded: false)
      let database: any DatabaseWriter =
        url.isInMemory
        ? try DatabaseQueue(path: path)
        : try DatabasePool(path: path)
      _ = try database.read { db in
        try #sql("SELECT 1").execute(db)
      }
      try #sql(
        """
        ATTACH DATABASE \(bind: path) AS \(quote: .sqliteDataCloudKitSchemaName)
        """
      )
      .execute(self)
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension SyncEngine {
    package struct SchemaError: LocalizedError {
      package enum Reason {
        case cycleDetected
        case invalidForeignKey(ForeignKey)
        case invalidForeignKeyAction(ForeignKey)
        case invalidTableName(String)
        case metadatabaseMismatch(attachedPath: String, syncEngineConfiguredPath: String)
        case noCloudKitContainer
        case nonNullColumnsWithoutDefault(tableName: String, columnNames: [String])
        case unknown
        case uniquenessConstraint
      }
      package let reason: Reason
      package let debugDescription: String

      package var errorDescription: String? {
        "Could not synchronize data with iCloud."
      }

      static let noCloudKitContainer = Self(
        reason: .noCloudKitContainer,
        debugDescription: """
          No default CloudKit container found. Make sure to enable iCloud entitlements in your \
          app's "Signing & Capabilities" and add a container identifier.
          """
      )
    }

    fileprivate func validateSchema() throws {
      let tableNames = Set(tables.map { $0.base.tableName })
      for tableName in tableNames {
        if tableName.contains(":") {
          throw SyncEngine.SchemaError(
            reason: .invalidTableName(tableName),
            debugDescription: "Table name contains invalid character ':'"
          )
        }
      }
      try userDatabase.read { db in
        for (tableName, foreignKeys) in foreignKeysByTableName {
          let invalidForeignKey = foreignKeys.first(where: { tablesByName[$0.table] == nil })
          if let invalidForeignKey {
            throw SyncEngine.SchemaError(
              reason: .invalidForeignKey(invalidForeignKey),
              debugDescription: """
                Foreign key \
                \(tableName.debugDescription).\(invalidForeignKey.from.debugDescription) \
                references table \(invalidForeignKey.table.debugDescription) that is not \
                synchronized. Update 'SyncEngine.init' to synchronize \
                \(invalidForeignKey.table.debugDescription). 
                """
            )
          }

          if foreignKeys.count == 1,
            let foreignKey = foreignKeys.first,
            [.restrict, .noAction].contains(foreignKey.onDelete)
          {
            throw SyncEngine.SchemaError(
              reason: .invalidForeignKeyAction(foreignKey),
              debugDescription: """
                Foreign key \(tableName.debugDescription).\(foreignKey.from.debugDescription) \
                action not supported. Must be 'CASCADE', 'SET DEFAULT' or 'SET NULL'.
                """
            )
          }
        }

        for table in tables {
          func open<T>(_: some SynchronizableTable<T>) throws {
            let columnsWithUniqueConstraints = try PragmaIndexList<T>
              .where { $0.isUnique && $0.origin.neq("pk") }
              .select(\.name)
              .fetchAll(db)
            if !columnsWithUniqueConstraints.isEmpty {
              throw SyncEngine.SchemaError(
                reason: .uniquenessConstraint,
                debugDescription: """
                  Uniqueness constraints are not supported for synchronized tables.
                  """
              )
            }
          }
          try open(table)
        }
      }
    }
  }

  package protocol SynchronizableTable<Base>: Hashable, Sendable {
    associatedtype Base: PrimaryKeyedTable & _SendableMetatype
    where
      Base.PrimaryKey.QueryOutput: IdentifierStringConvertible,
      Base.TableColumns.PrimaryColumn: WritableTableColumnExpression
    var base: Base.Type { get }
  }

  package struct SynchronizedTable<
    Base: PrimaryKeyedTable & _SendableMetatype
  >: SynchronizableTable
  where
    Base.PrimaryKey.QueryOutput: IdentifierStringConvertible,
    Base.TableColumns.PrimaryColumn: WritableTableColumnExpression
  {
    package init(for table: Base.Type = Base.self) {}
    package var base: Base.Type { Base.self }
  }

  private struct HashableSynchronizedTable: Hashable {
    let type: any SynchronizableTable
    init(_ type: any SynchronizableTable) {
      self.type = type
    }
    func hash(into hasher: inout Hasher) {
      hasher.combine(ObjectIdentifier(type.base))
    }
    static func == (lhs: Self, rhs: Self) -> Bool {
      lhs.type.base == rhs.type.base
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  private func tablesByOrder(
    userDatabase: UserDatabase,
    tables: [any SynchronizableTable],
    tablesByName: [String: any SynchronizableTable]
  ) throws -> [String: Int] {
    let tableDependencies = try userDatabase.read { db in
      var dependencies: OrderedDictionary<HashableSynchronizedTable, [any SynchronizableTable]> =
        [:]
      for table in tables {
        func open<T>(_: some SynchronizableTable<T>) throws -> [String] {
          try PragmaForeignKeyList<T>
            .order(by: \.table)
            .select(\.table)
            .fetchAll(db)
        }
        let toTables = try open(table)
        for toTable in toTables {
          guard let toTableType = tablesByName[toTable]
          else { continue }
          dependencies[HashableSynchronizedTable(table), default: []].append(toTableType)
        }
      }
      return dependencies
    }

    var visited = Set<HashableSynchronizedTable>()
    var marked = Set<HashableSynchronizedTable>()
    var result: [String: Int] = [:]
    for table in tableDependencies.keys {
      try visit(table: table)
    }
    return result

    func visit(table: HashableSynchronizedTable) throws {
      guard !visited.contains(table)
      else { return }
      guard !marked.contains(table)
      else {
        throw SyncEngine.SchemaError(
          reason: .cycleDetected,
          debugDescription: """
            Cycles are not currently permitted in schemas, e.g. a table that references itself.
            """
        )
      }

      marked.insert(table)
      for dependency in tableDependencies[table] ?? [] {
        try visit(table: HashableSynchronizedTable(dependency))
      }
      marked.remove(table)
      visited.insert(table)
      result[table.type.base.tableName] = result.count
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension Updates<SyncMetadata> {
    mutating func setLastKnownServerRecord(_ lastKnownServerRecord: CKRecord?) {
      self.zoneName = lastKnownServerRecord?.recordID.zoneID.zoneName ?? self.zoneName
      self.ownerName = lastKnownServerRecord?.recordID.zoneID.ownerName ?? self.ownerName
      self.lastKnownServerRecord = #bind(lastKnownServerRecord)
      self._lastKnownServerRecordAllFields = #bind(lastKnownServerRecord)
      if let lastKnownServerRecord {
        self.userModificationTime = #sql(
          """
          max(\(self.userModificationTime), \(lastKnownServerRecord.userModificationTime))
          """
        )
      }
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  private func upsert<T>(
    _: some SynchronizableTable<T>,
    record: CKRecord,
    columnNames: some Collection<String>
  ) -> QueryFragment {
    let setColumnNames = T.TableColumns.writableColumns.map(\.name)
      .filter { record.hasSet(key: $0) }
    guard !setColumnNames.isEmpty
    else {
      return ""
    }
    let columnNames = columnNames.filter { setColumnNames.contains($0) }
    let hasNonPrimaryKeyColumns = columnNames.contains { $0 != T.primaryKey.name }
    var query: QueryFragment = "INSERT INTO \(T.self) ("
    query.append(setColumnNames.map { "\(quote: $0)" }.joined(separator: ", "))
    query.append(") VALUES (")
    query.append(
      setColumnNames
        .map { columnName in
          if let asset = record[columnName] as? CKAsset {
            @Dependency(\.dataManager) var dataManager
            return (try? asset.fileURL.map { try dataManager.load($0) })?
              .queryFragment ?? "NULL"
          } else {
            return record.encryptedValues[columnName]?.queryFragment ?? "NULL"
          }
        }
        .joined(separator: ", ")
    )
    query.append(") ON CONFLICT(\(quote: T.primaryKey.name)) DO")
    if hasNonPrimaryKeyColumns {
      query.append(" UPDATE SET ")
      query.append(
        columnNames
          .filter { $0 != T.primaryKey.name }
          .map {
            """
            \(quote: $0) = "excluded".\(quote: $0)
            """
          }
          .joined(separator: ", ")
      )
    } else {
      query.append(" NOTHING")
    }
    return query
  }

  @TaskLocal package var _isSynchronizingChanges = false
  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  @TaskLocal package var _currentZoneID: CKRecordZone.ID?
  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  @DatabaseFunction("sqlitedata_icloud_currentZoneName")
  func currentZoneName() -> String? {
    _currentZoneID?.zoneName
  }
  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  @DatabaseFunction("sqlitedata_icloud_currentOwnerName")
  func currentOwnerName() -> String? {
    _currentZoneID?.ownerName
  }

  private struct ActivityCounts {
    var sendingChangesCount = 0
    var fetchingChangesCount = 0
  }

  #if DEBUG
    @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
    private struct NextRecordZoneChangeBatchLoggingState {
      var events: [String] = []
      var recordTypes: [String] = []
      var recordNames: [String] = []
      var tabularDescription: String? {
        guard !events.isEmpty
        else { return nil }
        var dataFrame: DataFrame = [
          "event": events,
          "recordType": recordTypes,
          "recordName": recordNames,
        ]
        dataFrame.sort(
          on: ColumnID("event", String.self),
          ColumnID("recordType", String.self),
          ColumnID("recordName", String.self)
        )
        var formattingOptions = FormattingOptions(
          maximumLineWidth: 120,
          maximumCellWidth: 80,
          maximumRowCount: 50,
          includesColumnTypes: false
        )
        formattingOptions.includesRowAndColumnCounts = false
        formattingOptions.includesRowIndices = false
        return
          dataFrame
          .description(options: formattingOptions)
          .replacing("\n", with: "\n  ")
      }
    }
  #endif
#endif
