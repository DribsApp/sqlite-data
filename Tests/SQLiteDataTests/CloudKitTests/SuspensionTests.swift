#if canImport(CloudKit)
  import CloudKit
  import ConcurrencyExtras
  import Foundation
  import GRDB
  import SQLiteData
  import SQLiteDataTestSupport
  import TestLocals
  import Testing

  final class RecordingActivity: SyncEngineActivity {
    let names = LockIsolated<[String]>([])
    func run<T: Sendable>(_ name: String, _ work: @Sendable () async -> T) async -> T {
      names.withValue { $0.append(name) }
      return await work()
    }
  }

  let recordingActivity = RecordingActivity()

  /// The issue a handler reports when the database refuses its write: GRDB's
  /// `SQLITE_ABORT` "Database is suspended", or `SQLITE_INTERRUPT`. Anything
  /// else a test reports is still a failure.
  func isSuspensionIssue(_ issue: Issue) -> Bool {
    if let error = issue.error as? DatabaseError {
      return error.isSuspensionShaped
    }
    // An issue recorded from a message alone: the same three errors, as
    // `DatabaseError.description` renders them.
    let text = issue.comments.map(\.rawValue).joined(separator: " ")
    return text.contains("SQLite error 4: Database is suspended")
      || text.contains("SQLite error 4: Transaction was aborted")
      || text.contains("SQLite error 9: ")
  }

  /// A SQL function that runs `action` the first time it is called (every
  /// time, with `everyTime`), then does nothing; plus a TEMP trigger calling
  /// it after every insert into "remindersLists". Both live on the pool's
  /// single writer connection.
  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  func onFirstRemindersListInsert(
    in userDatabase: UserDatabase,
    everyTime: Bool = false,
    _ action: @escaping @Sendable () throws -> Void
  ) async throws {
    let armed = LockIsolated(true)
    try await userDatabase.database.write { db in
      db.add(
        function: DatabaseFunction("sqlitedata_test_onFirstInsert", argumentCount: 0, pure: false) {
          _ in
          let isFirst = armed.withValue {
            defer { $0 = everyTime }
            return $0
          }
          if isFirst { try action() }
          return nil
        }
      )
      try db.execute(
        sql: """
          CREATE TEMP TRIGGER "sqlitedata_test_onFirstInsert"
          AFTER INSERT ON "remindersLists"
          BEGIN SELECT sqlitedata_test_onFirstInsert(); END
          """
      )
    }
  }

  /// Like the host's hold from the background: entering the retry's activity
  /// resumes the database, leaving it suspends it again.
  final class ResumingActivity: SyncEngineActivity {
    let retries = LockIsolated(0)
    let retriesInFlight = LockIsolated(0)
    func run<T: Sendable>(_ name: String, _ work: @Sendable () async -> T) async -> T {
      guard name == "retryAfterResume" else { return await work() }
      retries.withValue { $0 += 1 }
      retriesInFlight.withValue { $0 += 1 }
      NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
      let result = await work()
      NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
      retriesInFlight.withValue { $0 -= 1 }
      return result
    }
  }

  let resumingActivity = ResumingActivity()

  extension BaseCloudKitTests {
    @MainActor
    @Suite(.serialized, .taskLocal($syncEngineActivity, recordingActivity))
    final class SuspensionActivityTests: BaseCloudKitTests, @unchecked Sendable {
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func everyEventRunsInsideTheActivity() async throws {
        recordingActivity.names.withValue { $0 = [] }
        await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: syncEngine.private)
        #expect(
          recordingActivity.names.value == ["handleEvent.willFetchChanges", "handleEvent.didFetchChanges"]
        )
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func nextBatchRunsInsideTheActivity() async throws {
        recordingActivity.names.withValue { $0 = [] }
        _ = await syncEngine.nextRecordZoneChangeBatch(syncEngine: syncEngine.private)
        #expect(recordingActivity.names.value == ["nextRecordZoneChangeBatch"])
      }
    }

    /// These suites post the REAL GRDB notifications, which reach every pool
    /// opened with `observesSuspensionNotifications` in the process. A
    /// suspend posted by one would hit another's observing pool if they ran
    /// at the same time, so every suite that posts the notifications or opens
    /// such a pool shares this serialized parent.
    @Suite(.serialized)
    struct SuspensionNotificationTests {
      /// Opens its pool with `observesSuspensionNotifications`.
      @MainActor
      @Suite(.serialized, .taskLocal($observesSuspensionNotifications, true))
      final class SuspensionRetryTests: BaseCloudKitTests, @unchecked Sendable {
        func suspend() {
          NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
        }
        func resume() {
          NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        func remindersListRecord() -> CKRecord {
          let record = CKRecord(
            recordType: RemindersList.tableName,
            recordID: RemindersList.recordID(for: 1)
          )
          record.setValue(1, forKey: "id", at: now)
          record.setValue("Personal", forKey: "title", at: now)
          return record
        }

        /// Handles an event while the database is suspended. The handlers
        /// report the failed write (`withErrorReporting`), which is the
        /// expected production behaviour here, not a test failure.
        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        func handleWhileSuspended(
          _ event: SyncEngine.Event,
          syncEngine engine: (any SyncEngineProtocol)? = nil
        ) async {
          let engine = engine ?? syncEngine.private
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(event, syncEngine: engine)
          } matching: { isSuspensionIssue($0) }
        }

        func serialization(_ tag: String) throws -> CKSyncEngine.State.Serialization {
          let json = #"{"data":"\#(Data(tag.utf8).base64EncodedString())"}"#
          return try JSONDecoder().decode(
            CKSyncEngine.State.Serialization.self,
            from: Data(json.utf8)
          )
        }

        /// The persisted serialization of the private scope, JSON-encoded so
        /// it can be compared (the type is not `Equatable`).
        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        func persistedPrivateState() async throws -> String? {
          let data = try await syncEngine.metadatabase.read { db in
            try StateSerialization.find(#bind(.private)).select(\.data).fetchOne(db)
          }
          return try data.map { String(decoding: try JSONEncoder().encode($0), as: UTF8.self) }
        }

        func encoded(_ serialization: CKSyncEngine.State.Serialization) throws -> String {
          String(decoding: try JSONEncoder().encode(serialization), as: UTF8.self)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func fetchedBatchInterruptedBySuspensionIsReappliedOnTheNextEvent() async throws {
          defer { resume() }
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          // WAL reads still work while suspended; the write did not.
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)

          resume()
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 1)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func batchAppliedWithoutSuspensionIsNotRetried() async throws {
          await syncEngine.handleEvent(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: []),
            syncEngine: syncEngine.private
          )
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 1)
          try await userDatabase.userWrite { db in try RemindersList.find(1).delete().execute(db) }
          // Only the local row matters here, not the upload it enqueued.
          syncEngine.private.state.removePendingChanges()
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          // Nothing was queued, so the deleted row stays deleted.
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func stopClearsDeferredState() async throws {
          defer { resume() }
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          resume()
          syncEngine.stop()
          try await syncEngine.start()
          try await syncEngine.processPendingDatabaseChanges(scope: .private)
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          // The restarted engine fetches from its saved token instead; the
          // stale queue must not be replayed into it.
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func stateUpdateIsHeldUntilTheInterruptedBatchApplies() async throws {
          defer { resume() }
          let state = try serialization("after-batch")
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          // Still suspended: the retry before the state update fails again,
          // so the state must be held, not written.
          await handleWhileSuspended(.stateUpdate(stateSerialization: state))
          #expect(try await persistedPrivateState() == nil)
          resume()
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await persistedPrivateState() == encoded(state))
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func resumeRetriesTheQueueAndSavesTheHeldState() async throws {
          defer { resume() }
          let state = try serialization("held")
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          await handleWhileSuspended(.stateUpdate(stateSerialization: state))
          #expect(try await persistedPrivateState() == nil)

          // Something new on the server, so the fetch the resume starts
          // delivers an event (the mock delivers none for an empty fetch).
          let other = CKRecord(
            recordType: RemindersList.tableName,
            recordID: RemindersList.recordID(for: 2)
          )
          other.setValue(2, forKey: "id", at: now)
          other.setValue("Work", forKey: "title", at: now)
          _ = try syncEngine.modifyRecords(scope: .private, saving: [other])

          // No event from the test: the resume alone has to drive the retry.
          resume()
          let deadline = ContinuousClock.now + .seconds(15)
          while try await persistedPrivateState() == nil, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(20))
          }
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 2)
          #expect(try await persistedPrivateState() == encoded(state))
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func newerStateUpdateSupersedesTheHeldOne() async throws {
          defer { resume() }
          let older = try serialization("older")
          let newer = try serialization("newer")
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          await handleWhileSuspended(.stateUpdate(stateSerialization: older))
          resume()
          // The retry drains the queue first, then this state is saved; the
          // older held one must not be written over it, then or on any later
          // event.
          await syncEngine.handleEvent(
            .stateUpdate(stateSerialization: newer),
            syncEngine: syncEngine.private
          )
          #expect(try await persistedPrivateState() == encoded(newer))
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 1)
          #expect(try await persistedPrivateState() == encoded(newer))
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func eventsFromAStoppedEngineAreNotDeferred() async throws {
          defer { resume() }
          let stoppedEngine = syncEngine.private
          syncEngine.stop()
          try await syncEngine.start()
          try await syncEngine.processPendingDatabaseChanges(scope: .private)
          // A delivery still in flight from the engine `stop()` discarded.
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: []),
            syncEngine: stoppedEngine
          )
          await handleWhileSuspended(
            .stateUpdate(stateSerialization: try serialization("stale")),
            syncEngine: stoppedEngine
          )
          resume()
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          // The restarted engine fetches from its own saved token: the stopped
          // engine's batch must not have been queued for it to replay. (Its
          // state update here was handled while suspended, so this run does
          // not show that it is dropped; the next test does.)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)
          #expect(try await persistedPrivateState() == nil)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func stoppedEngineStateUpdateIsDroppedWhileResumed() async throws {
          let stoppedEngine = syncEngine.private
          syncEngine.stop()
          try await syncEngine.start()
          try await syncEngine.processPendingDatabaseChanges(scope: .private)
          // Not suspended, so the write would succeed if it were attempted.
          await syncEngine.handleEvent(
            .stateUpdate(stateSerialization: try serialization("stale")),
            syncEngine: stoppedEngine
          )
          #expect(try await persistedPrivateState() == nil)

          // The running engine's own state still saves.
          let current = try serialization("current")
          await syncEngine.handleEvent(
            .stateUpdate(stateSerialization: current),
            syncEngine: syncEngine.private
          )
          #expect(try await persistedPrivateState() == encoded(current))
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func interruptionErrorWithoutANotificationStillDefers() async throws {
          // No notification is posted: the counter sees nothing. The write
          // fails the way a suspended writer's does, which is the only signal
          // when GRDB interrupts the writer before our observer counts.
          try await onFirstRemindersListInsert(in: userDatabase) {
            throw DatabaseError(resultCode: .SQLITE_ABORT, message: "Database is suspended")
          }
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(
              .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: []),
              syncEngine: syncEngine.private
            )
          } matching: { isSuspensionIssue($0) }
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)

          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 1)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func recurringErrorWithoutSuspensionIsGivenUpAfterTheCap() async throws {
          // Every insert fails the way a suspended writer's does, but nothing
          // is ever suspended: not a suspension, and it will not go away.
          let attempts = LockIsolated(0)
          try await onFirstRemindersListInsert(in: userDatabase, everyTime: true) {
            attempts.withValue { $0 += 1 }
            throw DatabaseError(resultCode: .SQLITE_ABORT, message: "Database is suspended")
          }
          let held = try serialization("held")
          let later = try serialization("later")

          // Attempt 1.
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(
              .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: []),
              syncEngine: syncEngine.private
            )
          } matching: { isSuspensionIssue($0) }
          // Attempt 2, before the state update: still blocked, so it is held.
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(
              .stateUpdate(stateSerialization: held),
              syncEngine: syncEngine.private
            )
          } matching: { isSuspensionIssue($0) }
          #expect(try await persistedPrivateState() == nil)

          // Attempt 3 reaches the cap: the event is dropped, which unblocks
          // the scope and releases the held state.
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          } matching: { isSuspensionIssue($0) }
          #expect(attempts.value == 3)
          #expect(try await persistedPrivateState() == encoded(held))

          // No longer queued: nothing retries it, and a new state saves at once.
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          await syncEngine.handleEvent(
            .stateUpdate(stateSerialization: later),
            syncEngine: syncEngine.private
          )
          #expect(attempts.value == 3)
          #expect(try await persistedPrivateState() == encoded(later))
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func retryKeepsBatchOrderWhenASuspensionInterruptsIt() async throws {
          defer { resume() }
          suspend()
          // Batch 1 creates the list; batch 2, later, deletes it.
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(
              modifications: [],
              deletions: [(RemindersList.recordID(for: 1), RemindersList.tableName)]
            )
          )
          resume()

          // A suspension starts and ends inside batch 1's retried write.
          try await onFirstRemindersListInsert(in: userDatabase) {
            NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
          }
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          } matching: { isSuspensionIssue($0) }
          // Batch 2 must wait behind batch 1 instead of applying first (a
          // no-op delete) and letting batch 1 recreate the list afterwards.
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func newBatchWaitsBehindAnInterruptedRetry() async throws {
          defer { resume() }
          suspend()
          await handleWhileSuspended(
            .fetchedRecordZoneChanges(modifications: [remindersListRecord()], deletions: [])
          )
          resume()

          // The deletion arrives next. Retrying the creation first is
          // interrupted by a suspension that starts and ends inside its write.
          try await onFirstRemindersListInsert(in: userDatabase) {
            NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
          }
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(
              .fetchedRecordZoneChanges(
                modifications: [],
                deletions: [(RemindersList.recordID(for: 1), RemindersList.tableName)]
              ),
              syncEngine: syncEngine.private
            )
          } matching: { isSuspensionIssue($0) }
          // The deletion must have queued behind the creation, not applied
          // ahead of it.
          await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 0)
        }
      }

      @MainActor
      @Suite(
        .serialized,
        .taskLocal($observesSuspensionNotifications, true),
        .taskLocal($syncEngineActivity, resumingActivity)
      )
      final class SuspensionResumeRetryTests: BaseCloudKitTests, @unchecked Sendable {
        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func retryAfterResumeDoesNotRestartItselfWhileTheQueueCannotDrain() async throws {
          resumingActivity.retries.setValue(0)
          NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
          defer {
            // Drop the queue first: a resume with something still queued
            // starts another retry, whose activity would suspend the pools of
            // whichever test runs next.
            syncEngine.stop()
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
          }
          let record = CKRecord(
            recordType: RemindersList.tableName,
            recordID: RemindersList.recordID(for: 1)
          )
          record.setValue(1, forKey: "id", at: now)
          record.setValue("Personal", forKey: "title", at: now)
          await withKnownIssue(isIntermittent: true) {
            await syncEngine.handleEvent(
              .fetchedRecordZoneChanges(modifications: [record], deletions: []),
              syncEngine: syncEngine.private
            )
          } matching: { isSuspensionIssue($0) }

          // The mock's fetch delivers no event for an empty server, so the
          // queue stays put: the retry's own resume must not start another.
          NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
          let deadline = ContinuousClock.now + .seconds(15)
          while resumingActivity.retries.value == 0, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
          }
          try await Task.sleep(for: .milliseconds(300))
          while resumingActivity.retriesInFlight.value > 0, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
          }
          #expect(resumingActivity.retries.value == 1)
        }
      }

      /// Moved from `SyncEngineTests`: its pool observes the notifications,
      /// so its init-time migration would abort if it ran while another
      /// suite here has the database suspended.
      @MainActor
      @Suite(.serialized)
      final class SuspensionObservingMetadatabaseTests {
        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func metadatabaseInheritsSuspensionNotificationConfiguration() async throws {
          var configuration = Configuration()
          configuration.observesSuspensionNotifications = true
          let syncEngine = try await SyncEngine(
            container: MockCloudContainer(
              containerIdentifier: "test",
              privateCloudDatabase: MockCloudDatabase(databaseScope: .private),
              sharedCloudDatabase: MockCloudDatabase(databaseScope: .shared)
            ),
            userDatabase: UserDatabase(
              database: try DatabaseQueue(configuration: configuration)
            ),
            tables: [],
            startImmediately: false
          )

          #expect(syncEngine.metadatabase.configuration.observesSuspensionNotifications)
        }
      }

      @MainActor
      @Suite(.serialized)
      final class SuspensionNotObservedTests: BaseCloudKitTests, @unchecked Sendable {
        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func notObservingSuspensionNeverDefers() async throws {
          NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
          defer { NotificationCenter.default.post(name: Database.resumeNotification, object: nil) }
          let record = CKRecord(
            recordType: RemindersList.tableName,
            recordID: RemindersList.recordID(for: 1)
          )
          record.setValue(1, forKey: "id", at: now)
          record.setValue("Personal", forKey: "title", at: now)
          await syncEngine.handleEvent(
            .fetchedRecordZoneChanges(modifications: [record], deletions: []),
            syncEngine: syncEngine.private
          )
          // This pool ignores the notification, so the write lands and nothing is deferred.
          #expect(try await userDatabase.read { try RemindersList.all.fetchCount($0) } == 1)
        }

        @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
        @Test func stoppedEngineStateUpdateIsDroppedWithoutTracking() async throws {
          let stoppedEngine = syncEngine.private
          syncEngine.stop()
          try await syncEngine.start()
          try await syncEngine.processPendingDatabaseChanges(scope: .private)
          let json = #"{"data":"\#(Data("stale".utf8).base64EncodedString())"}"#
          let stale = try JSONDecoder().decode(
            CKSyncEngine.State.Serialization.self,
            from: Data(json.utf8)
          )
          await syncEngine.handleEvent(
            .stateUpdate(stateSerialization: stale),
            syncEngine: stoppedEngine
          )
          let persisted = try await syncEngine.metadatabase.read { db in
            try StateSerialization.find(#bind(.private)).select(\.data).fetchOne(db)
          }
          #expect(persisted == nil)
        }
      }
    }  // SuspensionNotificationTests
  }

  /// The overlap rule `SyncEngine` applies to each covered event.
  @Suite struct DatabaseSuspensionStateTests {
    @Test func onlyGRDBSuspensionErrorsAreSuspensionShaped() {
      func error(_ code: ResultCode, _ message: String? = nil) -> DatabaseError {
        DatabaseError(resultCode: code, message: message)
      }
      #expect(error(.SQLITE_ABORT, "Database is suspended").isSuspensionShaped)
      #expect(error(.SQLITE_ABORT, "Transaction was aborted").isSuspensionShaped)
      #expect(error(.SQLITE_INTERRUPT, "interrupted").isSuspensionShaped)
      #expect(!error(.SQLITE_ABORT, "Snapshot is lost.").isSuspensionShaped)
      #expect(!error(.SQLITE_ABORT, "app abort").isSuspensionShaped)
      #expect(!error(.SQLITE_ABORT_ROLLBACK).isSuspensionShaped)
      #expect(!error(.SQLITE_CONSTRAINT).isSuspensionShaped)
    }

    @Test func eventWithNoSuspensionDidNotOverlap() {
      let state = DatabaseSuspensionState()
      #expect(state.overlapsSuspension(since: state) == false)
    }

    @Test func suspensionThatEndedDuringTheEventOverlapped() {
      var state = DatabaseSuspensionState()
      let before = state
      state.didSuspend()
      state.didResume()
      #expect(state.overlapsSuspension(since: before))
    }

    @Test func eventEndingWhileSuspendedOverlapped() {
      var state = DatabaseSuspensionState()
      let before = state
      state.didSuspend()
      #expect(state.overlapsSuspension(since: before))
    }

    @Test func eventStartingWhileSuspendedOverlappedEvenIfResumedByTheEnd() {
      var state = DatabaseSuspensionState()
      state.didSuspend()
      let before = state
      state.didResume()
      // No new suspension began during the event, but it started while the
      // writer was refusing writes.
      #expect(state.overlapsSuspension(since: before))
    }
  }
#endif
