#if canImport(CloudKit)
  import CloudKit
  import SQLiteData
  import Testing

  extension BaseCloudKitTests {
    // A save that fails with 'quotaExceeded' is dropped from CKSyncEngine's
    // pending changes, and CKSyncEngine never retries it: the code is not one
    // of the transient errors it handles. A row that had never been uploaded
    // came back at the next start through the unknown-records sweep, but an
    // edit to a row already on the server did not: it stayed unsynced until
    // that row was next edited, even after the user freed space.
    @MainActor
    final class QuotaExceededTests: BaseCloudKitTests, @unchecked Sendable {
      /// Uploads list 1, then edits it while the account is full.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      private func blockAnEditOfAnUploadedRow() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Personal") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        setQuotaExceeded(true)
        try await userDatabase.userWrite { db in
          try RemindersList.find(1).update { $0.title = "Groceries" }.execute(db)
        }
        try await withKnownIssue {
          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        } matching: { issue in
          issue.description.contains(
            "Deferred 1 unsent 'remindersLists' save until iCloud has space"
          )
        }
      }

      private func setQuotaExceeded(_ isQuotaExceeded: Bool) {
        container.privateCloudDatabase.state.withValue {
          $0.isQuotaExceeded = isQuotaExceeded
        }
      }

      private func serverTitle(ofList id: Int) throws -> String? {
        try syncEngine.private.database
          .record(for: RemindersList.recordID(for: id))
          .encryptedValues["title"] as? String
      }

      /// What the next launch does: a fresh start, which also re-queues the
      /// default zone.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      private func restart() async throws {
        syncEngine.stop()
        try await syncEngine.start()
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func quotaFailedEditIsNotRequeuedImmediately() async throws {
        try await blockAnEditOfAnUploadedRow()

        // Re-queueing it now would only fail again, over and over, against
        // an account that is still full.
        syncEngine.private.state.assertPendingRecordZoneChanges([])
        #expect(syncEngine.hasChangesAwaitingICloudStorage)
        #expect(try serverTitle(ofList: 1) == "Personal")
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func quotaFailedEditIsRetriedAtNextStart() async throws {
        try await blockAnEditOfAnUploadedRow()

        setQuotaExceeded(false)
        try await restart()

        syncEngine.private.state.assertPendingRecordZoneChanges(
          [.saveRecord(RemindersList.recordID(for: 1))]
        )
        syncEngine.private.state.add(
          pendingRecordZoneChanges: [.saveRecord(RemindersList.recordID(for: 1))]
        )
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        #expect(try serverTitle(ofList: 1) == "Groceries")
        #expect(!syncEngine.hasChangesAwaitingICloudStorage)
      }

      // Only the first quota failure reports: retrying at each start against
      // an account that is still full must not report again every launch.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func retryAgainstAFullAccountDoesNotReportAgain() async throws {
        try await blockAnEditOfAnUploadedRow()

        try await restart()
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        syncEngine.private.state.assertPendingRecordZoneChanges([])
        #expect(syncEngine.hasChangesAwaitingICloudStorage)
        #expect(try serverTitle(ofList: 1) == "Personal")
      }

      // A save that goes through means the account has space again, so the
      // blocked rows go out without waiting for the next launch. The Mac app
      // can stay open for days.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func successfulSaveRetriesBlockedRows() async throws {
        try await blockAnEditOfAnUploadedRow()

        setQuotaExceeded(false)
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 2, title: "Work") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        syncEngine.private.state.assertPendingRecordZoneChanges(
          [.saveRecord(RemindersList.recordID(for: 1))]
        )
        syncEngine.private.state.add(
          pendingRecordZoneChanges: [.saveRecord(RemindersList.recordID(for: 1))]
        )
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        #expect(try serverTitle(ofList: 1) == "Groceries")
        #expect(!syncEngine.hasChangesAwaitingICloudStorage)
      }

      // Deletes go through on a full account too, so one succeeding says
      // nothing about space: retrying the blocked rows would only fail again.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func successfulDeleteDoesNotRetryBlockedRows() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 2, title: "Work") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        try await blockAnEditOfAnUploadedRow()

        try await userDatabase.userWrite { db in
          try RemindersList.find(2).delete().execute(db)
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        syncEngine.private.state.assertPendingRecordZoneChanges([])
        #expect(syncEngine.hasChangesAwaitingICloudStorage)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func rowDeletedWhileBlockedIsNotResaved() async throws {
        try await blockAnEditOfAnUploadedRow()

        try await userDatabase.userWrite { db in
          try RemindersList.find(1).delete().execute(db)
        }
        setQuotaExceeded(false)
        try await restart()

        #expect(
          !syncEngine.private.state.pendingRecordZoneChanges.contains(
            .saveRecord(RemindersList.recordID(for: 1))
          )
        )
        #expect(!syncEngine.hasChangesAwaitingICloudStorage)
        syncEngine.private.state.removePendingChanges()
      }
    }
  }
#endif
