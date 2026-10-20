#if canImport(CloudKit)
  import CloudKit
  import SQLiteData
  import Testing

  extension BaseCloudKitTests {
    @MainActor
    final class DroppedSaveReportingTests: BaseCloudKitTests, @unchecked Sendable {
      // A save the server rejects is dropped from the pending changes and
      // never retried. With nothing reported, a whole table can stop syncing
      // unnoticed: a Production schema missing a column rejected every save of
      // that table for days, and no device ever said so.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func rejectedSaveIsReported() async throws {
        // The server already holds the record, so saving a fresh local row
        // with the same id and no change tag draws 'serverRejectedRequest'.
        let serverRecord = CKRecord(
          recordType: RemindersList.tableName,
          recordID: RemindersList.recordID(for: 1)
        )
        serverRecord.setValue("Server", forKey: "title", at: 0)
        _ = try syncEngine.modifyRecords(scope: .private, saving: [serverRecord])

        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Local") }
        }

        try await withKnownIssue {
          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        } matching: { issue in
          issue.description.contains(
            "Dropped 1 unsent 'remindersLists' save after a non-retryable CloudKit error"
          )
        }
      }
    }
  }
#endif
