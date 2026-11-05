#if canImport(CloudKit)
  package import CloudKit
  import StructuredQueries
  package import StructuredQueriesCore

  /// A record whose save failed with `quotaExceeded`.
  ///
  /// CKSyncEngine drops such a save from its pending changes and never
  /// retries it, so without this table an edit made while the user's iCloud
  /// storage is full would stay unsynced until the row was edited again. The
  /// sync engine re-queues these rows at every start and whenever a later
  /// save succeeds, and removes a row once its save or delete goes through.
  @Table("sqlitedata_icloud_quotaBlockedRecordIDs")
  package struct QuotaBlockedRecordID: Equatable {
    package let recordName: String
    package let zoneName: String
    package let ownerName: String
  }

  extension QuotaBlockedRecordID {
    package init(recordID: CKRecord.ID) {
      recordName = recordID.recordName
      zoneName = recordID.zoneID.zoneName
      ownerName = recordID.zoneID.ownerName
    }
    package static func findAll(_ recordIDs: some Collection<CKRecord.ID>) -> Where<
      QuotaBlockedRecordID
    > {
      let condition: QueryFragment = recordIDs.map {
        "(\(bind: $0.recordName), \(bind: $0.zoneID.zoneName), \(bind: $0.zoneID.ownerName))"
      }
      .joined(separator: ", ")
      return Self.where {
        #sql("(\($0.recordName), \($0.zoneName), \($0.ownerName)) IN (\(condition))")
      }
    }
  }
#endif
