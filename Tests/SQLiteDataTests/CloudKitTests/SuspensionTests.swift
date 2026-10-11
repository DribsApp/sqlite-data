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
  }
#endif
