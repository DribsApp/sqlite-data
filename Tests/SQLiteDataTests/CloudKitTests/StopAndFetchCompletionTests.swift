#if canImport(CloudKit)
  import CloudKit
  import Foundation
  import SQLiteData
  import Testing

  extension BaseCloudKitTests {
    @MainActor
    @Suite
    final class StopAndFetchCompletionTests: BaseCloudKitTests, @unchecked Sendable {
      /// A fetch in flight on a stopped engine would otherwise keep its caller
      /// waiting until CloudKit answers.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func stopCancelsTheStoppedEnginesOperations() async throws {
        let stoppedPrivate = syncEngine.private
        let stoppedShared = syncEngine.shared
        syncEngine.stop()
        let deadline = ContinuousClock.now + .seconds(15)
        while stoppedPrivate._cancelOperationsCount.value == 0
          || stoppedShared._cancelOperationsCount.value == 0,
          ContinuousClock.now < deadline
        {
          try await Task.sleep(for: .milliseconds(10))
        }
        #expect(stoppedPrivate._cancelOperationsCount.value == 1)
        #expect(stoppedShared._cancelOperationsCount.value == 1)

        // The engines a restart builds are left alone.
        try await syncEngine.start()
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        #expect(syncEngine.private._cancelOperationsCount.value == 0)
        #expect(syncEngine.shared._cancelOperationsCount.value == 0)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func privateDidFetchChangesCountsACompletedFetch() async throws {
        let before = syncEngine.completedPrivateFetchCount
        await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
        #expect(syncEngine.completedPrivateFetchCount == before)
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: syncEngine.private)
        #expect(syncEngine.completedPrivateFetchCount == before + 1)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func sharedDidFetchChangesIsNotCounted() async throws {
        let before = syncEngine.completedPrivateFetchCount
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: syncEngine.shared)
        #expect(syncEngine.completedPrivateFetchCount == before)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func stoppedEngineDidFetchChangesIsNotCounted() async throws {
        let stoppedEngine = syncEngine.private
        syncEngine.stop()
        try await syncEngine.start()
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        let before = syncEngine.completedPrivateFetchCount
        // A delivery still in flight from the engine `stop()` discarded.
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: stoppedEngine)
        #expect(syncEngine.completedPrivateFetchCount == before)
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: syncEngine.private)
        #expect(syncEngine.completedPrivateFetchCount == before + 1)
      }
    }
  }
#endif
