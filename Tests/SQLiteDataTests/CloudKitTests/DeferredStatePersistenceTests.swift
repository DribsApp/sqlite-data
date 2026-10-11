#if canImport(CloudKit)
  import CloudKit
  import SQLiteData
  import Testing

  @Suite struct DeferredStatePersistenceTests {
    typealias Deferred = DeferredStatePersistence<String, Int>

    @Test func stateIsNotHeldWhileNothingIsUnapplied() {
      var deferred = Deferred()
      let r1 = deferred.holdIfBlocked(1, scope: .private)
      #expect(r1 == false)
      let r2 = deferred.takeHeldStateIfUnblocked(scope: .private)
      #expect(r2 == nil)
      #expect(deferred.hasAnythingDeferred == false)
    }

    @Test func unappliedEventHoldsTheNewestStateOfItsScope() {
      var deferred = Deferred()
      deferred.recordUnapplied("batch-1", scope: .private)
      let r3 = deferred.holdIfBlocked(1, scope: .private)
      #expect(r3)
      let r4 = deferred.holdIfBlocked(2, scope: .private)
      #expect(r4)
      // The shared scope has its own token and is not blocked.
      let r5 = deferred.holdIfBlocked(9, scope: .shared)
      #expect(r5 == false)
      let r6 = deferred.takeHeldStateIfUnblocked(scope: .private)
      #expect(r6 == nil)

      let r7 = deferred.takeUnapplied(scope: .private)
      #expect(r7 == ["batch-1"])
      let r8 = deferred.takeHeldStateIfUnblocked(scope: .private)
      #expect(r8 == 2)
      let r9 = deferred.takeHeldStateIfUnblocked(scope: .private)
      #expect(r9 == nil)
      #expect(deferred.hasAnythingDeferred == false)
    }

    @Test func unappliedEventsComeBackInArrivalOrder() {
      var deferred = Deferred()
      deferred.recordUnapplied("a", scope: .shared)
      deferred.recordUnapplied("b", scope: .shared)
      let r10 = deferred.takeUnapplied(scope: .shared)
      #expect(r10 == ["a", "b"])
      let r11 = deferred.takeUnapplied(scope: .shared)
      #expect(r11 == [])
    }

    @Test func resetDropsEverything() {
      var deferred = Deferred()
      deferred.recordUnapplied("a", scope: .private)
      _ = deferred.holdIfBlocked(1, scope: .private)
      deferred.reset()
      #expect(deferred.hasAnythingDeferred == false)
      let r12 = deferred.takeUnapplied(scope: .private)
      #expect(r12 == [])
      let r13 = deferred.takeHeldStateIfUnblocked(scope: .private)
      #expect(r13 == nil)
    }
  }
#endif
