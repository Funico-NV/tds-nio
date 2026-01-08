import Foundation
import NIO
import Logging
import Atomics

public final class TDSConnection {
    let channel: Channel
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    public var logger: Logger

    private let didClose: ManagedAtomic<Bool>

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
        self.didClose = ManagedAtomic(false)
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose.load(ordering: .relaxed) else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose.store(true, ordering: .relaxed)
        
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.eventLoop.submit {
            switch self.channel.isActive {
            case true:
                promise.succeed(())
            case false:
                self.channel.close(mode: .all, promise: promise)
            }
        }.cascadeFailure(to: promise)
        return promise.futureResult
    }
    
    deinit {
       // assert(self.didClose.load(ordering: .relaxed), "TDSConnection deinitialized before being closed.")
    }
}
