import NIO
import NIOSSL
import NIOTLS
import Logging

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        logger.debug("TDSConnection.send: enqueueing request: \(String(describing: type(of: request)))")
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let ctx = TDSRequestContext(delegate: request, promise: promise)
        self.channel.writeAndFlush(ctx).cascadeFailure(to: promise)
        return promise.futureResult
    }
}

public protocol TDSRequest: Sendable {
    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket]
    func log(to logger: Logger)
}

public enum TDSPacketResponse {
    case done
    case `continue`
    case respond(with: [TDSPacket])
    case kickoffSSL
}

final class TDSRequestContext: @unchecked Sendable {
    let delegate: TDSRequest
    let promise: EventLoopPromise<Void>
    private(set) var lastError: Error? // Make lastError read-only externally

    init(delegate: TDSRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }

    func setLastError(_ error: Error?) {
        self.lastError = error // Provide a controlled way to modify lastError
    }
}

final class TDSRequestHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = TDSPacket
    typealias OutboundIn = TDSRequestContext
    typealias OutboundOut = TDSPacket
    
    /// `TDSMessage` handlers
    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    var tlsConfiguration: TLSConfiguration?
    var serverHostname: String?
    
    var sslClientHandler: NIOSSLClientHandler? // Retain as is, but ensure thread safety manually
    
    var pipelineCoordinator: PipelineOrganizationHandler!
    
    enum State: Int {
        case start
        case sentPrelogin
        case sslHandshakeStarted
        case sslHandshakeComplete
        case sentLogin
        case loggedIn
    }
    
    private var state = State.start
    
    private var queue: [TDSRequestContext]
    
    let logger: Logger
    
    var currentRequest: TDSRequestContext? {
        get {
            self.queue.first
        }
    }
    
    public init(
        logger: Logger,
        _ firstDecoder: ByteToMessageHandler<TDSPacketDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSPacketEncoder>,
        _ tlsConfiguration: TLSConfiguration? = nil,
        _ serverHostname: String? = nil
    ) {
        self.logger = logger
        self.queue = []
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let packet = self.unwrapInboundIn(data)
        guard let request = self.currentRequest else {
            self.logger.debug("Received packet but no current request — discarding packet")
            return
        }

        self.logger.debug("Handling incoming packet for request: \(String(describing: type(of: request.delegate)))")

        do {
            let response = try request.delegate.handle(packet: packet, allocator: context.channel.allocator)
            switch response {
            case .kickoffSSL:
                guard case .sentPrelogin = state else {
                    throw TDSError.protocolError("Unexpected state to initiate SSL kickoff. If encryption is negotiated, the SSL exchange should immediately follow the PRELOGIN phase.")
                }
                try sslKickoff(context: context)
            case .respond(let packets):
                try write(context: context, packets: packets, promise: nil)
                context.flush()
            case .continue:
                return
            case .done:
                cleanupRequest(request)
            }
        } catch {
            cleanupRequest(request, error: error)
        }
    }

    private func sslKickoff(context: ChannelHandlerContext) throws {
        guard let tlsConfig = tlsConfiguration else {
            throw TDSError.protocolError("Encryption was requested but a TLS Configuration was not provided.")
        }

        self.logger.debug("Initiating SSL kickoff")

        do {
            let sslContext = try NIOSSLContext(configuration: tlsConfig)
            let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
            self.sslClientHandler = sslHandler

            let coordinator = PipelineOrganizationHandler(logger: logger, firstDecoder, firstEncoder, sslHandler)
            self.pipelineCoordinator = coordinator

            context.channel.pipeline.addHandler(coordinator, position: .before(self)).whenComplete { result in
                switch result {
                case .success:
                    self.logger.debug("Pipeline coordinator added successfully")
                    context.channel.pipeline.addHandler(sslHandler, position: .after(coordinator)).whenComplete { res in
                        switch res {
                        case .success:
                            self.state = .sslHandshakeStarted
                            self.logger.debug("SSL handler added to pipeline, handshake started")
                        case .failure(let err):
                            self.logger.error("Failed to add SSL handler: \(err.localizedDescription)")
                            self.errorCaught(context: context, error: err)
                        }
                    }
                case .failure(let err):
                    self.logger.error("Failed to add pipeline coordinator: \(err.localizedDescription)")
                    self.errorCaught(context: context, error: err)
                }
            }
        } catch {
            self.logger.error("Failed to create SSL context/handler: \(error.localizedDescription)")
            throw error
        }
    }

    private func cleanupRequest(_ request: TDSRequestContext, error: Error? = nil) {
        guard !self.queue.isEmpty else {
            self.logger.error("cleanupRequest called but queue is empty")
            if let err = error {
                request.promise.fail(err)
            } else {
                request.promise.succeed(())
            }
            return
        }

        self.queue.removeFirst()
        if let error = error {
            self.logger.debug("Request failed: \(error.localizedDescription). Remaining queue size: \(self.queue.count)")
            request.promise.fail(error)
        } else {
            self.logger.debug("Request completed successfully. Remaining queue size: \(self.queue.count)")
            request.promise.succeed(())
        }
    }

    private func write(context: ChannelHandlerContext, packets: [TDSPacket], promise: EventLoopPromise<Void>?) throws {
        var packets = packets
        guard let requestType = packets.first?.type else {
            return
        }

        self.logger.debug("Preparing to send packets for request type: \(requestType) (current state: \(state))")

        switch requestType {
        case .prelogin:
            switch state {
            case .start:
                state = .sentPrelogin
            case .sentPrelogin, .sslHandshakeStarted, .sslHandshakeComplete, .sentLogin, .loggedIn:
                throw TDSError.protocolError("PRELOGIN message must be the first message sent and may only be sent once per connection.")
            }
        case .tds7Login:
            switch state {
            case .sentPrelogin, .sslHandshakeComplete:
                state = .sentLogin
            case .start, .sslHandshakeStarted, .sentLogin, .loggedIn:
                throw TDSError.protocolError("LOGIN message must follow immediately after the PRELOGIN message or (if encryption is enabled) SSL negotiation and may only be sent once per connection.")
            }
        default:
            break
        }

        if let last = packets.popLast() {
            self.logger.debug("Writing \(packets.count + 1) packet(s) for request type: \(requestType)")
            for item in packets {
                context.write(self.wrapOutboundOut(item), promise: nil)
            }
            context.write(self.wrapOutboundOut(last), promise: promise)
        } else {
            promise?.succeed(())
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)
        let prevCount = self.queue.count
        self.queue.append(request)
        self.logger.debug("Enqueued request: \(String(describing: type(of: request.delegate))). Queue size: \(self.queue.count) (was: \(prevCount))")
        do {
            let packets = try request.delegate.start(allocator: context.channel.allocator)
            try write(context: context, packets: packets, promise: promise)
            context.flush()
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        context.close(mode: mode, promise: promise)
        
        for current in self.queue {
            current.promise.fail(TDSError.connectionClosed)
        }
        self.queue = []
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print(error.localizedDescription)
        context.fireErrorCaught(error)
    }
    
    
    private func _userInboundEventTriggered(context: ChannelHandlerContext, event: Any) throws {
        if let sslHandler = sslClientHandler, let sslHandshakeComplete = event as? TLSUserEvent, case .handshakeCompleted = sslHandshakeComplete {
            self.logger.debug("Received TLS handshake completed event")
            // SSL Handshake complete
            // Remove pipeline coordinator and rearrange message encoder/decoder

            let future = EventLoopFuture.andAllSucceed([
                context.channel.pipeline.removeHandler(self.pipelineCoordinator),
                context.channel.pipeline.removeHandler(self.firstDecoder),
                context.channel.pipeline.removeHandler(self.firstEncoder),
                context.channel.pipeline.addHandler(ByteToMessageHandler(TDSPacketDecoder(logger: logger)), position: .after(sslHandler)),
                context.channel.pipeline.addHandler(MessageToByteHandler(TDSPacketEncoder(logger: logger)), position: .after(sslHandler))
            ], on: context.eventLoop)

            future.whenSuccess { _ in
                self.logger.debug("Done w/ SSL Handshake and pipeline organization")
                self.state = .sslHandshakeComplete
                if let request = self.currentRequest {
                    self.cleanupRequest(request)
                }
            }

            future.whenFailure { error in
                self.errorCaught(context: context, error: error)
            }
        }
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        do {
            try self._userInboundEventTriggered(context: context, event: event)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        
    }
}
