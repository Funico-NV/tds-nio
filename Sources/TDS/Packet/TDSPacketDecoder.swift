import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSPacket

    let logger: Logger
    
    /// Creates a new `TDSPacketDecoder`.
    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        if buffer.readableBytes >= TDSPacket.headerLength {
            guard let length: UInt16 = buffer.getInteger(at: buffer.readerIndex + 2) else {
                throw TDSError.protocolError("Invalid packet header: missing length.")
            }
            if length < TDSPacket.headerLength {
                throw TDSError.protocolError("Invalid packet length: \(length).")
            }
            if length > buffer.readableBytes {
                return .needMoreData
            }
        }

        let readableBytesBefore = buffer.readableBytes
        if let packet = TDSPacket(from: &buffer) {
            if buffer.readableBytes >= readableBytesBefore {
                throw TDSError.protocolError("Packet decoder made no progress while reading data.")
            }
            context.fireChannelRead(wrapInboundOut(packet))
            return .continue
        }

        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.debug("Decoding last")
        if buffer.readableBytes > 0 {
            throw TDSError.protocolError("Unexpected end of stream while decoding packet data.")
        }
        return .needMoreData
    }
}

extension TDSPacketDecoder: Sendable {}
