public final class TDSTokenParser {
    private var buffer: ByteBuffer
    private var colMetadata: TDSTokens.ColMetadataToken?
    private var logger: Logger
    
    init(logger: Logger) {
        self.logger = logger
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
    }
    
    func writeAndParseTokens(_ inputBuffer: ByteBuffer) throws -> [TDSToken] {
        var packetMessageBuffer = inputBuffer
        buffer.writeBuffer(&packetMessageBuffer)
        return try parseTokens()
    }
    
    func parseTokens() throws -> [TDSToken] {
        var bufferCopy = buffer
        var parsedTokens: [TDSToken] = []
        while buffer.readableBytes > 0 {
            do {
                var token: TDSToken
                guard
                    let tokenByte = buffer.readByte(),
                    let tokenType = TDSTokens.TokenType(rawValue: tokenByte)
                else {
                    throw TDSError.protocolError("Parsed unknown token type.")
                }
                
                switch tokenType {
                case .error, .info:
                    token = try TDSTokenParser.parseErrorInfoToken(type: tokenType, from: &buffer)
                case .loginAck:
                    token = try TDSTokenParser.parseLoginAckToken(from: &buffer)
                case .envchange:
                    token = try TDSTokenParser.parseEnvChangeToken(from: &buffer)
                case .done, .doneInProc, .doneProc :
                    token = try TDSTokenParser.parseDoneToken(from: &buffer)
                case .colMetadata:
                    let colMetadataToken = try TDSTokenParser.parseColMetadataToken(from: &buffer)
                    colMetadata = colMetadataToken
                    token = colMetadataToken
                case .nbcRow:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseNbcRowToken(from: &buffer, with: colMetadata)
                case .row:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseRowToken(from: &buffer, with: colMetadata)
                default:
                    throw TDSError.protocolError("Parsing implementation incomplete")
                }
                
                parsedTokens.append(token)
                
            } catch {
                buffer = bufferCopy
                // Only wait for more bytes when we genuinely ran out of data.
                // Any other parsing error means the stream cannot make forward progress,
                // so surface it to the caller instead of silently stalling.
                if case TDSError.needMoreData = error {
                    return parsedTokens
                } else {
                    throw error
                }
            }
            
            bufferCopy = buffer
        }
        
        return parsedTokens
    }
}

extension TDSTokenParser: Sendable {}
