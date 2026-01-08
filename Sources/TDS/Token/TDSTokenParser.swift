public final class TDSTokenParser {
    private var buffer: ByteBuffer
    private var colMetadata: TDSTokens.ColMetadataToken?
    private var logger: Logger
    
    init(logger: Logger) {
        self.logger = logger
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
    }
    
    func writeAndParseTokens(_ inputBuffer: ByteBuffer, isFinalPacket: Bool = false) throws -> [TDSToken] {
        var packetMessageBuffer = inputBuffer
        buffer.writeBuffer(&packetMessageBuffer)
        let (tokens, needMoreData) = try parseTokens()
        if isFinalPacket && needMoreData {
            throw TDSError.protocolError("Unexpected end of message while parsing tokens.")
        }
        return tokens
    }
    
    func parseTokens() throws -> ([TDSToken], needMoreData: Bool) {
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
                case .row:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseRowToken(from: &buffer, with: colMetadata)
                case .nbcRow:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseNBCRowToken(from: &buffer, with: colMetadata)
                default:
                    throw TDSError.protocolError("Parsing implementation incomplete")
                }
                
                parsedTokens.append(token)
                
            } catch {
                buffer = bufferCopy
                if case TDSError.needMoreData = error {
                    return (parsedTokens, true)
                }
                throw error
            }
            
            bufferCopy = buffer
        }
        
        return (parsedTokens, false)
    }
}

extension TDSTokenParser: Sendable {}
