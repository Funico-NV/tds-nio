import Logging
import NIO
import Foundation
import Atomics

extension TDSConnection {
    
    public func query(_ sqlText: String) -> AsyncThrowingStream<SQLRow, Error> {
        AsyncThrowingStream { continuation in
            let request = RawSqlBatchRequest(sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText), logger: logger) { row in
                var sqlRow: SQLRow = [:]
                for col in row.columnMetadata.colData {
                    if let data = row.column(col.colName) {
                        sqlRow[col.colName] = try data.decode()
                    } else {
                        sqlRow[col.colName] = .null
                    }
                }
                continuation.yield(sqlRow)
            }
            
            self.send(request, logger: logger)
                .whenComplete { _ in continuation.finish() }
        }
    }
    
    public func query(_ sqlText: String) -> AsyncStream<TDSRow> {
        AsyncStream { continuation in
            let request = RawSqlBatchRequest(sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText), logger: logger) { row in
                continuation.yield(row)
            }
            
            self.send(request, logger: logger)
                .whenComplete { _ in continuation.finish() }
        }
    }
}

final class RawSqlBatchRequest: TDSRequest, Sendable {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    let onRow: @Sendable (TDSRow) throws -> ()
    let rowLookupTable: ManagedAtomic<TDSRow.LookupTable?>
    
    private let logger: Logger
    private let tokenParser: TDSTokenParser

    init(sqlBatch: TDSMessages.RawSqlBatchMessage, logger: Logger, _ onRow: @escaping @Sendable (TDSRow) throws -> ()) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
        self.rowLookupTable = ManagedAtomic(nil)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let parsedTokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        try handleParsedTokens(parsedTokens)
        guard packet.header.status == .eom else {
            return .continue
        }

        return .done
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        return try TDSMessage(payload: sqlBatch, allocator: allocator).packets
    }

    func log(to logger: Logger) {

    }
    
    func handleParsedTokens(_ tokens: [TDSToken]) throws {
        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in tokens {
            switch token.type {
            case .row:
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable.load(ordering: .relaxed) else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
                try onRow(row)
            case .colMetadata:
                guard let colMetadataToken = token as? TDSTokens.ColMetadataToken else {
                    throw TDSError.protocolError("Error reading column metadata token.")
                }
                self.rowLookupTable.store(TDSRow.LookupTable(colMetadata: colMetadataToken), ordering: .relaxed)
            default:
                break
            }
        }
    }
}

