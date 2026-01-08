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
                            do {
                                let sqlValue = try data.decode()
                                sqlRow[col.colName] = sqlValue
                            } catch {
                                continuation.finish(throwing: error)
                            }
                        } else {
                            sqlRow[col.colName] = .null
                        }
                    }
                    continuation.yield(sqlRow)
            }
            self.send(request, logger: logger)
                .whenComplete { result in
                    switch result {
                    case .success:
                        continuation.finish()
                    case .failure(let error):
                        continuation.finish(throwing: error)
                    }
                }
        }
    }
    
    public func tdsQuery(_ sqlText: String) -> AsyncThrowingStream<TDSRow, Error> {
        AsyncThrowingStream { continuation in
            let request = RawSqlBatchRequest(sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText), logger: logger) { row in
                continuation.yield(row)
            }
            self.send(request, logger: logger)
                .whenComplete { result in
                    switch result {
                    case .success:
                        continuation.finish()
                    case .failure(let error):
                        continuation.finish(throwing: error)
                    }
                }
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
        let parsedTokens = try tokenParser.writeAndParseTokens(packet.messageBuffer, isFinalPacket: packet.header.status == .eom)
        try handleParsedTokens(parsedTokens)
        if packet.header.status == .eom {
            return .done
        }
        return .continue
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
            case .row, .nbcRow:
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable.load(ordering: .relaxed) else {
                    throw TDSError.protocolError("Row data received before column metadata.")
                }
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
