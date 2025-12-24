import Atomics
import Foundation

public struct TDSRow {
    
    final class LookupTable: Sendable, AtomicReference {
        let colMetadata: TDSTokens.ColMetadataToken

        struct Value: Sendable {
            let index: Int
            let colData: TDSTokens.ColMetadataToken.ColumnData
        }

        let storage: [String: Value]

        init(colMetadata: TDSTokens.ColMetadataToken) {
            self.colMetadata = colMetadata
            let all = colMetadata.colData.enumerated().map { (index, colData) in
                (colData.colName, Value(index: index, colData: colData))
            }
            self.storage = [String: Value](all) { a, b in a }
        }

        func lookup(column: String) -> Value? {
            if let value = self.storage[column] {
                return value
            } else {
                return nil
                
            }
        }
    }

    public let dataRow: TDSTokens.RowToken

    public var columnMetadata: TDSTokens.ColMetadataToken {
        self.lookupTable.colMetadata
    }

    let lookupTable: LookupTable

    public func column(_ column: String) -> TDSData? {
        guard let entry = self.lookupTable.lookup(column: column) else {
            return nil
        }

        return TDSData(
            metadata: entry.colData,
            value: dataRow.colData[entry.index].data
        )
    }
}

extension TDSRow: Sendable {}

extension TDSRow: CustomStringConvertible {
    
    public var description: String {
        var row: [String: TDSData] = [:]
        for col in self.columnMetadata.colData {
            row[col.colName] = self.column(col.colName)
        }
        return row.description
    }
}

extension TDSRow {
    
    public var jsonData: Data {
        get throws {
            var row: [String: Any] = [:]
            for col in self.columnMetadata.colData {
                if let jsonValue = self.column(col.colName)?.jsonValue {
                    row[col.colName] = jsonValue
                }
            }
            return try JSONSerialization.data(withJSONObject: row, options: [])
        }
    }
}
