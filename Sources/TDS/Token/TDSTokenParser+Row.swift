extension TDSTokenParser {
    public static func parseRowToken(from buffer: inout ByteBuffer, with colMetadata: TDSTokens.ColMetadataToken) throws -> TDSTokens.RowToken {
        let allocator = ByteBufferAllocator()

        enum ColumnReadKind {
            case fixed(Int)
            case variable(nullMarker: UInt16)
            case plp
            case numeric
        }

        func readSlice(_ buf: inout ByteBuffer, length: Int) throws -> ByteBuffer {
            guard let slice = buf.readSlice(length: length) else { throw TDSError.needMoreData }
            var copy = allocator.buffer(capacity: slice.readableBytes)
            var mutable = slice
            copy.writeBuffer(&mutable)
            return copy
        }

        func readVariableLength(_ buf: inout ByteBuffer, nullMarker: UInt16) throws -> ByteBuffer? {
            guard let len: UInt16 = buf.readInteger(endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
            if len == nullMarker { return nil }
            return try readSlice(&buf, length: Int(len))
        }

        func readPLP(_ buf: inout ByteBuffer) throws -> ByteBuffer? {
            // PLP format:
            // UINT64 totalLength
            // if totalLength == 0xFFFFFFFFFFFFFFFF => NULL
            // else: repeated (UINT32 chunkLength, chunk data) until chunkLength == 0

            guard let totalLength: UInt64 = buf.readInteger(endianness: .little, as: UInt64.self) else {
                throw TDSError.needMoreData
            }

            // NULL PLP value
            if totalLength == UInt64.max {
                return nil
            }

            var result = allocator.buffer(capacity: Int(min(totalLength, UInt64(Int.max))))

            while true {
                guard let chunkLength: UInt32 = buf.readInteger(endianness: .little, as: UInt32.self) else {
                    throw TDSError.needMoreData
                }

                if chunkLength == 0 {
                    break
                }

                let slice = try readSlice(&buf, length: Int(chunkLength))
                var mutable = slice
                result.writeBuffer(&mutable)
            }

            return result
        }

        func readKind(for column: TDSTokens.ColMetadataToken.ColumnData) -> ColumnReadKind {
            switch column.dataType {
            case .tinyInt: return .fixed(1)
            case .smallInt: return .fixed(2)
            case .int: return .fixed(4)
            case .bigInt: return .fixed(8)
            case .bit: return .fixed(1)
            case .real: return .fixed(4)
            case .float: return .fixed(8)
            case .guid: return .fixed(16)
            case .numeric, .decimal: return .numeric
            case .intn, .floatn: return .numeric
            case .nvarchar, .nchar, .varchar, .char:
                if column.length >= 0xFFFF {
                    return .plp
                }
                return .variable(nullMarker: UInt16.max)
            case .varbinary, .binary, .image, .xml: return .variable(nullMarker: UInt16.max)
            default:
                if let fixed = expectedFixedSize(of: column) {
                    return .fixed(fixed)
                }
                return .variable(nullMarker: UInt16.max)
            }
        }

        func parseColumnData(
            _ buf: inout ByteBuffer,
            column: TDSTokens.ColMetadataToken.ColumnData
        ) throws -> TDSTokens.RowToken.ColumnData {

            switch readKind(for: column) {

            case .fixed(let size):
                return TDSTokens.RowToken.ColumnData(
                    data: try readSlice(&buf, length: size)
                )

            case .variable(let nullMarker):
                if let payload = try readVariableLength(&buf, nullMarker: nullMarker) {
                    return TDSTokens.RowToken.ColumnData(data: payload)
                }
                return TDSTokens.RowToken.ColumnData(data: allocator.buffer(capacity: 0))

            case .plp:
                if let plp = try readPLP(&buf) {
                    return TDSTokens.RowToken.ColumnData(data: plp)
                }
                return TDSTokens.RowToken.ColumnData(data: allocator.buffer(capacity: 0))

            case .numeric:
                guard let length: UInt8 = buf.readInteger() else {
                    throw TDSError.needMoreData
                }

                if length == 0 {
                    return TDSTokens.RowToken.ColumnData(data: allocator.buffer(capacity: 0))
                }

                let payload = try readSlice(&buf, length: Int(length))
                return TDSTokens.RowToken.ColumnData(data: payload)
            }
        }

        func expectedFixedSize(of column: TDSTokens.ColMetadataToken.ColumnData) -> Int? {
            switch column.dataType {
            case .tinyInt, .bit: 1
            case .smallInt: 2
            case .int, .real, .smallMoney, .smallDateTime: 4
            case .bigInt, .float, .money, .datetime: 8
            case .date: 3
            case .time: timePayloadLength(scale: column.scale)
            case .datetime2: timePayloadLength(scale: column.scale) + 3
            case .datetimeOffset: timePayloadLength(scale: column.scale) + 5
            case .guid: 16
            default: nil
            }
        }

        func timePayloadLength(scale: Int?) -> Int {
            switch max(0, min(scale ?? 7, 7)) {
            case 0...2: 3
            case 3...4: 4
            default: 5
            }
        }

        var colData: [TDSTokens.RowToken.ColumnData] = []
        for column in colMetadata.colData {
            colData.append(try parseColumnData(&buffer, column: column))
        }

        return TDSTokens.RowToken(colData: colData)
    }
}
