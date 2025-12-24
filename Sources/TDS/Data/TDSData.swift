import NIO
import Foundation

public struct TDSData {
    /// The object ID of the field's data type.
    public var metadata: Metadata

    public var value: ByteBuffer?

    public init(metadata: Metadata, value: ByteBuffer? = nil) {
        self.metadata = metadata
        self.value = value
    }
    
    public func decode() throws -> SQLValue {
        guard var value, value.readableBytes != 0 else {
            return .null
        }

        switch metadata.dataType {
        case .bit, .bitn:
            return .bool(value.readInteger() != 0)
        case .tinyInt:
            guard let int8: Int8 = value.readInteger() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .int(Int(int8))
        case .smallInt:
            guard let int16: Int16 = value.readInteger() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .int(Int(int16))
        case .int:
            guard let int32: Int32 = value.readInteger() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .int(Int(int32))
        case .bigInt:
            guard let int64: Int64 = value.readInteger() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .int(Int(int64))
        case .float, .floatn, .real:
            guard let float: Float = value.readFloat() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .float(float)
        case .decimal, .decimalLegacy, .numeric, .numericLegacy:
            guard let double: Double = value.readDouble() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .double(double)
        case .money:
            guard let double: Double = value.readMoney() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .double(double)
        case .smallMoney:
            guard let double: Double = value.readSmallMoney() else { throw TDSError.unsupportedType(metadata.dataType) }
            return .double(double)
        case .char, .varchar, .nvarchar:
            guard let string: String = value.readString(length: value.readableBytes) else { throw TDSError.unsupportedType(metadata.dataType) }
            return .string(string)
        case .datetime, .datetime2, .date, .time:
            return .date(try decodeDate(from: &value, type: metadata.dataType))
        case .null:
            return .null

        default:
            throw TDSError.unsupportedType(metadata.dataType)
        }
    }
}

extension TDSData: CustomStringConvertible, CustomDebugStringConvertible {
    
    public var description: String {
        guard let value, value.readableBytes != 0 else {
            return "<null>"
        }

        let description: String?

        switch self.metadata.dataType {
        case .bit, .bitn:
            description = self.bool?.description
        case .tinyInt:
            description = self.int8?.description
        case .smallInt:
            description = self.int16?.description
        case .int:
            description = self.int32?.description
        case .bigInt:
            description = self.int64?.description
        case .real:
            description = self.float?.description
        case .float, .floatn, .numeric, .numericLegacy, .decimal, .decimalLegacy, .smallMoney, .money, .moneyn:
            description = self.double?.description
        case .smallDateTime, .datetime, .datetimen, .date, .time, .datetime2, .datetimeOffset:
            description = self.date?.description
        case .charLegacy, .varcharLegacy, .char, .varchar, .nvarchar, .nchar, .text, .nText:
            description = self.string?.description
        case .binaryLegacy, .varbinaryLegacy, .varbinary, .binary:
            fatalError("Unimplemented")
        case .guid:
            fatalError("Unimplemented")
        case .xml, .image, .sqlVariant, .clrUdt:
            fatalError("Unimplemented")
        case .null:
            return "<null>"
        case .intn:
            switch value.readableBytes {
            case 1:
                description = self.int8?.description
            case 2:
                description = self.int16?.description
            case 4:
                description = self.int32?.description
            case 8:
                description = self.int64?.description
            default:
                fatalError("Unexpected number of readable bytes for INTNTYPE data type.")
            }
        }

        if let description {
            return "[\(self.metadata.dataType)] " + description
        } else {
            return "0x" + value.readableBytesView.hexdigest()
        }
    }

    public var debugDescription: String {
        return self.description
    }
}

extension TDSData: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        fatalError("TDSData cannot be statically represented as a single data type")
    }

    public init?(tdsData: TDSData) {
        self = tdsData
    }

    public var tdsData: TDSData? {
        return self
    }
}

fileprivate extension TDSData {
    
    func decodeDate(from buffer: inout ByteBuffer, type: TDSDataType) throws -> Date {
        switch type {

        // MARK: - DATETIME (8 bytes)
        case .datetime:
            let days: Int32 = buffer.readInteger(endianness: .little)!
            let ticks: Int32 = buffer.readInteger(endianness: .little)! // 1/300 sec

            let seconds = Double(ticks) / 300.0
            return sqlDate1900
                .addingTimeInterval(TimeInterval(days) * 86_400)
                .addingTimeInterval(seconds)

        // MARK: - SMALLDATETIME (4 bytes)
        case .smallDateTime:
            let days: Int16 = buffer.readInteger(endianness: .little)!
            let minutes: Int16 = buffer.readInteger(endianness: .little)!

            return sqlDate1900
                .addingTimeInterval(TimeInterval(days) * 86_400)
                .addingTimeInterval(TimeInterval(minutes) * 60)

        // MARK: - DATE (3 bytes)
        case .date:
            let days: Int32 = buffer.readInteger(endianness: .little, as: Int32.self)!

            return sqlDate0001
                .addingTimeInterval(TimeInterval(days) * 86_400)

        // MARK: - TIME(n)
        case .time:
            let ticks = try readVariableLengthInt(from: &buffer)
            let scale = metadata.scale ?? 7
            let seconds = Double(ticks) / pow(10, Double(scale))

            return sqlDate0001.addingTimeInterval(seconds)

        // MARK: - DATETIME2(n)
        case .datetime2:
            let timeTicks = try readVariableLengthInt(from: &buffer)
            let dateDays: Int32 = buffer.readInteger(endianness: .little)!

            let scale = metadata.scale ?? 7
            let seconds = Double(timeTicks) / pow(10, Double(scale))

            return sqlDate0001
                .addingTimeInterval(TimeInterval(dateDays) * 86_400)
                .addingTimeInterval(seconds)

        // MARK: - DATETIMEOFFSET(n)
        case .datetimeOffset:
            let timeTicks = try readVariableLengthInt(from: &buffer)
            let dateDays: Int32 = buffer.readInteger(endianness: .little)!
            let offsetMinutes: Int16 = buffer.readInteger(endianness: .little)!

            let scale = metadata.scale ?? 7
            let seconds = Double(timeTicks) / pow(10, Double(scale))

            let utcDate = sqlDate0001
                .addingTimeInterval(TimeInterval(dateDays) * 86_400)
                .addingTimeInterval(seconds)

            // Offset is applied *after* construction
            return utcDate.addingTimeInterval(TimeInterval(-offsetMinutes * 60))

        default:
            throw TDSError.unsupportedType(type)
        }
    }

    func readVariableLengthInt(from buffer: inout ByteBuffer) throws -> Int64 {
        let length = buffer.readableBytes
        guard (3...5).contains(length) else {
            throw TDSError.protocolError("Invalid TIME length: \(length)")
        }

        var value: Int64 = 0
        for i in 0..<length {
            let byte: UInt8 = buffer.readInteger()!
            value |= Int64(byte) << (8 * i)
        }
        return value
    }
}

private let sqlDate1900 = DateComponents(
    calendar: Calendar(identifier: .gregorian),
    timeZone: TimeZone(secondsFromGMT: 0),
    year: 1900, month: 1, day: 1
).date!

private let sqlDate0001 = DateComponents(
    calendar: Calendar(identifier: .gregorian),
    timeZone: TimeZone(secondsFromGMT: 0),
    year: 1, month: 1, day: 1
).date!
