import Foundation
import NIO

/// Date/Times
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d

extension TDSData {
    public init(date: Date) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeDatetimeOffset(date: date)
        self.init(metadata: Date.tdsMetadata, value: buffer)
    }
    
    public var date: Date? {
        guard var value = self.value else {
            return nil
        }
        
#if DEBUG
        let debugBytes = value.getBytes(at: value.readerIndex, length: value.readableBytes) ?? []
        let debugHexBytes = debugBytes.map { String(format: "%02X", $0) }.joined(separator: " ")
        print("""
        TDS date decode START
          type: \(self.metadata.dataType)
          scale: \(String(describing: self.metadata.scale))
          readableBytes: \(value.readableBytes)
          readerIndex: \(value.readerIndex)
          rawBytes: \(debugHexBytes)
          currentTimeZone: \(TimeZone.current.identifier) secondsFromGMT: \(TimeZone.current.secondsFromGMT())
        """)
#endif
        
        switch self.metadata.dataType {
        case .smallDateTime:
            guard
                value.readableBytes == 4,
                // One 2-byte unsigned integer that represents the number of days since January 1, 1900.
                let daysSinceJan1900 = value.readInteger(endianness: .little, as: UInt16.self),
                // One 2-byte unsigned integer that represents the number of minutes elapsed since 12 AM that day.
                let minutesElapsed = value.readInteger(endianness: .little, as: UInt16.self)
            else {
                return nil
            }
            
            var secondsSinceJan1900 = Int64(daysSinceJan1900) * _secondsInDay
            secondsSinceJan1900 += Int64(minutesElapsed) * 60
            
            let result = Date(timeInterval: Double(secondsSinceJan1900), since: _jan1)
#if DEBUG
            print("""
            TDS smallDateTime decode
              daysSinceJan1900: \(daysSinceJan1900)
              minutesElapsed: \(minutesElapsed)
              secondsSinceJan1900: \(secondsSinceJan1900)
              result: \(result)
              resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
              resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
            """)
#endif
            return result
        case .datetime:
            guard
                value.readableBytes == 8,
                // One 4-byte signed integer that represents the number of days since January 1, 1900. Negative numbers are allowed to represent dates since January 1, 1753.
                let daysSinceJan1900 = value.readInteger(endianness: .little, as: Int32.self),
                // One 4-byte unsigned integer that represents the number of one three-hundredths of a second (300 counts per second) elapsed since 12 AM that day.
                let oneThreeHundrethsOfASecondElapsed = value.readInteger(endianness: .little, as: UInt32.self)
            else {
                return nil
            }
            
            let secondsSinceJan1900 = Int64(daysSinceJan1900) * _secondsInDay
            let secondsSinceMidnight = Double(oneThreeHundrethsOfASecondElapsed) / 300
            let interval = Double(secondsSinceJan1900) + secondsSinceMidnight
            
            let result = Date(timeInterval: interval, since: _jan1900)
#if DEBUG
            print("""
            TDS datetime decode
              daysSinceJan1900: \(daysSinceJan1900)
              oneThreeHundrethsOfASecondElapsed: \(oneThreeHundrethsOfASecondElapsed)
              secondsSinceJan1900: \(secondsSinceJan1900)
              secondsSinceMidnight: \(secondsSinceMidnight)
              interval: \(interval)
              result: \(result)
              resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
              resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
            """)
#endif
            return result
        case .datetimen:
            guard
                value.readableBytes == 8,
                // One 4-byte signed integer that represents the number of days since January 1, 1900. Negative numbers are allowed to represent dates since January 1, 1753.
                let daysSinceJan1900 = value.readInteger(endianness: .little, as: Int32.self),
                // One 4-byte unsigned integer that represents the number of one three-hundredths of a second (300 counts per second) elapsed since 12 AM that day.
                let oneThreeHundrethsOfASecondElapsed = value.readInteger(endianness: .little, as: UInt32.self)
            else {
                return nil
            }
            
            let secondsSinceJan1900 = Int64(daysSinceJan1900) * _secondsInDay
            let secondsSinceMidnight = Double(oneThreeHundrethsOfASecondElapsed) / 300
            let interval = Double(secondsSinceJan1900) + secondsSinceMidnight
            
            let result = Date(timeInterval: interval, since: _jan1900)
#if DEBUG
            print("""
            TDS datetimen decode
              daysSinceJan1900: \(daysSinceJan1900)
              oneThreeHundrethsOfASecondElapsed: \(oneThreeHundrethsOfASecondElapsed)
              secondsSinceJan1900: \(secondsSinceJan1900)
              secondsSinceMidnight: \(secondsSinceMidnight)
              interval: \(interval)
              result: \(result)
              resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
              resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
            """)
#endif
            return result
        case .date:
            let result = value.readDate()
#if DEBUG
            if let result {
                print("""
                TDS date decode
                  result: \(result)
                  resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
                  resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
                """)
            } else {
                print("TDS date decode -> nil")
            }
#endif
            return result
        case .time:
            // time alone cannot be accurately represented with Swift's Date type
            return nil
        case .datetime2:
            let timeByteLength = value.readableBytes - 3
#if DEBUG
            print("TDS datetime2 branch -> timeByteLength: \(timeByteLength)")
#endif
            let result = value.readDatetime2(bytes: timeByteLength, scale: metadata.scale)
#if DEBUG
            if let result {
                print("""
                TDS datetime2 result
                  result: \(result)
                  resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
                  resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
                """)
            } else {
                print("TDS datetime2 result -> nil")
            }
#endif
            return result
        case .datetimeOffset:
            // datetimeoffset(n) is represented as a concatenation of datetime2(n) followed by one 2-byte signed integer that represents the time zone offset as the number of minutes from UTC. The time zone offset MUST be between -840 and 840.
            guard
                let localDateTime = value.readDatetime2(bytes: value.readableBytes - 5, scale: metadata.scale),
                let timezoneOffset = value.readInteger(endianness: .little, as: Int16.self),
                timezoneOffset >= -840 && timezoneOffset <= 840
            else {
                return nil
            }
            
#if DEBUG
            print("""
            TDS datetimeoffset intermediate
              localDateTime: \(localDateTime)
              localDateTimeISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: localDateTime))
              localDateTimeLocal: \(_tdsDebugLocalFormatter.string(from: localDateTime))
              timezoneOffsetMinutes: \(timezoneOffset)
              timezoneOffsetSeconds: \(Int(timezoneOffset) * 60)
            """)
#endif
            
            // `datetimeoffset` stores the local wall-clock date/time together with the
            // offset from UTC. `readDatetime2` reconstructs that wall-clock value on a
            // UTC calendar, so convert it to the absolute instant by subtracting the
            // stored offset.
            let result = localDateTime.addingTimeInterval(TimeInterval(-Int(timezoneOffset) * 60))
#if DEBUG
            print("""
            TDS datetimeoffset result
              result: \(result)
              resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
              resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
            """)
#endif
            return result
        default:
            return nil
        }
    }
}

extension ByteBuffer {
    fileprivate mutating func writeDatetimeOffset(date: Date) {
        let components = Calendar.current.dateComponents([.nanosecond, .timeZone], from: date)
        guard
            let nanoseconds = components.nanosecond,
            let secondsFromUTC = components.timeZone?.secondsFromGMT()
        else {
            return
        }
        
        // 10-n second increments since 12 AM within a day (5 bytes)
        let secondIncrements = UInt64(nanoseconds / 100)
        // The number of days since January 1, year 1. (3 bytes)
        let daysSinceJan1 = Int(date.timeIntervalSince(_jan1) / Double(_secondsInDay))
        // The time zone offset as the number of minutes from UTC (2 bytes)
        let minutesFromUTC = Int16(secondsFromUTC / 60)
        
#warning("TODO: Should only write 5 bytes")
        self.writeInteger(secondIncrements)
#warning("TODO: Should only write 3 bytes")
        self.writeInteger(daysSinceJan1)
        self.writeInteger(minutesFromUTC)
    }
    
    /// time(n) is represented as one unsigned integer that represents the number of 10-n second increments since 12 AM within a day.
    /// The length, in bytes, of that integer depends on the scale n as follows:
    /// * 3 bytes if 0 <= n < = 2.
    /// * 4 bytes if 3 <= n < = 4.
    /// * 5 bytes if 5 <= n < = 7.
    ///
    fileprivate mutating func readTimeComponents(bytes length: Int, scale: Int?) -> DateComponents? {
        let startReaderIndex = self.readerIndex
        guard var secondIncrements: Int = self.readByteLengthInteger(length: length), let scale = scale else {
#if DEBUG
            print("TDS time decode -> nil, length: \(length), scale: \(String(describing: scale)), startReaderIndex: \(startReaderIndex), endReaderIndex: \(self.readerIndex)")
#endif
            return nil
        }
        
#if DEBUG
        let rawSecondIncrements = secondIncrements
#endif
        
        if scale < 7 {
            for _ in scale..<7 {
                secondIncrements = secondIncrements * 10
            }
        }
        
        let nanoseconds = secondIncrements * 100
#if DEBUG
        print("""
        TDS time decode
          length: \(length)
          scale: \(scale)
          startReaderIndex: \(startReaderIndex)
          endReaderIndex: \(self.readerIndex)
          rawSecondIncrements: \(rawSecondIncrements)
          normalizedSecondIncrements: \(secondIncrements)
          nanoseconds: \(nanoseconds)
        """)
#endif
        return DateComponents(nanosecond: nanoseconds)
    }
    
    /// represented as one 3-byte unsigned integer that represents the number of days since January 1, year 1.
    fileprivate mutating func readDate() -> Date? {
        
        let startReaderIndex = self.readerIndex
        guard let daysSinceJan1: UInt32 = self.readByteLengthInteger(length: 3) else {
#if DEBUG
            print("TDS date-only component decode -> nil, startReaderIndex: \(startReaderIndex), endReaderIndex: \(self.readerIndex)")
#endif
            return nil
        }
        
        let secondsSinceJan1 = Int64(daysSinceJan1) * _secondsInDay
        let result = Date(timeInterval: Double(secondsSinceJan1), since: _jan1)
#if DEBUG
        print("""
        TDS date-only component decode
          startReaderIndex: \(startReaderIndex)
          endReaderIndex: \(self.readerIndex)
          daysSinceJan1: \(daysSinceJan1)
          secondsSinceJan1: \(secondsSinceJan1)
          result: \(result)
          resultISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: result))
          resultLocal: \(_tdsDebugLocalFormatter.string(from: result))
        """)
#endif
        return result
    }
    
    /// datetime2(n) is represented as a concatenation of time(n) followed by date as specified above.
    fileprivate mutating func readDatetime2(bytes length: Int, scale: Int?) -> Date? {
        
        guard
            let nanoseconds = self.readTimeComponents(bytes: length, scale: scale),
            let date = self.readDate()
        else {
            return nil
        }
        
        let decodedDate = _tdsCalendar.date(byAdding: nanoseconds, to: date)
#if DEBUG
        if let decodedDate {
            print("""
            TDS datetime2 decode
              timeByteLength: \(length)
              scale: \(String(describing: scale))
              baseDate: \(date)
              baseDateISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: date))
              baseDateLocal: \(_tdsDebugLocalFormatter.string(from: date))
              nanoseconds: \(String(describing: nanoseconds.nanosecond))
              decodedDate: \(decodedDate)
              decodedDateISO8601UTC: \(_tdsDebugISO8601Formatter.string(from: decodedDate))
              decodedDateLocal: \(_tdsDebugLocalFormatter.string(from: decodedDate))
            """)
        } else {
            print("TDS datetime2 decode -> nil, timeByteLength: \(length), scale: \(String(describing: scale)), baseDate: \(date), nanoseconds: \(String(describing: nanoseconds.nanosecond))")
        }
#endif
        return decodedDate
    }
}

extension Date: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .datetimeOffset, scale: 7)
    }
    
    public init?(tdsData: TDSData) {
        guard let date = tdsData.date else {
            return nil
        }
        self = date
    }
    
    public var tdsData: TDSData? {
        return .init(date: self)
    }
}

// MARK: Private
private let _microsecondsPerSecond: Int64 = 1_000_000
private let _secondsInDay: Int64 = 24 * 60 * 60
private let _jan1 = Date(timeIntervalSince1970: -62_135_742_702)
private let _jan1900 = Date(timeIntervalSince1970: -2_208_963_600)

private let _tdsCalendar: Calendar = {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0)!
    return calendar
}()

private let _tdsDebugISO8601Formatter: ISO8601DateFormatter = {
    let formatter = ISO8601DateFormatter()
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter
}()

private let _tdsDebugLocalFormatter: DateFormatter = {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = .current
    formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS ZZZZZ"
    return formatter
}()
