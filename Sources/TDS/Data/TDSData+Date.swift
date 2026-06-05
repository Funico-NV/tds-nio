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
            
            let secondsSinceMidnight = Double(minutesElapsed) * 60
            return _tdsDateSinceJan1900(
                days: Int(daysSinceJan1900),
                secondsSinceMidnight: secondsSinceMidnight,
                calendar: _tdsCalendar
            )
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
            
            let secondsSinceMidnight = Double(oneThreeHundrethsOfASecondElapsed) / 300
            return _tdsDateSinceJan1900(
                days: Int(daysSinceJan1900),
                secondsSinceMidnight: secondsSinceMidnight,
                calendar: _tdsCalendar
            )
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
            
            let secondsSinceMidnight = Double(oneThreeHundrethsOfASecondElapsed) / 300
            return _tdsDateSinceJan1900(
                days: Int(daysSinceJan1900),
                secondsSinceMidnight: secondsSinceMidnight,
                calendar: _tdsCalendar
            )
        case .date:
            return value.readDate()
        case .time:
            // time alone cannot be accurately represented with Swift's Date type
            return nil
        case .datetime2:
            return value.readDatetime2(bytes: value.readableBytes - 3, scale: metadata.scale, calendar: _tdsCalendar)
        case .datetimeOffset:
            // datetimeoffset(n) is represented as a concatenation of datetime2(n) followed by one 2-byte signed integer that represents the time zone offset as the number of minutes from UTC. The time zone offset MUST be between -840 and 840.
            guard
                let localDateTime = value.readDatetime2(bytes: value.readableBytes - 5, scale: metadata.scale, calendar: _tdsUTCCalendar),
                let timezoneOffset = value.readInteger(endianness: .little, as: Int16.self),
                timezoneOffset >= -840 && timezoneOffset <= 840
            else {
                return nil
            }
            
            // `datetimeoffset` stores the local wall-clock date/time together with the
            // offset from UTC. `readDatetime2` reconstructs that wall-clock value on a
            // UTC calendar, so convert it to the absolute instant by subtracting the
            // stored offset.
            return localDateTime.addingTimeInterval(TimeInterval(-Int(timezoneOffset) * 60))
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
        guard var secondIncrements: Int = self.readByteLengthInteger(length: length), let scale = scale else {
            return nil
        }
        
        if scale < 7 {
            for _ in scale..<7 {
                secondIncrements = secondIncrements * 10
            }
        }
        
        return DateComponents.init(nanosecond: secondIncrements * 100)
    }
    
    /// represented as one 3-byte unsigned integer that represents the number of days since January 1, year 1.
    fileprivate mutating func readDate() -> Date? {
        
        guard let daysSinceJan1: UInt32 = self.readByteLengthInteger(length: 3) else {
            return nil
        }
        
        return _tdsDateSinceJan1(days: Int(daysSinceJan1), calendar: _tdsCalendar)
    }
    
    /// datetime2(n) is represented as a concatenation of time(n) followed by date as specified above.
    fileprivate mutating func readDatetime2(bytes length: Int, scale: Int?, calendar: Calendar) -> Date? {
        
        guard
            let nanoseconds = self.readTimeComponents(bytes: length, scale: scale),
            let date = self.readDate()
        else {
            return nil
        }
        
        return calendar.date(byAdding: nanoseconds, to: date)
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

private let _tdsCalendar: Calendar = {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = .current
    return calendar
}()

private let _tdsUTCCalendar: Calendar = {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? .gmt
    return calendar
}()

private let _jan1 = _tdsReferenceDate(year: 1, calendar: _tdsCalendar)
private let _jan1900 = _tdsReferenceDate(year: 1900, calendar: _tdsCalendar)

private func _tdsReferenceDate(year: Int, calendar: Calendar) -> Date {
    guard let date = calendar.date(from: DateComponents(year: year, month: 1, day: 1)) else {
        preconditionFailure("Unable to create TDS reference date for year \(year).")
    }
    return date
}

private func _tdsDateSinceJan1(days: Int, calendar: Calendar) -> Date? {
    calendar.date(byAdding: .day, value: days, to: _tdsReferenceDate(year: 1, calendar: calendar))
}

private func _tdsDateSinceJan1900(days: Int, secondsSinceMidnight: Double, calendar: Calendar) -> Date? {
    guard let date = calendar.date(byAdding: .day, value: days, to: _tdsReferenceDate(year: 1900, calendar: calendar)) else {
        return nil
    }
    return date.addingTimeInterval(secondsSinceMidnight)
}
