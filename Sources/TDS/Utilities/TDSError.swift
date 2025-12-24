import Foundation

public enum TDSError: Error, LocalizedError {
    
    case protocolError(String)
    case connectionClosed
    case invalidCredentials
    case needMoreData
    case unsupportedType(TDSDataType)

    /// See `LocalizedError`.
    public var errorDescription: String? {
        return self.description
    }
}

extension TDSError: Sendable {}

extension TDSError: CustomStringConvertible {
    
    /// See `CustomStringConvertible`.
    public var description: String {
        let description: String
        switch self {
        case .protocolError(let message):
            description = "protocol error: \(message)"
        case .connectionClosed:
            description = "connection closed"
        case .invalidCredentials:
            description = "Invalid login credentials"
        case .needMoreData:
            description = "Need more data"
        case .unsupportedType(let dataType):
            description = "Unsupported data type: \(dataType)"
        }
        return "TDS error: \(description)"
    }
}
