//
//  SQLValue.swift
//  swift-tds
//
//  Created by Damian Van de Kauter on 24/12/2025.
//

import Foundation

public enum SQLValue {
    
    case null
    case bool(Bool)
    case int8(Int8)
    case int16(Int16)
    case int32(Int32)
    case int64(Int64)
    case float(Float)
    case double(Double)
    case string(String)
    case date(Date)
}
