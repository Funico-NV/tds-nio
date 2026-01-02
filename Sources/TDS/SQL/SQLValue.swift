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
    case int(Int)
    case float(Float)
    case double(Double)
    case string(String)
    case date(Date)
    case uuid(UUID)
}
