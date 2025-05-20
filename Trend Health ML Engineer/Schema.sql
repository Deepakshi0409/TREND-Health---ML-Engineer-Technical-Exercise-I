-- schema.sql

-- Main service request table
CREATE TABLE IF NOT EXISTS service_requests (
    unique_key TEXT PRIMARY KEY,
    created_date TEXT,
    closed_date TEXT,
    agency TEXT,
    complaint_type_id INTEGER,
    location_type TEXT,
    incident_zip TEXT,
    city TEXT,
    borough_id INTEGER,
    status TEXT,
    resolution_description TEXT,
    latitude REAL,
    longitude REAL,
    FOREIGN KEY (complaint_type_id) REFERENCES complaint_types(id),
    FOREIGN KEY (borough_id) REFERENCES boroughs(id)
);

-- Normalized complaint types
CREATE TABLE IF NOT EXISTS complaint_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    complaint_type TEXT,
    descriptor TEXT
);

-- Normalized boroughs
CREATE TABLE IF NOT EXISTS boroughs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    borough TEXT
);

-- Indexes for analytical performance
CREATE INDEX IF NOT EXISTS idx_sr_created_date ON service_requests(created_date);
CREATE INDEX IF NOT EXISTS idx_sr_status ON service_requests(status);
CREATE INDEX IF NOT EXISTS idx_sr_complaint_type_id ON service_requests(complaint_type_id);
CREATE INDEX IF NOT EXISTS idx_sr_borough_id ON service_requests(borough_id);
