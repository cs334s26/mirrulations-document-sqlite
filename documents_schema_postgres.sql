CREATE TABLE IF NOT EXISTS documents (
    -- Primary key
    document_id VARCHAR(50) NOT NULL PRIMARY KEY,
    document_api_link TEXT NOT NULL UNIQUE,

    -- Address fields
    address1 VARCHAR(200),
    address2 VARCHAR(200),
    city VARCHAR(100),
    state_province_region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),

    -- Agency and docket references
    agency_id VARCHAR(20) NOT NULL,
    docket_id VARCHAR(50) NOT NULL,

    -- Document metadata
    document_type VARCHAR(30) NOT NULL,
    document_title TEXT,
    subtype VARCHAR(100),
    object_id VARCHAR(50),
    page_count INTEGER,
    paper_length INTEGER,
    paper_width INTEGER,
    doc_abstract TEXT,
    subject TEXT,
    start_end_page TEXT,
    authors JSONB,
    additional_rins JSONB,

    -- CFR / Federal Register citation
    cfr_part TEXT,
    cfr_part_normalized JSONB NOT NULL DEFAULT '{"raw": null, "status": "empty", "references": []}'::jsonb,
    cfr_part_parse_status VARCHAR(20) NOT NULL DEFAULT 'empty',
    fr_doc_num VARCHAR(20),
    fr_vol_num VARCHAR(20),

    -- Comment-specific fields
    comment TEXT,
    comment_category VARCHAR(200),

    -- Submitter information
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(320),
    phone VARCHAR(20),
    fax VARCHAR(20),
    submitter_org VARCHAR(200),
    submitter_gov_agency VARCHAR(300),
    submitter_gov_agency_type VARCHAR(50),
    submitter_rep TEXT,

    -- Dates (ISO-8601 strings from source API)
    author_date TEXT,
    comment_start_date TEXT,
    comment_end_date TEXT,
    effective_date TEXT,
    implementation_date TEXT,
    modify_date TEXT,
    posted_date TEXT,
    postmark_date TEXT,
    receive_date TEXT,

    -- Boolean flags
    is_late_comment BOOLEAN,
    is_open_for_comment BOOLEAN NOT NULL DEFAULT FALSE,
    is_withdrawn BOOLEAN NOT NULL DEFAULT FALSE,
    within_comment_period BOOLEAN,

    -- Withdrawal/restriction info
    reason_withdrawn VARCHAR(1000),
    restriction_reason VARCHAR(1000),
    restriction_reason_type VARCHAR(20),

    -- Agency-specific / citation
    flex_field1 TEXT,
    flex_field2 TEXT,
    reg_writer_instruction TEXT,
    legacy_id VARCHAR(100),
    original_document_id VARCHAR(100),
    tracking_nbr VARCHAR(50),
    exhibit_location TEXT,
    exhibit_type VARCHAR(100),
    media JSONB,
    omb_approval TEXT,
    source_citation TEXT,

    -- Topics
    topics JSONB
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_documents_agency_id ON documents(agency_id);
CREATE INDEX IF NOT EXISTS idx_documents_docket_id ON documents(docket_id);
CREATE INDEX IF NOT EXISTS idx_documents_posted_date ON documents(posted_date);
CREATE INDEX IF NOT EXISTS idx_documents_comment_end_date ON documents(comment_end_date);
CREATE INDEX IF NOT EXISTS idx_documents_document_type ON documents(document_type);
CREATE INDEX IF NOT EXISTS idx_documents_cfr_part_parse_status ON documents(cfr_part_parse_status);
CREATE INDEX IF NOT EXISTS idx_documents_cfr_part_normalized_gin ON documents USING GIN (cfr_part_normalized);
