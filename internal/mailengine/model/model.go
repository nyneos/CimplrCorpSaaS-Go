package model

import "time"

type Envelope struct {
	From            string   `json:"from"`
	To              []string `json:"to"`
	Cc              []string `json:"cc"`
	Subject         string   `json:"subject"`
	Date            string   `json:"date"`
	MessageIDHeader string   `json:"message_id_header"`
	InReplyTo       string   `json:"in_reply_to,omitempty"`
	References      string   `json:"references,omitempty"`
}

type Body struct {
	TextPlain string `json:"text_plain"`
	TextHTML  string `json:"text_html"`
	Preferred string `json:"preferred"`
}

type Attachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	S3Key       string `json:"s3_key"`
	SHA256      string `json:"sha256"`
}

type ParsedEmail struct {
	MessageID         string                 `json:"message_id"`
	S3RawKey          string                 `json:"s3_raw_key"`
	S3ParsedKey       string                 `json:"s3_parsed_key"`
	Envelope          Envelope               `json:"envelope"`
	Body              Body                   `json:"body"`
	Attachments       []Attachment           `json:"attachments"`
	Headers           map[string]string      `json:"headers,omitempty"`
	ExtractedMetadata map[string]interface{} `json:"extracted_metadata,omitempty"`
	ParsedAt          time.Time              `json:"parsed_at"`
	Status            string                 `json:"status"`
}

type ParseRequest struct {
	S3RawKey string `json:"s3_raw_key"`
}

type ListNewRequest struct {
	After string `json:"after"`
	Limit int32  `json:"limit"`
}

type ParseBatchRequest struct {
	S3RawKeys []string `json:"s3_raw_keys"`
}

type ParseBatchResponse struct {
	Results []ParsedEmail `json:"results"`
	Errors  []string      `json:"errors,omitempty"`
}

type ExtractRequest struct {
	S3ParsedKey string `json:"s3_parsed_key"`
	Module      string `json:"module"`
}

type ExtractResponse struct {
	Intent            string                 `json:"intent"`
	ExtractedMetadata map[string]interface{} `json:"extracted_metadata"`
	Confidence        float64                `json:"confidence"`
}
