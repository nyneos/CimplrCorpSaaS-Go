package model

// StoragePutRequest is POST /v1/storage/put — save transformed (or other) file bytes
// to S3, local folder, SFTP, or an external HTTP API.
type StoragePutRequest struct {
	ServiceKey       string `json:"service_key,omitempty"`
	ContentBase64    string `json:"content_base64"`
	ContentType      string `json:"content_type,omitempty"`
	FileExt          string `json:"file_ext,omitempty"` // e.g. ".json" or "json"
	DestinationType  string `json:"destination_type"`   // S3 | LOCAL | SFTP | API
	OutputNamePrefix string `json:"output_name_prefix,omitempty"`
	AppendDatetime   bool   `json:"append_datetime"`
	S3Prefix         string `json:"s3_prefix,omitempty"`
	LocalFolder      string `json:"local_folder,omitempty"`
	SftpHost         string `json:"sftp_host,omitempty"`
	SftpPort         int    `json:"sftp_port,omitempty"`
	SftpUser         string `json:"sftp_user,omitempty"`
	SftpPassword     string `json:"sftp_password,omitempty"`
	SftpFolder       string `json:"sftp_folder,omitempty"`
	APIURL           string `json:"api_url,omitempty"`
	APIAuthToken     string `json:"api_auth_token,omitempty"`
}

// StoragePutResult is returned in the data envelope of /v1/storage/put.
type StoragePutResult struct {
	DestinationType string `json:"destination_type"`
	OutputFilename  string `json:"output_filename"`
	OutputLocation  string `json:"output_location"`
	S3Key           string `json:"s3_key,omitempty"`
}
