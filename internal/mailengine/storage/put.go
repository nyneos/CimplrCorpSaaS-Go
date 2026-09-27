package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"CimplrCorpSaas/internal/mailengine/model"
	"CimplrCorpSaas/internal/mailengine/s3store"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const (
	DestS3    = "S3"
	DestLocal = "LOCAL"
	DestSFTP  = "SFTP"
	DestAPI   = "API"
)

var unsafeFilenameChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// Put saves content to the configured destination and applies naming convention.
func Put(ctx context.Context, req model.StoragePutRequest) (*model.StoragePutResult, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.ContentBase64))
	if err != nil {
		return nil, fmt.Errorf("invalid content_base64: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("content_base64 is empty")
	}

	dt := strings.ToUpper(strings.TrimSpace(req.DestinationType))
	if dt == "" {
		dt = DestS3
	}
	switch dt {
	case DestS3, DestLocal, DestSFTP, DestAPI:
	default:
		return nil, fmt.Errorf("invalid destination_type %q (use S3, LOCAL, SFTP, or API)", req.DestinationType)
	}

	if dt == DestSFTP && (strings.TrimSpace(req.SftpHost) == "" || strings.TrimSpace(req.SftpUser) == "") {
		return nil, fmt.Errorf("sftp_host and sftp_user are required for SFTP")
	}
	if dt == DestAPI {
		u := strings.TrimSpace(req.APIURL)
		if u == "" {
			return nil, fmt.Errorf("api_url is required for API")
		}
		if !strings.HasPrefix(strings.ToLower(u), "http://") &&
			!strings.HasPrefix(strings.ToLower(u), "https://") {
			return nil, fmt.Errorf("api_url must start with http:// or https://")
		}
	}

	contentType := strings.TrimSpace(req.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	filename := buildOutputFilename(req.OutputNamePrefix, req.AppendDatetime, resolveFileExt(req.FileExt, contentType), time.Now())

	var location, s3Key string
	switch dt {
	case DestS3:
		s3Key, err = putS3(ctx, req.S3Prefix, filename, raw, contentType)
		location = s3Key
	case DestLocal:
		location, err = putLocal(req.LocalFolder, filename, raw)
	case DestSFTP:
		location, err = putSFTP(req, filename, raw)
	case DestAPI:
		location, err = putAPI(ctx, req, filename, raw, contentType)
	}
	if err != nil {
		return nil, err
	}

	return &model.StoragePutResult{
		DestinationType: dt,
		OutputFilename:  filename,
		OutputLocation:  location,
		S3Key:           s3Key,
	}, nil
}

func buildOutputFilename(prefix string, appendDatetime bool, ext string, at time.Time) string {
	ext = normalizeExt(ext)
	if ext == "" {
		ext = ".bin"
	}
	base := sanitizeFilenameBase(prefix)
	if base == "" {
		base = "transformed_" + shortID()
	}
	if appendDatetime {
		base = base + "_" + at.Format("20060102_150405")
	}
	return base + ext
}

func normalizeExt(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ""
	}
	if !strings.HasPrefix(v, ".") {
		v = "." + v
	}
	return v
}

// resolveFileExt picks extension from explicit file_ext, else infers from Content-Type (JSON mappings → .json).
func resolveFileExt(fileExt, contentType string) string {
	ext := normalizeExt(fileExt)
	if ext != "" && ext != ".bin" {
		return ext
	}
	ct := strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case strings.Contains(ct, "json"):
		return ".json"
	case strings.Contains(ct, "xml"):
		return ".xml"
	case strings.Contains(ct, "csv"):
		return ".csv"
	case strings.Contains(ct, "text/plain"):
		return ".txt"
	}
	if ext != "" {
		return ext
	}
	return ".json"
}

func sanitizeFilenameBase(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = unsafeFilenameChars.ReplaceAllString(name, "_")
	name = strings.Trim(name, "._-")
	if name == "" {
		return ""
	}
	if len(name) > 120 {
		name = name[:120]
	}
	return name
}

func shortID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano()%1e8)
	}
	return hex.EncodeToString(b[:])
}

func localBaseDir() string {
	dir := strings.TrimSpace(os.Getenv("EMAIL_TRANSFORMED_LOCAL_DIR"))
	if dir == "" {
		dir = "./transformed"
	}
	return dir
}

func putS3(ctx context.Context, prefix, filename string, body []byte, contentType string) (string, error) {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		prefix = "email/transformed/" + time.Now().UTC().Format("2006/01/02")
	}
	key := strings.TrimSuffix(prefix, "/") + "/" + filename
	if err := s3store.PutObject(ctx, key, body, contentType); err != nil {
		return "", fmt.Errorf("s3 put: %w", err)
	}
	return key, nil
}

func putLocal(subdir, filename string, body []byte) (string, error) {
	base := localBaseDir()
	subdir = strings.Trim(strings.TrimSpace(subdir), "/")
	dir := base
	if subdir != "" {
		dir = filepath.Join(base, filepath.FromSlash(subdir))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir local dir: %w", err)
	}
	full := filepath.Join(dir, filename)
	if err := os.WriteFile(full, body, 0o644); err != nil {
		return "", fmt.Errorf("write local file: %w", err)
	}
	abs, err := filepath.Abs(full)
	if err != nil {
		return full, nil
	}
	return abs, nil
}

func putSFTP(req model.StoragePutRequest, filename string, body []byte) (string, error) {
	host := strings.TrimSpace(req.SftpHost)
	user := strings.TrimSpace(req.SftpUser)
	port := req.SftpPort
	if port <= 0 {
		port = 22
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(req.SftpPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // tenant-configured hosts
		Timeout:         30 * time.Second,
	}
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return "", fmt.Errorf("sftp ssh dial: %w", err)
	}
	defer conn.Close()

	client, err := sftp.NewClient(conn)
	if err != nil {
		return "", fmt.Errorf("sftp client: %w", err)
	}
	defer client.Close()

	folder := strings.Trim(strings.TrimSpace(req.SftpFolder), "/")
	remotePath := filename
	if folder != "" {
		_ = client.MkdirAll(folder)
		remotePath = folder + "/" + filename
	}
	f, err := client.Create(remotePath)
	if err != nil {
		return "", fmt.Errorf("sftp create %s: %w", remotePath, err)
	}
	defer f.Close()
	if _, err := f.Write(body); err != nil {
		return "", fmt.Errorf("sftp write: %w", err)
	}
	return fmt.Sprintf("sftp://%s/%s", addr, remotePath), nil
}

func putAPI(ctx context.Context, req model.StoragePutRequest, filename string, body []byte, contentType string) (string, error) {
	apiURL := strings.TrimSpace(req.APIURL)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", filename)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(body); err != nil {
		return "", err
	}
	_ = w.WriteField("filename", filename)
	if contentType != "" {
		_ = w.WriteField("content_type", contentType)
	}
	_ = w.Close()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, &buf)
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", w.FormDataContentType())
	if token := strings.TrimSpace(req.APIAuthToken); token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("api post: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := string(respBody)
		if len(msg) > 400 {
			msg = msg[:400]
		}
		return "", fmt.Errorf("api status %d: %s", resp.StatusCode, msg)
	}
	return fmt.Sprintf("api:%s status=%d", apiURL, resp.StatusCode), nil
}
