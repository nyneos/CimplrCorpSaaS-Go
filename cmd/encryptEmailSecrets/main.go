// Command encryptEmailSecrets re-encrypts or decrypts email credential columns.
//
//	go run ./cmd/encryptEmailSecrets -dry-run
//	go run ./cmd/encryptEmailSecrets
//	go run ./cmd/encryptEmailSecrets -decrypt   # revert enc1: → plaintext
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/secretaes"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "count and print only; do not UPDATE")
	decrypt := flag.Bool("decrypt", false, "revert enc1: ciphertext back to plaintext")
	flag.Parse()

	_ = godotenv.Load(".env")
	_ = godotenv.Load("../.env")

	if strings.TrimSpace(os.Getenv("SECRETS_ENC_KEY")) == "" {
		log.Fatal("SECRETS_ENC_KEY is required (base64 32-byte AES key)")
	}

	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	if host == "" || user == "" || name == "" {
		log.Fatal("DB_HOST/DB_USER/DB_NAME required")
	}
	dsn := dbutil.BuildPostgresDSN(user, pass, host, port, name, dbutil.EffectiveSSLMode(host))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	type job struct {
		label   string
		selectQ string
		updateQ string
		enc     bool // true = select plaintext needing encrypt; false = select enc1 needing decrypt
	}

	var jobs []job
	if *decrypt {
		jobs = []job{
			{"inbox_config.graph_client_secret", `SELECT inbox_id::text, graph_client_secret FROM email_svc.inbox_config WHERE graph_client_secret LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET graph_client_secret = $2, updated_at = now() WHERE inbox_id::text = $1`, false},
			{"inbox_config.imap_password", `SELECT inbox_id::text, imap_password FROM email_svc.inbox_config WHERE imap_password LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET imap_password = $2, updated_at = now() WHERE inbox_id::text = $1`, false},
			{"inbox_config.oauth_access_token", `SELECT inbox_id::text, oauth_access_token FROM email_svc.inbox_config WHERE oauth_access_token LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET oauth_access_token = $2, updated_at = now() WHERE inbox_id::text = $1`, false},
			{"inbox_config.oauth_refresh_token", `SELECT inbox_id::text, oauth_refresh_token FROM email_svc.inbox_config WHERE oauth_refresh_token LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET oauth_refresh_token = $2, updated_at = now() WHERE inbox_id::text = $1`, false},
			{"inbox_config.google_workspace_private_key", `SELECT inbox_id::text, google_workspace_private_key FROM email_svc.inbox_config WHERE google_workspace_private_key LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET google_workspace_private_key = $2, updated_at = now() WHERE inbox_id::text = $1`, false},
			{"graph_tenant_config.client_secret", `SELECT tenant_key, client_secret FROM email_svc.graph_tenant_config WHERE client_secret LIKE 'enc1:%'`, `UPDATE email_svc.graph_tenant_config SET client_secret = $2, updated_at = now() WHERE tenant_key = $1`, false},
			{"google_workspace_tenant_config.private_key", `SELECT tenant_key, private_key FROM email_svc.google_workspace_tenant_config WHERE private_key LIKE 'enc1:%'`, `UPDATE email_svc.google_workspace_tenant_config SET private_key = $2, updated_at = now() WHERE tenant_key = $1`, false},
			{"transformation_rule_destinations.sftp_password", `SELECT destination_id::text, sftp_password FROM email_svc.transformation_rule_destinations WHERE sftp_password LIKE 'enc1:%'`, `UPDATE email_svc.transformation_rule_destinations SET sftp_password = $2 WHERE destination_id::text = $1`, false},
			{"transformation_rule_destinations.api_auth_token", `SELECT destination_id::text, api_auth_token FROM email_svc.transformation_rule_destinations WHERE api_auth_token LIKE 'enc1:%'`, `UPDATE email_svc.transformation_rule_destinations SET api_auth_token = $2 WHERE destination_id::text = $1`, false},
		}
	} else {
		jobs = []job{
			{"inbox_config.graph_client_secret", `SELECT inbox_id::text, graph_client_secret FROM email_svc.inbox_config WHERE graph_client_secret <> '' AND graph_client_secret NOT LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET graph_client_secret = $2, updated_at = now() WHERE inbox_id::text = $1`, true},
			{"inbox_config.imap_password", `SELECT inbox_id::text, imap_password FROM email_svc.inbox_config WHERE imap_password <> '' AND imap_password NOT LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET imap_password = $2, updated_at = now() WHERE inbox_id::text = $1`, true},
			{"inbox_config.oauth_access_token", `SELECT inbox_id::text, oauth_access_token FROM email_svc.inbox_config WHERE oauth_access_token <> '' AND oauth_access_token NOT LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET oauth_access_token = $2, updated_at = now() WHERE inbox_id::text = $1`, true},
			{"inbox_config.oauth_refresh_token", `SELECT inbox_id::text, oauth_refresh_token FROM email_svc.inbox_config WHERE oauth_refresh_token <> '' AND oauth_refresh_token NOT LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET oauth_refresh_token = $2, updated_at = now() WHERE inbox_id::text = $1`, true},
			{"inbox_config.google_workspace_private_key", `SELECT inbox_id::text, google_workspace_private_key FROM email_svc.inbox_config WHERE google_workspace_private_key <> '' AND google_workspace_private_key NOT LIKE 'enc1:%'`, `UPDATE email_svc.inbox_config SET google_workspace_private_key = $2, updated_at = now() WHERE inbox_id::text = $1`, true},
			{"graph_tenant_config.client_secret", `SELECT tenant_key, client_secret FROM email_svc.graph_tenant_config WHERE client_secret <> '' AND client_secret NOT LIKE 'enc1:%'`, `UPDATE email_svc.graph_tenant_config SET client_secret = $2, updated_at = now() WHERE tenant_key = $1`, true},
			{"google_workspace_tenant_config.private_key", `SELECT tenant_key, private_key FROM email_svc.google_workspace_tenant_config WHERE private_key <> '' AND private_key NOT LIKE 'enc1:%'`, `UPDATE email_svc.google_workspace_tenant_config SET private_key = $2, updated_at = now() WHERE tenant_key = $1`, true},
			{"transformation_rule_destinations.sftp_password", `SELECT destination_id::text, sftp_password FROM email_svc.transformation_rule_destinations WHERE COALESCE(sftp_password,'') <> '' AND sftp_password NOT LIKE 'enc1:%'`, `UPDATE email_svc.transformation_rule_destinations SET sftp_password = $2 WHERE destination_id::text = $1`, true},
			{"transformation_rule_destinations.api_auth_token", `SELECT destination_id::text, api_auth_token FROM email_svc.transformation_rule_destinations WHERE COALESCE(api_auth_token,'') <> '' AND api_auth_token NOT LIKE 'enc1:%'`, `UPDATE email_svc.transformation_rule_destinations SET api_auth_token = $2 WHERE destination_id::text = $1`, true},
		}
	}

	total := 0
	for _, j := range jobs {
		n, err := processJob(ctx, pool, j.label, j.selectQ, j.updateQ, *dryRun, *decrypt)
		if err != nil {
			log.Fatalf("%s: %v", j.label, err)
		}
		fmt.Printf("%s: %d rows\n", j.label, n)
		total += n
	}
	if *dryRun {
		fmt.Printf("dry-run total: %d (no writes)\n", total)
		return
	}
	if *decrypt {
		fmt.Printf("decrypted total: %d (plaintext restored)\n", total)
		return
	}
	fmt.Printf("encrypted total: %d\n", total)
}

func processJob(ctx context.Context, pool *pgxpool.Pool, label, selectQ, updateQ string, dryRun, decrypt bool) (int, error) {
	rows, err := pool.Query(ctx, selectQ)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			log.Printf("%s: skipped (%v)", label, err)
			return 0, nil
		}
		return 0, err
	}
	defer rows.Close()
	type pair struct{ id, val string }
	var items []pair
	for rows.Next() {
		var id, val string
		if err := rows.Scan(&id, &val); err != nil {
			return 0, err
		}
		items = append(items, pair{id, val})
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if dryRun {
		return len(items), nil
	}
	for _, it := range items {
		var out string
		var err error
		if decrypt {
			out, err = secretaes.Open(it.val)
		} else {
			out, err = secretaes.Seal(it.val)
		}
		if err != nil {
			return 0, fmt.Errorf("id=%s: %w", it.id, err)
		}
		if _, err := pool.Exec(ctx, updateQ, it.id, out); err != nil {
			return 0, fmt.Errorf("update id=%s: %w", it.id, err)
		}
	}
	return len(items), nil
}
