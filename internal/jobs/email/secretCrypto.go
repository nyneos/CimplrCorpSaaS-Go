package emailjobs

import "CimplrCorpSaas/internal/secretaes"

// SealSecret encrypts credential columns when SECRETS_ENC_KEY is set.
func SealSecret(v string) (string, error) {
	return secretaes.Seal(v)
}

// OpenSecret decrypts Seal output; values without enc1: prefix are legacy plaintext.
func OpenSecret(v string) string {
	return secretaes.MustOpen(v)
}
