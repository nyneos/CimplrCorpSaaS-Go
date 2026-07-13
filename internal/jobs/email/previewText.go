package emailjobs

import "strings"

// BodyPreviewMaxLen is the max plain-text stored in body_text_preview (list/search only).
// Full body is always loaded from S3 on message preview via messages/get.
const BodyPreviewMaxLen = 320000

// BodyPreviewForStorage returns plain text for the list column; does not affect preview UI.
func BodyPreviewForStorage(textPlain, textHTML string) string {
	preview := strings.TrimSpace(textPlain)
	if preview == "" {
		preview = strings.TrimSpace(textHTML)
	}
	if len(preview) > BodyPreviewMaxLen {
		return preview[:BodyPreviewMaxLen]
	}
	return preview
}
