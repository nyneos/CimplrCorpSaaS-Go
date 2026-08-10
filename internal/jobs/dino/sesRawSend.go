package jobs

import (
	"context"
	"fmt"
)

// owSendViaSESRaw was the legacy Main-local SES path for attachment mail.
// Sender identity and delivery are owned by Notification-Service — do not re-enable
// without routing through SEND_ENDPOINT_URL.
func owSendViaSESRaw(_ context.Context, _ owSendPayload) (string, error) {
	return "", fmt.Errorf("local SES send disabled: use Notification-Service bulk endpoint")
}
