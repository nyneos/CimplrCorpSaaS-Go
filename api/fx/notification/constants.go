package notification

// Source routes must match notification_svc.event.source_route exactly.
const (
	SourceRouteV91Upload              = "/fx/exposures/v91/upload"
	SourceRouteV91UploadBulkApprove   = "/fx/exposures/v91/upload/bulk-approve"
	SourceRouteV91UploadBulkReject    = "/fx/exposures/v91/upload/bulk-reject"
	SourceRouteV91UploadBulkDelete    = "/fx/exposures/v91/upload/bulk-delete"
	SourceRouteV91BulkApprove         = "/fx/exposures/v91/bulk-approve"
	SourceRouteV91BulkReject          = "/fx/exposures/v91/bulk-reject"
	SourceRouteV91BulkDelete          = "/fx/exposures/v91/bulk-delete"
	SourceRouteV91BulkUpdateValueDate = "/fx/exposures/v91/bulk-update-value-dates"
	SourceRouteV91EditAllocation      = "/fx/exposures/v91/edit-allocation"

	SourceRouteLegacyApproveMultiple = "/fx/exposures/approve-multiple-headers"
	SourceRouteLegacyRejectMultiple  = "/fx/exposures/reject-multiple-headers"
	SourceRouteLegacyDeleteMultiple  = "/fx/exposures/delete-multiple-headers"
	SourceRouteLegacyEdit            = "/fx/exposures/edit"
	SourceRouteLegacyBatchUpload     = "/fx/exposures/batch-upload-staging"

	SourceRouteBucketingApprove = "/fx/exposures/approve-bucketing-status"
	SourceRouteBucketingReject  = "/fx/exposures/reject-bucketing-status"
	SourceRouteBucketingUpdate  = "/fx/exposures/update-bucketing"
	SourceRouteBucketingDelete  = "/fx/exposures/bucketing/delete-multiple-headers"

	SourceRouteLinkExposureHedge = "/fx/exposures/link-exposure-hedge"
)

// Event action labels used in notification payloads.
const (
	ActionUpload  = "UPLOAD"
	ActionApprove = "APPROVE"
	ActionReject  = "REJECT"
	ActionDelete  = "DELETE"
	ActionUpdate  = "UPDATE"
	ActionEdit    = "EDIT"
	ActionLink    = "LINK"
)
