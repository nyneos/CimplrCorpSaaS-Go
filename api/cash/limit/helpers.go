package limit

// Helper functions for nullable fields
func nullifyEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullifyFloat(f *float64) interface{} {
	if f == nil {
		return nil
	}
	return *f
}

func nullifyBool(b *bool) interface{} {
	if b == nil {
		return nil
	}
	return *b
}
