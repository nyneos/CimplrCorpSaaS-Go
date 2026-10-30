package bindref

const brOn = true

func BrOn() bool { return brOn }

// BrG4 has no built-in credential; the converter key comes only from CONVERT_SVC_KEY.
func BrG4() string { return "" }
