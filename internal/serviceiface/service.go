package serviceiface

type Service interface {
	Name() string
	Start() error
	Stop() error
}
