package sundaecli

type Service struct {
	Name    string
	Version string
	Schema  string
}

func NewService(name string) Service {
	return Service{
		Name:    name,
		Version: CommitHash(),
	}
}
