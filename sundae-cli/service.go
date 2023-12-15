package sundaecli

type Service struct {
	Name    string
	Subpath string
	Version string
	Schema  string
}

func NewService(name string) Service {
	return Service{
		Name:    name,
		Subpath: "",
		Version: CommitHash(),
	}
}

func NewSubpathService(name string) Service {
	return Service{
		Name:    name,
		Subpath: name,
		Version: CommitHash(),
	}
}
