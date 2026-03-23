package build_version

// populated with ldflags at build time, see Makefile
var (
	version = "0.0.0"
)

func Get() string {
	return version
}