package managedsetup

import (
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
	"net/netip"
	"os"
	"reflect"
)

// Options configures only explicitly trusted reverse proxies. It never enables
// forwarded administrator identity or changes listener exposure.
type Options struct {
	TrustedProxyCIDRs []string `json:"trusted_proxy_cidrs,omitempty"`
}

func (o Options) Validate() error {
	if len(o.TrustedProxyCIDRs) > 32 {
		return errors.New("too many trusted proxies")
	}
	seen := map[string]bool{}
	for _, s := range o.TrustedProxyCIDRs {
		p, e := netip.ParsePrefix(s)
		if e != nil || p.Bits() == 0 || p.Addr().IsUnspecified() || seen[s] {
			return errors.New("invalid trusted proxy CIDR")
		}
		seen[s] = true
	}
	return nil
}
func sameOptions(a, b Options) bool { return reflect.DeepEqual(a, b) }
func ReadOptions(path string) (Options, error) {
	var options Options
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65536 {
		return options, errors.New("options input must be a private regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return options, errors.New("options input unavailable")
	}
	if runtimeconfig.DecodeStrict(raw, &options) != nil || options.Validate() != nil {
		return Options{}, errors.New("invalid installer options")
	}
	return options, nil
}
