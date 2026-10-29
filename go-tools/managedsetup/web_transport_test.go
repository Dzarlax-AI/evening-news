package managedsetup

import "testing"

func TestValidateWebTransport(t *testing.T) {
	tests := []struct {
		name    string
		site    string
		options Options
		local   bool
		wantErr bool
	}{
		{name: "hosted https explicit proxy", site: "https://news.example.invalid", options: Options{TrustedProxyCIDRs: []string{"172.20.0.2/32"}}},
		{name: "loopback http explicit profile", site: "http://localhost:8000", local: true},
		{name: "loopback https requires proxy", site: "https://127.0.0.1:8443", wantErr: true},
		{name: "loopback https with proxy", site: "https://127.0.0.1:8443", options: Options{TrustedProxyCIDRs: []string{"127.0.0.2/32"}}},
		{name: "hosted https missing proxy", site: "https://news.example.invalid", wantErr: true},
		{name: "hosted http", site: "http://news.example.invalid", options: Options{TrustedProxyCIDRs: []string{"172.20.0.2/32"}}, wantErr: true},
		{name: "loopback http without profile", site: "http://localhost:8000", wantErr: true},
		{name: "unsupported loopback address", site: "http://127.0.0.2:8000", local: true, wantErr: true},
		{name: "loopback hostname trailing dot", site: "http://localhost.:8000", local: true, wantErr: true},
		{name: "invalid proxy CIDR", site: "https://news.example.invalid", options: Options{TrustedProxyCIDRs: []string{"not-a-cidr"}}, wantErr: true},
		{name: "all addresses proxy CIDR", site: "https://news.example.invalid", options: Options{TrustedProxyCIDRs: []string{"0.0.0.0/0"}}, wantErr: true},
		{name: "local profile on hosted URL", site: "https://news.example.invalid", local: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWebTransport(tt.site, tt.options, tt.local)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateWebTransport() error=%v, wantErr=%t", err, tt.wantErr)
			}
		})
	}
}
