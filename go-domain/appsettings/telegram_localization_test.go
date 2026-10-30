package appsettings

import "testing"

func TestSerbianDestinationSeparation(t *testing.T) {
	for _, tc := range []struct {
		name, main, service, sr string
		enabled, valid          bool
	}{
		{"disabled empty", "@ru", "", "", false, true},
		{"enabled missing", "@ru", "", "", true, false},
		{"main collision", "@Ru", "", "@ru", true, false},
		{"service collision", "@ru", "@ops", "@ops", true, false},
		{"independent", "@ru", "@ops", "@sr", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTelegram(TelegramUpdate{ChatID: tc.main, ServiceChatID: tc.service, SerbianChatID: tc.sr, SerbianDeliveryEnabled: tc.enabled})
			if (err == nil) != tc.valid {
				t.Fatalf("validation valid=%v", err == nil)
			}
		})
	}
}
