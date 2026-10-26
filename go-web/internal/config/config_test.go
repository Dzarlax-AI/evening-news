package config

import "testing"

func TestDefaultsAndJWTSecretFallback(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@127.0.0.1/news")
	t.Setenv("ADMIN_USERNAME", "")
	t.Setenv("ADMIN_PASSWORD", "fixture-password")
	t.Setenv("JWT_SECRET", "")
	t.Setenv("DEVELOPMENT", "")
	t.Setenv("TRUST_FORWARD_AUTH", "")
	t.Setenv("WEB_BACKEND_URL", "http://backend.fixture.invalid")
	t.Setenv("WEB_BACKEND_TOKEN", "fixture-token")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdminUsername != "admin" || cfg.JWTSecret != "fixture-password" {
		t.Fatal("Python-compatible defaults were not retained")
	}
}

func TestJWTSecretDoesNotReplaceRequiredAdminPassword(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@127.0.0.1/news")
	t.Setenv("ADMIN_USERNAME", "")
	t.Setenv("ADMIN_PASSWORD", "")
	t.Setenv("JWT_SECRET", "fixture-signing-secret")
	t.Setenv("WEB_BACKEND_URL", "http://backend.fixture.invalid")
	t.Setenv("WEB_BACKEND_TOKEN", "fixture-token")
	if _, err := Load(); err == nil {
		t.Fatal("expected ADMIN_PASSWORD requirement")
	}
}

func TestFullWebConfigurationRequiresBackendAndWorkerMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@127.0.0.1/news")
	t.Setenv("ADMIN_PASSWORD", "fixture-password")
	t.Setenv("JWT_SECRET", "")
	t.Setenv("WEB_BACKEND_URL", "")
	t.Setenv("WEB_BACKEND_TOKEN", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected private backend configuration requirement")
	}
	t.Setenv("WEB_BACKEND_URL", "http://backend.fixture.invalid")
	t.Setenv("WEB_BACKEND_TOKEN", "fixture-token")
	t.Setenv("BACKGROUND_EXECUTION_MODE", "embedded")
	if _, err := Load(); err == nil {
		t.Fatal("expected worker-only Go web mode")
	}
}
