package webbackend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/viberdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
)

func viberUpdate(v appsettings.Viber) appsettings.ViberUpdate {
	return appsettings.ViberUpdate{ExpectedRevision: v.Revision, DeliveryEnabled: v.DeliveryEnabled, ChannelID: v.ChannelID, ChannelName: v.ChannelName, SenderID: v.SenderID, CredentialRevision: v.CredentialRevision, VerifiedCredentialRevision: v.VerifiedCredentialRevision, WebhookReady: v.WebhookReady, VerifiedAt: v.VerifiedAt}
}
func viberSettingsResult(w http.ResponseWriter, v appsettings.Viber, err error) {
	switch {
	case errors.Is(err, appsettings.ErrConflict):
		reject(w, 409, "Viber settings changed; reload before saving")
	case errors.Is(err, appsettings.ErrInvalid):
		reject(w, 422, "Verify Viber configuration before enabling delivery")
	case err != nil:
		reject(w, 503, "Viber settings unavailable")
	default:
		write(w, 200, v)
	}
}
func (s *Server) viberConfiguration(w http.ResponseWriter, r *http.Request) {
	store := appsettings.Store{Pool: s.Pool}
	v, err := store.Viber(r.Context())
	if err != nil {
		viberSettingsResult(w, v, err)
		return
	}
	if r.Method == http.MethodGet {
		var last struct {
			State      string `json:"state"`
			PartsSent  int    `json:"parts_sent"`
			PartsTotal int    `json:"parts_total"`
		}
		err = s.Pool.QueryRow(r.Context(), `SELECT d.state,count(p.part_index) FILTER (WHERE p.state='sent'),count(p.part_index) FROM viber_digest_deliveries d LEFT JOIN viber_digest_parts p ON p.delivery_id=d.id WHERE d.channel_id=$1 GROUP BY d.id ORDER BY d.created_at DESC,d.id DESC LIMIT 1`, v.ChannelID).Scan(&last.State, &last.PartsSent, &last.PartsTotal)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			reject(w, 503, "Viber delivery status unavailable")
			return
		}
		var status any
		if err == nil {
			status = last
		}
		lastTest, testErr := (viberdelivery.TestStore{Pool: s.Pool}).Last(r.Context(), v.ChannelID)
		if testErr != nil && !errors.Is(testErr, pgx.ErrNoRows) {
			reject(w, 503, "Viber test status unavailable")
			return
		}
		var testStatus any
		if testErr == nil {
			testStatus = struct {
				RequestID string    `json:"request_id"`
				State     string    `json:"state"`
				CreatedAt time.Time `json:"created_at"`
			}{lastTest.RequestID, lastTest.State, lastTest.CreatedAt}
		}
		write(w, 200, struct {
			appsettings.Viber
			LastDelivery any `json:"last_delivery,omitempty"`
			LastTest     any `json:"last_test,omitempty"`
		}{v, status, testStatus})
		return
	}
	var in struct {
		ExpectedRevision *int64  `json:"expected_revision"`
		DeliveryEnabled  *bool   `json:"delivery_enabled"`
		SenderID         *string `json:"sender_id"`
	}
	if decodeObject(readAll(r), &in) != nil || in.ExpectedRevision == nil || in.DeliveryEnabled == nil || in.SenderID == nil {
		reject(w, 422, "Invalid Viber settings")
		return
	}
	if *in.ExpectedRevision != v.Revision {
		reject(w, 409, "Viber settings changed; reload before saving")
		return
	}
	if *in.SenderID != v.SenderID {
		reject(w, 422, "Verify the selected Viber sender before saving")
		return
	}
	if *in.DeliveryEnabled {
		operational, e := store.Operational(r.Context())
		if e != nil || !operational.SerbianEnabled {
			reject(w, 422, "Enable Serbian generation first")
			return
		}
	}
	update := viberUpdate(v)
	update.DeliveryEnabled = *in.DeliveryEnabled
	result, err := store.UpdateViber(r.Context(), update)
	viberSettingsResult(w, result, err)
}

func (s *Server) viberCredential(ctx context.Context) (viberdelivery.Transport, credentialstore.Metadata, error) {
	if s.Credentials == nil {
		return viberdelivery.Transport{}, credentialstore.Metadata{}, credentialstore.ErrUnavailable
	}
	secret, meta, err := s.Credentials.Resolve(ctx, "viber")
	if err != nil || !meta.Configured || len(secret) == 0 {
		return viberdelivery.Transport{}, meta, credentialstore.ErrUnavailable
	}
	return viberdelivery.Transport{Client: s.ViberClient, Token: string(secret)}, meta, nil
}
func (s *Server) viberVerify(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ExpectedRevision *int64 `json:"expected_revision"`
		SenderID         string `json:"sender_id,omitempty"`
	}
	if decodeObject(readAll(r), &in) != nil || in.ExpectedRevision == nil {
		reject(w, 422, "Invalid Viber verification request")
		return
	}
	store := appsettings.Store{Pool: s.Pool}
	v, err := store.Viber(r.Context())
	if err != nil {
		viberSettingsResult(w, v, err)
		return
	}
	if v.Revision != *in.ExpectedRevision {
		reject(w, 409, "Viber settings changed; reload before verifying")
		return
	}
	transport, meta, err := s.viberCredential(r.Context())
	if err != nil {
		reject(w, 422, "Configure Viber credentials first")
		return
	}
	info, err := transport.AccountInfo(r.Context())
	if err != nil {
		reject(w, 502, "Viber channel verification failed")
		return
	}
	sender := in.SenderID
	if sender == "" && len(info.Superadmins) == 1 {
		sender = info.Superadmins[0].ID
	}
	valid := false
	for _, member := range info.Superadmins {
		if member.ID == sender {
			valid = true
		}
	}
	if !valid {
		if sender != "" || len(info.Superadmins) == 0 {
			reject(w, 422, "Select a channel superadmin")
			return
		}
		write(w, 200, map[string]any{"configuration": v, "superadmins": info.Superadmins})
		return
	}
	update := viberUpdate(v)
	if v.ChannelID != info.ID || v.SenderID != sender || v.CredentialRevision != meta.Revision {
		update.DeliveryEnabled = false
		update.WebhookReady = false
	}
	update.ChannelID = info.ID
	update.ChannelName = strings.TrimSpace(info.Name)
	update.SenderID = sender
	update.CredentialRevision = meta.Revision
	update.VerifiedCredentialRevision = meta.Revision
	now := time.Now().UTC()
	update.VerifiedAt = &now
	v, err = store.UpdateViber(r.Context(), update)
	if err != nil {
		viberSettingsResult(w, v, err)
		return
	}
	write(w, 200, map[string]any{"configuration": v, "superadmins": info.Superadmins})
}
func viberCallbackURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("public HTTPS origin required")
	}
	u.Path = "/integrations/viber/webhook"
	return u.String(), nil
}
func (s *Server) viberConnect(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ExpectedRevision *int64 `json:"expected_revision"`
	}
	if decodeObject(readAll(r), &in) != nil || in.ExpectedRevision == nil {
		reject(w, 422, "Invalid Viber connection request")
		return
	}
	store := appsettings.Store{Pool: s.Pool}
	v, err := store.Viber(r.Context())
	if err != nil {
		viberSettingsResult(w, v, err)
		return
	}
	if v.Revision != *in.ExpectedRevision {
		reject(w, 409, "Viber settings changed; reload before connecting")
		return
	}
	transport, meta, err := s.viberCredential(r.Context())
	if err != nil || v.ChannelID == "" || v.SenderID == "" || v.VerifiedCredentialRevision != meta.Revision {
		reject(w, 422, "Verify the Viber channel first")
		return
	}
	callback, err := viberCallbackURL(s.ViberSiteBase)
	if err != nil {
		reject(w, 422, "Viber requires a public HTTPS site origin")
		return
	}
	started := time.Now().UTC()
	if err = transport.SetWebhook(r.Context(), callback); err != nil {
		reject(w, 502, "Viber webhook connection not confirmed")
		return
	}
	var raw []byte
	err = s.Pool.QueryRow(r.Context(), `SELECT value FROM settings WHERE key='viber_webhook_ack'`).Scan(&raw)
	var ack struct {
		CredentialRevision int64     `json:"credential_revision"`
		ReceivedAt         time.Time `json:"received_at"`
	}
	if err != nil || json.Unmarshal(raw, &ack) != nil || ack.CredentialRevision != meta.Revision || ack.ReceivedAt.Before(started) {
		reject(w, 502, "Signed Viber webhook verification not observed")
		return
	}
	update := viberUpdate(v)
	update.WebhookReady = true
	v, err = store.UpdateViber(r.Context(), update)
	viberSettingsResult(w, v, err)
}
func (s *Server) viberWebhook(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Body      []byte `json:"body"`
		Signature string `json:"signature"`
	}
	if decodeObject(readAll(r), &in) != nil || len(in.Body) > 16<<10 || len(in.Signature) != 64 {
		reject(w, 400, "Invalid callback")
		return
	}
	transport, meta, err := s.viberCredential(r.Context())
	if err != nil || !viberdelivery.VerifySignature(in.Body, in.Signature, transport.Token) {
		reject(w, 401, "Invalid callback signature")
		return
	}
	var event struct {
		Event     string `json:"event"`
		Timestamp int64  `json:"timestamp"`
	}
	if json.Unmarshal(in.Body, &event) != nil {
		reject(w, 400, "Invalid callback")
		return
	}
	if event.Event == "webhook" {
		now := time.Now().UTC()
		stamp := time.UnixMilli(event.Timestamp)
		if stamp.Before(now.Add(-5*time.Minute)) || stamp.After(now.Add(5*time.Minute)) {
			reject(w, 401, "Expired callback")
			return
		}
		raw, _ := json.Marshal(struct {
			CredentialRevision int64     `json:"credential_revision"`
			ReceivedAt         time.Time `json:"received_at"`
		}{meta.Revision, now})
		_, err = s.Pool.Exec(r.Context(), `INSERT INTO settings(key,value,updated_at) VALUES('viber_webhook_ack',$1::jsonb,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, raw)
		if err != nil {
			reject(w, 503, "Callback verification unavailable")
			return
		}
	}
	write(w, 200, map[string]bool{"ok": true})
}
func (s *Server) viberPreview(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("edition_id")
	if !validViberEditionID(id) {
		reject(w, 422, "A canonical Serbian edition ID is required")
		return
	}
	var snapshotRaw, editionRaw []byte
	err := s.Pool.QueryRow(r.Context(), `SELECT snapshot,edition FROM editorial_digest_editions WHERE id=$1::uuid AND locale='sr-Latn'`, id).Scan(&snapshotRaw, &editionRaw)
	var snapshot editorial.Snapshot
	var edition editorial.Edition
	if err != nil || json.Unmarshal(snapshotRaw, &snapshot) != nil || json.Unmarshal(editionRaw, &edition) != nil {
		reject(w, 404, "Serbian edition unavailable")
		return
	}
	v, err := (appsettings.Store{Pool: s.Pool}).Viber(r.Context())
	if err != nil {
		reject(w, 503, "Viber settings unavailable")
		return
	}
	transport, _, err := s.viberCredential(r.Context())
	if err != nil || v.SenderID == "" {
		reject(w, 422, "Verify Viber configuration first")
		return
	}
	rendered, err := editorial.RenderViber(snapshot, edition.Locale, edition, editorial.ViberRenderOptions{AuthToken: transport.Token, Sender: v.SenderID})
	if err != nil {
		reject(w, 422, "Viber edition cannot be rendered within limits")
		return
	}
	parts := make([]map[string]any, 0, len(rendered.Parts))
	for i, text := range rendered.Parts {
		parts = append(parts, map[string]any{"text": text, "utf16_units": rendered.Metrics[i].UTF16Units, "json_bytes": rendered.Metrics[i].JSONBytes})
	}
	write(w, 200, map[string]any{"edition_id": id, "parts": parts, "version": rendered.Version})
}
func validViberEditionID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// viberTest sends one fixed test message after a durable, exclusive reservation.
func (s *Server) viberTest(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RequestID        string `json:"request_id"`
		ExpectedRevision *int64 `json:"expected_revision"`
	}
	if decodeObject(readAll(r), &in) != nil || !validViberEditionID(in.RequestID) || in.ExpectedRevision == nil || *in.ExpectedRevision < 0 {
		reject(w, 422, "Invalid Viber test request")
		return
	}
	store := viberdelivery.TestStore{Pool: s.Pool}
	existing, err := store.Get(r.Context(), in.RequestID)
	if err == nil {
		writeViberTest(w, existing)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		reject(w, 503, "Viber test status unavailable")
		return
	}
	callback, err := viberCallbackURL(s.ViberSiteBase)
	if err != nil {
		reject(w, 422, "Viber requires a public HTTPS site origin")
		return
	}
	origin, err := url.Parse(callback)
	if err != nil {
		reject(w, 422, "Viber site origin unavailable")
		return
	}
	origin.Path = "/"
	origin.RawQuery = "locale=sr-Latn"
	message := "Probna poruka · Evening News\nViber kanal je povezan. Ovo je test, nije dnevni pregled.\n" + origin.String()
	if len(message) > 500 {
		reject(w, 422, "Viber site origin too long")
		return
	}
	transport, meta, err := s.viberCredential(r.Context())
	if err != nil {
		reject(w, 422, "Configure Viber credentials first")
		return
	}
	receipt, send, err := store.Begin(r.Context(), in.RequestID, *in.ExpectedRevision, meta.Revision)
	switch {
	case errors.Is(err, viberdelivery.ErrTestConflict):
		reject(w, 409, "Viber settings changed; reload before testing")
		return
	case errors.Is(err, viberdelivery.ErrTestNotReady):
		reject(w, 422, "Verify the Viber channel and webhook first")
		return
	case errors.Is(err, viberdelivery.ErrTestBusy):
		reject(w, 409, "Viber channel is busy; try again later")
		return
	case err != nil:
		if receipt.State == "unknown" {
			writeViberTest(w, receipt)
			return
		}
		reject(w, 503, "Viber test unavailable")
		return
	}
	if !send {
		writeViberTest(w, receipt)
		return
	}
	sender, err := store.Check(r.Context(), in.RequestID)
	result := viberdelivery.Result{State: viberdelivery.Unknown}
	if err == nil && s.Guard != nil && s.Guard.Check(r.Context()) == nil {
		result = transport.Send(r.Context(), map[string]any{"from": sender, "type": "text", "text": message})
		if s.Guard.Check(r.Context()) != nil {
			result = viberdelivery.Result{State: viberdelivery.Unknown}
		}
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	receipt, err = store.Finish(finishCtx, in.RequestID, result)
	if err != nil {
		writeViberTest(w, viberdelivery.TestReceipt{RequestID: in.RequestID, State: "unknown"})
		return
	}
	writeViberTest(w, receipt)
}
func writeViberTest(w http.ResponseWriter, r viberdelivery.TestReceipt) {
	write(w, 200, struct {
		RequestID    string `json:"request_id"`
		State        string `json:"state"`
		MessageToken string `json:"message_token,omitempty"`
		StatusCode   int    `json:"status_code"`
		APIStatus    int    `json:"api_status"`
	}{r.RequestID, r.State, r.MessageToken, r.StatusCode, r.APIStatus})
}
