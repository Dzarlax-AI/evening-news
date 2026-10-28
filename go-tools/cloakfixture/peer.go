package cloakfixture

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type CDPPeer struct {
	conn                           net.Conn
	reader                         *wsutil.Reader
	seq                            int
	contextID, targetID, sessionID string
	targets, contexts              map[string]bool
}

func NewPeer(ctx context.Context, browser, proxy string) (*CDPPeer, error) {
	if strings.ContainsAny(browser, "/\\@?#") || browser == "" {
		return nil, errors.New("cdp_discovery_invalid")
	}
	client := http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, e := http.NewRequestWithContext(ctx, "GET", "http://"+browser+"/json/version", nil)
	if e != nil {
		return nil, errors.New("cdp_discovery_invalid")
	}
	res, e := client.Do(req)
	if e != nil {
		return nil, errors.New("cdp_discovery_invalid")
	}
	defer res.Body.Close()
	b, e := io.ReadAll(io.LimitReader(res.Body, 16385))
	var version struct {
		Websocket string `json:"webSocketDebuggerUrl"`
	}
	if e != nil || res.StatusCode != 200 || len(b) > 16384 || json.Unmarshal(b, &version) != nil {
		return nil, errors.New("cdp_discovery_invalid")
	}
	u, e := url.Parse(version.Websocket)
	if e != nil || u.Scheme != "ws" || u.Path == "" || u.User != nil {
		return nil, errors.New("cdp_discovery_invalid")
	}
	u.Host = browser
	u.RawQuery = ""
	u.Fragment = ""
	conn, buffer, _, e := ws.Dial(ctx, u.String())
	if e != nil {
		return nil, errors.New("cdp_handshake_rejected")
	}
	var src io.Reader = conn
	if buffer != nil {
		src = buffer
	}
	p := &CDPPeer{conn: conn, reader: wsutil.NewClientSideReader(src)}
	p.reader.MaxFrameSize = 2 * 1024 * 1024
	success := false
	defer func() {
		if !success {
			p.Close(context.Background())
		}
	}()
	var v struct {
		Product string `json:"product"`
	}
	if p.command(ctx, "Browser.getVersion", nil, "", &v) != nil || v.Product == "" {
		return nil, errors.New("cdp_discovery_invalid")
	}
	var cr struct {
		ID string `json:"browserContextId"`
	}
	if p.command(ctx, "Target.createBrowserContext", map[string]any{"disposeOnDetach": true, "proxyServer": proxy, "proxyBypassList": "<-loopback>"}, "", &cr) != nil || cr.ID == "" {
		return nil, errors.New("peer_context_create_failed")
	}
	p.contextID = cr.ID
	var target struct {
		ID string `json:"targetId"`
	}
	if p.command(ctx, "Target.createTarget", map[string]any{"url": "about:blank", "browserContextId": cr.ID}, "", &target) != nil || target.ID == "" {
		return nil, errors.New("peer_target_create_failed")
	}
	p.targetID = target.ID
	var session struct {
		ID string `json:"sessionId"`
	}
	if p.command(ctx, "Target.attachToTarget", map[string]any{"targetId": target.ID, "flatten": true}, "", &session) != nil || session.ID == "" {
		return nil, errors.New("peer_attach_failed")
	}
	p.sessionID = session.ID
	if p.targets, e = p.ids(ctx, false); e != nil {
		return nil, e
	}
	if p.contexts, e = p.ids(ctx, true); e != nil {
		return nil, e
	}
	if e = p.probe(ctx); e != nil {
		return nil, e
	}
	success = true
	return p, nil
}
func (p *CDPPeer) command(ctx context.Context, method string, params any, session string, out any) error {
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if p.conn.SetDeadline(deadline) != nil {
		return errors.New("cdp_connection_closed")
	}
	p.seq++
	value := map[string]any{"id": p.seq, "method": method, "params": params}
	if session != "" {
		value["sessionId"] = session
	}
	b, _ := json.Marshal(value)
	if wsutil.WriteClientMessage(p.conn, ws.OpText, b) != nil {
		return errors.New("cdp_connection_closed")
	}
	for {
		hdr, e := p.reader.NextFrame()
		if e != nil {
			return errors.New("cdp_connection_closed")
		}
		if hdr.OpCode == ws.OpClose {
			return errors.New("cdp_connection_closed")
		}
		b, e = io.ReadAll(io.LimitReader(p.reader, 2*1024*1024+1))
		if e != nil || len(b) > 2*1024*1024 {
			return errors.New("cdp_message_too_large")
		}
		if hdr.OpCode != ws.OpText {
			continue
		}
		var reply struct {
			ID     int             `json:"id"`
			Error  json.RawMessage `json:"error"`
			Result json.RawMessage `json:"result"`
		}
		if json.Unmarshal(b, &reply) != nil {
			return errors.New("cdp_message_invalid")
		}
		if reply.ID != p.seq {
			continue
		}
		if len(reply.Error) > 0 {
			return errors.New("cdp_command_failed")
		}
		if out != nil && json.Unmarshal(reply.Result, out) != nil {
			return errors.New("cdp_message_invalid")
		}
		return nil
	}
}
func (p *CDPPeer) ids(ctx context.Context, contexts bool) (map[string]bool, error) {
	out := map[string]bool{}
	if contexts {
		var v struct {
			IDs []string `json:"browserContextIds"`
		}
		if e := p.command(ctx, "Target.getBrowserContexts", nil, "", &v); e != nil {
			return nil, e
		}
		for _, id := range v.IDs {
			out[id] = true
		}
	} else {
		var v struct {
			Targets []struct {
				ID string `json:"targetId"`
			} `json:"targetInfos"`
		}
		if e := p.command(ctx, "Target.getTargets", nil, "", &v); e != nil {
			return nil, e
		}
		for _, row := range v.Targets {
			out[row.ID] = true
		}
	}
	return out, nil
}
func (p *CDPPeer) probe(ctx context.Context) error {
	t, e := p.ids(ctx, false)
	if e != nil {
		return e
	}
	c, e := p.ids(ctx, true)
	if e != nil {
		return e
	}
	if !t[p.targetID] || !c[p.contextID] {
		return errors.New("peer_target_or_context_lost")
	}
	var v struct {
		Result struct {
			Value int `json:"value"`
		} `json:"result"`
	}
	if p.command(ctx, "Runtime.evaluate", map[string]any{"expression": "1 + 1", "returnByValue": true}, p.sessionID, &v) != nil || v.Result.Value != 2 {
		return errors.New("peer_session_probe_failed")
	}
	return nil
}
func same(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
func (p *CDPPeer) AssertClean(ctx context.Context) error {
	if e := p.probe(ctx); e != nil {
		return e
	}
	t, e := p.ids(ctx, false)
	if e != nil {
		return e
	}
	c, e := p.ids(ctx, true)
	if e != nil {
		return e
	}
	if !same(t, p.targets) || !same(c, p.contexts) {
		return errors.New("news_browser_resource_leak")
	}
	return nil
}
func (p *CDPPeer) Close(ctx context.Context) error {
	if p.conn == nil {
		return nil
	}
	var e error
	if p.contextID != "" {
		e = p.command(ctx, "Target.disposeBrowserContext", map[string]any{"browserContextId": p.contextID}, "", nil)
		p.contextID = ""
	}
	p.conn.Close()
	p.conn = nil
	return e
}
