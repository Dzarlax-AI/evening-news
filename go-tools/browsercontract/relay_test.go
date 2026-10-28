package browsercontract

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The adapter starts the unchanged relay with test-only loopback endpoints.
const relayAdapter = `import asyncio,sys
sys.path.insert(0,'/opt/news-browser')
from relay import Relay
async def main():
 r=Relay('127.0.0.1',target_port=int(sys.argv[1]),allow_loopback_test_peer=True)
 s=await asyncio.start_server(r.handle,'127.0.0.1',0)
 print(s.sockets[0].getsockname()[1],flush=True)
 async with s: await s.serve_forever()
asyncio.run(main())`

func TestRelayPeerRestrictionAndForwarding(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Docker host networking requires Linux")
	}
	imageName := image(t)
	target, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var accepted atomic.Int32
	go func() {
		for {
			c, e := target.Accept()
			if e != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer c.Close()
				b, _ := io.ReadAll(io.LimitReader(c, 100))
				_, _ = c.Write(append([]byte("reply:"), b...))
			}()
		}
	}()
	targetPort := target.Addr().(*net.TCPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := fixture(t)
	args := append(containerArgs(f, "host"), imageName, "-u", "-c", relayAdapter, strconv.Itoa(targetPort))
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal("relay fixture start failed")
	}
	defer func() {
		cleanupFixture(t, f)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal("relay fixture did not start")
	}
	port, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || port < 1 || port > 65535 {
		t.Fatal("invalid fixture port")
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	denied := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2")}, Timeout: time.Second}
	c, err := denied.Dial("tcp4", address)
	if err != nil {
		t.Fatal("denied peer could not connect")
	}
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c.Write([]byte("denied"))
	_, readErr := io.ReadAll(c)
	_ = c.Close()
	if timeout, ok := readErr.(net.Error); ok && timeout.Timeout() {
		t.Fatal("denied peer was not closed")
	}
	if accepted.Load() != 0 {
		t.Fatal("denied peer reached target")
	}
	allowed, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		t.Fatal("allowed peer connect failed")
	}
	_ = allowed.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = allowed.Write([]byte("hello"))
	_ = allowed.(*net.TCPConn).CloseWrite()
	reply, err := io.ReadAll(allowed)
	_ = allowed.Close()
	if err != nil || string(reply) != "reply:hello" || accepted.Load() != 1 {
		t.Fatal("relay did not forward both directions")
	}
}
