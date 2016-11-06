package node_discovery

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
	"net"
	"github.com/stretchr/testify/require"
	"net/url"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sort"
	"github.com/Sirupsen/logrus"
)

// Fake Network Error
type timeoutError struct{}
func (e *timeoutError) Error() string   { return "fake i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// Custom PacketConn for testing purpose
type FakeConn struct {
	// Emulate input/output network with channels for tests
	InputChan chan []byte
	OutputChan chan []byte
	// Simulate timeout
	ReadTimeout time.Duration
	WriteTimeout time.Duration
}
func NewFakeConn() *FakeConn {
	return &FakeConn {
		InputChan: make(chan []byte),
		OutputChan: make(chan []byte),
		ReadTimeout: 30 * time.Second,
		WriteTimeout: time.Second,
	}
}
func FakeListen(t *testing.T) *NodeDiscover {
	conn := NewFakeConn()
	addr := &net.IPAddr{}
	nd, err := Listen(
		CustomConnectionOpt(conn, addr),
		LeaveTimeoutOpt(time.Second),
	)
	require.NoError(t, err)
	return nd
}

func (fc *FakeConn) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case input := <- fc.InputChan:
		copy(b, input)
		return len(input), nil, nil
	case <- time.After(fc.ReadTimeout):
		return 0, nil, &timeoutError{}
	}
}

func (fc *FakeConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	select {
	case fc.OutputChan <- b:
		return len(b), nil
	case <- time.After(fc.WriteTimeout):
		return 0, &timeoutError{}
	}
}

func (fc *FakeConn) SetReadDeadline(t time.Time) error {
	fc.ReadTimeout = t.Sub(time.Now())
	return nil
}

func (fc *FakeConn) SetWriteDeadline(t time.Time) error {
	fc.WriteTimeout = t.Sub(time.Now())
	return nil
}

func (fc *FakeConn) Close() error {
	close(fc.InputChan)
	close(fc.OutputChan)
	return nil
}

func (fc *FakeConn) LocalAddr() net.Addr { return nil }
func (fc *FakeConn) RemoteAddr() net.Addr { return nil }
func (fc *FakeConn) SetDeadline(t time.Time) error { return nil }

func TestHeartbeatOpt(t *testing.T) {
	opt := options{}
	hb := 42 * time.Millisecond
	optF := HeartbeatOpt(hb)
	err := optF(&opt)
	if assert.NoError(t, err) {
		assert.Equal(t, hb, opt.heartbeat)
	}
}

func TestLeaveTimeoutOpt(t *testing.T) {
	opt := options{}
	timeout := 42 * time.Millisecond
	optF := LeaveTimeoutOpt(timeout)
	err := optF(&opt)
	if assert.NoError(t, err) {
		assert.Equal(t, timeout, opt.leaveTimeout)
	}
}

func TestCustomConnectionOpt(t *testing.T) {
	opt := options{}
	addr := &net.IPAddr{}
	conn := NewFakeConn()
	optF := CustomConnectionOpt(conn, addr)
	err := optF(&opt)
	if assert.NoError(t, err) {
		assert.Equal(t, conn, opt.conn)
		assert.Equal(t, addr, opt.addr)
	}
}

func TestMulticastOpt(t *testing.T) {
	// Error case
	errSample := []struct{
		addr string
		port int
		iface *net.Interface
	} {
		{ "esf43534.DFG3", 1234, nil, },
		{ "137.3.4.1", 1234, nil, },
		{ "224.5.4.3", 77777, nil, },
	}
	for _, s := range errSample {
		o := options{}
		optF := MulticastOpt(s.addr, s.port, s.iface)
		err := optF(&o)
		assert.Error(t, err)
	}

	// Normal case
	okSample := []struct{
		addr string
		port int
		iface *net.Interface
	} {
		{ "224.5.4.3", 4321, nil, },
		{ "224.1.1.3", 44719, &net.Interface{ Name: "toto", }, },
	}
	for _, s := range okSample {
		o := options{}
		optF := MulticastOpt(s.addr, s.port, s.iface)
		err := optF(&o)
		if assert.NoError(t, err) {
			addr := &net.UDPAddr{
				IP: net.ParseIP(s.addr),
				Port: s.port,
			}
			assert.Equal(t, addr, o.addr)
		}
	}
}

func TestListen(t *testing.T) {
	// Error case
	t.Run("Error case", func(t *testing.T) {
		errOpt := MulticastOpt("esf43534.DFG3", 1234, nil )
		_, err := Listen(errOpt)
		assert.Error(t, err)
	})

	// Default options
	t.Run("Default options", func(t *testing.T) {
		nd, err := Listen()
		defer nd.Close()
		require.NoError(t, err)
		assert.Equal(t, DEFAULT_LEAVE_TIMEOUT, nd.leaveTimeout)
		assert.NotNil(t, nd.conn)
		assert.NotNil(t, nd.addr)
		assert.NotNil(t, nd.sendTicker)
	})

	// Custom options
	t.Run("Custom options", func(t *testing.T) {
		fakeConn := NewFakeConn()
		addr := &net.IPAddr{}
		timeout := 42 * time.Second
		nd, err := Listen(
			CustomConnectionOpt(fakeConn, addr),
			LeaveTimeoutOpt(timeout))
		defer nd.Close()
		require.NoError(t, err)
		assert.Equal(t, addr, nd.addr)
		assert.Equal(t, fakeConn, nd.conn)
		assert.Equal(t, timeout, nd.leaveTimeout)
	})

	// Test Receive HeartBeat
	// TODO
}

func TestSendHeartbeat(t *testing.T) {
	svc1 := "http://svc1:1231"
	svc2 := "https://svc2:1232"
	svc3 := "ftp://svc3:1233"
	services := [][]string {
		{},
		{svc1},
		{svc1, svc2, svc3},
	}
	for _, svc := range services {
		// Create fake connection
		nd := FakeListen(t)

		// Register services
		for _, s := range svc {
			sURL, errURL := url.Parse(s)
			require.NoError(t, errURL)
			nd.Register(sURL)
		}

		// With fake connection, read heartbeat, decode it and check it
		var hb heartBeatMsg
		select {
		case msg := <-(nd.conn.(*FakeConn)).OutputChan:
			errMsg := msgpack.Unmarshal(msg, &hb)
			require.NoError(t, errMsg)
		case <- time.After(10 * time.Second):
			require.Fail(t, "Did not receive heartbeat message")
		}

		assert.Equal(t, PROTOCOL_VERSION, hb.Version)
		sort.Strings(svc)
		sort.Strings(hb.Services)
		assert.Equal(t, svc, hb.Services)
		nd.Close()
	}
}

func TestNodeDiscover_Register(t *testing.T) {
	nd := FakeListen(t)
	defer nd.Close()
	svcUrl, err := url.Parse("http://svc1:1231")
	require.NoError(t, err)

	// Test registration
	nd.Register(svcUrl)
	nd.registers.RLock()
	assert.True(t, nd.registers.r[svcUrl.String()])
	nd.registers.RUnlock()

	// Test Deregistration
	nd.Deregister(svcUrl)
	nd.registers.RLock()
	assert.False(t, nd.registers.r[svcUrl.String()])
	nd.registers.RUnlock()
}

func TestNodeDiscover_Subscribe(t *testing.T) {
	nd := FakeListen(t)
	defer nd.Close()
	subCh := make(chan NodeEvent)

	// Test Subscrive
	nd.Subscribe(subCh)
	nd.subscribers.RLock()
	assert.True(t, nd.subscribers.s[subCh])
	nd.subscribers.RUnlock()

	// Test Unsubscibe
	nd.Unsubscribe(subCh)
	nd.subscribers.RLock()
	assert.False(t, nd.subscribers.s[subCh])
	nd.subscribers.RUnlock()
}

func TestSetLogger(t *testing.T) {
	customLog := logrus.New()
	SetLogger(customLog)
	assert.Equal(t, customLog, log)
}

func TestEvents(t *testing.T) {
	// Create fake connection
	nd := FakeListen(t)
	defer nd.Close()

	// Subscribe to events
	subCh := make(chan NodeEvent)
	nd.Subscribe(subCh)

	// Repeat multiple times to check if a service can be rediscovered
	for i := 0; i < 3 ; i++ {
		// Send fake service
		svc := "http://svc:1231"
		hb := heartBeatMsg{
			Version: PROTOCOL_VERSION,
			Services: []string{svc},
		}
		hbData, err := msgpack.Marshal(&hb)
		require.NoError(t, err)
		(nd.conn.(*FakeConn)).InputChan <- hbData

		svcUrl, errUrl := url.Parse(svc)
		require.NoError(t, errUrl)
		// Receive join service event
		select {
		case msg := <-subCh:
			assert.Equal(t, ServiceJoinEvent, msg.Type)
			assert.Equal(t, svcUrl, msg.Service)
		case <-time.After(4 * time.Second):
			require.Fail(t, "Did not receive join event message")
		}

		// Resend heartbeat, nothing must happen
		(nd.conn.(*FakeConn)).InputChan <- hbData

		// Receive leave service event
		select {
		case msg := <-subCh:
			assert.Equal(t, ServiceLeaveEvent, msg.Type)
			assert.Equal(t, svcUrl, msg.Service)
		case <-time.After(4 * time.Second):
			require.Fail(t, "Did not receive leave event message")
		}
	}
}