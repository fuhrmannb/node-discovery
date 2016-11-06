/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package node_discovery

import (
	"net"
	"sync"
	"time"
	"net/url"
	"github.com/vmihailenco/msgpack"
	"github.com/pkg/errors"
	"github.com/Sirupsen/logrus"
)

var log logrus.FieldLogger = logrus.New()

func SetLogger(logger logrus.FieldLogger) {
	log = logger
}

const (
	MAX_UDP_PACKET = 65536
	PROTOCOL_VERSION = "beta1"
	RECV_TIMEOUT = 5 * time.Second

	DEFAULT_HEARTBEAT = time.Second
	DEFAULT_LEAVE_TIMEOUT = 10 * time.Second
)

type registerData struct {
	sync.RWMutex
	r map[string]bool
}
type subscribersData struct {
	sync.RWMutex
	s map[chan NodeEvent]bool
}
type servicesData struct {
	sync.RWMutex
	s map[string]*time.Timer
}

type NodeDiscover struct {
	conn net.PacketConn
	addr net.Addr
	connWG sync.WaitGroup

	sendTicker *time.Ticker
	leaveTimeout time.Duration

	registers registerData
	subscribers subscribersData
	services servicesData
	events chan NodeEvent

	closed chan struct{}
}

type heartBeatMsg struct {
	Version string `msgpack:"version"`
	Services []string `msgpack:"services"`
}

type EventType int
const (
	ServiceJoinEvent EventType = iota
	ServiceLeaveEvent
)

type NodeEvent struct {
	Type EventType
	Service *url.URL
}

type options struct {
	conn net.PacketConn
	addr net.Addr
	heartbeat time.Duration
	leaveTimeout time.Duration
}
type NodeDiscoverOpt func(*options) (error)

func HeartbeatOpt(heartbeat time.Duration) NodeDiscoverOpt {
	return func(o *options) error {
		o.heartbeat = heartbeat
		return nil
	}
}

func LeaveTimeoutOpt(timeout time.Duration) NodeDiscoverOpt {
	return func(o *options) error {
		o.leaveTimeout = timeout
		return nil
	}
}

func CustomConnectionOpt(connection net.PacketConn, addr net.Addr) NodeDiscoverOpt {
	return func(o *options) error {
		o.conn = connection
		o.addr = addr
		return nil
	}
}

func MulticastOpt(multicastAddress string, port int, iface *net.Interface) NodeDiscoverOpt {
	return func(o *options) error {
		ip := net.ParseIP(multicastAddress)
		if ip == nil {
			return errors.New("Given multicast address option is not a valid IP")
		}
		addr := &net.UDPAddr{
			IP: ip,
			Port: port,
		}
		o.addr = addr
		udpConn, err := net.ListenMulticastUDP("udp", iface, addr)
		if err != nil {
			return err
		}
		o.conn = udpConn
		return nil
	}
}

func Listen(customOpts ...NodeDiscoverOpt) (*NodeDiscover, error) {
	opts := &options {
		conn: nil,
		addr: nil,
		// Default options
		heartbeat: DEFAULT_HEARTBEAT,
		leaveTimeout: DEFAULT_LEAVE_TIMEOUT,
	}
	// Manage custom options
	for _, o := range customOpts {
		err := o(opts)
		if err != nil {
			return nil, err
		}
	}
	// If no connection provided, set to default multicast network
	if opts.conn == nil {
		MulticastOpt("224.0.42.1", 5432, nil)(opts)
	}

	nd := &NodeDiscover{
		conn: opts.conn,
		addr: opts.addr,
		sendTicker: time.NewTicker(opts.heartbeat),
		leaveTimeout: opts.leaveTimeout,
		registers: registerData{
			r: make(map[string]bool),
		},
		subscribers: subscribersData{
			s: make(map[chan NodeEvent]bool),
		},
		services: servicesData{
			s: make(map[string]*time.Timer),
		},
		events: make(chan NodeEvent),
		closed: make(chan struct{}),
	}

	// Start routines
	nd.connWG.Add(2)
	go nd.recvHeartbeat()
	go nd.sendHeartbeat()
	go nd.manageEvents()

	log.Infof("node-discovery: Listen connections from %v", nd.addr.String())

	return nd, nil
}

func (nd *NodeDiscover) recvHeartbeat() {
	// Manage wait group → use for closing UDP connection
	defer nd.connWG.Done()

	// Receive loop
	buf := make([]byte, MAX_UDP_PACKET)
	for {
		select {
		case <- nd.closed:
			log.Debug("node-discovery: recvHeartbeat routine finished")
			return
		default:
			// Receive packet
			nd.conn.SetReadDeadline(time.Now().Add(RECV_TIMEOUT))
			n, _, err := nd.conn.ReadFrom(buf)
			if err != nil {
				// Check timeout
				if err.(net.Error).Timeout() {
					log.Debugf("node-discovery: Did not receive packet during %v seconds", RECV_TIMEOUT.Seconds())
				} else {
					log.Warnf("node-discovery: Fail to receive packet: %v", err)
				}
				continue
			}
			log.Debugf("node-discovery: New packet received, size: %v", n)
			// Try to parse it
			var msg heartBeatMsg
			err = msgpack.Unmarshal(buf[:n], &msg)
			if err != nil {
				log.Warnf("node-discovery: Fail to decode received heartbeat: %v", err)
				continue
			}
			// Manage Packet
			nd.managePacket(msg)
		}
	}
}

func (nd *NodeDiscover) managePacket(msg heartBeatMsg) {
	for _, svcRaw := range msg.Services {
		// Parse service URL
		svcURL, err := url.Parse(svcRaw)
		if (err != nil) {
			log.Warnf("node-discovery: Fail to parse received service URL: %v", svcRaw)
			continue
		}
		// Check if service already exists
		nd.services.Lock()
		timer, exist := nd.services.s[svcRaw]
		if exist {
			log.Debugf("node-discovery: update entry for service: %v", svcRaw)
			// Reset timeout timer of the service
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(nd.leaveTimeout)
		} else {
			log.Debugf("node-discovery: new service discovered: %v", svcRaw)
			// Store new service with a new timer
			nd.services.s[svcRaw] = time.AfterFunc(nd.leaveTimeout, func() {
				// If timeout, send leave event
				nd.events <- NodeEvent{
					Type: ServiceLeaveEvent,
					Service: svcURL,
				}
				// Remove from list
				nd.services.Lock()
				delete(nd.services.s, svcRaw)
				nd.services.Unlock()
			})

			// Emit new service event
			nd.events <- NodeEvent{
				Type: ServiceJoinEvent,
				Service: svcURL,
			}
		}
		nd.services.Unlock()
	}
}

func (nd *NodeDiscover) manageEvents() {
	// Event loop
	for {
		select {
		case <- nd.closed:
			log.Debug("node-discovery: manageEvents routine finished")
			return
		case event := <- nd.events:
			// Send event to all the listeners
			nd.subscribers.RLock()
			for s := range nd.subscribers.s {
				s <- event
			}
			nd.subscribers.RUnlock()
		}
	}
}

func (nd *NodeDiscover) generateHeartbeat() heartBeatMsg {
	nd.registers.RLock()
	defer nd.registers.RUnlock()

	// Construct service list
	services := []string{}
	for s := range nd.registers.r {
		services = append(services, s)
	}

	// Construct packet
	return heartBeatMsg{
		Version: PROTOCOL_VERSION,
		Services: services,
	}
}

func (nd *NodeDiscover) sendHeartbeat() {
	// Manage wait group → use for closing UDP connection
	defer nd.connWG.Done()

	// Send loop
	for {
		select {
		case <- nd.closed:
			log.Debug("node-discovery: sendHeartbeat routine finished")
			return
		case <- nd.sendTicker.C:
			// Create heartbeat message
			msg := nd.generateHeartbeat()

			// Serialize it
			data, err := msgpack.Marshal(msg)
			if err != nil {
				log.Warnf("node-discovery: Fail to encode heartbeat: %v", err)
				continue
			}

			// Sent id
			_, err2 := nd.conn.WriteTo(data, nd.addr)
			if err2 != nil {
				log.Warnf("node-discovery: Fail to sent packet: %v", err)
				continue
			}
			log.Debugf("node-discovery: heartbeat sent with following services: %v", msg.Services)
		}
	}
}

func (nd *NodeDiscover) Subscribe(eventCh chan NodeEvent) {
	nd.subscribers.Lock()
	nd.subscribers.s[eventCh] = true
	nd.subscribers.Unlock()
	log.Info("node-discovery: Subscriber added")
}

func (nd *NodeDiscover) Unsubscribe(eventCh chan NodeEvent) {
	nd.subscribers.Lock()
	delete(nd.subscribers.s, eventCh)
	nd.subscribers.Unlock()
	log.Info("node-discovery: Subscriber removed")
}

func (nd *NodeDiscover) Register(service *url.URL) {
	nd.registers.Lock()
	nd.registers.r[service.String()] = true
	nd.registers.Unlock()
	log.Infof("node-discovery: Service %v registered", service.String())
}

func (nd *NodeDiscover) Deregister(service *url.URL) {
	nd.registers.Lock()
	delete(nd.registers.r, service.String())
	nd.registers.Unlock()
	log.Infof("node-discovery: Service %v deregistered", service.String())
}

func (nd *NodeDiscover) Close() {
	// Trigger close signal to routines
	log.Debug("node-discovery: Emit closing signal to routines...")
	close(nd.closed)

	// Wait for send/recv routines to stop before closing connection
	log.Debug("node-discovery: Waiting for routines to finish...")
	nd.connWG.Wait()

	log.Debug("node-discovery: Closing connection....")
	nd.conn.Close()
	log.Infof("node-discovery: Connection close to %v", nd.addr.String())
}