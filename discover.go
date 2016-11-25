/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */

/*
Package discovery is useful to create connected applications that need to be discovered into a local network.
This solution is compatible with multicast enabled networks. It can be used without third party software.

This package is based on nodes sharing periodically information between them. Each node can register service URLs
that will be exchanged to others, and subscribe to service events.

Exchanges are based on the Node Discovery Protocol. More detail here:
https://github.com/StarburstComputing/node-discovery/wiki/Node-Discovery-Protocol

More informations on this package on the Github repository:
https://github.com/StarburstComputing/node-discovery
 */
package discovery

import (
	"net"
	"sync"
	"time"
	"net/url"
	"gopkg.in/vmihailenco/msgpack.v2"
	"errors"
	"github.com/Sirupsen/logrus"
)

// The logger used by the package. Can be changed with the SetLogger() function.
var log logrus.FieldLogger = logrus.New()

// SetLogger sets a specific logger for the package.
// The given logger must implement the logrus.FieldLogger interface, see Logrus documentation and code for more
// informations.
func SetLogger(logger logrus.FieldLogger) {
	log = logger
}

const (
	// ProtocolVersion represents the version of the Node Discovery Protocol used in this package.
	ProtocolVersion = "beta1"
	// MaxPacketSize is the max size in bytes to receive data from the network.
	// Used for heartbeats reception.
	MaxPacketSize = 65536
	// ReceiveTimeout is the duration that specify the read timeout of the network connection.
	// Used for receive connection not to be stuck infinitely.
	ReceiveTimeout = 5 * time.Second
	// DefaultHeartbeat is the default time period to send heartbeat messages.
	// Can be changed using the HeartbeatOpt() option in the initialisation of the NodeDiscover.
	DefaultHeartbeat = time.Second
	// DefaultLeaveTimeout is the default time period when a service is considered inactive and has left the network.
	// Can be changed using the LeaveTimeoutOpt() option in the initialisation of the NodeDiscover.
	DefaultLeaveTimeout = 10 * time.Second
)

// Internal representation of the registered services.
type registersData struct {
	sync.RWMutex
	// The registered services. Before accessing, please use the RWMutex.
	r map[string]bool
}
// Internal representation of the subscribed channels.
type subscribersData struct {
	sync.RWMutex
	// The subscribed channels. Before accessing, please use the RWMutex.
	s map[chan NodeEvent]bool
}
// Internal representation of the received services.
// The attached timer represents the action to do when the service times out (become inactive).
type servicesData struct {
	sync.RWMutex
	// The received services. Before accessing, please use the RWMutex.
	s map[string]*time.Timer
}

// NodeDiscover is a structure representing a node.
// To construct a NodeDiscovery, use Listen() function.
type NodeDiscover struct {
	// The network connection.
	conn net.PacketConn
	// The address to send heartbeat.
	addr net.Addr
	// Used to synchronized routines before closing the connection.
	connWG sync.WaitGroup
	// The timer to send periodically heartbeats.
	sendTicker *time.Ticker
	// The custom leave timeout.
	leaveTimeout time.Duration
	// Registered services.
	registers registersData
	// Subscribed channels.
	subscribers subscribersData
	// Received services.
	services servicesData
	// Channel managing events.
	events chan NodeEvent
	// Channel used to trigger a close connection call.
	closed chan struct{}
}

// The representation of a heartbeat message with its MessagePack serialization.
// See the Node Discovery Protocol to get information about field meanings.
type heartBeatMsg struct {
	Version string `msgpack:"version"`
	Services []string `msgpack:"services"`
}

// EventType represents types of event triggered by the node discovery.
type EventType int
const (
	// ServiceJoinEvent is triggered when a new service joins the network.
	ServiceJoinEvent EventType = iota
	// ServiceLeaveEvent is triggered when a service leaves the network.
	ServiceLeaveEvent
)

// NodeEvent is a structure containing event information.
type NodeEvent struct {
	// Type of the event (service joining/leaving).
	Type EventType
	// Service related to the event.
	Service *url.URL
}

// The options used to initialize the NodeDiscovery.
// See functional options for more information.
type options struct {
	conn net.PacketConn
	addr net.Addr
	heartbeat time.Duration
	leaveTimeout time.Duration
}

// NodeDiscoverOpt represents a functional option to customize a NodeDiscover object during its initialization.
// See the documentation of the following implementations for common use cases.
//
// You can also create your own customize option. Have a look to the source code to see examples.
type NodeDiscoverOpt func(*options) (error)

// HeartbeatOpt is a functional option to specify a custom period to sent heartbeat to other nodes.
//
// Example:
//     nd, err := Listen(HeartbeatOpt(2 * time.Seconds))
func HeartbeatOpt(heartbeat time.Duration) NodeDiscoverOpt {
	return func(o *options) error {
		o.heartbeat = heartbeat
		return nil
	}
}

// LeaveTimeoutOpt is a functional option to specify a custom timeout duration for services.
// If no heartbeat containing the service is received by the node until the timeout period, then the service will be
// considered as inactive.
//
// Example:
//     nd, err := Listen(LeaveTimeoutOpt(5 * time.Seconds))
func LeaveTimeoutOpt(timeout time.Duration) NodeDiscoverOpt {
	return func(o *options) error {
		o.leaveTimeout = timeout
		return nil
	}
}

// CustomConnectionOpt is a functional option to specify a custom network connection and address.
// The connection must implement the net.PacketConn interface, and the address the net.Addr interface.
//
// This function must be used for uncommon cases (such as tests or use of specific network layers).
// See MulticastOpt() for common uses.
//
// Note: NodeDiscover object manages the closing of the given connection.
func CustomConnectionOpt(connection net.PacketConn, addr net.Addr) NodeDiscoverOpt {
	return func(o *options) error {
		o.conn = connection
		o.addr = addr
		return nil
	}
}

// MulticastOpt is a functional option to specify custom options for the multicast network used by the NodeDiscover Object.
//
// MulticastAddress parameter corresponds to the multicast UDP address to listen.
// Port parameter corresponds to the port to listen.
// Interface parameter correponds to the network interface to listen. It can be a specific interface ("eth0", "lo") or
// all the interfaces (represented by nil). See net.Interface documentation for more details.
//
// Example:
//     interface, _ := InterfaceByName("eth0")
//     nd, err := Listen(MulticastOpt("224.1.2.3, 1234, interface)
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

// Listen allows to create a NodeDiscover object connected to a Node Discovery Network.
// All NodeDiscover objects must be created with this function.
//
// Example of a basic use of the function:
//     nd, err := Listen()
// The default behavior of Listen() is to construct an UDP multicast network on 224.0.42.1:5432 listening to all the
// network interfaces. Default heartbeat period is 1s and service timeout is 10s.
//
// Listen function accepts functional options. See the documentation of NodeDiscovery options to know how to customize
// NodeDiscover initialization and behavior.
func Listen(customOpts ...NodeDiscoverOpt) (*NodeDiscover, error) {
	opts := &options {
		conn: nil,
		addr: nil,
		// Default options
		heartbeat: DefaultHeartbeat,
		leaveTimeout: DefaultLeaveTimeout,
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
		registers: registersData{
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

// Go routine function managing heartbeat receiving and decoding.
func (nd *NodeDiscover) recvHeartbeat() {
	// Manage wait group → use for closing UDP connection
	defer nd.connWG.Done()

	// Receive loop
	buf := make([]byte, MaxPacketSize)
	for {
		select {
		case <- nd.closed:
			log.Debug("node-discovery: recvHeartbeat routine finished")
			return
		default:
			// Receive packet
			nd.conn.SetReadDeadline(time.Now().Add(ReceiveTimeout))
			n, _, err := nd.conn.ReadFrom(buf)
			if err != nil {
				// Check timeout
				if err.(net.Error).Timeout() {
					log.Debugf("node-discovery: Did not receive packet during %v seconds", ReceiveTimeout.Seconds())
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

// Function to manage event creation from a heartbeat message
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

// Go routing function managing events.
// Creating events are sent to subscribed channels.
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

// Function to create easily a node-specific heartbeat message.
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
		Version: ProtocolVersion,
		Services: services,
	}
}

// Go routine function managing heartbeat sending
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

// Subscribe method allows the subscription of the given channel to node events.
// See NodeEvent documentation about service events.
func (nd *NodeDiscover) Subscribe(eventCh chan NodeEvent) {
	nd.subscribers.Lock()
	nd.subscribers.s[eventCh] = true
	nd.subscribers.Unlock()
	log.Info("node-discovery: Subscriber added")
}

// Unsubscribe method allows the removing of the given channel from node events.
//
// If the given channel was not subscribed previously, this method does nothing.
func (nd *NodeDiscover) Unsubscribe(eventCh chan NodeEvent) {
	nd.subscribers.Lock()
	delete(nd.subscribers.s, eventCh)
	nd.subscribers.Unlock()
	log.Info("node-discovery: Subscriber removed")
}

// Register method allows the registration of the given service URL.
// Other nodes of the network will be able to discover this service.
func (nd *NodeDiscover) Register(service *url.URL) {
	nd.registers.Lock()
	nd.registers.r[service.String()] = true
	nd.registers.Unlock()
	log.Infof("node-discovery: Service %v registered", service.String())
}

// Deregister allows the removing of the given service URL registration.
// After a certain amount of time (leaveTimeout), other nodes of the network will receive a leave event about this
// service.
//
// If the given service URL was not registered previously, this method does nothing.
func (nd *NodeDiscover) Deregister(service *url.URL) {
	nd.registers.Lock()
	delete(nd.registers.r, service.String())
	nd.registers.Unlock()
	log.Infof("node-discovery: Service %v deregistered", service.String())
}

// Close method closes the network connection and stops the node activity.
//
// Note: the connection can not be reopened in this current implementation.
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