# Node Discovery

[![Build Status](https://travis-ci.org/StarburstComputing/node-discovery.svg?branch=master)](https://travis-ci.org/StarburstComputing/node-discovery)
[![GoDoc](https://godoc.org/github.com/StarburstComputing/node-discovery?status.svg)](https://godoc.org/github.com/StarburstComputing/node-discovery)

Simple decentralized discovery library written in Go

## Description

Node Discovery is a Go library useful to create connected applications that
need to be discovered into a local network.
This solution is compatible with multicast enabled networks, it is adapted to
home environment where no infrastructure platforms are available (such as DNS).

This package is based on nodes sharing periodically information between them.
Each node can:
* Register service URLs that will be exchanged to others
* Subscribe to service events (when a service join or leave the cluster).

The exchanges is based on the _Node Discovery Protocol_.
More information on this norm can be found [here](https://github.com/StarburstComputing/node-discovery/wiki/Node-Discovery-Protocol).

This software has been tested with the following operating systems:
* `linux/amd64`

It must also be compatible with the other operating systems supported by the
Golang language.

## Installation

In a simple way, just use `go get`:

```
$ go get github.com/StarburstComputing/node-discovery
```

In your code, import the package:

```go
import "github.com/StarburstComputing/node-discovery"
```

## Quickstart

Some examples are available into the `examples` directory.

To test one of the examples:

```
# We start here the simple_discovery example
$ go run examples/simple_discovery.go
```

Examples can also be run into a Docker container.
Assuming `docker` and `docker-compose` are installed in your environment, examples can be started with this command:

```
# We start here 3 instances of the simple-discovery example
$ docker-compose scale simple-discovery=3
```

The docker way allows you to test multiple nodes without having multiple devices.

## Using the Node Discovery package

### Basics

First of all, you must create a _NodeDiscover_ object using the `Listen()` function:

```go
myNodeDiscovery := discovery.Listen()
```

`Listen()` arguments are functional options. The following options are proposed by the API:
* `MulticastOpt`: Use multicast UDP connection with a custom address and port. 
   By default, the discovery node will create a multicast UDP network on `224.0.42.1:5432`.
* `CustomConnectionOpt`: Specify a custom connection and address to create discovery network.
   This option can be used for non-UDP multicast network use.
* `HeartbeatOpt`: Change the default heartbeat frequency (default: `1s`)
* `LeaveTimeoutOpt`: Change the timeout for an inactive service (default: `10s`)

You can also create your own option function by passing a custom `NodeDiscoverOpt`.
See the source code and this [post](http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for more
details.

At the end of the program, don't forget to close the connection:

```go
myNodeDiscovery.Close()
```

### Register services

Services are simple URLs. Each node can register as many URLs as they want using the `Register()` method:

```go
serviceURL, _ := url.Parse("http://my-service.com:5542")
myNodeDiscovery.Register(serviceURL)
```

A service can be unregistered with the `Unregister()` method.

### Subscribe to service events

Each node can receive a event when a service:
* Join the network
* Leave the network (after unregistration or timeout)

In this Go implementation, events are managed with channels. The `Subscribe()` method accepts channel object and events
will populate the subscribed channels:

```go
subCh := make(chan discovery.NodeEvent)
myNodeDiscovery.Subscribe(subCh)
```

To remove subscription, use `Unsubscribe()` method.

When an event is emitted, a `NodeEvent` structure is created with the following fields:
* `Type`: Kind of event (_Join_ or _Leave_)
* `Service`: The URL of the related service

### Logging

Node Discovery uses [Logrus](https://github.com/Sirupsen/logrus) as log manager.
You can use your custom log manager by calling `SetLogger()` function.
The given logger must implement the `logrus.FieldLogger` interface.

## How to contribute

This project is open-source (Apache 2.0 license), feel free to report bugs and contribute!
To propose a change, please open a Pull Request, follow the Go standard and write unit tests related to your changes.

If you want to develop into the software, we recommend to use [Glide](http://glide.sh) instead of the `go get` solution:

```
$ git clone https://github.com/StarburstComputing/node-discovery
$ glide install
```
