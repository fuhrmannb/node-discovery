package main

import (
	"github.com/fuhrmannb/node-discovery"
	"net/url"
	"os"
	"os/signal"
	"github.com/Sirupsen/logrus"
)

func main() {
	// Initialize logger
	log := logrus.New()
	// You can change log level to debug with:
	// log.Level = logrus.DebugLevel
	discovery.SetLogger(log)

	log.Print("Initializing discovery...")
	// Create our node discovery to receive/send requests
	nodeDiscovery, err := discovery.Listen()
	if err != nil {
		log.Fatalf("Node Discovery cannot be created: %v", err)
	}

	// Register a service (Note: a single node can register multiple services)
	// First, define our service (simple URL)
	hostname, errHostname := os.Hostname()
	if errHostname != nil {
		log.Fatalf("Error during retrieving hostname! %v", errHostname)
	}

	serviceURL, err := url.Parse("http://" + hostname + ":5542")
	if err != nil {
		log.Fatalf("URL cannot be parsed! %v", err)
	}

	// Register into
	nodeDiscovery.Register(serviceURL)

	// Subscribe to some events and print them
	// First, create a channel to collect events
	subCh := make(chan discovery.NodeEvent)

	// Create a routine to print received events, reading previous created channel
	go func() {
		for e := range subCh {
			// Two kind of events: Join and Leave
			switch e.Type {
			case discovery.ServiceJoinEvent:
				log.Printf("A new service has joined the cluster: %v", e.Service.String())
			case discovery.ServiceLeaveEvent:
				log.Printf("A service has left the cluster: %v", e.Service.String())
			}
		}
	}()

	// Subscribe channel to receive events
	nodeDiscovery.Subscribe(subCh)

	log.Print("Discovery initialized. Press CTRL+C to quit the app")
	// Manage CTRL-C Signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	for sig := range signalCh {
		// Close discovery and quit
		log.Printf("Receiving %v signal, exiting...", sig.String())
		nodeDiscovery.Close()
		os.Exit(0)
	}

}