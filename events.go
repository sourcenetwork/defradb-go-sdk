package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/sourcenetwork/defradb/event"
)

// relayIdle is how long the relay waits before polling again once the
// library has no event for it.
// ponytail: fixed poll interval; a blocking wait needs a library call (#2476).
const relayIdle = 10 * time.Millisecond

// relay publishes the node's events on a Go bus, polling the library's
// event subscription.
type relay struct {
	bus  event.Bus
	sub  C.uintptr_t
	quit chan struct{}
	done chan struct{}
}

// startRelay subscribes to the node's events and relays them until stop.
func startRelay(ptr C.uintptr_t) (*relay, error) {
	r := C.create_merge_complete_subscription(ptr)
	if r.status != 0 {
		message := C.GoString(r.error)
		C.defra_free_string(r.error)
		return nil, ffiError("events", message)
	}
	rl := &relay{
		bus:  event.NewChannelBus(0, 100),
		sub:  r.subscription_handle,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	go rl.run()
	return rl, nil
}

func (r *relay) run() {
	defer close(r.done)
	for {
		select {
		case <-r.quit:
			return
		default:
		}
		p := C.poll_subscription(r.sub)
		switch p.status {
		case 0:
			raw := C.GoString(p.value)
			C.defra_free_string(p.value)
			if msg, ok := goEvent(raw); ok {
				r.bus.Publish(msg)
			}
			continue
		case 2:
		case 3:
			return
		default:
			if p.error != nil {
				C.defra_free_string(p.error)
			}
		}
		select {
		case <-r.quit:
			return
		case <-time.After(relayIdle):
		}
	}
}

// stop ends the relay, closes the library subscription and the bus.
func (r *relay) stop() {
	close(r.quit)
	<-r.done
	_ = C.close_subscription(r.sub)
	r.bus.Close()
}

// goEvent is the Go DefraDB event the library's event JSON describes.
func goEvent(raw string) (event.Message, bool) {
	var e struct {
		Type         string `json:"type"`
		DocID        string `json:"doc_id"`
		Cid          string `json:"cid"`
		CollectionID string `json:"collection_id"`
		Block        string `json:"block"`
		IsRetry      bool   `json:"is_retry"`
		IsRelay      bool   `json:"is_relay"`
		Topic        string `json:"topic"`
		ByPeer       string `json:"by_peer"`
		PeerID       string `json:"peer_id"`
		EventType    string `json:"event_type"`
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return event.Message{}, false
	}
	id, _ := cid.Decode(e.Cid)
	switch e.Type {
	case "update":
		block, _ := base64.StdEncoding.DecodeString(e.Block)
		return event.NewMessage(event.UpdateName, event.Update{
			DocID:        e.DocID,
			Cid:          id,
			CollectionID: e.CollectionID,
			Block:        block,
			IsRetry:      e.IsRetry,
			IsRelay:      e.IsRelay,
		}), true
	case "merge_complete":
		return event.NewMessage(event.MergeCompleteName, event.MergeComplete{Merge: event.Merge{
			DocID:        e.DocID,
			ByPeer:       e.ByPeer,
			Cid:          id,
			CollectionID: e.CollectionID,
		}}), true
	case "topic_peer_event":
		return event.NewMessage(event.TopicPeerEventName, event.TopicPeerEvent{
			PeerID:    e.PeerID,
			Topic:     e.Topic,
			EventType: e.EventType,
		}), true
	case "se_artifact_received":
		return event.NewMessage(event.SEArtifactReceivedName, event.SEArtifactReceived{DocID: e.DocID}), true
	}
	return event.Message{}, false
}
