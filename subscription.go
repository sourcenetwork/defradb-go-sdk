package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"time"

	"github.com/sourcenetwork/defradb/client"
)

// subscribe relays the results of the GraphQL subscription id names until
// ctx ends or the library closes it, then closes the subscription.
// ponytail: polls every relayIdle; a blocking wait needs a library call (#2476).
func subscribe(ctx context.Context, id string) <-chan client.GQLResult {
	out := make(chan client.GQLResult)
	go func() {
		defer close(out)
		var a cargs
		defer a.free()
		cid := a.s(id)
		defer func() { _ = C.close_graphql_subscription(cid) }()
		for {
			p := C.poll_graphql_subscription(cid)
			switch p.status {
			case 0:
				raw := C.GoString(p.value)
				C.defra_free_string(p.value)
				select {
				case out <- gqlResult(raw):
				case <-ctx.Done():
					return
				}
				continue
			case 2:
			case 3:
				return
			default:
				message := ""
				if p.error != nil {
					message = C.GoString(p.error)
					C.defra_free_string(p.error)
				}
				select {
				case out <- client.GQLResult{Errors: []error{ffiError("subscription", message)}}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(relayIdle):
			}
		}
	}()
	return out
}
