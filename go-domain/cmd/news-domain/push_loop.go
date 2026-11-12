package main

import (
	"context"
	"log"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/apnspush"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
)

func runPushLoop(ctx context.Context, service *apnspush.Service, credentials *credentialstore.Store, guard *authority.Guard) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var sender *apnspush.Sender
	var revision int64 = -1
	for {
		select {
		case <-ctx.Done():
			if sender != nil {
				sender.CloseIdleConnections()
			}
			return
		case <-ticker.C:
			if guard == nil || guard.Check(ctx) != nil {
				continue
			}
			var current *apnspush.Sender
			if service.SenderEnabled(ctx) && credentials != nil {
				raw, meta, err := credentials.Resolve(ctx, "apns")
				if err == nil && meta.Configured && len(raw) > 0 {
					if sender == nil || meta.Revision != revision {
						if sender != nil {
							sender.CloseIdleConnections()
						}
						sender = nil
						revision = meta.Revision
						config, key, e := apnspush.ParseCredential(raw)
						if e == nil {
							sender = &apnspush.Sender{Credential: config, Key: key}
						}
					}
					current = sender
				}
			}
			if err := service.Process(ctx, current, 8); err != nil {
				log.Print("APNs delivery pass unavailable")
			}
		}
	}
}
