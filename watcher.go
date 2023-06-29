package etcdutil

import (
	"context"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type WatcherHandler interface {
	OnPut(key, value []byte)
	OnDelete(key []byte)
	OnError(error)
	OnResetBegin()
	OnResetEnd()
}

type Watcher struct {
	client  *clientv3.Client
	watcher clientv3.Watcher
	handler WatcherHandler

	key  string
	opts []clientv3.OpOption

	started int32
	stopped int32
}

func NewWatcher(client *clientv3.Client, handler WatcherHandler, key string, opts ...clientv3.OpOption) *Watcher {
	return &Watcher{
		client:  client,
		watcher: clientv3.NewWatcher(client),
		handler: handler,
		key:     key,
		opts:    opts,
	}
}

func (w *Watcher) Start() {
	if atomic.AddInt32(&w.started, 1) == 1 {
		go w.run()
	}
}

func (w *Watcher) Stop() error {
	if atomic.AddInt32(&w.stopped, 1) == 1 {
		return w.watcher.Close()
	}
	return nil
}

func (w *Watcher) run() {
	rsp, err := w.client.KV.Get(context.Background(), w.key, w.opts...)
	if err != nil {
		w.Stop()
		w.handler.OnError(err)
		return
	}

	w.handler.OnResetBegin()
	rev := rsp.Header.Revision
	for _, kv := range rsp.Kvs {
		w.handler.OnPut(kv.Key, kv.Value)
	}
	w.handler.OnResetEnd()

	wOpts := append([]clientv3.OpOption{clientv3.WithRev(rev)}, w.opts...)
	wch := w.watcher.Watch(context.Background(), w.key, wOpts...)

	for wResp := range wch {
		if wResp.Canceled {
			if wResp.Err() == rpctypes.ErrCompacted {
				go w.run()
			} else {
				w.Stop()
				w.handler.OnError(wResp.Err())
			}
			return
		}
		for _, event := range wResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				w.handler.OnPut(event.Kv.Key, event.Kv.Value)
			case mvccpb.DELETE:
				w.handler.OnDelete(event.Kv.Key)
			}
		}
	}
}
