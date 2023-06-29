package etcdutil

import (
	"context"
	"errors"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Lease struct {
	client *clientv3.Client
	lease  clientv3.Lease
	id     clientv3.LeaseID

	key   string
	value string

	stopped int32
}

var ErrLeaseInterrupted error = errors.New("lease interrupted")

func NewLease(client *clientv3.Client, key, value string, ttl int64, onError func(error)) (*Lease, error) {
	lease := clientv3.NewLease(client)
	lRsp, err := lease.Grant(context.Background(), ttl)
	if err != nil {
		return nil, err
	}

	_, err = client.KV.Put(context.Background(), key, value, clientv3.WithLease(lRsp.ID))
	if err != nil {
		return nil, err
	}

	lch, err := lease.KeepAlive(context.Background(), lRsp.ID)
	if err != nil {
		return nil, err
	}

	ret := &Lease{
		client: client,
		lease:  lease,
		id:     lRsp.ID,
		key:    key,
		value:  value,
	}

	go func() {
		for range lch {
		}
		if atomic.LoadInt32(&ret.stopped) == 0 {
			ret.Stop()
			onError(ErrLeaseInterrupted)
		}
	}()

	return ret, nil
}

func (l *Lease) Stop() {
	if atomic.AddInt32(&l.stopped, 1) == 1 {
		l.client.KV.Delete(context.Background(), l.key)
		l.lease.Close()
	}
}
