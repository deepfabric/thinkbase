package dictionary

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/store"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(name string, limit int) *dictionary {
	return &dictionary{
		name:  name,
		limit: limit,
		db:    store.New(name),
		mp:    make(map[string]struct{}),
	}
}

func (p *dictionary) Destroy() error {
	if p.db == nil {
		return nil
	}
	err := p.db.Destroy()
	p.db = nil
	return err
}

func (p *dictionary) GetOrSet(v value.Value) (bool, error) {
	p.Lock()
	defer p.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return false, err
	}
	if _, ok := p.mp[string(k)]; ok {
		return true, nil
	}
	if p.size < p.limit {
		p.mp[string(k)] = struct{}{}
		p.size += len(k)
		return false, nil
	}
	return p.db.GetOrSet(k, []byte{})
}
