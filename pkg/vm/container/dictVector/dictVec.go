package dictVector

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container"
	"github.com/deepfabric/thinkbase/pkg/vm/container/store"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(name string, limit int) *dictVector {
	return &dictVector{
		name:  name,
		limit: limit,
		db:    store.New(name),
		mp:    make(map[string]value.Array),
	}
}

func (p *dictVector) Destroy() error {
	if p.db == nil {
		return nil
	}
	err := p.db.Destroy()
	p.db = nil
	return err
}

func (p *dictVector) PopKey() (string, error) {
	p.RLock()
	defer p.RUnlock()
	if p.db == nil {
		return "", nil
	}
	if len(p.mp) == 0 {
		k, err := p.db.Lkey()
		if err != nil {
			return "", err
		}
		return string(k), nil
	}
	for k := range p.mp {
		return k, nil
	}
	return "", nil
}

func (p *dictVector) Delete(k string) error {
	p.Lock()
	defer p.Unlock()
	delete(p.mp, k)
	return nil
}

func (p *dictVector) Pops(k string, limit int) (value.Array, error) {
	var r value.Array

	p.Lock()
	defer p.Unlock()
	for {
		a, ok := p.mp[k]
		if !ok {
			err := p.fill(k)
			switch {
			case err == container.Empty:
				return nil, container.NotExist
			case err != nil:
				return nil, err

			}
			continue
		}
		size := 0
		for size < limit && len(a) > 0 {
			size += a[0].Size()
			r = append(r, a[0])
			a[0] = nil
			a = a[1:]
		}
		if len(a) > 0 {
			p.mp[k] = a
		} else {
			delete(p.mp, k)
		}
		p.size -= r.Size()
		return r, nil
	}
}

func (p *dictVector) PopsArray(k string, limit int) (value.Array, error) {
	var r value.Array

	p.Lock()
	defer p.Unlock()
	for {
		a, ok := p.mp[k]
		if !ok {
			err := p.fillArray(k)
			switch {
			case err == container.Empty:
				return nil, container.NotExist
			case err != nil:
				return nil, err

			}
			continue
		}
		size := 0
		for size < limit && len(a) > 0 {
			size += a[0].Size()
			r = append(r, a[0])
			a[0] = nil
			a = a[1:]
		}
		if len(a) > 0 {
			p.mp[k] = a
		} else {
			delete(p.mp, k)
		}
		p.size -= r.Size()
		return r, nil
	}
}

func (p *dictVector) Push(k string, a value.Array) error {
	p.Lock()
	defer p.Unlock()
	if p.size*Scale < p.limit {
		if v, ok := p.mp[k]; ok {
			p.mp[k] = append(v, a...)
		} else {
			p.mp[k] = a
			p.size += len(a)
		}
		p.size += a.Size()
		return nil
	}
	for _, v := range a {
		data, err := encoding.EncodeValue(v)
		if err != nil {
			return err
		}
		if err := p.db.Lpush([]byte(k), data); err != nil {
			return err
		}
	}
	return nil
}

func (p *dictVector) fill(k string) error {
	size := p.limit - p.size*Scale
	if size < 0 {
		size = 1024
	}
	vs, err := p.db.Lpops([]byte(k), size)
	if err != nil {
		return err
	}
	for _, v := range vs {
		a, err := getValue(v)
		if err != nil {
			return err
		}
		p.size += a.Size()
		p.mp[k] = append(p.mp[k], a)
	}
	return nil
}

func (p *dictVector) fillArray(k string) error {
	size := p.limit - p.size*Scale
	if size < 0 {
		size = 1024
	}
	vs, err := p.db.Lpops([]byte(k), size)
	if err != nil {
		return err
	}
	for _, v := range vs {
		a, err := getArray(v)
		if err != nil {
			return err
		}
		p.size += a.Size()
		p.mp[k] = append(p.mp[k], value.Array{a}...)
	}
	return nil
}

func getValue(data []byte) (value.Value, error) {
	v, _, err := encoding.DecodeValue(data)
	if err != nil {
		return nil, err
	}
	return v.(value.Value), nil
}

func getArray(data []byte) (value.Array, error) {
	v, _, err := encoding.DecodeValue(data)
	if err != nil {
		return nil, err
	}
	return v.(value.Array), nil
}
