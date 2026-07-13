package goav

import (
	"sync"

	"github.com/thesyncim/goav/av"
)

const mediaPoolMaxEntries = 64

type mediaPools struct {
	mu sync.Mutex

	metadata    []av.Metadata
	audioPlanes [][]av.Plane
	videoPlanes [][]av.Plane
}

func newMediaPools() *mediaPools {
	return &mediaPools{}
}

func runtimeMediaPools(rt *runtime) *mediaPools {
	if rt == nil {
		return nil
	}
	return rt.mediaPools
}

func (p *mediaPools) takeMetadata() av.Metadata {
	if p == nil {
		return make(av.Metadata)
	}
	p.mu.Lock()
	index := len(p.metadata) - 1
	if index < 0 {
		p.mu.Unlock()
		return make(av.Metadata)
	}
	metadata := p.metadata[index]
	p.metadata[index] = nil
	p.metadata = p.metadata[:index]
	p.mu.Unlock()
	return metadata
}

func (p *mediaPools) putMetadata(metadata av.Metadata) {
	if p == nil || metadata == nil {
		return
	}
	for key := range metadata {
		delete(metadata, key)
	}
	p.mu.Lock()
	if len(p.metadata) < mediaPoolMaxEntries {
		p.metadata = append(p.metadata, metadata)
	}
	p.mu.Unlock()
}

func takeMediaPlanes(pools *mediaPools, count int) []av.Plane {
	if pools == nil {
		return make([]av.Plane, count)
	}
	return pools.takePlanes(count)
}

func (p *mediaPools) takePlanes(count int) []av.Plane {
	if p == nil {
		return make([]av.Plane, count)
	}
	var planes []av.Plane
	p.mu.Lock()
	switch count {
	case 1:
		planes, p.audioPlanes = popPlaneSlice(p.audioPlanes)
	case 3:
		planes, p.videoPlanes = popPlaneSlice(p.videoPlanes)
	}
	p.mu.Unlock()
	if planes != nil {
		return planes[:count]
	}
	return make([]av.Plane, count)
}

func (p *mediaPools) putPlanes(planes []av.Plane) {
	if p == nil || len(planes) == 0 {
		return
	}
	count := len(planes)
	for i := range planes {
		planes[i].Reset()
	}
	p.mu.Lock()
	switch count {
	case 1:
		if len(p.audioPlanes) < mediaPoolMaxEntries {
			p.audioPlanes = append(p.audioPlanes, planes[:count])
		}
	case 3:
		if len(p.videoPlanes) < mediaPoolMaxEntries {
			p.videoPlanes = append(p.videoPlanes, planes[:count])
		}
	}
	p.mu.Unlock()
}

func popPlaneSlice(pool [][]av.Plane) ([]av.Plane, [][]av.Plane) {
	index := len(pool) - 1
	if index < 0 {
		return nil, pool
	}
	planes := pool[index]
	pool[index] = nil
	return planes, pool[:index]
}
