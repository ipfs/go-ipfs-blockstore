package blockstore

import (
	"context"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	metrics "github.com/ipfs/go-metrics-interface"
)

type cacheHave bool
type cacheSize int

// arccache wraps a BlockStore with an Adaptive Replacement Cache (ARC) that
// does not store the actual blocks, just metadata about them: existence and
// size. This provides block access-time improvements, allowing
// to short-cut many searches without querying the underlying datastore.
type arccache struct {
	cache *lru.TwoQueueCache
	lks   [256]sync.RWMutex

	blockstore Blockstore
	viewer     Viewer

	hits  metrics.Counter
	total metrics.Counter
}

var _ Blockstore = (*arccache)(nil)
var _ Viewer = (*arccache)(nil)

func newARCCachedBS(ctx context.Context, bs Blockstore, lruSize int) (*arccache, error) {
	cache, err := lru.New2Q(lruSize)
	if err != nil {
		return nil, err
	}
	c := &arccache{cache: cache, blockstore: bs}
	c.hits = metrics.NewCtx(ctx, "arc.hits_total", "Number of ARC cache hits").Counter()
	c.total = metrics.NewCtx(ctx, "arc_total", "Total number of ARC cache requests").Counter()
	if v, ok := bs.(Viewer); ok {
		c.viewer = v
	}
	return c, nil
}

func (b *arccache) getLock(k cid.Cid) *sync.RWMutex {
	return &b.lks[mutexKey(k)]
}

func mutexKey(k cid.Cid) uint8 {
	return k.KeyString()[len(k.KeyString())-1]
}

func (b *arccache) DeleteBlock(ctx context.Context, k cid.Cid) error {
	if !k.Defined() {
		return nil
	}

	if has, _, ok := b.queryCache(k); ok && !has {
		return nil
	}

	lk := b.getLock(k)
	lk.Lock()
	defer lk.Unlock()

	b.cache.Remove(k) // Invalidate cache before deleting.
	err := b.blockstore.DeleteBlock(ctx, k)
	if err == nil {
		b.cacheHave(k, false)
	}
	return err
}

func (b *arccache) Has(ctx context.Context, k cid.Cid) (bool, error) {
	if !k.Defined() {
		return false, nil
	}

	if has, _, ok := b.queryCache(k); ok {
		return has, nil
	}

	lk := b.getLock(k)
	lk.RLock()
	defer lk.RUnlock()

	has, err := b.blockstore.Has(ctx, k)
	if err != nil {
		return false, err
	}
	b.cacheHave(k, has)
	return has, nil
}

func (b *arccache) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	if !k.Defined() {
		return -1, ErrNotFound
	}

	if has, blockSize, ok := b.queryCache(k); ok {
		if !has {
			// don't have it, return
			return -1, ErrNotFound
		}
		if blockSize >= 0 {
			// have it and we know the size
			return blockSize, nil
		}
		// we have it but don't know the size, ask the datastore.
	}

	lk := b.getLock(k)
	lk.RLock()
	defer lk.RUnlock()

	blockSize, err := b.blockstore.GetSize(ctx, k)
	if err == ErrNotFound {
		b.cacheHave(k, false)
	} else if err == nil {
		b.cacheSize(k, blockSize)
	}
	return blockSize, err
}

func (b *arccache) View(ctx context.Context, k cid.Cid, callback func([]byte) error) error {
	// shortcircuit and fall back to Get if the underlying store
	// doesn't support Viewer.
	if b.viewer == nil {
		blk, err := b.Get(ctx, k)
		if err != nil {
			return err
		}
		return callback(blk.RawData())
	}

	if !k.Defined() {
		return ErrNotFound
	}

	if has, _, ok := b.queryCache(k); ok && !has {
		// short circuit if the cache deterministically tells us the item
		// doesn't exist.
		return ErrNotFound
	}

	lk := b.getLock(k)
	lk.RLock()
	defer lk.RUnlock()

	return b.viewer.View(ctx, k, callback)
}

func (b *arccache) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		return nil, ErrNotFound
	}

	if has, _, ok := b.queryCache(k); ok && !has {
		return nil, ErrNotFound
	}

	lk := b.getLock(k)
	lk.RLock()
	defer lk.RUnlock()

	bl, err := b.blockstore.Get(ctx, k)
	if bl == nil && err == ErrNotFound {
		b.cacheHave(k, false)
	} else if bl != nil {
		b.cacheSize(k, len(bl.RawData()))
	}
	return bl, err
}

func (b *arccache) Put(ctx context.Context, bl blocks.Block) error {
	if has, _, ok := b.queryCache(bl.Cid()); ok && has {
		return nil
	}

	lk := b.getLock(bl.Cid())
	lk.Lock()
	defer lk.Unlock()

	err := b.blockstore.Put(ctx, bl)
	if err == nil {
		b.cacheSize(bl.Cid(), len(bl.RawData()))
	}
	return err
}

func (b *arccache) PutMany(ctx context.Context, bs []blocks.Block) error {
	mxs := [256]*sync.RWMutex{}
	var good []blocks.Block
	for _, block := range bs {
		// call put on block if result is inconclusive or we are sure that
		// the block isn't in storage
		if has, _, ok := b.queryCache(block.Cid()); !ok || (ok && !has) {
			good = append(good, block)
			mxs[mutexKey(block.Cid())] = &b.lks[mutexKey(block.Cid())]
		}
	}

	for _, mx := range mxs {
		if mx != nil {
			mx.Lock()
		}
	}

	defer func() {
		for _, mx := range mxs {
			if mx != nil {
				mx.Unlock()
			}
		}
	}()

	err := b.blockstore.PutMany(ctx, good)
	if err != nil {
		return err
	}
	for _, block := range good {
		b.cacheSize(block.Cid(), len(block.RawData()))
	}
	return nil
}

func (b *arccache) HashOnRead(ctx context.Context, enabled bool) {
	b.blockstore.HashOnRead(ctx, enabled)
}

func (b *arccache) cacheHave(c cid.Cid, have bool) {
	b.cache.Add(string(c.Hash()), cacheHave(have))
}

func (b *arccache) cacheSize(c cid.Cid, blockSize int) {
	b.cache.Add(string(c.Hash()), cacheSize(blockSize))
}

// queryCache checks if the CID is in the cache. If so, it returns:
//
//  * exists (bool): whether the CID is known to exist or not.
//  * size (int): the size if cached, or -1 if not cached.
//  * ok (bool): whether present in the cache.
//
// When ok is false, the answer in inconclusive and the caller must ignore the
// other two return values. Querying the underying store is necessary.
//
// When ok is true, exists carries the correct answer, and size carries the
// size, if known, or -1 if not.
func (b *arccache) queryCache(k cid.Cid) (exists bool, size int, ok bool) {
	b.total.Inc()
	if !k.Defined() {
		log.Error("undefined cid in arccache")
		// Return cache invalid so the call to blockstore happens
		// in case of invalid key and correct error is created.
		return false, -1, false
	}

	h, ok := b.cache.Get(string(k.Hash()))
	if ok {
		b.hits.Inc()
		switch h := h.(type) {
		case cacheHave:
			return bool(h), -1, true
		case cacheSize:
			return true, int(h), true
		}
	}
	return false, -1, false
}

func (b *arccache) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return b.blockstore.AllKeysChan(ctx)
}

func (b *arccache) GCLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).GCLock(ctx)
}

func (b *arccache) PinLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).PinLock(ctx)
}

func (b *arccache) GCRequested(ctx context.Context) bool {
	return b.blockstore.(GCBlockstore).GCRequested(ctx)
}
