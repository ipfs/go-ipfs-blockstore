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
	// We use this lock to make count & cache updates atomic.
	// 1. This doesn't add any additional contention because the cache internally uses a lock.
	// 2. It's safe to not use the lock on basic cache reads for the same reason.
	//
	// We take the _write_ lock when updating the counts, and take the read lock for everything
	// else.
	lk sync.RWMutex

	// Caching strategy:
	// 1. On read-have, we cache the have unless there was a concurrent delete.
	// 2. On read-have-not, we cache unless there was a concurrent put. If there was a
	//    concurrent put, we invalidate the cache if it says we have the key to avoid
	//    flip-flopping.
	// 3. On delete, we cache _have not_ unless there was a concurrent put.
	// 4. On put, we cache _have_ unless there was a concurrent delete.
	putCount, delCount uint64
	cache              *lru.TwoQueueCache

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

func (b *arccache) readCounts() (put, del uint64) {
	b.lk.RLock()
	defer b.lk.RUnlock()
	return b.putCount, b.delCount
}

func (b *arccache) cacheOnRead(k cid.Cid, has bool, size int, oldPutCount, oldDelCount uint64) {
	// We're intentionally taking the _read_ locks. These locks protect the counters, not the
	// cache itself.
	b.lk.RLock()
	defer b.lk.RUnlock()

	// 1. If we have it but there was a delete operation, don't cache a new value as we might have
	// deleted it.
	if has && oldDelCount != b.delCount {
		// We do not need to invalidate the cache here. The block may not exist anymore, but
		// there's no way to see it exist, not exist, then re-exist without an intervening
		// put (unlike in step 2).
		//
		// If we observe the block not existing, we'll either invalidate the cache (step 2,
		// if the cache says it exists) or cache the new state (step 3).
		//
		// NOTE: There is no way to cache here without re-reading from disk. The key being
		// un-cached doesn't mean it wasn't deleted as it could (unlikely) have been evicted
		// from the cache.
		return
	}

	// 2. If we didn't have it, but there was a put, check to see if this key is in the cache.
	if !has && oldPutCount != b.putCount {
		if has, _, ok := b.queryCache(k); ok && has {
			// If we have it, we need to invalidate the cache because we're about to
			// tell the user that the block isn't there but the cache says it's there.
			// This could be because:
			//
			// 1. It's actually there (we tried to read the block before it was put).
			// 2. There's an ongoing delete (we tried to read the block after it was
			//    deleted).
			//
			// In the second case, we can't leave it in the cache. Otherwise, future
			// calls to "has" will return "true" without an intervening put.
			b.invalidateCache(k)

			// If the cache says we don't have it, we're safe to leave that in the cache.
		}
		// At this point, we cannot cache anything. Even if the target key isn't in the
		// cache, that doesn't mean the block isn't in the blockstore as it could have been
		// evicted.
		return
	}

	// 3. Ok, we're clear to cache.
	if has && size >= 0 {
		b.cacheSize(k, size)
	} else {
		b.cacheHave(k, has)
	}
}

func (b *arccache) DeleteBlock(k cid.Cid) error {
	if !k.Defined() {
		return nil
	}

	if has, _, ok := b.queryCache(k); ok && !has {
		return nil
	}

	putCount, _ := b.readCounts()

	err := b.blockstore.DeleteBlock(k)
	if err != nil {
		return err
	}

	b.lk.Lock()
	defer b.lk.Unlock()

	// Record that there was a delete.
	b.delCount++
	if b.putCount == putCount {
		b.cacheHave(k, false)
	} else {
		// Ok, there was a concurrent put. We don't actually know which happened first, so
		// we invalidate the cache.
		b.invalidateCache(k)
	}
	return nil
}

func (b *arccache) Has(k cid.Cid) (bool, error) {
	if !k.Defined() {
		return false, nil
	}

	if has, _, ok := b.queryCache(k); ok {
		return has, nil
	}

	putCount, delCount := b.readCounts()

	has, err := b.blockstore.Has(k)
	if err == nil {
		b.cacheOnRead(k, has, -1, putCount, delCount)
	}

	return has, err
}

func (b *arccache) GetSize(k cid.Cid) (int, error) {
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

	putCount, delCount := b.readCounts()

	blockSize, err := b.blockstore.GetSize(k)
	if err == nil || err == ErrNotFound {
		b.cacheOnRead(k, err == nil, blockSize, putCount, delCount)
	}
	return blockSize, err
}

func (b *arccache) View(k cid.Cid, callback func([]byte) error) error {
	// shortcircuit and fall back to Get if the underlying store
	// doesn't support Viewer.
	if b.viewer == nil {
		blk, err := b.Get(k)
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

	putCount, delCount := b.readCounts()

	var cbErr error
	err := b.viewer.View(k, func(buf []byte) error {
		// The callback could run for an arbitrary amount of time. Cache early.
		b.cacheOnRead(k, true, len(buf), putCount, delCount)

		// Then call the callback, but stash the error instead of returning it. We _really_
		// need to be able to distinguish between a callback error and a blockstore error.
		cbErr = callback(buf)
		return nil
	})

	// If we had the block, we already cached that result. Now cache the not-found result, if
	// applicable.
	switch err {
	case nil:
		// Was found, return the inner error.
		return cbErr
	case ErrNotFound:
		// Wasn't found, cache.
		b.cacheOnRead(k, false, -1, putCount, delCount)
		return ErrNotFound
	default:
		// Blockstore error.
		return err
	}
}

func (b *arccache) Get(k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		return nil, ErrNotFound
	}

	if has, _, ok := b.queryCache(k); ok && !has {
		return nil, ErrNotFound
	}

	putCount, delCount := b.readCounts()

	bl, err := b.blockstore.Get(k)
	if err == nil && bl != nil { // bl != nil just to be doubly safe.
		b.cacheOnRead(k, true, len(bl.RawData()), putCount, delCount)
	} else if err == ErrNotFound {
		b.cacheOnRead(k, false, -1, putCount, delCount)
	}
	return bl, err
}

func (b *arccache) Put(bl blocks.Block) error {
	if has, _, ok := b.queryCache(bl.Cid()); ok && has {
		return nil
	}

	_, delCount := b.readCounts()

	err := b.blockstore.Put(bl)
	if err != nil {
		return err
	}

	b.lk.Lock()
	defer b.lk.Unlock()
	b.putCount++
	if b.delCount != delCount {
		// We raced a delete, invalidate the cache for all blocks we put. We may have them
		// (put won) we may not (delete won).
		b.invalidateCache(bl.Cid())
	} else {
		// There wasn't a concurrent delete, or it hasn't finished yet. Cache the new blocks.
		// If there's an in-progress delete, _it_ will see the increase to putCount and it
		// will invalidate the cache for us.
		b.cacheSize(bl.Cid(), len(bl.RawData()))
	}
	return nil
}

func (b *arccache) PutMany(bs []blocks.Block) error {
	good := make([]blocks.Block, 0, len(bs))
	for _, block := range bs {
		// call put on block if result is inconclusive or we are sure that
		// the block isn't in storage
		if has, _, ok := b.queryCache(block.Cid()); !ok || (ok && !has) {
			good = append(good, block)
		}
	}

	_, delCount := b.readCounts()

	err := b.blockstore.PutMany(good)
	if err != nil {
		return err
	}

	b.lk.Lock()
	defer b.lk.Unlock()
	b.putCount++
	if b.delCount != delCount {
		// We raced a delete, invalidate the cache for all blocks we put. We may have them
		// (put won) we may not (delete won).
		for _, block := range good {
			b.invalidateCache(block.Cid())
		}
	} else {
		// There wasn't a concurrent delete, or it hasn't finished yet. Cache the new blocks.
		// If there's an in-progress delete, _it_ will see the increase to putCount and it
		// will invalidate the cache for us.
		for _, block := range good {
			b.cacheSize(block.Cid(), len(block.RawData()))
		}
	}
	return nil
}

func (b *arccache) HashOnRead(enabled bool) {
	b.blockstore.HashOnRead(enabled)
}

func cacheKey(c cid.Cid) string {
	return string(c.Hash())
}

func (b *arccache) cacheHave(c cid.Cid, have bool) {
	b.cache.Add(cacheKey(c), cacheHave(have))
}

func (b *arccache) cacheSize(c cid.Cid, blockSize int) {
	b.cache.Add(cacheKey(c), cacheSize(blockSize))
}

func (b *arccache) invalidateCache(c cid.Cid) {
	b.cache.Remove(cacheKey(c))
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

	h, ok := b.cache.Get(cacheKey(k))
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

func (b *arccache) GCLock() Unlocker {
	return b.blockstore.(GCBlockstore).GCLock()
}

func (b *arccache) PinLock() Unlocker {
	return b.blockstore.(GCBlockstore).PinLock()
}

func (b *arccache) GCRequested() bool {
	return b.blockstore.(GCBlockstore).GCRequested()
}
