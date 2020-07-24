package blockstore

import (
	"context"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
)

var exampleBlock = blocks.NewBlock([]byte("foo"))

func testArcCached(ctx context.Context, bs Blockstore) (*arccache, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	opts := DefaultCacheOpts()
	opts.HasBloomFilterSize = 0
	opts.HasBloomFilterHashes = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*arccache), nil
	}
	return nil, err
}

func createStores(t *testing.T) (*arccache, Blockstore, *callbackDatastore) {
	cd := &callbackDatastore{f: func() {}, ds: ds.NewMapDatastore()}
	bs := NewBlockstore(syncds.MutexWrap(cd))
	arc, err := testArcCached(context.TODO(), bs)
	if err != nil {
		t.Fatal(err)
	}
	return arc, bs, cd
}

func trap(message string, cd *callbackDatastore, t *testing.T) {
	cd.SetFunc(func() {
		t.Fatal(message)
	})
}
func untrap(cd *callbackDatastore) {
	cd.SetFunc(func() {})
}

func TestRemoveCacheEntryOnDelete(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)

	arc.Put(ctx, exampleBlock)

	cd.Lock()
	writeHitTheDatastore := false
	cd.Unlock()

	cd.SetFunc(func() {
		writeHitTheDatastore = true
	})

	arc.DeleteBlock(ctx, exampleBlock.Cid())
	arc.Put(ctx, exampleBlock)
	if !writeHitTheDatastore {
		t.Fail()
	}
}

func TestElideDuplicateWrite(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)

	arc.Put(ctx, exampleBlock)
	trap("write hit datastore", cd, t)
	arc.Put(ctx, exampleBlock)
}

func TestHasRequestTriggersCache(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)

	arc.Has(ctx, exampleBlock.Cid())
	trap("has hit datastore", cd, t)
	if has, err := arc.Has(ctx, exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}

	untrap(cd)
	err := arc.Put(ctx, exampleBlock)
	if err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(ctx, exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
}

func TestGetFillsCache(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)

	if bl, err := arc.Get(ctx, exampleBlock.Cid()); bl != nil || err == nil {
		t.Fatal("block was found or there was no error")
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(ctx, exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}
	if _, err := arc.GetSize(ctx, exampleBlock.Cid()); err != ErrNotFound {
		t.Fatal("getsize was true but there is no such block")
	}

	untrap(cd)

	if err := arc.Put(ctx, exampleBlock); err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(ctx, exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
	if blockSize, err := arc.GetSize(ctx, exampleBlock.Cid()); blockSize == -1 || err != nil {
		t.Fatal("getsize returned invalid result", blockSize, err)
	}
}

func TestGetAndDeleteFalseShortCircuit(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)

	arc.Has(ctx, exampleBlock.Cid())
	arc.GetSize(ctx, exampleBlock.Cid())

	trap("get hit datastore", cd, t)

	if bl, err := arc.Get(ctx, exampleBlock.Cid()); bl != nil || err != ErrNotFound {
		t.Fatal("get returned invalid result")
	}

	if arc.DeleteBlock(ctx, exampleBlock.Cid()) != nil {
		t.Fatal("expected deletes to be idempotent")
	}
}

func TestArcCreationFailure(t *testing.T) {
	if arc, err := newARCCachedBS(context.TODO(), nil, -1); arc != nil || err == nil {
		t.Fatal("expected error and no cache")
	}
}

func TestInvalidKey(t *testing.T) {
	ctx := context.Background()

	arc, _, _ := createStores(t)

	bl, err := arc.Get(ctx, cid.Cid{})

	if bl != nil {
		t.Fatal("blocks should be nil")
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHasAfterSucessfulGetIsCached(t *testing.T) {
	ctx := context.Background()

	arc, bs, cd := createStores(t)

	bs.Put(ctx, exampleBlock)

	arc.Get(ctx, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	arc.Has(ctx, exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulGetIsCached(t *testing.T) {
	ctx := context.Background()

	arc, bs, cd := createStores(t)

	bs.Put(ctx, exampleBlock)

	arc.Get(ctx, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	arc.GetSize(ctx, exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulHas(t *testing.T) {
	ctx := context.Background()

	arc, bs, _ := createStores(t)

	bs.Put(ctx, exampleBlock)
	has, err := arc.Has(ctx, exampleBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected to have block")
	}

	if size, err := arc.GetSize(ctx, exampleBlock.Cid()); err != nil {
		t.Fatal(err)
	} else if size != len(exampleBlock.RawData()) {
		t.Fatalf("expected size %d, got %d", len(exampleBlock.RawData()), size)
	}
}

func TestGetSizeMissingZeroSizeBlock(t *testing.T) {
	ctx := context.Background()

	arc, bs, cd := createStores(t)
	emptyBlock := blocks.NewBlock([]byte{})
	missingBlock := blocks.NewBlock([]byte("missingBlock"))

	bs.Put(ctx, emptyBlock)

	arc.Get(ctx, emptyBlock.Cid())

	trap("has hit datastore", cd, t)
	if blockSize, err := arc.GetSize(ctx, emptyBlock.Cid()); blockSize != 0 || err != nil {
		t.Fatal("getsize returned invalid result")
	}
	untrap(cd)

	arc.Get(ctx, missingBlock.Cid())

	trap("has hit datastore", cd, t)
	if _, err := arc.GetSize(ctx, missingBlock.Cid()); err != ErrNotFound {
		t.Fatal("getsize returned invalid result")
	}
}

func TestDifferentKeyObjectsWork(t *testing.T) {
	ctx := context.Background()

	arc, bs, cd := createStores(t)

	bs.Put(ctx, exampleBlock)

	arc.Get(ctx, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	cidstr := exampleBlock.Cid().String()

	ncid, err := cid.Decode(cidstr)
	if err != nil {
		t.Fatal(err)
	}

	arc.Has(ctx, ncid)
}

func TestPutManyCaches(t *testing.T) {
	ctx := context.Background()

	arc, _, cd := createStores(t)
	arc.PutMany(ctx, []blocks.Block{exampleBlock})

	trap("has hit datastore", cd, t)
	arc.Has(ctx, exampleBlock.Cid())
	arc.GetSize(ctx, exampleBlock.Cid())
	untrap(cd)
	arc.DeleteBlock(ctx, exampleBlock.Cid())

	arc.Put(ctx, exampleBlock)
	trap("PunMany has hit datastore", cd, t)
	arc.PutMany(ctx, []blocks.Block{exampleBlock})
}
