// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestBinaryReader_LabelValuesOwnBackingMemory(t *testing.T) {
	dir, id, bkt := labelValuesLifetimeBlock(t)
	fn := filepath.Join(dir, id.String(), block.IndexHeaderFilename)
	_, err := WriteBinary(context.Background(), bkt, id, fn, dummyHistogram)
	testutil.Ok(t, err)
	buf, err := os.ReadFile(fn)
	testutil.Ok(t, err)
	r, err := newMemoryBinaryReader(buf, 3, NewBinaryReaderMetrics(nil))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, r.Close()) })

	values, err := r.LabelValues("a")
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"alpha-value", "beta-value"}, values)

	// Invalidate the backing bytes without unmapping memory, so this regression
	// fails deterministically instead of crashing the entire test process.
	clear(buf)
	testutil.Equals(t, []string{"alpha-value", "beta-value"}, values)
}

func TestLazyBinaryReader_LabelValuesSurviveUnload(t *testing.T) {
	for _, lazyDownload := range []bool{false, true} {
		for _, idle := range []bool{false, true} {
			t.Run(fmt.Sprintf("lazyDownload=%v/idle=%v", lazyDownload, idle), func(t *testing.T) {
				dir, id, bkt := labelValuesLifetimeBlock(t)
				r, err := NewLazyBinaryReader(context.Background(), log.NewNopLogger(), bkt, dir, id, 3, NewLazyBinaryReaderMetrics(nil), NewBinaryReaderMetrics(nil), nil, lazyDownload)
				testutil.Ok(t, err)
				t.Cleanup(func() { testutil.Ok(t, r.Close()) })

				values, err := r.LabelValues("a")
				testutil.Ok(t, err)
				if idle {
					testutil.Ok(t, r.unloadIfIdleSince(time.Now().UnixNano()))
				} else {
					testutil.Ok(t, r.Close())
				}
				testutil.Assert(t, r.reader == nil)

				// BucketStore merges each block's values after its reader call has
				// returned. The strings must remain valid even after the mmap closes.
				testutil.Equals(t, []string{"aardvark-value", "alpha-value", "beta-value", "zulu-value"}, strutil.MergeSlices(0, values, []string{"aardvark-value", "zulu-value"}))
				again, err := r.LabelValues("a")
				testutil.Ok(t, err)
				testutil.Equals(t, []string{"alpha-value", "beta-value"}, again)
				testutil.Equals(t, []string{"alpha-value", "beta-value"}, values)
			})
		}
	}
}

func labelValuesLifetimeBlock(t *testing.T) (string, ulid.ULID, objstore.Bucket) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(dir, "bkt"))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })
	id, err := e2eutil.CreateBlock(ctx, dir, []labels.Labels{
		labels.FromStrings("a", "alpha-value"),
		labels.FromStrings("a", "beta-value"),
	}, 100, 0, 1000, labels.FromStrings("cluster", "test"), 124, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
	return dir, id, bkt
}
