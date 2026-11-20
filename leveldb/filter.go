// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/filter"
)

type iFilter struct {
	filter.Filter
}

func (f iFilter) Contains(filter, key []byte) bool {
	return f.Filter.Contains(filter, internalKey(key).ukey())
}

func (f iFilter) NewGenerator() filter.FilterGenerator {
	return iFilterGenerator{f.Filter.NewGenerator()}
}

type iFilterGenerator struct {
	filter.FilterGenerator
}

func (g iFilterGenerator) Add(key []byte) {
	g.FilterGenerator.Add(internalKey(key).ukey())
}

// effectiveFilter wraps the current effective filter so that it can be
// stored in an atomic.Value and swapped at runtime. The wrapped filter
// may be nil, which means filtering is disabled.
type effectiveFilter struct {
	filter.Filter
}

// setFilter sets the current effective filter. The given filter should
// already be wrapped with iFilter, or nil to disable filtering.
func (s *session) setFilter(f filter.Filter) {
	s.filterC.Store(effectiveFilter{f})
}

// getFilter returns the current effective filter, or nil if filtering
// is disabled.
func (s *session) getFilter() filter.Filter {
	return s.filterC.Load().(effectiveFilter).Filter
}
