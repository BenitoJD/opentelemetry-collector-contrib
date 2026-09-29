// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/xextension/storage"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/testutil"
)

func newDefaultConfig(tempDir string) *Config {
	cfg := NewConfigWithID("testfile")
	cfg.PollInterval = 200 * time.Millisecond
	cfg.StartAt = "beginning"
	cfg.Include = []string{fmt.Sprintf("%s/*", tempDir)}
	cfg.OutputIDs = []string{"fake"}
	return cfg
}

func newTestFileOperator(t *testing.T, cfgMod func(*Config)) (*Input, chan *entry.Entry, string) {
	fakeOutput := testutil.NewFakeOutput(t)

	tempDir := t.TempDir()

	cfg := newDefaultConfig(tempDir)
	if cfgMod != nil {
		cfgMod(cfg)
	}
	set := componenttest.NewNopTelemetrySettings()
	op, err := cfg.Build(set)
	require.NoError(t, err)

	err = op.SetOutputs([]operator.Operator{fakeOutput})
	require.NoError(t, err)

	return op.(*Input), fakeOutput.Received, tempDir
}

func openTemp(tb testing.TB, tempDir string) *os.File {
	return openTempWithPattern(tb, tempDir, "")
}

func openTempWithPattern(tb testing.TB, tempDir, pattern string) *os.File {
	file, err := os.CreateTemp(tempDir, pattern)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = file.Close() })
	return file
}

func writeString(tb testing.TB, file *os.File, s string) {
	_, err := file.WriteString(s)
	require.NoError(tb, err)
}

func waitForOne(t *testing.T, c chan *entry.Entry) *entry.Entry {
	select {
	case e := <-c:
		return e
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Timed out waiting for message")
		return nil
	}
}

func waitForMessage(t *testing.T, c chan *entry.Entry, expected string) {
	select {
	case e := <-c:
		require.Equal(t, expected, e.Body.(string))
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Timed out waiting for message", expected)
	}
}

func waitForByteMessage(t *testing.T, c chan *entry.Entry, expected []byte) {
	select {
	case e := <-c:
		require.Equal(t, expected, e.Body.([]byte))
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Timed out waiting for message", "%+v", expected)
	}
}

func expectNoMessages(t *testing.T, c chan *entry.Entry) {
	expectNoMessagesUntil(t, c, 200*time.Millisecond)
}

func expectNoMessagesUntil(t *testing.T, c chan *entry.Entry, d time.Duration) {
	select {
	case e := <-c:
		require.FailNow(t, "Received unexpected message", "Message: %s", e.Body.(string))
	case <-time.After(d):
	}
}

// firstSavePersister wraps a mock persister and signals the first time offsets
// are saved, which happens at the end of the first poll cycle, after any
// discovered file has been positioned. Tests should wait for this signal
// instead of sleeping for a fixed number of poll intervals.
type firstSavePersister struct {
	operator.Persister
	saved chan struct{}
	once  sync.Once
}

func newFirstSavePersister() *firstSavePersister {
	return &firstSavePersister{
		Persister: testutil.NewUnscopedMockPersister(),
		saved:     make(chan struct{}),
	}
}

func (p *firstSavePersister) Set(ctx context.Context, key string, value []byte) error {
	err := p.Persister.Set(ctx, key, value)
	p.once.Do(func() { close(p.saved) })
	return err
}

func (p *firstSavePersister) Batch(ctx context.Context, ops ...*storage.Operation) error {
	err := p.Persister.Batch(ctx, ops...)
	p.once.Do(func() { close(p.saved) })
	return err
}
