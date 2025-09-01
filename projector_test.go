package projector

import (
	"context"
	"errors"
	"testing"
	"time"

	es "github.com/shogotsuneto/go-simple-eventstore"
)

// fakeConsumer implements es.Consumer for testing
type fakeConsumer struct {
	batches     [][]es.Envelope // pre-scripted batches to return
	cursors     []es.Cursor     // corresponding cursors for each batch
	batchIndex  int             // current batch index
	fetchErr    error           // error to return on Fetch
	commitErr   error           // error to return on Commit
	fetchCalls  []fetchCall     // record of all Fetch calls
	commitCalls []es.Cursor     // record of all Commit calls
}

type fetchCall struct {
	cursor es.Cursor
	limit  int
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{
		batches:     [][]es.Envelope{},
		cursors:     []es.Cursor{},
		fetchCalls:  []fetchCall{},
		commitCalls: []es.Cursor{},
	}
}

func (f *fakeConsumer) AddBatch(batch []es.Envelope, cursor es.Cursor) {
	f.batches = append(f.batches, batch)
	f.cursors = append(f.cursors, cursor)
}

func (f *fakeConsumer) SetFetchError(err error) {
	f.fetchErr = err
}

func (f *fakeConsumer) SetCommitError(err error) {
	f.commitErr = err
}

func (f *fakeConsumer) Fetch(ctx context.Context, cursor es.Cursor, limit int) ([]es.Envelope, es.Cursor, error) {
	f.fetchCalls = append(f.fetchCalls, fetchCall{cursor: cursor, limit: limit})

	if f.fetchErr != nil {
		return nil, nil, f.fetchErr
	}

	if f.batchIndex >= len(f.batches) {
		// Return empty batch (simulates no new events)
		return []es.Envelope{}, cursor, nil
	}

	batch := f.batches[f.batchIndex]
	nextCursor := f.cursors[f.batchIndex]
	f.batchIndex++

	return batch, nextCursor, nil
}

func (f *fakeConsumer) Commit(ctx context.Context, cursor es.Cursor) error {
	f.commitCalls = append(f.commitCalls, cursor)
	return f.commitErr
}

// Helper to create test events
func createTestEvent(eventID string, data string) es.Envelope {
	return es.Envelope{
		EventID: eventID,
		Type:    "test.event",
		Data:    []byte(data),
	}
}

func TestWorkerDefaults(t *testing.T) {
	consumer := newFakeConsumer()
	applied := []appliedBatch{}

	worker := &Worker{
		Source: consumer,
		Start:  es.Cursor("start"),
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			applied = append(applied, appliedBatch{batch: batch, cursor: next})
			return nil
		},
		MaxBatches: 1, // Process only one batch for this test
	}

	// Add one batch of events
	events := []es.Envelope{
		createTestEvent("1", "event1"),
		createTestEvent("2", "event2"),
	}
	consumer.AddBatch(events, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify defaults were applied
	if len(consumer.fetchCalls) != 1 {
		t.Fatalf("expected 1 fetch call, got %d", len(consumer.fetchCalls))
	}

	fetchCall := consumer.fetchCalls[0]
	if string(fetchCall.cursor) != "start" {
		t.Errorf("expected first fetch with cursor 'start', got %q", fetchCall.cursor)
	}
	if fetchCall.limit != 512 { // default BatchSize
		t.Errorf("expected default batch size 512, got %d", fetchCall.limit)
	}

	// Verify Apply was called with correct batch and cursor
	if len(applied) != 1 {
		t.Fatalf("expected 1 apply call, got %d", len(applied))
	}

	appliedBatch := applied[0]
	if len(appliedBatch.batch) != 2 {
		t.Errorf("expected 2 events in applied batch, got %d", len(appliedBatch.batch))
	}
	if string(appliedBatch.cursor) != "cursor1" {
		t.Errorf("expected cursor 'cursor1', got %q", appliedBatch.cursor)
	}

	// Verify Commit was called with correct cursor
	if len(consumer.commitCalls) != 1 {
		t.Fatalf("expected 1 commit call, got %d", len(consumer.commitCalls))
	}
	if string(consumer.commitCalls[0]) != "cursor1" {
		t.Errorf("expected commit with cursor 'cursor1', got %q", consumer.commitCalls[0])
	}
}

func TestWorkerCustomBatchSize(t *testing.T) {
	consumer := newFakeConsumer()

	worker := &Worker{
		Source:     consumer,
		Start:      es.Cursor("start"),
		BatchSize:  100,
		MaxBatches: 1,
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			return nil
		},
	}

	// Add a batch with events to avoid infinite loop
	events := []es.Envelope{createTestEvent("1", "event1")}
	consumer.AddBatch(events, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(consumer.fetchCalls) < 1 {
		t.Fatalf("expected at least 1 fetch call, got %d", len(consumer.fetchCalls))
	}

	if consumer.fetchCalls[0].limit != 100 {
		t.Errorf("expected custom batch size 100, got %d", consumer.fetchCalls[0].limit)
	}
}

func TestWorkerFetchError(t *testing.T) {
	consumer := newFakeConsumer()
	expectedErr := errors.New("fetch failed")
	consumer.SetFetchError(expectedErr)

	worker := &Worker{
		Source: consumer,
		Start:  es.Cursor("start"),
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			t.Error("Apply should not be called when fetch fails")
			return nil
		},
	}

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != expectedErr {
		t.Errorf("expected fetch error %v, got %v", expectedErr, err)
	}
}

func TestWorkerApplyError(t *testing.T) {
	consumer := newFakeConsumer()
	expectedErr := errors.New("apply failed")

	worker := &Worker{
		Source: consumer,
		Start:  es.Cursor("start"),
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			return expectedErr
		},
	}

	// Add one batch of events
	events := []es.Envelope{createTestEvent("1", "event1")}
	consumer.AddBatch(events, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != expectedErr {
		t.Errorf("expected apply error %v, got %v", expectedErr, err)
	}

	// Verify Commit was not called due to apply failure
	if len(consumer.commitCalls) != 0 {
		t.Errorf("expected no commit calls when apply fails, got %d", len(consumer.commitCalls))
	}
}

func TestWorkerCommitError(t *testing.T) {
	consumer := newFakeConsumer()
	expectedErr := errors.New("commit failed")
	consumer.SetCommitError(expectedErr)

	applied := []appliedBatch{}
	worker := &Worker{
		Source: consumer,
		Start:  es.Cursor("start"),
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			applied = append(applied, appliedBatch{batch: batch, cursor: next})
			return nil
		},
	}

	// Add one batch of events
	events := []es.Envelope{createTestEvent("1", "event1")}
	consumer.AddBatch(events, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != expectedErr {
		t.Errorf("expected commit error %v, got %v", expectedErr, err)
	}

	// Verify Apply was called before commit failed
	if len(applied) != 1 {
		t.Errorf("expected apply to be called once before commit failed, got %d", len(applied))
	}
}

func TestWorkerContextCancellation(t *testing.T) {
	consumer := newFakeConsumer()

	worker := &Worker{
		Source:    consumer,
		Start:     es.Cursor("start"),
		IdleSleep: 50 * time.Millisecond, // Short sleep for faster test
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			return nil
		},
	}

	// No batches added, so worker will idle

	ctx, cancel := context.WithCancel(context.Background())
	
	// Cancel context after a short delay
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()

	err := worker.Run(ctx)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestWorkerMaxBatches(t *testing.T) {
	consumer := newFakeConsumer()
	applied := []appliedBatch{}

	worker := &Worker{
		Source:     consumer,
		Start:      es.Cursor("start"),
		MaxBatches: 2, // Process only 2 batches
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			applied = append(applied, appliedBatch{batch: batch, cursor: next})
			return nil
		},
	}

	// Add 3 batches but only 2 should be processed
	consumer.AddBatch([]es.Envelope{createTestEvent("1", "event1")}, es.Cursor("cursor1"))
	consumer.AddBatch([]es.Envelope{createTestEvent("2", "event2")}, es.Cursor("cursor2"))
	consumer.AddBatch([]es.Envelope{createTestEvent("3", "event3")}, es.Cursor("cursor3"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Should have processed exactly 2 batches
	if len(applied) != 2 {
		t.Errorf("expected 2 applied batches, got %d", len(applied))
	}

	// Verify correct cursors progression
	if string(applied[0].cursor) != "cursor1" {
		t.Errorf("expected first applied cursor 'cursor1', got %q", applied[0].cursor)
	}
	if string(applied[1].cursor) != "cursor2" {
		t.Errorf("expected second applied cursor 'cursor2', got %q", applied[1].cursor)
	}
}

func TestWorkerLogger(t *testing.T) {
	consumer := newFakeConsumer()
	logs := []logEntry{}

	worker := &Worker{
		Source:     consumer,
		Start:      es.Cursor("start"),
		MaxBatches: 1,
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			return nil
		},
		Logger: func(msg string, kv ...any) {
			logs = append(logs, logEntry{msg: msg, kv: kv})
		},
	}

	// Add one batch
	consumer.AddBatch([]es.Envelope{createTestEvent("1", "event1")}, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify some logs were generated
	if len(logs) == 0 {
		t.Error("expected some log entries, got none")
	}

	// Check that the starting log is present
	found := false
	for _, log := range logs {
		if log.msg == "worker starting" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'worker starting' log entry")
	}
}

func TestWorkerNilLogger(t *testing.T) {
	consumer := newFakeConsumer()

	worker := &Worker{
		Source:     consumer,
		Start:      es.Cursor("start"),
		MaxBatches: 1,
		Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
			return nil
		},
		Logger: nil, // Nil logger should not cause panic
	}

	// Add one batch
	consumer.AddBatch([]es.Envelope{createTestEvent("1", "event1")}, es.Cursor("cursor1"))

	ctx := context.Background()
	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("expected no error with nil logger, got %v", err)
	}
}

// Helper types for test data collection
type appliedBatch struct {
	batch  []es.Envelope
	cursor es.Cursor
}

type logEntry struct {
	msg string
	kv  []any
}