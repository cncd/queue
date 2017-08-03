package redis

import (
	"context"
	"github.com/alicebob/miniredis"
	"github.com/cncd/queue"
	"sync"
	"testing"
	"time"
)

var noContext = context.Background()

func TestRedisQueue(t *testing.T) {
	want := &queue.Task{ID: "1"}

	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	q, err := New(
		WithAddr(s.Addr()),
		WithPassword(""),
		WithDB(0),
		WithPenddingQueueName("pending-queue"),
	)

	q.Push(noContext, want)
	info := q.Info(noContext)
	if info.Stats.Pending != 1 {
		t.Errorf("expect task in pending queue")
		return
	}

	if !s.Exists("pending-queue") {
		t.Fatal("'pending-queue' should not have existed anymore")
	}

	got, _ := q.Poll(noContext, func(*queue.Task) bool { return true })
	if got.ID != want.ID {
		t.Errorf("expect task returned form queue")
		return
	}
	info = q.Info(noContext)
	if info.Stats.Pending != 0 {
		t.Errorf("expect task removed from pending queue")
		return
	}
	if info.Stats.Running != 1 {
		t.Errorf("expect task in running queue")
		return
	}

	q.Done(noContext, got.ID)
	info = q.Info(noContext)
	if info.Stats.Pending != 0 {
		t.Errorf("expect task removed from pending queue")
		return
	}
	if info.Stats.Running != 0 {
		t.Errorf("expect task removed from running queue")
		return
	}

}

func TestRedisQueueExpire(t *testing.T) {
	want := &queue.Task{ID: "1"}

	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	qQ, err := New(
		WithAddr(s.Addr()),
		WithPassword(""),
		WithDB(0),
		WithPenddingQueueName("pending-queue"),
	)
	q := qQ.(*conn)
	q.extension = 0
	q.Push(noContext, want)
	info := q.Info(noContext)
	if info.Stats.Pending != 1 {
		t.Errorf("expect task in pending queue")
		return
	}

	got, _ := q.Poll(noContext, func(*queue.Task) bool { return true })
	if got.ID != want.ID {
		t.Errorf("expect task returned form queue")
		return
	}

	q.tracking()
	if info.Stats.Pending != 1 {
		t.Errorf("expect task re-added to pending queue")
		return
	}
}

func TestRedisQueueWait(t *testing.T) {
	want := &queue.Task{ID: "1"}
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	qQ, err := New(
		WithAddr(s.Addr()),
		WithPassword(""),
		WithDB(0),
		WithPenddingQueueName("pending-queue"),
	)
	q := qQ.(*conn)
	q.Push(noContext, want)

	got, _ := q.Poll(noContext, func(*queue.Task) bool { return true })
	if got.ID != want.ID {
		t.Errorf("expect task returned form queue")
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		q.Wait(noContext, got.ID)
		wg.Done()
	}()

	<-time.After(time.Millisecond)
	q.Done(noContext, got.ID)
	wg.Wait()
}
