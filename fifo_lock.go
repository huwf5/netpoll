package netpoll

import (
	"runtime"
	"sync/atomic"
)

const (
	fifoClosing key = iota
	fifoProcessing
	fifoFlushing
	fifoTotal
)

type fifoLocker struct {
	keychain [fifoTotal]int32
}

func (l *fifoLocker) closeBy(w who) (success bool) {
	return atomic.CompareAndSwapInt32(&l.keychain[fifoClosing], 0, int32(w))
}

func (l *fifoLocker) isCloseBy(w who) (yes bool) {
	return atomic.LoadInt32(&l.keychain[fifoClosing]) == int32(w)
}

func (l *fifoLocker) status(k key) int32 {
	return atomic.LoadInt32(&l.keychain[k])
}

func (l *fifoLocker) force(k key, v int32) {
	atomic.StoreInt32(&l.keychain[k], v)
}

func (l *fifoLocker) lock(k key) (success bool) {
	return atomic.CompareAndSwapInt32(&l.keychain[k], 0, 1)
}

func (l *fifoLocker) unlock(k key) {
	atomic.StoreInt32(&l.keychain[k], 0)
}

func (l *fifoLocker) stop(k key) {
	for !atomic.CompareAndSwapInt32(&l.keychain[k], 0, 2) && atomic.LoadInt32(&l.keychain[k]) != 2 {
		runtime.Gosched()
	}
}

func (l *fifoLocker) isUnlock(k key) bool {
	return atomic.LoadInt32(&l.keychain[k]) == 0
}
