package history

import "sync"

type WaitGroup struct {
	sync.RWMutex
	newAddCount int            // 需要新 add 的数量
	waitGroup   sync.WaitGroup // waitGroup
}

func NewWaitGroup(n int) *WaitGroup {
	w := &WaitGroup{newAddCount: 0, waitGroup: sync.WaitGroup{}}
	w.waitGroup.Add(n)
	return w
}

func (waitGroup *WaitGroup) Add(n int) {
	waitGroup.Lock()
	waitGroup.newAddCount += n
	waitGroup.Unlock()
}

func (waitGroup *WaitGroup) Wait() {
	waitGroup.Lock()
	if waitGroup.newAddCount > 0 {
		waitGroup.waitGroup.Add(waitGroup.newAddCount)
		waitGroup.newAddCount = 0
	}
	waitGroup.Unlock()
	waitGroup.waitGroup.Wait()
}

func (waitGroup *WaitGroup) Done() {
	waitGroup.Lock()
	defer waitGroup.Unlock()
	if waitGroup.newAddCount > 0 {
		waitGroup.newAddCount -= 1
		return
	}
	waitGroup.waitGroup.Done()
}
