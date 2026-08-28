// Package watch 实现本机 Watch 长轮询的挂起与唤醒（DESIGN §4.3、ADR-0004）：
// API 层注册 waiter 挂起，FSM Apply 到 Publish/删除时唤醒本机挂起的 waiter。
// 仅进程内生效——节点落后或分区只表现为超时，由客户端重试兜底。
package watch

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metrics = struct {
	waiters       prometheus.Gauge
	notifications prometheus.Counter
}{
	waiters: promauto.NewGauge(prometheus.GaugeOpts{
		Name: "watch_waiters",
		Help: "当前挂起的 Watch 数（DESIGN §7）",
	}),
	notifications: promauto.NewCounter(prometheus.CounterOpts{
		Name: "watch_notifications_total",
		Help: "Watch 唤醒投递次数（DESIGN §7）",
	}),
}

// Event 是一次 Apply 产生的变更通知。Deleted 为 true 时 Version 是 Tombstone 版本。
type Event struct {
	Version int64
	Deleted bool
}

type waiter struct {
	ch chan Event
}

// Hub 保存本机全部挂起的 Watch。事件缓冲为 1、非阻塞投递：同一 waiter 短暂错过
// 的通知无需补发，waiter 被唤醒后按事件重读本机 PG，最新状态不会丢失。
type Hub struct {
	mu      sync.Mutex
	waiters map[string]map[*waiter]struct{}
}

func NewHub() *Hub {
	return &Hub{waiters: make(map[string]map[*waiter]struct{})}
}

// Register 注册一个 waiter，返回释放函数与事件通道。调用方须在退出时释放。
func (h *Hub) Register(namespace, dataId string) (release func(), events <-chan Event) {
	w := &waiter{ch: make(chan Event, 1)}
	k := namespace + "\x00" + dataId

	h.mu.Lock()
	set, ok := h.waiters[k]
	if !ok {
		set = make(map[*waiter]struct{})
		h.waiters[k] = set
	}
	set[w] = struct{}{}
	count := len(h.waiters)
	h.mu.Unlock()
	metrics.waiters.Set(float64(count))

	released := false
	release = func() {
		h.mu.Lock()
		if !released {
			released = true
			if set, ok := h.waiters[k]; ok {
				delete(set, w)
				if len(set) == 0 {
					delete(h.waiters, k)
				}
			}
			count = len(h.waiters)
		}
		h.mu.Unlock()
		metrics.waiters.Set(float64(count))
	}
	return release, w.ch
}

// Notify 向该 Namespace + DataId 的全部挂起 waiter 投递事件（非阻塞）。
func (h *Hub) Notify(namespace, dataId string, ev Event) {
	h.mu.Lock()
	set := h.waiters[namespace+"\x00"+dataId]
	chans := make([]chan Event, 0, len(set))
	for w := range set {
		chans = append(chans, w.ch)
	}
	h.mu.Unlock()

	for _, ch := range chans {
		select {
		case ch <- ev:
			metrics.notifications.Inc()
		default:
			// waiter 尚有未消费事件；唤醒后按事件版本重读 PG，状态不丢。
		}
	}
}
