package rcache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/micro/go-micro/registry"
	"github.com/micro/go-plugins/registry/etcdv3"
)

func Test_cache_updateByTimer(t *testing.T) {
	type args struct {
		service string
	}
	tests := []struct {
		name   string
		args   args
	}{
		{
			name: "no cache",
			args:args{
				service: "wpt.api.api",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(etcdv3.NewRegistry(registry.Addrs("localhost:2379")), WithTTL(time.Minute)).(*cache)
			go func() {
				time.AfterFunc(time.Second, c.Stop)
			}()

			// c.refreshByTimer(tt.args.service)
			fmt.Printf("%+v", c.cache)
			fmt.Printf("%+v", c.ttls)
		})
	}
}

func TestCache_getFromRegistry(t *testing.T) {
	var (
		m sync.Map
		wg sync.WaitGroup
	)
	type s struct {
		p *int
	}

	for i :=0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			 ss := new(sync.Mutex)
			v, _ := m.LoadOrStore(i, ss)
			ss.Lock()
			val := v.(*sync.Mutex)
			_ =val
			// val.Lock()

			println("1111")
		}(i)
	}

	wg.Wait()
}

func BenchmarkCache_getFromRegistry(b *testing.B) {

}
