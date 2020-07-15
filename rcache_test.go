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

var (
	_ registry.Registry = (*regTest)(nil)
	_ registry.Watcher = (*regWatcher)(nil)
)

type regWatcher struct {
	stop bool

	result *registry.Result
}

func NewRegTest() *regTest {
	return &regTest{
		w: &regWatcher{
			stop:   false,
			result: &registry.Result{
				Action: "update",
				Service: getService(),
			},
		},
	}
}

func (w *regWatcher) Next() (*registry.Result, error) {
	count := 0
	if !w.stop {
		time.Sleep(time.Second*10)
		svc := getService()

		r := &registry.Result{}
		if count % 2 == 1 {
			r.Action = "delete"
			r.Service = w.result.Service
		} else {
			r.Action = "create"
			r.Service = svc
			w.result = r
		}

		count++

		return r, nil
	}

	return nil, fmt.Errorf("stoped")
}

func (w *regWatcher) Stop()  {
	w.stop = true
}

type regTest struct {
	options registry.Options

	w *regWatcher
}

func (r *regTest)Init(opts...registry.Option) error {
	for _, o := range opts {
		o(&r.options)
	}
	return nil
}
func (r *regTest)Options() registry.Options                                             {
	return r.options
}
func (r *regTest)Register(*registry.Service, ...registry.RegisterOption) error {
	return nil
}
func (r *regTest)Deregister(*registry.Service) error                           {
	return nil
}
func (r *regTest)GetService(string) ([]*registry.Service, error)               {
	return []*registry.Service{r.w.result.Service}, nil
}
func (r *regTest)ListServices() ([]*registry.Service, error)                   {
	return []*registry.Service{r.w.result.Service}, nil
}
func (r *regTest)Watch(...registry.WatchOption) (registry.Watcher, error)      {
	return &regWatcher{
		stop: false,
	}, nil
}
func (r *regTest)String() string {
	return "TestRegistry"
}

func getService() *registry.Service {
	return &registry.Service{
		Name:      "test-service",
		Version:   "",
		Metadata:  nil,
		Endpoints: nil,
		Nodes:     nil,
	}
}

func TestCacheUpdate(t *testing.T) {
	r := NewRegTest()
	c := New(r, WithTTL(time.Second *20))
	s := getService()

	go func() {
		t.Run("normal get service", func(t *testing.T) {
			for {
				time.Sleep(time.Second)
				services, err := c.GetService(s.Name)
				if err != nil {
					t.Error(err)
				}

				for _, svc := range services {
					fmt.Println(svc.Name)
				}
			}
		})
	}()

	go func() {
		result := &registry.Result{
			Action:  "update",
			Service: s,
		}

		for {
			time.Sleep(time.Second*3)
			c.(*cache).update(result)
		}
	}()

	time.Sleep(time.Minute*10)
}