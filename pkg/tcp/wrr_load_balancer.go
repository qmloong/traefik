package tcp

import (
	"fmt"
	"sync"
	"time"

	"github.com/traefik/traefik/v2/pkg/log"
	"k8s.io/apimachinery/pkg/util/rand"
)

type server struct {
	name string
	Handler
	weight int
}

// WRRLoadBalancer is a naive RoundRobin load balancer for TCP services.
type WRRLoadBalancer struct {
	servers       []server
	lock          sync.Mutex
	currentWeight int
	index         int
}

// NewWRRLoadBalancer creates a new WRRLoadBalancer.
func NewWRRLoadBalancer() *WRRLoadBalancer {
	rand.Seed(time.Now().UnixNano())
	return &WRRLoadBalancer{
		index: rand.Int(),
	}
}

// ServeTCP forwards the connection to the right service.
func (b *WRRLoadBalancer) ServeTCP(conn WriteCloser) {
	b.lock.Lock()
	next, err := b.next()
	b.lock.Unlock()

	if err != nil {
		log.WithoutContext().Errorf("Error during load balancing: %v", err)
		conn.Close()
		return
	}

	next.ServeTCP(conn)
}

// AddServer appends a server to the existing list.
func (b *WRRLoadBalancer) AddServer(name string, serverHandler Handler) {
	w := 1
	b.AddWeightServer(name, serverHandler, &w)
}

// RemoveServer remove a server to the existing list.
func (b *WRRLoadBalancer) RemoveServer(name string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for i, s := range b.servers {
		if s.name == name {
			b.servers = append(b.servers[0:i], b.servers[i+1:]...)
			fmt.Printf("alexmlqi: %v\n", name)
			fmt.Printf("alexmlqi server: %v\n", b.servers)
			break
		}
	}
}

// AddWeightServer appends a server to the existing list with a weight.
func (b *WRRLoadBalancer) AddWeightServer(name string, serverHandler Handler, weight *int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	w := 1
	if weight != nil {
		w = *weight
	}
	b.servers = append(b.servers, server{name: name, Handler: serverHandler, weight: w})
}

func (b *WRRLoadBalancer) maxWeight() int {
	max := -1
	for _, s := range b.servers {
		if s.weight > max {
			max = s.weight
		}
	}
	return max
}

func (b *WRRLoadBalancer) weightGcd() int {
	divisor := -1
	for _, s := range b.servers {
		if divisor == -1 {
			divisor = s.weight
		} else {
			divisor = gcd(divisor, s.weight)
		}
	}
	return divisor
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (b *WRRLoadBalancer) next() (Handler, error) {
	if len(b.servers) == 0 {
		return nil, fmt.Errorf("no balancers in the pool")
	}

	// The algo below may look messy, but is actually very simple
	// it calculates the GCD  and subtracts it on every iteration, what interleaves balancers
	// and allows us not to build an iterator every time we readjust weights

	// Maximum weight across all enabled balancers
	max := b.maxWeight()
	if max == 0 {
		return nil, fmt.Errorf("all balancers have 0 weight")
	}

	// GCD across all enabled balancers
	gcd := b.weightGcd()

	for {
		b.index = (b.index + 1) % len(b.servers)
		if b.index == 0 {
			b.currentWeight -= gcd
			if b.currentWeight <= 0 {
				b.currentWeight = max
			}
		}
		srv := b.servers[b.index]
		if srv.weight >= b.currentWeight {
			return srv, nil
		}
	}
}
