package loadbalancer

import (
	"net/http"
	"sync"
)

type LoadBalancer struct {
	servers []string
	mu      sync.Mutex
	current int
}

func NewLoadBalancer(servers []string) *LoadBalancer {
	return &LoadBalancer{
		servers: servers,
		current: 0,
	}
}

func (lb *LoadBalancer) GetNextServer() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	server := lb.servers[lb.current]
	lb.current = (lb.current + 1) % len(lb.servers)
	return server
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	server := lb.GetNextServer()
	http.Redirect(w, r, server+r.RequestURI, http.StatusTemporaryRedirect)
}
