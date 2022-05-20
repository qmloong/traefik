package tcp

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/healthcheck"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/metrics"
	"github.com/traefik/traefik/v2/pkg/server/service"
	"github.com/vulcand/oxy/roundrobin"
)

const (
	defaultHealthCheckInterval = 30 * time.Second
	defaultHealthCheckTimeout  = 5 * time.Second
)

var (
	singleton *healthcheck.HealthCheck
	once      sync.Once
)

func getHealthCheck(registry metrics.Registry) *healthcheck.HealthCheck {
	once.Do(func() {
		singleton = healthcheck.NewHealthCheck(registry)
	})
	return singleton
}

type LoadBalancerWithHealthCheck struct {
	serviceName       string
	wrrLoadBalancer   *WRRLoadBalancer
	healthCheckConfig *dynamic.HTTPHealthCheck
	healthCheck       *healthcheck.HealthCheck

	handlerLock sync.Mutex
	healthy     map[string]Handler
	unhealthy   map[string]Handler
	all         map[string]Handler
}

func NewLoadBalancerWithHealthCheck(serviceName string, lb *WRRLoadBalancer, config *dynamic.HTTPHealthCheck, registry metrics.Registry) *LoadBalancerWithHealthCheck {
	var heathCheck *healthcheck.HealthCheck
	if config != nil {
		if config.ServersTransport == nil {
			config.ServersTransport = &dynamic.ServersTransport{
				InsecureSkipVerify: true,
			}
		}
		heathCheck = getHealthCheck(registry)
	}
	return &LoadBalancerWithHealthCheck{
		serviceName:       serviceName,
		wrrLoadBalancer:   lb,
		healthCheckConfig: config,
		healthCheck:       heathCheck,
		healthy:           map[string]Handler{},
		unhealthy:         map[string]Handler{},
		all:               map[string]Handler{},
	}
}

func (b *LoadBalancerWithHealthCheck) Servers() []*url.URL {
	b.handlerLock.Lock()
	defer b.handlerLock.Unlock()

	res := make([]*url.URL, 0, len(b.healthy))
	for name := range b.healthy {
		u, err := url.Parse(b.healthCheckConfig.Scheme + "://" + name)
		if err != nil {
			log.FromContext(context.Background()).Errorf("failed to parse server: %v", err.Error())
			continue
		}
		res = append(res, u)
	}
	return res
}

func (b *LoadBalancerWithHealthCheck) RemoveServer(u *url.URL) error {
	log.FromContext(context.Background()).Errorf("Health check failed, remove server: %v", u.Host)
	b.handlerLock.Lock()
	defer b.handlerLock.Unlock()

	badHandler, ok := b.healthy[u.Host]
	if !ok {
		log.FromContext(context.Background()).Warnf("Remove a not existed bad tcp handler: %v", u.Host)
		return nil
	}
	b.unhealthy[u.Host] = badHandler
	delete(b.healthy, u.Host)

	b.wrrLoadBalancer.RemoveServer(u.Host)

	return nil
}

func (b *LoadBalancerWithHealthCheck) UpsertServer(u *url.URL, options ...roundrobin.ServerOption) error {
	log.FromContext(context.Background()).Infof("UpsertServer: %v", u.Host)
	b.handlerLock.Lock()
	defer b.handlerLock.Unlock()

	badHandler, ok := b.unhealthy[u.Host]
	if !ok {
		log.FromContext(context.Background()).Warnf("Remove a not existed bad tcp handler: %v", u.Host)
		return nil
	}
	b.healthy[u.Host] = badHandler

	b.wrrLoadBalancer.AddServer(u.Host, b.healthy[u.Host])

	delete(b.unhealthy, u.Host)

	return nil
}

func (h *LoadBalancerWithHealthCheck) ServeTCP(conn WriteCloser) {
	h.wrrLoadBalancer.ServeTCP(conn)
}

func (h *LoadBalancerWithHealthCheck) AddServer(address string, serverHandler Handler) {
	if h.healthCheck == nil {
		h.wrrLoadBalancer.AddServer(address, serverHandler)
		return
	}
	log.FromContext(context.Background()).Infof("AddServer: %v", address)
	h.all[address] = serverHandler
	h.healthy[address] = serverHandler
}

func (h *LoadBalancerWithHealthCheck) LaunchHealthCheck() {
	if h.healthCheck == nil {
		return
	}
	ctx := context.Background()
	backendConfigs := make(map[string]*healthcheck.BackendConfig)
	hcOpts := buildHealthCheckOptions(ctx, h, h.serviceName, &h.healthCheckConfig.ServerHealthCheck)
	if hcOpts == nil {
		return
	}
	var err error
	hcOpts.Transport, err = service.CreateRoundTripper(h.healthCheckConfig.ServersTransport)
	if err != nil {
		log.FromContext(ctx).Errorf("Failed to create Transport for health check %s with %s, ServerTransport %v, err: %v",
			h.serviceName, *hcOpts, h.healthCheckConfig.ServersTransport, err)
		return
	}
	hcOpts.LB = h
	log.FromContext(ctx).Debugf("Setting up healthcheck for service %s with %s", h.serviceName, *hcOpts)
	backendConfigs[h.serviceName] = healthcheck.NewBackendConfig(*hcOpts, h.serviceName)

	for name, serverHandler := range h.healthy {
		h.wrrLoadBalancer.AddServer(name, serverHandler)
	}

	h.healthCheck.SetBackendsConfiguration(ctx, backendConfigs)
}

func buildHealthCheckOptions(ctx context.Context, lb healthcheck.Balancer, backend string, hc *dynamic.ServerHealthCheck) *healthcheck.Options {
	if hc == nil || hc.Path == "" {
		return nil
	}

	logger := log.FromContext(ctx)

	interval := defaultHealthCheckInterval
	if hc.Interval != "" {
		intervalOverride, err := time.ParseDuration(hc.Interval)
		switch {
		case err != nil:
			logger.Errorf("Illegal health check interval for '%s': %s", backend, err)
		case intervalOverride <= 0:
			logger.Errorf("Health check interval smaller than zero for service '%s'", backend)
		default:
			interval = intervalOverride
		}
	}

	timeout := defaultHealthCheckTimeout
	if hc.Timeout != "" {
		timeoutOverride, err := time.ParseDuration(hc.Timeout)
		switch {
		case err != nil:
			logger.Errorf("Illegal health check timeout for backend '%s': %s", backend, err)
		case timeoutOverride <= 0:
			logger.Errorf("Health check timeout smaller than zero for backend '%s', backend", backend)
		default:
			timeout = timeoutOverride
		}
	}

	if timeout >= interval {
		logger.Warnf("Health check timeout for backend '%s' should be lower than the health check interval. Interval set to timeout + 1 second (%s).", backend, interval)
	}

	followRedirects := true
	if hc.FollowRedirects != nil {
		followRedirects = *hc.FollowRedirects
	}

	return &healthcheck.Options{
		Scheme:          hc.Scheme,
		Path:            hc.Path,
		Port:            hc.Port,
		Interval:        interval,
		Timeout:         timeout,
		LB:              lb,
		Hostname:        hc.Hostname,
		Headers:         hc.Headers,
		FollowRedirects: followRedirects,
	}
}
