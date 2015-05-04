// Package resolver contains functions to handle resolving .mesos domains
package resolver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/mesos/mesos-go/detector"
	_ "github.com/mesos/mesos-go/detector/zoo" // Registers the ZK detector
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesosphere/mesos-dns/logging"
	"github.com/mesosphere/mesos-dns/records"
	"github.com/mesosphere/mesos-dns/util"
	"github.com/miekg/dns"
)

var (
	recurseCnt = 3
)

// Resolver holds configuration state and the resource records
type Resolver struct {
	version    string
	config     records.Config
	rs         *records.RecordGenerator
	rsLock     sync.RWMutex
	leader     string
	leaderLock sync.RWMutex
	rng        *rand.Rand

	// pluggable external DNS resolution, mainly for unit testing
	extResolver func(r *dns.Msg, nameserver string, proto string, cnt int) (*dns.Msg, error)
	// pluggable ZK detection, mainly for unit testing
	startZKdetection func(zkurl string, leaderChanged func(string)) error
}

// New returns a Resolver with the given version and configuration.
func New(version string, config records.Config) *Resolver {
	r := &Resolver{
		version: version,
		config:  config,
		rs:      &records.RecordGenerator{},
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	r.extResolver = r.defaultExtResolver
	r.startZKdetection = startDefaultZKdetector
	return r
}

// return the current (read-only) record set. attempts to write to the returned
// object will likely result in a data race.
func (res *Resolver) records() *records.RecordGenerator {
	res.rsLock.RLock()
	defer res.rsLock.RUnlock()
	return res.rs
}

// LaunchDNS starts a (TCP and UDP) DNS server for the Resolver,
// returning a error channel to which errors are asynchronously sent.
func (res *Resolver) LaunchDNS() <-chan error {
	// Handers for Mesos requests
	dns.HandleFunc(res.config.Domain+".", panicRecover(res.HandleMesos))
	// Handler for nonMesos requests
	dns.HandleFunc(".", panicRecover(res.HandleNonMesos))

	errCh := make(chan error, 2)
	_, e1 := res.Serve("tcp")
	go func() { errCh <- <-e1 }()
	_, e2 := res.Serve("udp")
	go func() { errCh <- <-e2 }()
	return errCh
}

// Serve starts a DNS server for net protocol (tcp/udp), returns immediately.
// the returned signal chan is closed upon the server successfully entering the listening phase.
// if the server aborts then an error is sent on the error chan.
func (res *Resolver) Serve(proto string) (<-chan struct{}, <-chan error) {
	defer util.HandleCrash()

	ch := make(chan struct{})
	server := &dns.Server{
		Addr:              net.JoinHostPort(res.config.Listener, strconv.Itoa(res.config.Port)),
		Net:               proto,
		TsigSecret:        nil,
		NotifyStartedFunc: func() { close(ch) },
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		err := server.ListenAndServe()
		if err != nil {
			errCh <- fmt.Errorf("Failed to setup %q server: %v", proto, err)
		} else {
			logging.Error.Printf("Not listening/serving any more requests.")
		}
	}()
	return ch, errCh
}

// LaunchZK launches a Zookeeper detector, returns immediately two chans: the first fires an empty
// struct whenever there's a new (non-nil) mesos leader, the second if there's an unrecoverable
// error in the master detector.
func (res *Resolver) LaunchZK(initialDetectionTimeout time.Duration) (<-chan struct{}, <-chan error) {
	var startedOnce sync.Once
	startedCh := make(chan struct{})
	errCh := make(chan error, 1)
	leaderCh := make(chan struct{}, 1) // the first write never blocks

	listenerFunc := func(newLeader string) {
		defer func() {
			if newLeader != "" {
				leaderCh <- struct{}{}
				startedOnce.Do(func() { close(startedCh) })
			}
		}()
		res.leaderLock.Lock()
		defer res.leaderLock.Unlock()
		res.leader = newLeader
	}
	go func() {
		defer util.HandleCrash()

		err := res.startZKdetection(res.config.Zk, listenerFunc)
		if err != nil {
			errCh <- err
			return
		}

		logging.VeryVerbose.Println("Warning: waiting for initial information from Zookeper.")
		select {
		case <-startedCh:
			logging.VeryVerbose.Println("Info: got initial information from Zookeper.")
		case <-time.After(initialDetectionTimeout):
			errCh <- fmt.Errorf("timed out waiting for initial ZK detection, exiting")
		}
	}()
	return leaderCh, errCh
}

// Reload triggers a new state load from the configured Mesos master.
func (res *Resolver) Reload() {
	t := records.RecordGenerator{}

	// Being very conservative
	res.leaderLock.RLock()
	currentLeader := res.leader
	res.leaderLock.RUnlock()
	err := t.ParseState(currentLeader, res.config)

	if err == nil {
		timestamp := uint32(time.Now().Unix())
		// may need to refactor for fairness
		res.rsLock.Lock()
		defer res.rsLock.Unlock()
		res.config.SOASerial = timestamp
		res.rs = &t
	} else {
		logging.VeryVerbose.Println("Warning: master not found; keeping old DNS state")
	}
}

// extQueryTimeout is the default external resolver query timeout.
const extQueryTimeout = 5 * time.Second

// defaultExtResolver queries other nameserver, potentially recurses; callers should probably be invoking extResolver
// instead since that's the pluggable entrypoint into external resolution.
func (res *Resolver) defaultExtResolver(r *dns.Msg, nameserver, proto string, cnt int) (in *dns.Msg, err error) {
	defer logging.CurLog.NonMesosRecursed.Inc()

	timeout := extQueryTimeout
	if res.config.Timeout != 0 {
		timeout = time.Duration(res.config.Timeout) * time.Second
	}

	c := dns.Client{
		Net:          proto,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}

	for i := 0; i < cnt; i++ {
		in, _, err = c.Exchange(r, nameserver)
		if err != nil || len(in.Ns) == 0 || (in.Authoritative && len(in.Answer) > 0) {
			break
		} else if soa, ok := in.Ns[0].(*dns.SOA); ok {
			nameserver = soa.Ns
		}
	}

	return in, err
}

// formatSRV returns the SRV resource record for target
func (res *Resolver) formatSRV(name string, target string) (*dns.SRV, error) {
	ttl := uint32(res.config.TTL)

	h, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, errors.New("invalid target")
	}
	p, _ := strconv.Atoi(port)

	return &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   name,
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(p),
		Target:   h,
	}, nil
}

// returns the A resource record for target
// assumes target is a well formed IPv4 address
func (res *Resolver) formatA(dom string, target string) (*dns.A, error) {
	ttl := uint32(res.config.TTL)

	a := net.ParseIP(target)
	if a == nil {
		return nil, errors.New("invalid target")
	}

	return &dns.A{
		Hdr: dns.RR_Header{
			Name:   dom,
			Rrtype: dns.TypeA,
			Class:  dns.ClassINET,
			Ttl:    ttl},
		A: a.To4(),
	}, nil
}

// formatSOA returns the SOA resource record for the mesos domain
func (res *Resolver) formatSOA(dom string) (*dns.SOA, error) {
	ttl := uint32(res.config.TTL)

	return &dns.SOA{
		Hdr: dns.RR_Header{
			Name:   dom,
			Rrtype: dns.TypeSOA,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ns:      res.config.SOARname,
		Mbox:    res.config.SOAMname,
		Serial:  res.config.SOASerial,
		Refresh: res.config.SOARefresh,
		Retry:   res.config.SOARetry,
		Expire:  res.config.SOAExpire,
		Minttl:  ttl,
	}, nil
}

// formatNS returns the NS  record for the mesos domain
func (res *Resolver) formatNS(dom string) (*dns.NS, error) {
	ttl := uint32(res.config.TTL)

	return &dns.NS{
		Hdr: dns.RR_Header{
			Name:   dom,
			Rrtype: dns.TypeNS,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ns: res.config.SOAMname,
	}, nil
}

// reorders answers for very basic load balancing
func shuffleAnswers(rng *rand.Rand, answers []dns.RR) []dns.RR {
	n := len(answers)
	for i := 0; i < n; i++ {
		r := i + rng.Intn(n-i)
		answers[r], answers[i] = answers[i], answers[r]
	}

	return answers
}

// HandleNonMesos handles non-mesos queries by recursing to a configured
// external resolver.
func (res *Resolver) HandleNonMesos(w dns.ResponseWriter, r *dns.Msg) {
	var err error
	var m *dns.Msg

	// tracing info
	logging.CurLog.NonMesosRequests.Inc()

	// If external request are disabled
	if !res.config.ExternalOn {
		m = new(dns.Msg)
		// set refused
		m.SetRcode(r, 5)
	} else {

		proto := "udp"
		if _, ok := w.RemoteAddr().(*net.TCPAddr); ok {
			proto = "tcp"
		}

		for _, resolver := range res.config.Resolvers {
			nameserver := net.JoinHostPort(resolver, "53")
			m, err = res.extResolver(r, nameserver, proto, recurseCnt)
			if err == nil {
				break
			}
		}
	}

	// extResolver returns nil Msg sometimes cause of perf
	if m == nil {
		m = new(dns.Msg)
		m.SetRcode(r, 2)
		err = fmt.Errorf("failed external DNS lookup of %q: %v", r.Question[0].Name, err)
	}
	if err != nil {
		logging.Error.Println(r.Question[0].Name)
		logging.Error.Println(err)
		logging.CurLog.NonMesosFailed.Inc()
	} else {
		// nxdomain
		if len(m.Answer) == 0 {
			logging.CurLog.NonMesosNXDomain.Inc()
		} else {
			logging.CurLog.NonMesosSuccess.Inc()
		}
	}

	reply(w, m)
}

// HandleMesos is a resolver request handler that responds to a resource
// question with resource answer(s)
// it can handle {A, SRV, ANY}
func (res *Resolver) HandleMesos(w dns.ResponseWriter, r *dns.Msg) {
	logging.CurLog.MesosRequests.Inc()

	m := &dns.Msg{MsgHdr: dns.MsgHdr{
		Authoritative:      true,
		RecursionAvailable: res.config.RecurseOn,
	}}
	m.SetReply(r)

	var errs multiError
	rs := res.records()
	name := strings.ToLower(cleanWild(r.Question[0].Name))
	switch r.Question[0].Qtype {
	case dns.TypeSRV:
		errs = append(errs, res.handleSRV(rs, name, m, r))
	case dns.TypeA:
		errs = append(errs, res.handleA(rs, name, m))
	case dns.TypeSOA:
		errs = append(errs, res.handleSOA(m, r))
	case dns.TypeNS:
		errs = append(errs, res.handleNS(m, r))
	case dns.TypeANY:
		errs = append(errs,
			res.handleSRV(rs, name, m, r),
			res.handleA(rs, name, m),
			res.handleSOA(m, r),
			res.handleNS(m, r),
		)
	}

	if len(m.Answer) == 0 {
		errs = append(errs, res.handleEmpty(rs, name, m, r))
	} else {
		shuffleAnswers(res.rng, m.Answer)
		logging.CurLog.MesosSuccess.Inc()
	}

	if !errs.Nil() {
		logging.Error.Println(errs)
		logging.CurLog.MesosFailed.Inc()
	}

	reply(w, m)
}

func (res *Resolver) handleSRV(rs *records.RecordGenerator, name string, m, r *dns.Msg) error {
	var errs multiError
	for _, srv := range rs.SRVs[name] {
		srvRR, err := res.formatSRV(r.Question[0].Name, srv)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		m.Answer = append(m.Answer, srvRR)
		host := strings.Split(srv, ":")[0]
		if len(rs.As[host]) == 0 {
			continue
		}

		aRR, err := res.formatA(host, rs.As[host][0])
		if err != nil {
			errs = append(errs, err)
			continue
		}

		m.Extra = append(m.Extra, aRR)
	}
	return errs
}

func (res *Resolver) handleA(rs *records.RecordGenerator, name string, m *dns.Msg) error {
	var errs multiError
	for _, a := range rs.As[name] {
		rr, err := res.formatA(name, a)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		m.Answer = append(m.Answer, rr)
	}
	return errs
}

func (res *Resolver) handleSOA(m, r *dns.Msg) error {
	rr, err := res.formatSOA(r.Question[0].Name)
	if err != nil {
		return err
	}
	m.Ns = append(m.Ns, rr)
	return nil
}

func (res *Resolver) handleNS(m, r *dns.Msg) error {
	rr, err := res.formatNS(r.Question[0].Name)
	logging.Error.Println("NS request")
	if err != nil {
		return err
	}
	m.Ns = append(m.Ns, rr)
	return nil
}

func (res *Resolver) handleEmpty(rs *records.RecordGenerator, name string, m, r *dns.Msg) error {
	qType := r.Question[0].Qtype
	switch qType {
	case dns.TypeSOA, dns.TypeNS, dns.TypeSRV:
		logging.CurLog.MesosSuccess.Inc()
		return nil
	}

	m.Rcode = dns.RcodeNameError
	if qType == dns.TypeAAAA && len(rs.SRVs[name])+len(rs.As[name]) > 0 {
		m.Rcode = dns.RcodeSuccess
	}

	logging.CurLog.MesosNXDomain.Inc()
	logging.VeryVerbose.Println("total A rrs:\t" + strconv.Itoa(len(rs.As)))
	logging.VeryVerbose.Println("failed looking for " + r.Question[0].String())

	rr, err := res.formatSOA(r.Question[0].Name)
	if err != nil {
		return err
	}

	m.Ns = append(m.Ns, rr)
	return nil
}

// reply writes the given dns.Msg out to the given dns.ResponseWriter,
// compressing the message first and truncating it accordingly.
func reply(w dns.ResponseWriter, m *dns.Msg) {
	m.Compress = true // https://github.com/mesosphere/mesos-dns/issues/{170,173,174}
	if err := w.WriteMsg(truncate(m, isUDP(w))); err != nil {
		logging.Error.Println(err)
	}
}

// isUDP returns true if the transmission channel in use is UDP.
func isUDP(w dns.ResponseWriter) bool {
	return strings.HasPrefix(w.RemoteAddr().Network(), "udp")
}

// truncate sets the TC bit in the given dns.Msg if its length exceeds the
// permitted length of the given transmission channel.
// See https://tools.ietf.org/html/rfc1035#section-4.2.1
func truncate(m *dns.Msg, udp bool) *dns.Msg {
	m.Truncated = udp && m.Len() > dns.MinMsgSize
	return m
}

func (res *Resolver) configureHTTP() {
	// webserver + available routes
	ws := new(restful.WebService)
	ws.Route(ws.GET("/v1/version").To(res.RestVersion))
	ws.Route(ws.GET("/v1/config").To(res.RestConfig))
	ws.Route(ws.GET("/v1/hosts/{host}").To(res.RestHost))
	ws.Route(ws.GET("/v1/hosts/{host}/ports").To(res.RestPorts))
	ws.Route(ws.GET("/v1/services/{service}").To(res.RestService))
	restful.Add(ws)
}

// LaunchHTTP starts an HTTP server for the Resolver, returning a error channel
// to which errors are asynchronously sent.
func (res *Resolver) LaunchHTTP() <-chan error {
	defer util.HandleCrash()

	res.configureHTTP()
	portString := ":" + strconv.Itoa(res.config.HTTPPort)

	errCh := make(chan error, 1)
	go func() {
		var err error
		defer func() { errCh <- err }()

		if err = http.ListenAndServe(portString, nil); err != nil {
			err = fmt.Errorf("Failed to setup http server: %v", err)
		} else {
			logging.Error.Println("Not serving http requests any more.")
		}
	}()
	return errCh
}

// RestConfig handles HTTP requests of Resolver configuration.
func (res *Resolver) RestConfig(req *restful.Request, resp *restful.Response) {
	if err := resp.WriteAsJson(res.config); err != nil {
		logging.Error.Println(err)
	}
}

// RestVersion handles HTTP requests of Mesos-DNS version.
func (res *Resolver) RestVersion(req *restful.Request, resp *restful.Response) {
	err := resp.WriteAsJson(map[string]string{
		"Service": "Mesos-DNS",
		"Version": res.version,
		"URL":     "https://github.com/mesosphere/mesos-dns",
	})
	if err != nil {
		logging.Error.Println(err)
	}
}

// RestHost handles HTTP requests of DNS A records of the given host.
func (res *Resolver) RestHost(req *restful.Request, resp *restful.Response) {
	host := req.PathParameter("host")
	// clean up host name
	dom := strings.ToLower(cleanWild(host))
	if dom[len(dom)-1] != '.' {
		dom += "."
	}
	rs := res.records()

	type record struct {
		Host string `json:"host"`
		IP   string `json:"ip"`
	}

	aRRs := rs.As[dom]
	records := make([]record, 0, len(aRRs))
	for _, ip := range aRRs {
		records = append(records, record{dom, ip})
	}

	if len(records) == 0 {
		records = append(records, record{})
	}

	if err := resp.WriteAsJson(records); err != nil {
		logging.Error.Println(err)
	}

	stats(dom, res.config.Domain+".", len(aRRs) > 0)
}

func stats(domain, zone string, success bool) {
	if strings.HasSuffix(domain, zone) {
		logging.CurLog.MesosRequests.Inc()
		if success {
			logging.CurLog.MesosSuccess.Inc()
		} else {
			logging.CurLog.MesosNXDomain.Inc()
		}
	} else {
		logging.CurLog.NonMesosRequests.Inc()
		logging.CurLog.NonMesosFailed.Inc()
	}
}

// RestPorts is an HTTP handler which is currently not implemented.
func (res *Resolver) RestPorts(req *restful.Request, resp *restful.Response) {
	err := resp.WriteErrorString(http.StatusNotImplemented, "To be implemented...")
	if err != nil {
		logging.Error.Println(err)
	}
}

// RestService handles HTTP requests of DNS SRV records for the given name.
func (res *Resolver) RestService(req *restful.Request, resp *restful.Response) {
	service := req.PathParameter("service")
	// clean up service name
	dom := strings.ToLower(cleanWild(service))
	if dom[len(dom)-1] != '.' {
		dom += "."
	}
	rs := res.records()

	type record struct {
		Service string `json:"service"`
		Host    string `json:"host"`
		IP      string `json:"ip"`
		Port    string `json:"port"`
	}

	srvRRs := rs.SRVs[dom]
	records := make([]record, 0, len(srvRRs))
	for _, s := range srvRRs {
		host, port, _ := net.SplitHostPort(s)
		var ip string
		if r := rs.As[host]; len(r) != 0 {
			ip = r[0]
		}
		records = append(records, record{service, host, ip, port})
	}

	if len(records) == 0 {
		records = append(records, record{})
	}

	if err := resp.WriteAsJson(records); err != nil {
		logging.Error.Println(err)
	}

	stats(dom, res.config.Domain+".", len(srvRRs) > 0)
}

// panicRecover catches any panics from the resolvers and sets an error
// code of server failure
func panicRecover(f func(w dns.ResponseWriter, r *dns.Msg)) func(w dns.ResponseWriter, r *dns.Msg) {
	return func(w dns.ResponseWriter, r *dns.Msg) {
		defer func() {
			if rec := recover(); rec != nil {
				m := new(dns.Msg)
				m.SetRcode(r, 2)
				_ = w.WriteMsg(m)
				logging.Error.Println(rec)
			}
		}()
		f(w, r)
	}
}

// Start a Zookeeper listener to track leading master, invokes callback function when
// master changes are reported.
func startDefaultZKdetector(zkurl string, leaderChanged func(string)) error {

	// start listener
	logging.Verbose.Println("Starting master detector for ZK ", zkurl)
	md, err := detector.New(zkurl)
	if err != nil {
		return fmt.Errorf("failed to create master detector: %v", err)
	}

	// and listen for master changes
	if err := md.Detect(detector.OnMasterChanged(func(info *mesos.MasterInfo) {
		leader := ""
		if leaderChanged != nil {
			defer func() {
				leaderChanged(leader)
			}()
		}
		logging.VeryVerbose.Println("Updated Zookeeper info: ", info)
		if info == nil {
			logging.Error.Println("No leader available in Zookeeper.")
		} else {
			if host := info.GetHostname(); host != "" {
				leader = host
			} else {
				// unpack IPv4
				octets := make([]byte, 4, 4)
				binary.BigEndian.PutUint32(octets, info.GetIp())
				ipv4 := net.IP(octets)
				leader = ipv4.String()
			}
			leader = fmt.Sprintf("%s:%d", leader, info.GetPort())
			logging.Verbose.Println("new master in Zookeeper ", leader)
		}
	})); err != nil {
		return fmt.Errorf("failed to initialize master detector: %v", err)
	}
	return nil
}

// cleanWild strips any wildcards out thus mapping cleanly to the
// original serviceName
func cleanWild(name string) string {
	if strings.Contains(name, ".*") {
		return strings.Replace(name, ".*", "", -1)
	}
	return name
}

type multiError []error

func (e multiError) Error() string {
	errs := make([]string, len(e))
	for i := range errs {
		if e[i] != nil {
			errs[i] = e[i].Error()
		}
	}
	return strings.Join(errs, "; ")
}

func (e multiError) Nil() bool {
	for _, err := range e {
		if err != nil {
			return false
		}
	}
	return true
}
