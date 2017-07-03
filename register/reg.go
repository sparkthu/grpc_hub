package register

import (
	"github.com/coreos/etcd/clientv3"
	"time"
	"golang.org/x/net/context"
	"errors"
	"log"
)

const (
	DefaultRoot = "hub"
	DefaultCluster = "default"
)

type RegService struct{
	cli *clientv3.Client
	path string
	running bool
}
var (
	defaultTimeout = 15 * time.Second
	defaultTtl = (int64)(30)
)


func (r *RegService)createNode(timeout time.Duration, ttlSecond int64) error {

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	resp, err := r.cli.Grant(ctx, ttlSecond)
	if err != nil {
		return errors.New("Setup ttl failed:" + err.Error())
	} else {
		log.Println("LeaseID:", resp.ID)
	}
	keepaliveChan, err := r.cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		return errors.New("Setup keepalive failed:" + err.Error())
	}
	go func() {
		for {
			keep := <- keepaliveChan
			if keep !=  nil {
				log.Printf("keepalive channel event:%v, id:%d, ttl:%d", keep, keep.ID, keep.TTL)
			}

		}

	}()
	ctx, _ = context.WithTimeout(context.Background(), timeout)
	_, err = r.cli.Put(ctx,
		r.path, "{\"isActive\":true}", clientv3.WithLease(resp.ID))
	if err != nil {
		return errors.New("Put key failed:" + err.Error())
	}

	return nil
}

func SimpleRegisterService(endpoints []string, serviceName string, ipport string) (reg *RegService, err error) {
	return RegisterService(endpoints, DefaultRoot, DefaultCluster, serviceName, ipport, defaultTimeout, defaultTtl)
}
func RegisterService(endpoints []string, root, cluster, serviceName, ipport string, timeout time.Duration, ttlSecond int64) (reg *RegService, err error) {
	if timeout <= 0 {
		timeout= defaultTimeout
	}
	if ttlSecond <= 0 {
		ttlSecond = defaultTtl
	}
	if len(serviceName) == 0 {
		return nil, errors.New("serviceName is empty!")
	}
	path := root + "/" + serviceName + "/" + cluster + "/" + ipport
	cli, err := clientv3.New(clientv3.Config{Endpoints:endpoints, DialTimeout: timeout})
	if err !=  nil {
		return nil, errors.New("Connect etcd failed:"+ err.Error())
	}

	reg = &RegService{cli:cli, path:path, running:true}
	if err1 := reg.createNode(timeout, ttlSecond); err1 != nil {
		return nil, err1
	}
	go func() {
		// check the node
		ticker :=  time.NewTicker((time.Duration(ttlSecond) - 1) * time.Second)
		for range ticker.C {
			if reg.running {
				//reg.createNode(timeout, ttlSecond)

				//if getResp, err := cli.Get(ctx, path); err == nil {
				//	if len(getResp.Kvs) == 0 && reg.running{
				//		// node is deleted!
				//		reg.createNode(timeout, ttlSecond)
				//	}
				//}
			}
		}
	}()
	return reg, nil
}

func (r *RegService) Unregister() error {
	r.running = false
	if r != nil && r.cli != nil {
		defer r.cli.Close()
		_, err := r.cli.Delete(context.Background(), r.path)
		return err
	}
	return errors.New("Not Registerd")
}