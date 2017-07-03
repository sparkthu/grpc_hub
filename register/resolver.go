package register

import (
	"google.golang.org/grpc/naming"
	"github.com/coreos/etcd/clientv3"
	"errors"
	"context"
	"strings"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"log"
)

type ResolveMode uint8
const (
	SameClusterFirst ResolveMode = iota
	SameClusterOnly
	AllClusterEqual

)

func NewSimpleResolver(endpoints []string) (*HubResolver) {
	return &HubResolver{endpoints, DefaultRoot, DefaultCluster, SameClusterFirst}
}

func NewResolver(endpoints []string, root string, cluster string, mode ResolveMode) (*HubResolver) {
	return &HubResolver{endpoints, root, cluster, mode}
}
type HubResolver struct {
	endpoints []string
	root  string
	cluster string
	resolveMode ResolveMode
}

type HubWatcher struct {
	hubResolver  *HubResolver
	cli *clientv3.Client

	sameCluster  map[string]string
	otherCluster map[string]string
	lastQueried  map[string]string
	watchChannel clientv3.WatchChan
}
func (h *HubResolver)Resolve(target string) (naming.Watcher, error) {
	log.Println("Resolve request for target:" + target)
	cli, err := clientv3.New(clientv3.Config{Endpoints:h.endpoints, DialTimeout: defaultTimeout})
	if err !=  nil {
		return nil, errors.New("Connect etcd failed!")
	}
	watcher := HubWatcher{h, cli, make(map[string]string),make(map[string]string),make(map[string]string), nil}
	path := h.root + "/" + target + "/";
	resp, err := cli.Get(context.Background(), path, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		keys := strings.Split(string(kv.Key), "/")
		if len(keys) != 4 {
			// not a valid key
			continue
		}
		if len(keys[3]) < 1 {
			continue
		}

		if strings.EqualFold(h.cluster, keys[2]) {
			// same cluster
			watcher.sameCluster[keys[3]] = keys[2]
		} else {
			watcher.otherCluster[keys[3]] = keys[2]
		}

	}
	watcher.watchChannel = cli.Watch(context.Background(), path, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision))

	return &watcher, nil
}

func (h *HubWatcher) Next() ([]*naming.Update, error) {
	for {
		updates := make([]*naming.Update, 0)
		readMap := make(map[string]string)
		if h.hubResolver.resolveMode == SameClusterFirst {
			if len(h.sameCluster) > 0 {
				for k, v := range h.sameCluster {
					readMap[k] = v
				}
			} else {
				for k, v := range h.otherCluster {
					readMap[k] = v
				}
			}
		} else if h.hubResolver.resolveMode == SameClusterOnly {
			for k, v := range h.sameCluster {
				readMap[k] = v
			}
		} else if h.hubResolver.resolveMode == AllClusterEqual {
			for k, v := range h.sameCluster {
				readMap[k] = v
			}
			for k, v := range h.otherCluster {
				readMap[k] = v
			}
		}
		for item := range readMap {
			if _, ok := h.lastQueried[item]; !ok {
				h.lastQueried[item] = readMap[item]
				updates = append(updates, &naming.Update{Op:naming.Add, Addr:item})
			}
		}
		for item := range h.lastQueried {
			if _, ok := readMap[item]; !ok {
				delete(h.lastQueried, item)
				updates = append(updates, &naming.Update{Op:naming.Delete, Addr:item})
			}
		}
		if len(updates) != 0 {
			log.Println("Call Next got response:")
			for _, up := range updates {
				log.Println(up.Op, up.Addr)
			}
			return updates, nil
		} else {
			resp :=  <- h.watchChannel
			if len(resp.Events) > 0 {
				log.Println("Channel got event, length:", len(resp.Events))
				for _, event := range resp.Events {
					log.Printf("Event kv: %v, Type:%d ", event.Kv, event.Type)
				}

			}
			for _, event := range resp.Events {
				keys := strings.Split(string(event.Kv.Key), "/")
				if event.Type == mvccpb.PUT {
					if strings.EqualFold(h.hubResolver.cluster, keys[2]) {
						// same cluster
						h.sameCluster[keys[3]] = keys[2]
					} else {
						h.otherCluster[keys[3]] = keys[2]
					}
				} else if event.Type == mvccpb.DELETE {
					if strings.EqualFold(h.hubResolver.cluster, keys[2]) {
						// same cluster
						delete(h.sameCluster, keys[3])
					} else {
						delete(h.otherCluster, keys[3])
					}
				}
			}

	}
	}

}
func (h *HubWatcher) Close() {
	h.cli.Close()
}