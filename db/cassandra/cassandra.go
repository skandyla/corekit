package cassandra

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

func InitCluster(connection string) (*gocql.ClusterConfig, error) {

	connectionURL, err := url.Parse(connection)
	if err != nil {
		return nil, errors.Wrapf(err, "Incorrect Database connection string format should be cassandra://[user:password@]ip1[:port],[ip2[:port]]/keyspace[?dc=dc_name] but have %v", connection)
	}

	//validate hosts
	hosts := strings.Split(connectionURL.Host, ",")
	fmt.Printf("Cassandra connection hosts:%+v\n", hosts)
	if len(hosts) == 0 {
		return nil, errors.New(fmt.Sprintf("Connection string must content a host"))
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = strings.TrimLeft(connectionURL.Path, "/")
	if len(cluster.Keyspace) == 0 {
		return nil, errors.New(fmt.Sprintf("Connection string must content a keyspace"))
	}

	if connectionURL.User != nil {
		pwd, ok := connectionURL.User.Password()
		if !ok {
			return nil, errors.New(fmt.Sprintf("Connection string must content a password for user"))
		}
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: connectionURL.User.Username(),
			Password: pwd,
		}
	}

	if connectionURL.Query().Get("dc") != "" {
		//cluster.PoolConfig = gocql.PoolConfig{
		//	HostSelectionPolicy: gocql.DCAwareRoundRobinPolicy(connectionURL.Query().Get("dc")),
		//}
		//cluster.ProtoVersion = 3
		//cluster.CQLVersion = "3.0.0"
		cluster.HostFilter = gocql.DataCentreHostFilter(connectionURL.Query().Get("dc"))
		cluster.Consistency = gocql.LocalQuorum
		fmt.Println("Cassandra connection.Dc:", connectionURL.Query().Get("dc"))
	}

	//if connectionURL.Query().Get("init_host_lookup") != "" {
	//	cluster.DisableInitialHostLookup = connectionURL.Query().Get("init_host_lookup") == "false"
	//}

	return cluster, nil
}

//cluster, err := cassandra.InitCluster(dbCassandraConStr)
//cluster.Timeout = time.Minute
//casSession, err := cluster.CreateSession()
//if err != nil {
//	log.Fatalf("Cluster CreateSession Err: %+v", err)
//}
//fmt.Println("cassandra init done")
