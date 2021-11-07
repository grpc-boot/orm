package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"net"
	"time"

	"github.com/grpc-boot/base"
	"go.uber.org/atomic"
)

var (
	ErrNoMasterConn = errors.New("mysql group: no master connection available")
	ErrNoSlaveConn  = errors.New("mysql group: no slave connection available")
)

type GroupOption struct {
	Masters []PoolOption `yaml:"masters" json:"masters"`
	Slaves  []PoolOption `yaml:"slaves" json:"slaves"`
	//单位s
	RetryInterval int64 `yaml:"retryInterval" json:"retryInterval"`
}

type Group interface {
	GetMaster() (index int, mPoll Pool, badTime int64)
	GetSlave() (index int, mPoll Pool, badTime int64)
	GetBadPool(isMaster bool) (list []int)
	InsertObj(obj interface{}) (result sql.Result, err error)
	DeleteObj(obj interface{}) (result sql.Result, err error)
	UpdateObj(obj interface{}) (result sql.Result, err error)
	Query(query Query, useMaster bool) (rows *sql.Rows, err error)
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

func isLostError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	}

	if errVal, ok := err.(*net.OpError); ok {
		log.Printf("exec sql error:%s", errVal.Error())
		return true
	}
	return false
}

type group struct {
	Group

	masters map[int]Pool
	slaves  map[int]Pool

	masterBadPool map[int]*atomic.Int64
	slaveBadPool  map[int]*atomic.Int64

	retryInterval int64
	masterLen     int
	slaveLen      int
}

func NewMysqlGroup(groupOption *GroupOption) (Group, error) {
	if len(groupOption.Slaves) == 0 {
		groupOption.Slaves = groupOption.Masters
	}

	g := &group{
		masterLen:     len(groupOption.Masters),
		slaveLen:      len(groupOption.Slaves),
		retryInterval: groupOption.RetryInterval,
		masterBadPool: make(map[int]*atomic.Int64, len(groupOption.Masters)),
		slaveBadPool:  make(map[int]*atomic.Int64, len(groupOption.Slaves)),
	}

	g.masters = make(map[int]Pool, g.masterLen)
	g.slaves = make(map[int]Pool, g.slaveLen)

	for index, _ := range groupOption.Masters {
		pool, err := newMysqlPool(&groupOption.Masters[index])
		if err != nil {
			return nil, err
		}

		g.masters[index] = pool
		g.masterBadPool[index] = &atomic.Int64{}
	}

	for index, _ := range groupOption.Slaves {
		pool, err := newMysqlPool(&groupOption.Slaves[index])
		if err != nil {
			return nil, err
		}

		g.slaves[index] = pool
		g.slaveBadPool[index] = &atomic.Int64{}
	}

	return g, nil
}

func (g *group) downMaster(index int) {
	if index >= g.masterLen {
		return
	}

	if g.masterBadPool[index].Load() > 0 {
		return
	}
	g.masterBadPool[index].CAS(0, time.Now().Unix())
}

func (g *group) upMaster(index int) {
	if index >= g.masterLen {
		return
	}
	g.masterBadPool[index].Store(0)
}

func (g *group) downSlave(index int) {
	if index >= g.slaveLen {
		return
	}

	if g.slaveBadPool[index].Load() > 0 {
		return
	}
	g.slaveBadPool[index].CAS(0, time.Now().Unix())
}

func (g *group) upSlave(index int) {
	if index >= g.slaveLen {
		return
	}
	g.slaveBadPool[index].Store(0)
}

func (g *group) GetBadPool(isMaster bool) (list []int) {
	if isMaster {
		list = make([]int, 0, g.masterLen)
		for index := 0; index < g.masterLen; index++ {
			if g.masterBadPool[index].Load() > 0 {
				list = append(list, index)
			}
		}
		return
	}

	list = make([]int, 0, g.slaveLen)
	for index := 0; index < g.slaveLen; index++ {
		if g.slaveBadPool[index].Load() > 0 {
			list = append(list, index)
		}
	}
	return
}

func (g *group) GetMaster() (index int, mPoll Pool, badTime int64) {
	if g.masterLen == 1 {
		return 0, g.masters[0], g.masterBadPool[0].Load()
	}

	current := time.Now().Unix()
	for index, mPoll = range g.masters {
		badTime = g.masterBadPool[index].Load()
		if badTime == 0 {
			return index, mPoll, badTime
		}

		if badTime+g.retryInterval < current {
			g.masterBadPool[index].Store(current)
			return index, mPoll, badTime
		}
	}

	return 0, g.masters[0], g.masterBadPool[0].Load()
}

func (g *group) GetSlave() (index int, mPoll Pool, badTime int64) {
	if g.slaveLen == 1 {
		return 0, g.slaves[0], g.slaveBadPool[0].Load()
	}

	current := time.Now().Unix()
	for index, mPoll = range g.slaves {
		badTime = g.slaveBadPool[index].Load()
		if badTime == 0 {
			return index, mPoll, badTime
		}

		if badTime+g.retryInterval < current {
			g.slaveBadPool[index].Store(current)
			return index, mPoll, badTime
		}
	}

	return 0, g.slaves[0], g.slaveBadPool[0].Load()
}

func (g *group) Exec(handler func(mPool Pool) (sql.Result, error)) (result sql.Result, err error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.GetMaster()
		result, err = handler(pool)
		if err == nil {
			if badTime > 0 {
				g.upMaster(index)
			}

			return result, nil
		}

		if isLostError(err) {
			g.downMaster(index)
			continue
		}

		if badTime > 0 {
			g.upMaster(index)
		}

		return result, err
	}
	return nil, ErrNoMasterConn
}

func (g *group) QuerySlave(handler func(mPool Pool) (*sql.Rows, error)) (rows *sql.Rows, err error) {
	for start := 0; start < g.slaveLen; start++ {
		index, pool, badTime := g.GetSlave()
		rows, err = handler(pool)
		if err == nil {
			if badTime > 0 {
				g.upSlave(index)
			}

			return rows, err
		}

		if isLostError(err) {
			g.downSlave(index)
			continue
		}

		if badTime > 0 {
			g.upSlave(index)
		}
		return rows, err
	}

	return nil, ErrNoSlaveConn
}

func (g *group) QueryMaster(handler func(mPool Pool) (*sql.Rows, error)) (rows *sql.Rows, err error) {
	for start := 0; start < g.slaveLen; start++ {
		index, pool, badTime := g.GetMaster()
		rows, err = handler(pool)
		if err == nil {
			if badTime > 0 {
				g.upMaster(index)
			}

			return rows, err
		}

		if isLostError(err) {
			g.downMaster(index)
			continue
		}

		if badTime > 0 {
			g.upMaster(index)
		}
		return rows, err
	}

	return nil, ErrNoMasterConn
}

func (g *group) Query(query Query, useMaster bool) (rows *sql.Rows, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	if useMaster {
		return g.QueryMaster(func(mPool Pool) (*sql.Rows, error) {
			return mPool.Query(query.Sql(&args), args...)
		})
	}

	return g.QuerySlave(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(query.Sql(&args), args...)
	})
}

func (g *group) InsertObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlInsertObjs(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.Exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Execute(sqlStr, args...)
	})
}

func (g *group) DeleteObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlDeleteByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.Exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Execute(sqlStr, args...)
	})
}

func (g *group) UpdateObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlUpdateByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.Exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Execute(sqlStr, args...)
	})
}

func (g *group) Begin() (*sql.Tx, error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.GetMaster()
		tx, err := pool.Begin()
		if err == nil {
			if badTime > 0 {
				g.upMaster(index)
			}

			return tx, nil
		}

		if isLostError(err) {
			g.downMaster(index)
			continue
		}

		if badTime > 0 {
			g.upMaster(index)
		}

		return tx, err
	}
	return nil, ErrNoMasterConn
}

func (g *group) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.GetMaster()
		tx, err := pool.BeginTx(ctx, opts)
		if err == nil {
			if badTime > 0 {
				g.upMaster(index)
			}

			return tx, nil
		}

		if isLostError(err) {
			g.downMaster(index)
			continue
		}

		if badTime > 0 {
			g.upMaster(index)
		}

		return tx, err
	}
	return nil, ErrNoMasterConn
}
