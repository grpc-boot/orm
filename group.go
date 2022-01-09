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
	// BadPool 获取BadPool列表
	BadPool(isMaster bool) (list []int)
	// Query 查询
	Query(useMaster bool, sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	// QueryContext with context 查询
	QueryContext(ctx context.Context, useMaster bool, sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	// Exec 执行
	Exec(sqlStr string, args ...interface{}) (result sql.Result, err error)
	// ExecContext with context 执行
	ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error)

	// Tables 获取表列表
	Tables(pattern string, useMaster bool) (tableList []string, err error)
	Table(table string, useMaster bool) (t *Table, err error)

	// InsertObj 插入对象
	InsertObj(obj interface{}) (result sql.Result, err error)
	// InsertObjContext with context 插入对象
	InsertObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// DeleteObj 删除对象
	DeleteObj(obj interface{}) (result sql.Result, err error)
	// DeleteObjContext with context 删除对象
	DeleteObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// UpdateObj 更新对象
	UpdateObj(obj interface{}) (result sql.Result, err error)
	// UpdateObjContext with context 更新对象
	UpdateObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// Find 根据Query查询
	Find(query Query, useMaster bool) (rows []map[string]string, err error)
	// FindContext with context 根据Query查询
	FindContext(ctx context.Context, query Query, useMaster bool) (rows []map[string]string, err error)
	// FindAll 根据Query查询，返回对象列表
	FindAll(query Query, obj interface{}, useMaster bool) (objList []interface{}, err error)
	// FindAllContext with context 根据Query查询，返回对象列表
	FindAllContext(ctx context.Context, query Query, obj interface{}, useMaster bool) (objList []interface{}, err error)
	// FindOne 查询一个
	FindOne(table string, where Where, useMaster bool) (row map[string]string, err error)
	// FindOneContext with context  查询一个
	FindOneContext(ctx context.Context, table string, where Where, useMaster bool) (row map[string]string, err error)
	// FindOneObj 查询一个对象
	FindOneObj(where Where, obj interface{}, useMaster bool) (err error)
	// FindOneObjContext with context  查询一个对象
	FindOneObjContext(ctx context.Context, where Where, obj interface{}, useMaster bool) (err error)
	// Insert 插入
	Insert(table string, rows ...Row) (result sql.Result, err error)
	// InsertContext with context 插入
	InsertContext(ctx context.Context, table string, rows ...Row) (result sql.Result, err error)
	// DeleteAll 删除
	DeleteAll(table string, where Where) (result sql.Result, err error)
	// DeleteAllContext with context 删除
	DeleteAllContext(ctx context.Context, table string, where Where) (result sql.Result, err error)
	// UpdateAll 更新
	UpdateAll(table string, set Row, where Where) (result sql.Result, err error)
	// UpdateAllContext with context 更新
	UpdateAllContext(ctx context.Context, table string, set Row, where Where) (result sql.Result, err error)

	// Begin 开启事务
	Begin() (Transaction, error)
	// BeginTx with context 开启事务
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
}

type group struct {
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

func (g *group) isBadConnError(index int, badTime int64, err error, master bool) (isBadConn bool) {
	if err == nil {
		if badTime > 0 {
			g.up(index, master)
		}
		return false
	}

	if err == driver.ErrBadConn {
		g.down(index, master)
		return true
	}

	if errVal, ok := err.(*net.OpError); ok {
		log.Printf("exec sql error:%s", errVal.Error())
		g.down(index, master)
		return true
	}

	if badTime > 0 {
		g.up(index, master)
	}

	return false
}

func (g *group) down(index int, isMaster bool) {
	if isMaster {
		if index >= g.masterLen {
			return
		}

		if g.masterBadPool[index].Load() > 0 {
			return
		}
		g.masterBadPool[index].CAS(0, time.Now().Unix())
		return
	}

	if index >= g.slaveLen {
		return
	}

	if g.slaveBadPool[index].Load() > 0 {
		return
	}
	g.slaveBadPool[index].CAS(0, time.Now().Unix())
}

func (g *group) up(index int, isMaster bool) {
	if isMaster {
		if index >= g.masterLen {
			return
		}
		g.masterBadPool[index].Store(0)
		return
	}

	if index >= g.slaveLen {
		return
	}
	g.slaveBadPool[index].Store(0)
}

func (g *group) getMaster() (index int, mPoll Pool, badTime int64) {
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

func (g *group) getSlave() (index int, mPoll Pool, badTime int64) {
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

func (g *group) exec(handler func(mPool Pool) (sql.Result, error)) (result sql.Result, err error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.getMaster()
		result, err = handler(pool)
		if g.isBadConnError(index, badTime, err, true) {
			continue
		}
		return result, err
	}
	return nil, ErrNoMasterConn
}

func (g *group) query(handler func(mPool Pool) (*sql.Rows, error), useMaster bool) (rows *sql.Rows, err error) {
	var funcPool = g.getSlave
	if useMaster {
		funcPool = g.getMaster
	}

	for start := 0; start < g.slaveLen; start++ {
		index, pool, badTime := funcPool()
		rows, err = handler(pool)
		if g.isBadConnError(index, badTime, err, useMaster) {
			continue
		}
		return rows, err
	}

	return nil, ErrNoSlaveConn
}

func (g *group) BadPool(isMaster bool) (list []int) {
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

func (g *group) Query(useMaster bool, sqlStr string, args ...interface{}) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows
	)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (g *group) QueryContext(ctx context.Context, useMaster bool, sqlStr string, args ...interface{}) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows
	)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.QueryContext(ctx, sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (g *group) Exec(sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Exec(sqlStr, args...)
	})
}

func (g *group) ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.ExecContext(ctx, sqlStr, args...)
	})
}

func (g *group) InsertObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlInsertObjs(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Exec(sqlStr, args...)
	})
}

func (g *group) InsertObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlInsertObjs(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.ExecContext(ctx, sqlStr, args...)
	})
}

func (g *group) DeleteObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlDeleteByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Exec(sqlStr, args...)
	})
}

func (g *group) DeleteObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlDeleteByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.ExecContext(ctx, sqlStr, args...)
	})
}

func (g *group) UpdateObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlUpdateByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.Exec(sqlStr, args...)
	})
}

func (g *group) UpdateObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlUpdateByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return g.exec(func(mPool Pool) (sql.Result, error) {
		return mPool.ExecContext(ctx, sqlStr, args...)
	})
}

func (g *group) Find(query Query, useMaster bool) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (g *group) FindContext(ctx context.Context, query Query, useMaster bool) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.QueryContext(ctx, sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (g *group) FindAll(query Query, obj interface{}, useMaster bool) (objList []interface{}, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToObjList(sqlRows, obj)
}

func (g *group) FindAllContext(ctx context.Context, query Query, obj interface{}, useMaster bool) (objList []interface{}, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.QueryContext(ctx, sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	return ToObjList(sqlRows, obj)
}

func (g *group) FindOne(table string, where Where, useMaster bool) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = AcquireQuery4Mysql().From(table).Where(where).Limit(1)
		sqlStr = query.Sql(&args)
	)

	defer func() {
		base.ReleaseArgs(&args)
		query.Close()
	}()

	rows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	tRows, err := ToMap(rows)
	if err != nil {
		return
	}

	if len(tRows) > 0 {
		return tRows[0], nil
	}
	return
}

func (g *group) FindOneContext(ctx context.Context, table string, where Where, useMaster bool) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = AcquireQuery4Mysql().From(table).Where(where).Limit(1)
		sqlStr = query.Sql(&args)
	)

	defer func() {
		base.ReleaseArgs(&args)
		query.Close()
	}()

	rows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.QueryContext(ctx, sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}

	tRows, err := ToMap(rows)
	if err != nil {
		return
	}

	if len(tRows) > 0 {
		return tRows[0], nil
	}
	return
}

func (g *group) FindOneObj(where Where, obj interface{}, useMaster bool) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, where, obj)
	if err != nil {
		return err
	}

	rows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}
	return ToObj(rows, obj)
}

func (g *group) FindOneObjContext(ctx context.Context, where Where, obj interface{}, useMaster bool) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, where, obj)
	if err != nil {
		return err
	}

	rows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.QueryContext(ctx, sqlStr, args...)
	}, useMaster)

	if err != nil {
		return
	}
	return ToObj(rows, obj)
}

func (g *group) Insert(table string, rows ...Row) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return g.Exec(sqlStr, args...)
}

func (g *group) InsertContext(ctx context.Context, table string, rows ...Row) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return g.ExecContext(ctx, sqlStr, args...)
}

func (g *group) DeleteAll(table string, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, where)
	)
	defer base.ReleaseArgs(&args)

	return g.Exec(sqlStr, args...)
}

func (g *group) DeleteAllContext(ctx context.Context, table string, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, where)
	)
	defer base.ReleaseArgs(&args)

	return g.ExecContext(ctx, sqlStr, args...)
}

func (g *group) UpdateAll(table string, set Row, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, where)
	)
	defer base.ReleaseArgs(&args)

	return g.Exec(sqlStr, args...)
}

func (g *group) UpdateAllContext(ctx context.Context, table string, set Row, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, where)
	)
	defer base.ReleaseArgs(&args)

	return g.ExecContext(ctx, sqlStr, args...)
}

func (g *group) Begin() (Transaction, error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.getMaster()
		tx, err := pool.Begin()
		if g.isBadConnError(index, badTime, err, true) {
			continue
		}
		return tx, err
	}
	return nil, ErrNoMasterConn
}

func (g *group) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.getMaster()
		tx, err := pool.BeginTx(ctx, opts)
		if g.isBadConnError(index, badTime, err, true) {
			continue
		}
		return tx, err
	}
	return nil, ErrNoMasterConn
}
