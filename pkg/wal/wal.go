package wal

import (
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/tidwall/wal"
	"github.com/woodliu/prometheusWriter/pkg/lib"
	"log"
	"sync/atomic"
)

func Replay(es *tsdb.CircularExemplarStorage) {
	var wr prompb.WriteRequest
	lwal, err := LoadWal()
	if err != nil {
		log.Fatalln(err)
	}
	fidx, err := lwal.FirstIndex()
	if err != nil {
		log.Fatalln(err)
	}

	lidx, err := lwal.LastIndex()
	if err != nil {
		log.Fatalln(err)
	}

	for i := fidx; i <= lidx; i++ {
		data, err := lwal.Read(i)
		if err != nil {
			log.Println("读取日志失败", err)
		}
		err = proto.Unmarshal(data, &wr)
		if nil != err {
			log.Println("1111", err)
			continue
		}
		for _, v := range wr.Timeseries {
			lib.AddNewExemplar(es, v.Labels, v.Exemplars)
		}
	}
}

type Wal struct {
	idx uint64
	*wal.Log
}

func (w *Wal) Write(p []byte) (n int, err error) {
	atomic.AddUint64(&w.idx, 1)
	return len(p), w.Log.Write(w.idx, p)
}

func newWal() (*Wal, error) {
	walLog, err := wal.Open("mylog", nil)
	if err != nil {
		return nil, err
	}
	idx, err := walLog.LastIndex()
	if err != nil {
		return nil, err
	}
	return &Wal{
		idx: idx,
		Log: walLog,
	}, nil
}

var _wal *Wal

func LoadWal() (*Wal, error) {
	if _wal == nil {
		return newWal()
	} else {
		return _wal, nil
	}
}
