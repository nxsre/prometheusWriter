package main

import (
	"bufio"
	"bytes"
	"context"
	jsoniter "github.com/json-iterator/go"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

type RecoverableError struct {
	error
}

type HttpClient struct {
	url     *url.URL
	Client  *http.Client
	timeout time.Duration
}

var MetricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

type MetricPoint struct {
	Metric  string            `json:"metric"` // 指标名称
	TagsMap map[string]string `json:"tags"`   // 数据标签
	Time    int64             `json:"time"`   // 时间戳，单位是秒
	Value   float64           `json:"value"`  // 内部字段，最终转换之后的float64数值
}

func (c *HttpClient) remoteWritePost(req []byte) error {
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(req))
	if err != nil {
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "opcai")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		var ht *nethttp.Tracer
		httpReq, ht = nethttp.TraceRequest(
			parentSpan.Tracer(),
			httpReq,
			nethttp.OperationName("Remote Store"),
			nethttp.ClientTrace(false),
		)
		defer ht.Finish()
	}

	httpResp, err := c.Client.Do(httpReq)
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so are
		// recoverable.
		return RecoverableError{err}
	}
	defer func() {
		io.Copy(io.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 512))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return RecoverableError{err}
	}
	return err
}

func buildWriteRequest(samples []prompb.TimeSeries) ([]byte, error) {
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}
	jb, err := jsoniter.MarshalIndent(req, "", "  ")
	log.Println(string(jb), err)
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	compressed := snappy.Encode(nil, data)
	return compressed, nil
}

type sample struct {
	labels labels.Labels
	t      int64
	v      float64
}

const (
	LABEL_NAME = "__name__"
)

func convertOne(item *MetricPoint) (prompb.TimeSeries, error) {
	pt := prompb.TimeSeries{}
	pt.Samples = []prompb.Sample{{}}
	pt.Exemplars = []prompb.Exemplar{{}}
	s := sample{}
	s.t = item.Time
	s.v = item.Value
	// name
	if !MetricNameRE.MatchString(item.Metric) {
		return pt, errors.New("invalid metrics name")
	}
	nameLs := labels.Label{
		Name:  LABEL_NAME,
		Value: item.Metric,
	}
	s.labels = append(s.labels, nameLs)
	for k, v := range item.TagsMap {
		if model.LabelNameRE.MatchString(k) {
			ls := labels.Label{
				Name:  k,
				Value: v,
			}
			s.labels = append(s.labels, ls)
		}
	}

	pt.Labels = labelsToLabelsProto(s.labels, pt.Labels)
	// 时间赋值问题,使用毫秒时间戳
	tsMs := time.Unix(s.t, 0).UnixNano() / 1e6
	pt.Samples[0].Timestamp = tsMs
	pt.Samples[0].Value = s.v

	pt.Exemplars[0].Value = s.v
	pt.Exemplars[0].Timestamp = tsMs
	pt.Exemplars[0].Labels = pt.Labels

	return pt, nil
}

func labelsToLabelsProto(labels labels.Labels, buf []prompb.Label) []prompb.Label {
	result := buf[:0]
	if cap(buf) < len(labels) {
		result = make([]prompb.Label, 0, len(labels))
	}
	for _, l := range labels {
		result = append(result, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return result
}

func (c *HttpClient) RemoteWrite(items []MetricPoint) (err error) {
	if len(items) == 0 {
		return
	}
	ts := make([]prompb.TimeSeries, len(items))
	for i := range items {
		ts[i], err = convertOne(&items[i])
		if err != nil {
			return
		}
	}
	data, err := buildWriteRequest(ts)
	if err != nil {
		return
	}
	err = c.remoteWritePost(data)
	return
}

func NewClient(ur string, timeout time.Duration) (c *HttpClient, err error) {
	u, err := url.Parse(ur)
	if err != nil {
		return
	}
	c = &HttpClient{
		url:     u,
		Client:  &http.Client{},
		timeout: timeout,
	}
	return
}

func main() {
	c, err := NewClient("http://10.211.55.19:9090/api/v1/write", 3*time.Second)
	if err != nil {
		log.Fatalln(err)
	}
	metrics := []MetricPoint{
		{Metric: "opcai1",
			TagsMap: map[string]string{"env": "testing", "op": "opcai"},
			Time:    time.Now().Add(-1 * time.Minute).Unix(),
			Value:   1},
		{Metric: "opcai2",
			TagsMap: map[string]string{"env": "testing", "op": "opcai"},
			Time:    time.Now().Add(-2 * time.Minute).Unix(),
			Value:   2},
		{Metric: "opcai3",
			TagsMap: map[string]string{"env": "testing", "op": "opcai"},
			Time:    time.Now().Unix(),
			Value:   3},
		{Metric: "opcai4",
			TagsMap: map[string]string{"env": "testing", "op": "opcai"},
			Time:    time.Now().Add(time.Second).Unix(),
			Value:   8},
	}
	err = c.RemoteWrite(metrics)
	if err != nil {
		log.Fatalln(err)
	}
}
