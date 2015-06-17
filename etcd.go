// 
// Go Config
// Copyright (c) 2015, 2015 Brian W. Wolter, All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//   * Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
// 
//   * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     
//   * Neither the names of Brian W. Wolter nor the names of the contributors may
//     be used to endorse or promote products derived from this software without
//     specific prior written permission.
//     
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
// OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// OF THE POSSIBILITY OF SUCH DAMAGE.
// 

package conf

import (
  "fmt"
  "log"
  "time"
  "strings"
  "net/url"
  "net/http"
  "io/ioutil"
  "encoding/json"
)

const CONTENT_TYPE_FORM_ENCODED = "application/x-www-form-urlencoded"

var InvalidIndexError     = fmt.Errorf("Invalid index")
var ComparisonFailedError = fmt.Errorf("Comparison failed")

var httpClient = &http.Client{Transport:http.DefaultTransport}

/**
 * An etcd node
 */
type etcdNode struct {
  Created     int64             `json:"createdIndex"`
  Modified    int64             `json:"modifiedIndex"`
  Key         string            `json:"key"`
  Encoded     string            `json:"value"`
  Directory   bool              `json:"dir"`
  Subnodes    []*etcdNode       `json:"nodes"`
}

/**
 * Obtain the decoded value
 */
func (n *etcdNode) Value() (interface{}, error) {
  return n.Encoded, nil
}

/**
 * An etcd response
 */
type etcdResponse struct {
  Action      string            `json:"action"`
  Node        *etcdNode         `json:"node"`
  Previous    *etcdNode         `json:"prevNode"`
}

/**
 * An etcd error
 */
type etcdError struct {
  Code        int               `json:"errorCode"`
  Message     string            `json:"message"`
}

/**
 * Error
 */
func (e etcdError) Error() string {
  return e.Message
}

/**
 * Key observer
 */
type etcdObserver func(string, interface{})

/**
 * An etcd backed configuration
 */
type EtcdConfig struct {
  endpoint    *url.URL
  cache       *etcdCache
  timeout     time.Duration
}

/**
 * Create an etcd-backed configuration
 */
func NewEtcdConfig(endpoint string, timeout time.Duration) (*EtcdConfig, error) {
  
  u, err := url.Parse(endpoint)
  if err != nil {
    return nil, err
  }
  
  etcd := &EtcdConfig{}
  etcd.endpoint = u
  etcd.cache = newEtcdCache(etcd)
  etcd.timeout = timeout
  
  return etcd, nil
}

/**
 * Obtain a configuration node
 */
func (e *EtcdConfig) get(key string, wait, recurse bool, prev *etcdResponse, timeout time.Duration) (*etcdResponse, error) {
  var u string
  
  path := keyToEtcdPath(key)
  if !wait {
    u = fmt.Sprintf("/v2/keys/%s", path)
  }else if prev != nil {
    u = fmt.Sprintf("/v2/keys/%s?wait=true&waitIndex=%d&recursive=%v", path, prev.Node.Modified + 1, recurse)
  }else{
    u = fmt.Sprintf("/v2/keys/%s?wait=true&recursive=%v", path, recurse)
  }
  
  rel, err := url.Parse(u)
  if err != nil {
    return nil, err
  }
  
  abs := e.endpoint.ResolveReference(rel)
  log.Printf("[%s] GET %s", key, abs.String())
  
  var t time.Duration
  if wait {
    t = time.Minute
  }else if timeout > 0 {
    t = timeout
  }else{
    t = e.timeout
  }
  
  req, err := http.NewRequest("GET", abs.String(), nil)
  if err != nil {
    return nil, err
  }
  
  rsp, err := performRequest(req, t)
  if rsp != nil {
    defer rsp.Body.Close()
  }
  if err != nil {
    return nil, err
  }
  
  return handleResponse(rsp)
}

/**
 * Obtain a configuration value and it's modification index, which can be used in atomic
 * operations. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) GetWithIndex(key string) (interface{}, int64, error) {
  var res interface{}
  
  // always fetch, don't use the cache on get anymore
  rsp, err := e.get(key, false, false, nil, 0)
  if err != nil {
    return nil, -1, err
  }else if rsp.Node == nil {
    return nil, -1, NoSuchKeyError
  }else{
    e.cache.Set(key, rsp)
  }
  
  // obtain our result value
  if rsp.Node.Directory && rsp.Node.Subnodes != nil {
    values := make([]interface{}, len(rsp.Node.Subnodes))
    for i, n := range rsp.Node.Subnodes {
      values[i], err = n.Value()
      if err != nil {
        return nil, -1, err
      }
    }
    res = values
  }else{
    value, err := rsp.Node.Value()
    if err != nil {
      return nil, -1, err
    }
    res = value
  }
  
  return res, rsp.Node.Modified, nil
}

/**
 * Obtain a configuration value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Get(key string) (interface{}, error) {
  v, _, err := e.GetWithIndex(key)
  return v, err
}

/**
 * Watch a configuration value for changes asynchronously.
 */
func (e *EtcdConfig) Watch(key string, observer etcdObserver) {
  e.cache.AddObserver(key, observer)
}

/**
 * Set a configuration value
 */
func (e *EtcdConfig) set(key, method string, dir bool, value, prevValue interface{}, prevIndex int64, timeout time.Duration) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", keyToEtcdPath(key)))
  if err != nil {
    return nil, err
  }
  
  // add the value
  vals := url.Values{}
  if dir {
    vals.Set("dir", "true")
  }else{
    vals.Set("value", encodeValue(value))
  }
  
  // if a previous node is provided, an atomic compare-and-swap update is performed
  if prevIndex > 0 {
    vals.Set("prevIndex", encodeValue(prevIndex))
  }else if prevIndex < 0 {
    vals.Set("prevExist", "false")
  }else if prevValue != nil {
    vals.Set("prevValue", encodeValue(prevValue))
  }
  
  abs := e.endpoint.ResolveReference(rel)
  req, err := http.NewRequest(method, abs.String(), strings.NewReader(vals.Encode()))
  if err != nil {
    return nil, err
  }
  
  req.Header.Add("Content-Type", CONTENT_TYPE_FORM_ENCODED)
  
  log.Printf("[%s] PUT %s", key, abs.String())
  log.Printf("[%s]   > %s", key, vals.Encode())
  
  var t time.Duration
  if timeout > 0 {
    t = timeout
  }else{
    t = e.timeout
  }
  
  rsp, err := performRequest(req, t)
  if rsp != nil {
    defer rsp.Body.Close()
  }
  if err != nil {
    return nil, err
  }
  
  return handleResponse(rsp)
}

/**
 * Set a configuration value. The canonical updated value and it's modification index,
 * which can be used in atomic operations, are returned. This method will block until
 * it either succeeds or fails.
 */
func (e *EtcdConfig) SetWithIndex(key string, value interface{}) (interface{}, int64, error) {
  
  rsp, err := e.set(key, "PUT", false, value, nil, -1, 0)
  if err != nil {
    return nil, -1, err
  }else if rsp.Node == nil {
    return nil, -1, NoSuchKeyError
  }
  
  e.cache.Set(key, rsp)
  
  res, err := rsp.Node.Value()
  if err != nil {
    return nil, -1, err
  }
  
  return res, rsp.Node.Modified, nil
}

/**
 * Set a configuration value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Set(key string, value interface{}) (interface{}, error) {
  v, _, err := e.SetWithIndex(key, value)
  return v, err
}

/**
 * Set a configuration value via an atomic compare-and-swap operation. This method will
 * block until it either succeeds or fails.
 * 
 * The prev value is the raft index of the previous state of the key. If this value is positive
 * the service ensures that the previous state is current and, if so, performs the update. If
 * the value is negative the service ensures that there is no previous state (i.e., the key
 * has not yet been created). A value of zero is an error.
 */
func (e *EtcdConfig) CompareAndSwap(key string, value interface{}, prev int64) (interface{}, int64, error) {
  if prev == 0 {
    return nil, -1, InvalidIndexError
  }
  
  rsp, err := e.set(key, "PUT", false, value, nil, prev, 0)
  if err != nil {
    return nil, -1, err
  }else if rsp.Node == nil {
    return nil, -1, NoSuchKeyError
  }
  
  e.cache.Set(key, rsp)
  
  res, err := rsp.Node.Value()
  if err != nil {
    return nil, -1, err
  }
  
  return res, rsp.Node.Modified, nil
}

/**
 * Delete a configuration node
 */
func (e *EtcdConfig) delete(key string) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", keyToEtcdPath(key)))
  if err != nil {
    return nil, err
  }
  
  abs := e.endpoint.ResolveReference(rel)
  req, err := http.NewRequest("DELETE", abs.String(), nil)
  if err != nil {
    return nil, err
  }
  
  log.Printf("[%s] DELETE %s", key, abs.String())
  rsp, err := httpClient.Do(req)
  if rsp != nil {
    defer rsp.Body.Close() // always close Body
  }
  if err != nil {
    return nil, err
  }
  
  return handleResponse(rsp)
}

/**
 * Delete a configuration key/value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Delete(key string) error {
  
  rsp, err := e.delete(key)
  if err != nil {
    return err
  }
  
  e.cache.Set(key, rsp)
  return nil
}

/**
 * Translate a key to a path. Keys are specified as "a.b.c" and paths are specified as "a/b/c"
 */
func keyToEtcdPath(key string) string {
  var path string
  
  // do it the easy way for now
  parts := strings.Split(key, ".")
  
  for i, p := range parts {
    if i > 0 { path += "/" }
    path += url.QueryEscape(p)
  }
  
  return path
}

/**
 * Perform a request
 */
func performRequest(req *http.Request, timeout time.Duration) (*http.Response, error) {
  var rsp *http.Response
  var err error
  
  crsp := make(chan *http.Response)
  cerr := make(chan error)
  
  go func(){
    rsp, err := httpClient.Do(req)
    if err != nil {
      if rsp != nil { rsp.Body.Close() }
      cerr <- err
    }else{
      crsp <- rsp
    }
  }()
  
  select {
    case rsp = <- crsp:
      return rsp, nil
    case err = <- cerr:
      return nil, err
    case <- time.After(timeout):
      httpClient.Transport.(*http.Transport).CancelRequest(req)
      return nil, TimeoutError
  }
}

/**
 * Read a response
 */
func handleResponse(rsp *http.Response) (*etcdResponse, error) {
  
  data, err := ioutil.ReadAll(rsp.Body)
  if err != nil {
    return nil, err
  }
  
  switch rsp.StatusCode {
    
    case http.StatusOK, http.StatusCreated:
      etcrsp := &etcdResponse{}
      err = json.Unmarshal(data, etcrsp)
      if err != nil {
        return nil, err
      }else{
        return etcrsp, nil
      }
      
    case http.StatusNotFound:
      return nil, NoSuchKeyError
      
    default:
      etcerr := &etcdError{}
      err = json.Unmarshal(data, etcerr)
      if err != nil {
        return nil, err
      }else{
        return nil, normalizeError(etcerr)
      }
      
  }
  
}

/**
 * Attempt to normalize an error
 */
func normalizeError(err *etcdError) error {
  if err.Code == 101 {
    return ComparisonFailedError
  }else{
    return err
  }
}

/**
 * Encode a value
 */
func encodeValue(value interface{}) string {
  switch v := value.(type) {
    case string:
      return v
    default:
      return fmt.Sprintf("%v", v)
  }
}
