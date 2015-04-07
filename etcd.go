// 
// Go Config
// Copyright (c) 2014, 2015 Brian W. Wolter, All rights reserved.
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
  "io"
  "fmt"
  "log"
  "time"
  "sync"
  "strings"
  "net/url"
  "net/http"
  "io/ioutil"
  "encoding/json"
)

const CONTENT_TYPE_FORM_ENCODED = "application/x-www-form-urlencoded"

var httpClient = &http.Client{}

/**
 * An etcd node
 */
type etcdNode struct {
  Created     uint64            `json:"createdIndex"`
  Modified    uint64            `json:"modifiedIndex"`
  Key         string            `json:"key"`
  Encoded     string            `json:"value"`
  Directory   bool              `json:"dir"`
  Subnodes    []*etcdNode       `json:"nodes"`
  // value       interface{}
}

/**
 * Obtain the decoded value
 */
func (n *etcdNode) Value() (interface{}, error) {
  // if n.value == nil && n.Encoded != "" && n.Encoded != "null" {
  //   if err := json.Unmarshal([]byte(n.Encoded), &n.value); err != nil {
  //     return nil, err
  //   }
  // }
  // return n.value, nil
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

type etcdObserver func(string, interface{})

/**
 * Cache
 */
type etcdCacheEntry struct {
  sync.RWMutex
  key         string
  response    *etcdResponse
  watching    bool
  observers   []etcdObserver
  finalize    chan struct{}
}

/**
 * Create a cache entry
 */
func newEtcdCacheEntry(key string, rsp *etcdResponse) *etcdCacheEntry {
  return &etcdCacheEntry{key: key, response:rsp, observers: make([]etcdObserver, 0)}
}

/**
 * Obtain the response
 */
func (e *etcdCacheEntry) Response() *etcdResponse {
  e.RLock()
  defer e.RUnlock()
  return e.response
}

/**
 * Set the response
 */
func (e *etcdCacheEntry) SetResponse(rsp *etcdResponse) {
  e.Lock()
  defer e.Unlock()
  e.response = rsp
}

/**
 * Add an observer for this entry and begin watching if we aren't already
 */
func (e *etcdCacheEntry) AddObserver(c *EtcdConfig, observer etcdObserver) {
  e.Lock()
  defer e.Unlock()
  e.observers = append(e.observers, observer)
  e.startWatching(c)
}

/**
 * Remove all observers for this entry
 */
func (e *etcdCacheEntry) RemoveAllObservers() {
  e.Lock()
  defer e.Unlock()
  e.observers = make([]etcdObserver, 0)
}

/**
 * Are we watching this entry
 */
func (e *etcdCacheEntry) IsWatching() bool {
  e.RLock()
  defer e.RUnlock()
  return e.watching
}

/**
 * Start watching this entry for updates if we aren't already
 */
func (e *etcdCacheEntry) Watch(c *EtcdConfig) {
  e.Lock()
  defer e.Unlock()
  e.startWatching(c)
}

/**
 * Start watching this entry for updates if we aren't already
 */
func (e *etcdCacheEntry) startWatching(c *EtcdConfig) {
  // no locking; this must only be called by another method that handles synchronization
  if !e.watching {
    if e.finalize == nil {
      e.finalize = make(chan struct{})
    }
    e.watching = true
    go e.watch(c)
  }
}

/**
 * Watch a property
 */
func (e *etcdCacheEntry) watch(c *EtcdConfig) {
  errcount := 0
  backoff  := time.Second
  maxboff  := time.Second * 15
  for {
    var err error
    
    e.RLock()
    key := e.key
    rsp := e.response
    e.RUnlock()
    
    recurse := rsp != nil && rsp.Node != nil && rsp.Node.Directory
    rsp, err = c.get(key, true, recurse, rsp)
    if err == io.EOF || err == io.ErrUnexpectedEOF {
      errcount = 0
      continue
    }else if err != nil {
      errcount++
      delay := backoff * time.Duration(errcount * errcount)
      if delay > maxboff { delay = maxboff }
      log.Printf("[%s] Could not watch (backing off %v) %v", key, delay, err)
      <- time.After(delay)
      continue
    }
    
    errcount = 0
    e.Lock()
    e.response = rsp
    
    var observers []etcdObserver
    if c := len(e.observers); c > 0 {
      observers = make([]etcdObserver, c)
      copy(observers, e.observers)
    }
    
    e.Unlock()
    
    val, err := rsp.Node.Value()
    if err != nil {
      log.Printf("[%s] Could not decode value (nobody will be notified): %v", key, err)
      continue
    }
    
    if observers != nil {
      for _, o := range observers {
        go o(key, val)
      }
    }
    
  }
}

/**
 * Stop watching this entry for updates
 */
func (e *etcdCacheEntry) Cancel() {
  e.Lock()
  defer e.Unlock()
  if e.watching {
    e.finalize <- struct{}{}
    e.watching = false
  }
}

/**
 * Cache
 */
type etcdCache struct {
  sync.RWMutex
  config      *EtcdConfig
  props       map[string]*etcdCacheEntry
}

/**
 * Create a cache
 */
func newEtcdCache(config *EtcdConfig) *etcdCache {
  return &etcdCache{config: config, props: make(map[string]*etcdCacheEntry)}
}

/**
 * Obtain a response from the cache
 */
func (c *etcdCache) Get(key string) (*etcdResponse, bool) {
  c.RLock()
  defer c.RUnlock()
  e, ok := c.props[key]
  if ok {
    return e.Response(), true
  }else{
    return nil, false
  }
}

/**
 * Get or create a cache entry. Returns (entry, created or not); (no sync)
 */
func (c *etcdCache) getOrCreate(key string) (*etcdCacheEntry, bool) {
  e, ok := c.props[key]
  if ok {
    return e, false
  }else{
    e = newEtcdCacheEntry(key, nil)
    c.props[key] = e
    return e, true
  }
}

/**
 * Set a response from the cache
 */
func (c *etcdCache) Set(key string, rsp *etcdResponse) {
  c.Lock()
  defer c.Unlock()
  c.set(key, rsp)
}

/**
 * Set a response from the cache (no sync)
 */
func (c *etcdCache) set(key string, rsp *etcdResponse) *etcdCacheEntry {
  e, ok := c.props[key]
  if ok {
    e.SetResponse(rsp)
  }else{
    e = newEtcdCacheEntry(key, rsp)
    c.props[key] = e
  }
  return e
}

/**
 * Set and start watching a key
 */
func (c *etcdCache) SetAndWatch(key string, rsp *etcdResponse) {
  c.Lock()
  defer c.Unlock()
  e := c.set(key, rsp)
  e.Watch(c.config)
}

/**
 * Add an observer and begin watching if necessary
 */
func (c *etcdCache) AddObserver(key string, observer etcdObserver) {
  c.Lock()
  defer c.Unlock()
  e, _ := c.getOrCreate(key)
  e.AddObserver(c.config, observer)
}

/**
 * Delete a response from the cache
 */
func (c *etcdCache) Delete(key string) {
  c.Lock()
  defer c.Unlock()
  delete(c.props, key)
}

/**
 * An etcd backed configuration
 */
type EtcdConfig struct {
  endpoint    *url.URL
  cache       *etcdCache
}

/**
 * Create an etcd-backed configuration
 */
func NewEtcdConfig(endpoint string) (*EtcdConfig, error) {
  
  u, err := url.Parse(endpoint)
  if err != nil {
    return nil, err
  }
  
  etcd := &EtcdConfig{}
  etcd.endpoint = u
  etcd.cache = newEtcdCache(etcd)
  
  return etcd, nil
}

/**
 * Obtain a configuration node
 */
func (e *EtcdConfig) get(key string, wait, recurse bool, prev *etcdResponse) (*etcdResponse, error) {
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
  rsp, err := httpClient.Get(abs.String())
  if rsp != nil {
    defer rsp.Body.Close() // always close Body
  }
  if err != nil {
    return nil, err
  }
  
  switch rsp.StatusCode {
    case http.StatusOK: // ok
    case http.StatusNotFound:
      return nil, NoSuchKeyError
    case http.StatusBadRequest:
      return nil, ClientError
    default:
      return nil, ServiceError
  }
  
  data, err := ioutil.ReadAll(rsp.Body)
  if err != nil {
    return nil, err
  }
  
  etc := &etcdResponse{}
  if err := json.Unmarshal(data, etc); err != nil {
    return nil, err
  }
  
  return etc, nil
}

/**
 * Obtain a configuration value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Get(key string) (interface{}, error) {
  var err error
  
  rsp, ok := e.cache.Get(key)
  if !ok || rsp == nil {
    rsp, err = e.get(key, false, false, nil)
    if err != nil {
      return nil, err
    }else if rsp.Node == nil {
      return nil, NoSuchKeyError
    }else{
      e.cache.SetAndWatch(key, rsp)
    }
  }
  
  if rsp.Node.Directory && rsp.Node.Subnodes != nil {
    values := make([]interface{}, len(rsp.Node.Subnodes))
    for i, n := range rsp.Node.Subnodes {
      values[i], err = n.Value()
      if err != nil {
        return nil, err
      }
    }
    return values, nil
  }else{
    return rsp.Node.Value()
  }
  
}

/**
 * Watch a configuration value for changes.
 */
func (e *EtcdConfig) Watch(key string, observer etcdObserver) {
  e.cache.AddObserver(key, observer)
}

/**
 * Set a configuration value
 */
func (e *EtcdConfig) set(key, method string, dir bool, value interface{}) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", keyToEtcdPath(key)))
  if err != nil {
    return nil, err
  }
  
  vals := url.Values{}
  if dir {
    vals.Set("dir", "true")
  }else{
    switch v := value.(type) {
      case string:
        vals.Set("value", v)
      default:
        vals.Set("value", fmt.Sprintf("%v", v))
    }
  }
  
  abs := e.endpoint.ResolveReference(rel)
  req, err := http.NewRequest(method, abs.String(), strings.NewReader(vals.Encode()))
  if err != nil {
    return nil, err
  }
  
  req.Header.Add("Content-Type", CONTENT_TYPE_FORM_ENCODED)
  
  log.Printf("[%s] PUT %s", key, abs.String())
  rsp, err := httpClient.Do(req)
  if rsp != nil {
    defer rsp.Body.Close() // always close Body
  }
  if err != nil {
    return nil, err
  }
  
  switch rsp.StatusCode {
    case http.StatusOK, http.StatusCreated:
      // ok
    case http.StatusBadRequest:
      return nil, ClientError
    default:
      return nil, ServiceError
  }
  
  data, err := ioutil.ReadAll(rsp.Body)
  if err != nil {
    return nil, err
  }
  
  etc := &etcdResponse{}
  if err := json.Unmarshal(data, etc); err != nil {
    return nil, err
  }
  
  return etc, nil
}

/**
 * Set a configuration value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Set(key string, value interface{}) (interface{}, error) {
  
  rsp, err := e.set(key, "PUT", false, value)
  if err != nil {
    return nil, err
  }else if rsp.Node == nil {
    return nil, NoSuchKeyError
  }
  
  e.cache.SetAndWatch(key, rsp)
  return rsp.Node.Value()
}

/**
 * Create an empty directory
 */
func (e *EtcdConfig) Mkdir(dir string) (error) {
  
  rsp, err := e.set(dir, "PUT", true, nil)
  if err != nil {
    return err
  }else if rsp.Node == nil {
    return NoSuchKeyError
  }
  
  // not cached or watched...
  return nil
}

/**
 * Add ordered values to an existing directory.
 */
func (e *EtcdConfig) Add(dir string, value interface{}) (interface{}, error) {
  
  rsp, err := e.set(dir, "POST", false, value)
  if err != nil {
    return nil, err
  }else if rsp.Node == nil {
    return nil, NoSuchKeyError
  }
  
  return rsp.Node.Value()
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
  
  switch rsp.StatusCode {
    case http.StatusOK:
      // ok
    case http.StatusNotFound:
      return nil, NoSuchKeyError
    case http.StatusBadRequest:
      return nil, ClientError
    default:
      return nil, ServiceError
  }
  
  data, err := ioutil.ReadAll(rsp.Body)
  if err != nil {
    return nil, err
  }
  
  etc := &etcdResponse{}
  if err := json.Unmarshal(data, etc); err != nil {
    return nil, err
  }
  
  return etc, nil
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



