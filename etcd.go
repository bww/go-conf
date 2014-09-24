// 
// Go Config
// Copyright (c) 2014 Brian W. Wolter, All rights reserved.
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
  Value       string            `json:"value"`
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
  if !e.watching {
    e.watching = true
    go e.watch(c)
  }
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
  if !e.watching {
    e.watching = true
    go e.watch(c)
  }
}

/**
 * Watch a property
 */
func (e *etcdCacheEntry) watch(c *EtcdConfig) {
  for {
    var err error
    errcount := 0
    
    e.RLock()
    key := e.key
    rsp := e.response
    e.RUnlock()
    
    rsp, err = c.get(key, true, rsp)
    if err != nil {
      log.Printf("[%s] could not watch: %v", key, err)
      errcount++
      continue
    }
    
    log.Printf("[%s] GOT: %+v", key, rsp.Node)
    
    e.Lock()
    
    log.Printf("[%s] updated: %v", key, rsp.Node.Value)
    e.response = rsp
    
    var observers []etcdObserver
    if c := len(e.observers); c > 0 {
      observers = make([]etcdObserver, c)
      copy(observers, e.observers)
    }
    
    e.Unlock()
    
    if observers != nil {
      for _, o := range observers {
        o(key, rsp.Node.Value)
      }
    }
    
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
 * Set a response from the cache
 */
func (c *etcdCache) Set(key string, rsp *etcdResponse) {
  c.Lock()
  defer c.Unlock()
  c.props[key] = newEtcdCacheEntry(key, rsp)
}

/**
 * Set and start watching a key
 */
func (c *etcdCache) SetAndWatch(key string, rsp *etcdResponse) {
  c.Lock()
  defer c.Unlock()
  e, ok := c.props[key]
  if ok {
    e.SetResponse(rsp)
  }else{
    e = newEtcdCacheEntry(key, rsp)
    c.props[key] = e
  }
  e.Watch(c.config)
  
}

/**
 * Add an observer and begin watching if necessary
 */
func (c *etcdCache) AddObserver(key string, observer etcdObserver) {
  c.Lock()
  defer c.Unlock()
  e, ok := c.props[key]
  if !ok {
    e := newEtcdCacheEntry(key, nil)
    c.props[key] = e
  }
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
func (e *EtcdConfig) get(key string, wait bool, prev *etcdResponse) (*etcdResponse, error) {
  var u string
  
  path := keyToEtcdPath(key)
  if !wait {
    u = fmt.Sprintf("/v2/keys/%s", path)
  }else if prev != nil {
    u = fmt.Sprintf("/v2/keys/%s?wait=true&waitIndex=%d", path, prev.Node.Modified + 1)
  }else{
    u = fmt.Sprintf("/v2/keys/%s?wait=true", path)
  }
  
  rel, err := url.Parse(u)
  if err != nil {
    return nil, err
  }
  
  abs := e.endpoint.ResolveReference(rel)
  log.Printf("[%s] GET %s", key, abs.String())
  rsp, err := httpClient.Get(abs.String())
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
  
  defer rsp.Body.Close()
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
  
  rsp, ok := e.cache.Get(key)
  if !ok || rsp == nil {
    var err error
    
    rsp, err = e.get(key, false, nil)
    if err != nil {
      return nil, err
    }else if rsp.Node == nil {
      return nil, NoSuchKeyError
    }
    
    e.cache.SetAndWatch(key, rsp)
    
  }
  
  return rsp.Node.Value, nil
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
func (e *EtcdConfig) set(key string, value interface{}) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", keyToEtcdPath(key)))
  if err != nil {
    return nil, err
  }
  
  vals := url.Values{}
  switch v := value.(type) {
    case string:
      vals.Set("value", v)
    default:
      vals.Set("value", fmt.Sprintf("%v", v))
  }
  
  abs := e.endpoint.ResolveReference(rel)
  req, err := http.NewRequest("PUT", abs.String(), strings.NewReader(vals.Encode()))
  if err != nil {
    return nil, err
  }
  
  req.Header.Add("Content-Type", CONTENT_TYPE_FORM_ENCODED)
  
  log.Printf("[%s] PUT %s", key, abs.String())
  rsp, err := httpClient.Do(req)
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
  
  defer rsp.Body.Close()
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
  
  rsp, err := e.set(key, value)
  if err != nil {
    return nil, err
  }else if rsp.Node == nil {
    return nil, NoSuchKeyError
  }
  
  e.cache.Set(key, rsp)
  return rsp.Node.Value, nil
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
  
  defer rsp.Body.Close()
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



