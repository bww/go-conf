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
  "sync"
  "strings"
  "net/url"
  "net/http"
  "io/ioutil"
  "encoding/json"
)

const CONTENT_TYPE_FORM_ENCODED = "application/x-www-form-urlencoded"

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

/**
 * Cache
 */
type etcdCache struct {
  sync.RWMutex
  props       map[string]*etcdResponse
}

/**
 * Create a cache
 */
func newEtcdCache() *etcdCache {
  return &etcdCache{props: make(map[string]*etcdResponse)}
}

/**
 * Obtain a response from the cache
 */
func (c *etcdCache) Get(key string) (*etcdResponse, bool) {
  c.RLock()
  defer c.RUnlock()
  r, ok := c.props[key]
  return r, ok
}

/**
 * Set a response from the cache
 */
func (c *etcdCache) Set(key string, rsp *etcdResponse) {
  c.Lock()
  defer c.Unlock()
  c.props[key] = rsp
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
  httpClient  *http.Client
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
  
  client  := &http.Client{}
  cache   := newEtcdCache()
  
  return &EtcdConfig{u, client, cache}, nil
}

/**
 * Obtain a configuration node
 */
func (e *EtcdConfig) get(key string) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", e.keyToPath(key)))
  if err != nil {
    return nil, err
  }
  
  abs := e.endpoint.ResolveReference(rel)
  rsp, err := e.httpClient.Get(abs.String())
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
  
  etc, err := e.get(key)
  if err != nil {
    return nil, err
  }else if etc.Node == nil {
    return nil, NoSuchKeyError
  }
  
  return etc.Node.Value, nil
}

/**
 * Set a configuration value
 */
func (e *EtcdConfig) set(key string, value interface{}) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", e.keyToPath(key)))
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
  
  rsp, err := e.httpClient.Do(req)
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
  
  etc, err := e.set(key, value)
  if err != nil {
    return nil, err
  }else if etc.Node == nil {
    return nil, NoSuchKeyError
  }
  
  return etc.Node.Value, nil
}

/**
 * Delete a configuration node
 */
func (e *EtcdConfig) delete(key string) (*etcdResponse, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/v2/keys/%s", e.keyToPath(key)))
  if err != nil {
    return nil, err
  }
  
  abs := e.endpoint.ResolveReference(rel)
  req, err := http.NewRequest("DELETE", abs.String(), nil)
  if err != nil {
    return nil, err
  }
  
  rsp, err := e.httpClient.Do(req)
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
  _, err := e.delete(key)
  if err != nil {
    return err
  }else{
    return nil
  }
}
  

/**
 * Translate a key to a path. Keys are specified as "a.b.c" and paths are specified as "a/b/c"
 */
func (e *EtcdConfig) keyToPath(key string) string {
  var path string
  
  // do it the easy way for now
  parts := strings.Split(key, ".")
  
  for i, p := range parts {
    if i > 0 { path += "/" }
    path += url.QueryEscape(p)
  }
  
  return path
}



