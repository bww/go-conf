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
  "strings"
  "net/url"
  "net/http"
)

/**
 * A configuration
 */
type Config interface {
  
  /**
   * Obtain a configuration value
   */
  Get(key string) (interface{}, error)
  
}

/**
 * An etcd backed configuration
 */
type EtcdConfig struct {
  endpoint    *url.URL
  httpClient  *http.Client
}

/**
 * Create an etcd-backed configuration
 */
func NewEtcdConfig(endpoint string) (*EtcdConfig, error) {
  
  u, err := url.Parse(endpoint)
  if err != nil {
    return err
  }
  
  hc := &http.Client{}
  
  return &EtcdConfig{u, hc}, nil
}

/**
 * Obtain a configuration value. This method will block until it either succeeds or fails.
 */
func (e *EtcdConfig) Get(key string) (interface{}, error) {
  
  rel, err := url.Parse(fmt.Sprintf("/%s", e.keyToPath(key)))
  if err != nil {
    return nil, err
  }
  
  u := e.endpoint.ResolveReference(rel)
  rsp, err := httpClient.Get(u.String())
  if err != nil {
    return nil, err
  }
  
  
  
  return nil, nil
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



