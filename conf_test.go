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
  "testing"
)

func TestConf(t *testing.T) {
  
  e, err := NewEtcdConfig("http://localhost:4001/")
  if err != nil {
    t.Errorf("Could not fetch: %v", err)
    return
  }
  
  key := "a.b.c"
  
  v, err := e.Set(key, "The value")
  if err != nil {
    t.Errorf("Could not set: %v", err)
  }else{
    t.Logf("%v -> %v", key, v)
  }
  
  v, err = e.Get(key)
  if err != nil {
    t.Errorf("Could not fetch: %v", err)
  }else{
    t.Logf("%v -> %v", key, v)
  }
  
  err = e.Delete(key)
  if err != nil {
    t.Errorf("Could not delete: %v", err)
  }else{
    t.Logf("%v -> (deleted)", key)
  }
  
  v, err = e.Get(key)
  if err != NoSuchKeyError {
    t.Errorf("Should not exist: %v: %v", key, err)
  }else{
    t.Logf("%v -> %v", key, v)
  }
  
  w := make(chan struct{})
  
  e.Watch(key, func(key string, val interface{}) {
    t.Logf("Value changed: %v: %v", key, val)
    w <- struct{}{}
  })
  
  <- w
  
}

