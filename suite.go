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

/**
 * A configuration suite. A suite represents a number of underlying configurations
 * which are organized in order of their priority.
 */
type ConfigSuite struct {
  suite   []Config
}

/**
 * Create a config suite representing the provided underlying configurations, in order
 * of their priority from highest to lowest.
 */
func NewConfigSuite(c ...Config) *ConfigSuite {
  return &ConfigSuite{c}
}

/**
 * Obtain a configuration value.
 */
func (s *ConfigSuite) Get(key string) (interface{}, error) {
  if s.suite != nil {
    for _, c := range s.suite {
      v, err := c.Get(key)
      if err == nil {
        return v, nil
      }else if err != NoSuchKeyError {
        return nil, err
      }
    }
  }
  return nil, NoSuchKeyError
}

/**
 * Set a configuration value. The canonical form of the value is returned.
 */
func (s *ConfigSuite) Set(key string, value interface{}) (interface{}, error) {
  var first interface{}
  if s.suite != nil {
    for _, c := range s.suite {
      v, err := c.Set(key, value)
      if err != nil {
        return nil, err
      }else if first == nil {
        first = v
      }
    }
  }
  return first, nil
}

/**
 * Delete a configuration key/value.
 */
func (s *ConfigSuite) Delete(key string) error {
  if s.suite != nil {
    for _, c := range s.suite {
      err := c.Delete(key)
      if err != nil {
        return err
      }
    }
  }
  return nil
}
