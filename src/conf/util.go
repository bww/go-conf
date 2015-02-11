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
  "reflect"
)

/**
 * Convert a value to a string
 */
func AsString(v interface{}) (string, error) {
  if v == nil {
    return "", nil
  }else if s, ok := v.(string); ok {
    return s, nil
  }else{
    return fmt.Sprint(v), nil
  }
}

/**
 * Convert a value to a number
 */
func AsInt(v interface{}) (int64, error) {
  z := reflect.ValueOf(v)
  switch z.Kind() {
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
      return int64(z.Int()), nil
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
      return int64(z.Uint()), nil
    case reflect.Float32, reflect.Float64:
      return int64(z.Float()), nil
    default:
      return 0, fmt.Errorf("Cannot cast (%T) %v to numeric", v, v)
  }
}

/**
 * Convert a value to a number
 */
func AsFloat(v interface{}) (float64, error) {
  z := reflect.ValueOf(v)
  switch z.Kind() {
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
      return float64(z.Int()), nil
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
      return float64(z.Uint()), nil
    case reflect.Float32, reflect.Float64:
      return z.Float(), nil
    default:
      return 0, fmt.Errorf("Cannot cast (%T) %v to numeric", v, v)
  }
}

/**
 * Convert a value to a bool
 */
func AsBool(v interface{}) (bool, error) {
  z := reflect.ValueOf(v)
  switch z.Kind() {
    case reflect.Bool:
      return z.Bool(), nil
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
      return z.Int() != 0, nil
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
      return z.Uint() != 0, nil
    case reflect.Float32, reflect.Float64:
      return z.Float() != 0, nil
    default:
      return false, fmt.Errorf("Cannot cast (%T) %v to bool", v, v)
  }
}

