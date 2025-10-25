use std::io::{Read, Write};
use std::net::TcpStream;
use memchr::memchr;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Instant, Duration};

pub struct RedisValue {
  pub value: String,
  pub expire_time: Option<Instant>,
}
pub struct Redis {
  pub data: Mutex<HashMap<String, RedisValue>>,
  pub list: Mutex<HashMap<String, Vec<String>>>,
}

impl Redis {
  pub fn handle_request(&self, stream: &mut TcpStream) {
    loop {
      let mut buffer = [0; 1024];
      stream.read(&mut buffer).unwrap();
      let resp = RedisRespResult::from_bytes(&buffer[..], 0);
      match resp {
        RedisRespResult(Some(RedisResp::Arrays(items)), _) => {
          match &items[0] {
            RedisResp::BulkString(split) => {
              let string = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
              match string.as_str() {
                "ECHO" => {
                  match &items[1] {
                    RedisResp::BulkString(split) => {
                      let string = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                      stream.write(format!("${}\r\n{}\r\n", split.1 - split.0, string).as_bytes()).unwrap();
                    }
                    _ => panic!("Expected BulkString"),
                  }
                }
                "PING" => {
                  stream.write(b"+PONG\r\n").unwrap();
                }
                "SET" => {
                  match &items[1] {
                    RedisResp::BulkString(split) => {
                      let key = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                      match &items[2] {
                        RedisResp::BulkString(split) => {
                          let value = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                          if items.len() > 3 {
                            match &items[3] {
                              RedisResp::BulkString(split) => {
                                let expire_type = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                                match expire_type.as_str() {
                                  "PX" => {
                                    match &items[4] {
                                      RedisResp::BulkString(split) => {
                                        let duration = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                                        let expire_time = duration.parse::<u64>().unwrap();
                                        let mut data = self.data.lock().unwrap();
                                        data.insert(key, RedisValue{value: value, expire_time: Some(Instant::now() + Duration::from_millis(expire_time))});
                                        stream.write(format!("+OK\r\n").as_bytes()).unwrap();
                                      }
                                      _ => panic!("Expected PX value"),
                                    }
                                  }
                                  "EX" => {
                                    match &items[4] {
                                      RedisResp::BulkString(split) => {
                                        let duration = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                                        let expire_time = duration.parse::<u64>().unwrap();
                                        let mut data = self.data.lock().unwrap();
                                        data.insert(key, RedisValue{value: value, expire_time: Some(Instant::now() + Duration::from_secs(expire_time))});
                                        stream.write(format!("+OK\r\n").as_bytes()).unwrap();
                                      }
                                      _ => panic!("Expected EX value"),
                                    }
                                  }
                                  _ => {
                                    stream.write(b"-ERR Unknown command\r\n").unwrap();
                                  }
                                }
                              }
                              _ => panic!("Expected BulkString"),
                            }
                          } else {
                            println!("SET {} {}", key, value);
                            let mut data = self.data.lock().unwrap();
                            data.insert(key, RedisValue{value: value, expire_time: None});
                            stream.write(format!("+OK\r\n").as_bytes()).unwrap();
                          }
                        }
                        _ => panic!("Expected BulkString"),
                      }
                    }
                    _ => panic!("Expected BulkString"),
                  }
                }
                "GET" => {
                  match &items[1] {
                    RedisResp::BulkString(split) => {
                      let key = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                      println!("GET {}", key);
                      let mut data = self.data.lock().unwrap();
                      if let Some(value) = data.get(&key) {
                        if let Some(expire_time) = value.expire_time {
                          if Instant::now() > expire_time {
                            data.remove(&key);
                            stream.write(format!("$-1\r\n").as_bytes()).unwrap();
                          } else {
                            stream.write(format!("${}\r\n{}\r\n", value.value.len(), value.value).as_bytes()).unwrap();
                          }
                        } else {
                          // No expiration time, just return the value
                          stream.write(format!("${}\r\n{}\r\n", value.value.len(), value.value).as_bytes()).unwrap();
                        }
                      } else {
                        stream.write(format!("$-1\r\n").as_bytes()).unwrap();
                      }
                    }
                    _ => panic!("Expected BulkString"),
                  }
                }
                "RPUSH" => {
                  if items.len() < 3 {
                    stream.write(b"-Wrong number of arguments for 'RPUSH' command\r\n").unwrap();
                    continue;
                  }
                  match &items[1] {
                    RedisResp::BulkString(split) => {
                      let key = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                      match &items[2] {
                        RedisResp::BulkString(_) => {
                          let mut list = self.list.lock().unwrap();
                          if let Some(existing_list) = list.get_mut(&key) {
                            for item in items[2..].iter() {
                              match item {
                                RedisResp::BulkString(split) => {
                                  let value = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                                  existing_list.push(value);
                                }
                                _ => panic!("Expected BulkString"),
                              }
                            }
                            stream.write(format!(":{}\r\n", existing_list.len()).as_bytes()).unwrap();
                          } else {
                            let mut new_list = vec![];
                            for item in items[2..].iter() {
                              match item {
                                RedisResp::BulkString(split) => {
                                  let value = String::from_utf8(buffer[split.0..split.1].to_vec()).unwrap();
                                  new_list.push(value);
                                }
                                _ => panic!("Expected BulkString"),
                              }
                            }
                            let len = new_list.len();
                            list.insert(key, new_list);
                            stream.write(format!(":{}\r\n", len).as_bytes()).unwrap();
                          }
                        }
                        _ => panic!("Expected BulkString"),
                      }
                    }
                    _ => panic!("Expected BulkString"),
                  }
                }
                _ => {
                  stream.write(b"-ERR Unknown command\r\n").unwrap();
                }
              }
            }
            _ => panic!("Expected BulkString"),
          }
        }
        _ => {
          stream.write(b"-ERR Unknown command\r\n").unwrap();
        }
      }
    }
  }
}


// start index, end index
#[derive(Debug)]
struct RespSplit(usize, usize);

// parsed result, next start index
#[derive(Debug)]
struct RedisRespResult(Option<RedisResp>, usize);

#[derive(Debug)]
pub enum RedisResp {
    SimpleString(RespSplit),
    Integer(RespSplit),
    Error(RespSplit),
    BulkString(RespSplit),
    Arrays(Vec<RedisResp>),
    Unknown(String),
}

impl RedisRespResult {
    fn from_bytes(value: &[u8], start: usize) -> Self {
      match value[start] {
        // SimpleString: +<string>\r\n
        b'+' => {
          let end = start + memchr(b'\r', &value[start..]).unwrap();
          RedisRespResult(Some(RedisResp::SimpleString(RespSplit(start+1, end))), end+2)
        }
        // Integer: :[<+|->]<value>\r\n
        b':' => {
          let start = 2;
          let end = value.iter().position(|&b| b == b'\r').unwrap();
          RedisRespResult(Some(RedisResp::Integer(RespSplit(start, end))), end+2)
        }
        // BulkString: $<length>\r\n<string>\r\n
        b'$' => {
          // first \r\n
          let first_cr = memchr(b'\r', &value[start..]).unwrap();
          let len = str::from_utf8(&value[start+1..start+first_cr]).unwrap().parse::<usize>().unwrap();
          let end = start + first_cr + 2 + len;
          RedisRespResult(Some(RedisResp::BulkString(RespSplit(start + first_cr + 2, end))), end+2)
        }
        // Arrays: *<length>\r\n<array>\r\n
        b'*' => {
          let first_cr = memchr(b'\r', &value[start..]).unwrap();
          let length = str::from_utf8(&value[start + 1..first_cr]).unwrap().parse::<usize>().unwrap();
          let mut items = Vec::new();
          let mut next = start + first_cr + 2;
          for _ in 0..length {
            let item = RedisRespResult::from_bytes(value, next);
            match item {
              RedisRespResult(Some(resp), new_next) => {
                items.push(resp);
                next = new_next;
              }
              _ => panic!("Expected Some(RedisResp)"),
            }
          }
          RedisRespResult(Some(RedisResp::Arrays(items)), next)
        }
        _ => {
          RedisRespResult(None, 0)
        }
      }
    }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_redis_resp() {
    let resp: RedisRespResult = RedisRespResult::from_bytes(&b"+PONG\r\n"[..], 0);
    match resp {
      RedisRespResult(Some(RedisResp::SimpleString(split)), _) => {
        assert_eq!(String::from_utf8(b"+PONG\r\n"[split.0..split.1].to_vec()).unwrap(), "PONG");
      }
      RedisRespResult(None, _) => panic!("Expected Some(RedisRespResult)"),
      _ => panic!("Expected SimpleString"),
    }
  }

  #[test]
  fn test_redis_resp_integer() {
    let bytes = b":+123\r\n";
    let resp: RedisRespResult = RedisRespResult::from_bytes(&bytes[..], 0);
    match resp {
      RedisRespResult(Some(RedisResp::Integer(split)), _) => {
        assert_eq!(str::from_utf8(&bytes[split.0..split.1]).unwrap().parse::<usize>().unwrap(), 123);
      }
      RedisRespResult(None, _) => panic!("Expected Some(RedisRespResult)"),
      _ => panic!("Expected Integer"),
    }
  }

  #[test]
  fn test_redis_resp_bulk_string() {
    let bytes = b"$4\r\nping\r\n";
    let resp: RedisRespResult = RedisRespResult::from_bytes(&bytes[..], 0);
    match resp {
      RedisRespResult(Some(RedisResp::BulkString(split)), _) => {
        assert_eq!(String::from_utf8(bytes[split.0..split.1].to_vec()).unwrap(), "ping");
      }
      RedisRespResult(None, _) => panic!("Expected Some(RedisRespResult)"),
      _ => panic!("Expected BulkString"),
    }
  }

  #[test]
  fn test_redis_resp_arrays() {
    let bytes = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    let resp: RedisRespResult = RedisRespResult::from_bytes(&bytes[..], 0);
    match resp {
      RedisRespResult(Some(RedisResp::Arrays(items)), _) => {
        assert_eq!(items.len(), 2);
        match &items[0] {
          RedisResp::BulkString(split) => {
            assert_eq!(String::from_utf8(bytes[split.0..split.1].to_vec()).unwrap(), "ECHO")
          }
          _ => panic!("Expected BulkString"),
        }
        match &items[1] {
          RedisResp::BulkString(split) => {
            assert_eq!(String::from_utf8(bytes[split.0..split.1].to_vec()).unwrap(), "hey")
          }
          _ => panic!("Expected BulkString"),
        }
      }
      RedisRespResult(None, _) => panic!("Expected Some(RedisRespResult)"),
      _ => panic!("Expected Arrays"),
    }
  }
}
