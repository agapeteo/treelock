use lazy_static::lazy_static;
use std::{path::Path, usize};

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone)]
pub struct SearchKey<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T>>(Vec<T>);

impl<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T>> SearchKey<T> {
    pub fn from_vec(val: Vec<T>) -> Self {
        SearchKey(val)
    }
    pub fn from_val(val: T) -> Self {
        SearchKey(vec![val])
    }
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

impl SearchKey<String> {
    pub fn as_str_key(&self) -> SearchKey<&str> {
        SearchKey(self.0.iter().map(|s| s.as_ref()).collect())
    }
}

pub trait HasMax<T> {
    fn max_value() -> T;
}
impl HasMax<char> for char {
    fn max_value() -> char {
        char::MAX
    }
}

impl HasMax<String> for String {
    fn max_value() -> String {
        char::MAX.to_string()
    }
}

lazy_static! {
    static ref MAX_STRING: String = char::MAX.to_string();
}

impl HasMax<&str> for &str {
    fn max_value() -> &'static str {
        MAX_STRING.as_str()
    }
}

impl<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T>> SearchKey<T> {
    pub fn first(&self) -> Option<&T> {
        self.0.first()
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.0.get(idx)
    }

    fn prefix_keys(&self, inclusive: bool) -> Vec<SearchKey<T>> {
        let inner = &self.0;
        let mut result = Vec::new();

        let offset = if inclusive { 1 } else { 0 };

        for i in 1..inner.len() + offset {
            result.push(SearchKey(Vec::from(&inner[0..i])));
        }
        result
    }
    pub fn prefix_keys_inclusive(&self) -> Vec<SearchKey<T>> {
        self.prefix_keys(true)
    }

    pub fn prefix_keys_exclusive(&self) -> Vec<SearchKey<T>> {
        self.prefix_keys(false)
    }

    pub fn append_key(&mut self, key: T) {
        self.0.push(key);
    }

    pub fn append_max(&mut self) {
        self.0.push(T::max_value());
    }

    pub fn common_prefix_with(&self, other: &SearchKey<T>) -> usize {
        let max_idx = usize::min(self.0.len(), other.0.len());
        for i in 0..max_idx {
            if self.0[i] != other.0[i] {
                return i;
            }
        }
        max_idx
    }
}

impl TryFrom<&Path> for SearchKey<String> {
    type Error = ();

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let mut vec = Vec::new();
        for p in value {
            if let Some(str) = p.to_str() {
                // vec.push(Key::String(str.to_string()));
                vec.push(str.to_string());
            } else {
                return Err(());
            }
        }
        Ok(Self(vec))
    }
}

impl<'p> TryFrom<&'p Path> for SearchKey<&'p str> {
    type Error = ();

    fn try_from(value: &'p Path) -> Result<Self, Self::Error> {
        let mut vec: Vec<&'p str> = Vec::new();
        for p in value {
            if let Some(str) = p.to_str() {
                vec.push(str);
            } else {
                return Err(());
            }
        }
        Ok(Self(vec))
    }
}

#[cfg(test)]
mod tests {
    use crate::key::SearchKey;
    use std::path::Path;

    #[test]
    fn test_from_path() {
        let path = Path::new("/home/user/music");
        let expected = SearchKey(vec!["/", "home", "user", "music"]);

        let search_key: SearchKey<&str> = path.try_into().unwrap();

        assert_eq!(expected, search_key);

        let path = Path::new("/home/user/music/");
        let search_key: SearchKey<&str> = path.try_into().unwrap();
        assert_eq!(expected, search_key);

        let path = Path::new("/");
        let expected = SearchKey(vec!["/".to_string()]);
        let search_key: SearchKey<String> = path.try_into().unwrap();
        assert_eq!(expected, search_key);
    }

    #[test]
    fn test_max() {
        let mut sk_start: SearchKey<String> = Path::new("/home/user/music").try_into().unwrap();
        sk_start.append_key("mozart".to_string());
        sk_start.append_max();
        // println!("sk: {:?}", sk_start);
    }

    #[test]
    fn test_common_prefix() {
        let key_1: SearchKey<&str> = Path::new("/home").try_into().unwrap();
        let key_2: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        assert_eq!(key_1.common_prefix_with(&key_2), 2);

        let key_1: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        let key_2: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        assert_eq!(key_1.common_prefix_with(&key_2), 3);

        let key_1: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        let key_2: SearchKey<&str> = Path::new("/etc").try_into().unwrap();
        assert_eq!(key_1.common_prefix_with(&key_2), 1);
    }
}
