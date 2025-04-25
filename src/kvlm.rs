use std::{error::Error, str::from_utf8};

use ordered_hash_map::OrderedHashMap;

pub fn kvlm_parse(raw: &[u8]) -> Result<OrderedHashMap<String, Vec<Vec<u8>>>, Box<dyn Error>> {
    let mut map = OrderedHashMap::new();
    kvlm_parse_rec(raw, &mut map)?;
    Ok(map)
}

fn kvlm_parse_rec(
    raw: &[u8],
    map: &mut OrderedHashMap<String, Vec<Vec<u8>>>,
) -> Result<(), Box<dyn Error>> {
    if raw.is_empty() {
        return Ok(());
    }
    if raw[0] == b'\n' {
        map.insert(String::new(), vec![raw[1..].to_vec()]);
        return Ok(());
    }

    let spc = raw
        .iter()
        .position(|&b| b == b' ')
        .ok_or("kvlm missing space");

    let spc = spc.unwrap();
    let key = &raw[..spc];

    let mut end: usize = spc;
    loop {
        end += 1 + raw[end + 1..]
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(raw.len() - end - 1);

        if end + 1 >= raw.len() || raw[end + 1] != b' ' {
            break;
        }
    }

    let key = from_utf8(key)?.to_string();
    if let Some(v) = map.get_mut(&key) {
        v.push(kvlm_clean_value(raw[spc + 1..end].to_vec()));
    } else {
        map.insert(key, vec![kvlm_clean_value(raw[spc + 1..end].to_vec())]);
    }

    kvlm_parse_rec(&raw[end + 1..], map)
}

fn kvlm_clean_value(mut vec: Vec<u8>) -> Vec<u8> {
    let mut skip = 0;
    let mut i = 1;
    loop {
        if i + skip >= vec.len() {
            break;
        }
        if vec[i + skip - 1] == b'\n' && vec[i + skip] == b' ' {
            skip += 1;
        }
        if i + skip >= vec.len() {
            break;
        }
        vec[i] = vec[i + skip];
        i += 1;
    }
    vec.truncate(i);
    vec
}

pub fn kvlm_serialize(map: &OrderedHashMap<String, Vec<Vec<u8>>>) -> Vec<u8> {
    let mut rest = None;
    let mut v = map
        .iter()
        .filter_map(|(k, v)| {
            if k.is_empty() {
                rest = Some(v);
                None
            } else {
                let start = k.bytes();
                let end = v
                    .iter()
                    .flat_map(|v| {
                        v.split(|&b| b == b'\n')
                            .flat_map(|v| b" ".iter().chain(v.iter().chain(b"\n")))
                    })
                    .copied();
                let ret = start.chain(end);
                Some(ret)
            }
        })
        .flatten()
        .collect::<Vec<_>>();

    if let Some(rest) = rest {
        v.push(b'\n');
        for b in rest.iter() {
            for &b in b.iter() {
                v.push(b);
            }
        }
    }

    v
}

#[cfg(test)]
mod tests {
    use log::debug;
    use ordered_hash_map::OrderedHashMap;
    use std::{collections::HashMap, ops::Deref, str::from_utf8};

    use super::{kvlm_parse, kvlm_serialize};

    static KVLM: &[u8] = br#"tree 29ff16c9c14e2652b22f8b78bb08a5a07930c147
parent 206941306e8a8af65b66eaaaea388a7ae24d49a0
author Thibault Polge <thibault@thb.lt> 1527025023 +0200
committer Thibault Polge <thibault@thb.lt> 1527025044 +0200
gpgsig -----BEGIN PGP SIGNATURE-----
 
 iQIzBAABCAAdFiEExwXquOM8bWb4Q2zVGxM2FxoLkGQFAlsEjZQACgkQGxM2FxoL
 kGQdcBAAqPP+ln4nGDd2gETXjvOpOxLzIMEw4A9gU6CzWzm+oB8mEIKyaH0UFIPh
 rNUZ1j7/ZGFNeBDtT55LPdPIQw4KKlcf6kC8MPWP3qSu3xHqx12C5zyai2duFZUU
 wqOt9iCFCscFQYqKs3xsHI+ncQb+PGjVZA8+jPw7nrPIkeSXQV2aZb1E68wa2YIL
 3eYgTUKz34cB6tAq9YwHnZpyPx8UJCZGkshpJmgtZ3mCbtQaO17LoihnqPn4UOMr
 V75R/7FjSuPLS8NaZF4wfi52btXMSxO/u7GuoJkzJscP3p4qtwe6Rl9dc1XC8P7k
 NIbGZ5Yg5cEPcfmhgXFOhQZkD0yxcJqBUcoFpnp2vu5XJl2E5I/quIyVxUXi6O6c
 /obspcvace4wy8uO0bdVhc4nJ+Rla4InVSJaUaBeiHTW8kReSFYyMmDCzLjGIu1q
 doU61OM3Zv1ptsLu3gUE6GU27iWYj2RWN3e3HE4Sbd89IFwLXNdSuM0ifDLZk7AQ
 WBhRhipCCgZhkj9g2NEk7jRVslti1NdN5zoQLaJNqSwO1MtxTmJ15Ksk3QP6kfLB
 Q52UWybBzpaP9HEd4XnR+HuQ4k2K0ns2KgNImsNvIyFwbpMUyUWLMPimaV1DWUXo
 5SBjDB/V/W2JBFR+XKHFJeFwYhj7DD/ocsGr4ZMx/lgc8rjIBkI=
 =lgTX
 -----END PGP SIGNATURE-----

Create first draft"#;
    #[test]
    fn test_kvlm_parse() {
        let map = kvlm_parse(KVLM).unwrap();
        assert_bytes_eq(
            map.get("tree"),
            vec![b"29ff16c9c14e2652b22f8b78bb08a5a07930c147".to_vec()],
            "tree",
        );
        assert_bytes_eq(
            map.get("parent"),
            vec![b"206941306e8a8af65b66eaaaea388a7ae24d49a0".to_vec()],
            "parent",
        );
        assert_bytes_eq(
            map.get("gpgsig"),
            vec![
                br#"-----BEGIN PGP SIGNATURE-----

iQIzBAABCAAdFiEExwXquOM8bWb4Q2zVGxM2FxoLkGQFAlsEjZQACgkQGxM2FxoL
kGQdcBAAqPP+ln4nGDd2gETXjvOpOxLzIMEw4A9gU6CzWzm+oB8mEIKyaH0UFIPh
rNUZ1j7/ZGFNeBDtT55LPdPIQw4KKlcf6kC8MPWP3qSu3xHqx12C5zyai2duFZUU
wqOt9iCFCscFQYqKs3xsHI+ncQb+PGjVZA8+jPw7nrPIkeSXQV2aZb1E68wa2YIL
3eYgTUKz34cB6tAq9YwHnZpyPx8UJCZGkshpJmgtZ3mCbtQaO17LoihnqPn4UOMr
V75R/7FjSuPLS8NaZF4wfi52btXMSxO/u7GuoJkzJscP3p4qtwe6Rl9dc1XC8P7k
NIbGZ5Yg5cEPcfmhgXFOhQZkD0yxcJqBUcoFpnp2vu5XJl2E5I/quIyVxUXi6O6c
/obspcvace4wy8uO0bdVhc4nJ+Rla4InVSJaUaBeiHTW8kReSFYyMmDCzLjGIu1q
doU61OM3Zv1ptsLu3gUE6GU27iWYj2RWN3e3HE4Sbd89IFwLXNdSuM0ifDLZk7AQ
WBhRhipCCgZhkj9g2NEk7jRVslti1NdN5zoQLaJNqSwO1MtxTmJ15Ksk3QP6kfLB
Q52UWybBzpaP9HEd4XnR+HuQ4k2K0ns2KgNImsNvIyFwbpMUyUWLMPimaV1DWUXo
5SBjDB/V/W2JBFR+XKHFJeFwYhj7DD/ocsGr4ZMx/lgc8rjIBkI=
=lgTX
-----END PGP SIGNATURE-----"#
                    .to_vec(),
            ],
            "gpgsig",
        );
        assert_bytes_eq(map.get(""), vec![b"Create first draft".to_vec()], "comment");
    }

    #[test]
    fn test_serialize() {
        let map = kvlm_parse(KVLM).unwrap();
        let ser = kvlm_serialize(&map);
        assert_bytes_eq(map.get(""), vec![b"Create first draft".to_vec()], "comment");
        debug!("{}", from_utf8(&ser).unwrap());
        assert_eq!(readable_map(&map), readable_map(&kvlm_parse(&ser).unwrap()));
    }

    fn readable_map(map: &OrderedHashMap<String, Vec<Vec<u8>>>) -> HashMap<String, Vec<String>> {
        map.clone()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.iter()
                        .map(|v| from_utf8(v).unwrap().to_string())
                        .collect::<Vec<String>>(),
                )
            })
            .collect::<HashMap<String, Vec<String>>>()
    }

    fn assert_bytes_eq(actual: Option<&Vec<Vec<u8>>>, expected: Vec<Vec<u8>>, msg: &str) {
        assert!(actual.is_some(), "{}: {}", msg, "value does not exist in map");
        actual.unwrap()
            .iter()
            .zip(expected)
            .for_each(|(actual, expected)| {
                assert_eq!(
                    from_utf8(actual.deref())
                        .expect(&format!("{}: {}", msg, "could not parse actual")),
                    from_utf8(&expected)
                        .expect(&format!("{}: {}", msg, "failed to parse expected"))
                );
            });
    }
}
