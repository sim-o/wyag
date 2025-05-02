use anyhow::Context;
use ordered_hash_map::OrderedHashMap;

pub fn kvlm_parse<'a>(raw: &'a mut [u8]) -> anyhow::Result<OrderedHashMap<&'a [u8], Vec<&'a [u8]>>> {
    let map: OrderedHashMap<&'a _, Vec<&'a _>> = OrderedHashMap::new();
    let map = kvlm_parse_rec(raw, map).context("parsing kvlm")?;
    Ok(map)
}

fn kvlm_parse_rec<'a>(
    raw: &'a mut [u8],
    mut map: OrderedHashMap<&'a [u8], Vec<&'a [u8]>>,
) -> anyhow::Result<OrderedHashMap<&'a [u8], Vec<&'a [u8]>>> {
    if raw.is_empty() {
        return Ok(map);
    }
    if raw[0] == b'\n' {
        map.insert(b"", vec![&raw[1..]]);
        return Ok(map);
    }

    let spc = raw
        .iter()
        .position(|&b| b == b' ')
        .ok_or("kvlm missing space");

    let spc = spc.unwrap();
    let (key, raw) = raw.split_at_mut(spc);

    let mut end: usize = 0;
    loop {
        end += 1 + raw[end + 1..]
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(raw.len() - end - 1);

        if end + 1 >= raw.len() || raw[end + 1] != b' ' {
            break;
        }
    }

    let raw = &mut raw[1..];
    end -= 1;

    let (value, rest) = raw.split_at_mut(end);
    let value = kvlm_clean_value(value);
    if let Some(v) = map.get_mut(key) {
        v.push(value);
    } else {
        map.insert(key, vec![value]);
    }

    kvlm_parse_rec(&mut rest[1..], map)
}

fn kvlm_clean_value(vec: &mut [u8]) -> &[u8] {
    if vec.is_empty() {
        return vec;
    }

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

    &vec[..i]
}

pub fn kvlm_serialize(map: &OrderedHashMap<&[u8], Vec<&[u8]>>) -> Vec<u8> {
    let mut rest = None;
    let mut v: Vec<u8> = map
        .iter()
        .filter_map(|(k, v)| {
            if k.is_empty() {
                rest = Some(v);
                None
            } else {
                let start = k.iter();
                let end = v.iter().flat_map(|&v| {
                    v.split(|&b| b == b'\n')
                        .flat_map(|v| b" ".iter().chain(v.iter().chain(b"\n")))
                });
                let ret = start.chain(end).copied();
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
    use std::{collections::HashMap, str::from_utf8};

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
        let mut kvlm = KVLM.to_vec();
        let map = kvlm_parse(&mut kvlm).unwrap();
        assert_bytes_eq(
            map.get(&b"tree"[..]),
            vec![&b"29ff16c9c14e2652b22f8b78bb08a5a07930c147"[..]],
            "tree",
        );
        assert_bytes_eq(
            map.get(&b"parent"[..]),
            vec![&b"206941306e8a8af65b66eaaaea388a7ae24d49a0"[..]],
            "parent",
        );
        assert_bytes_eq(
            map.get(&b"gpgsig"[..]),
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
                    .as_slice(),
            ],
            "gpgsig",
        );
        assert_bytes_eq(
            map.get(b"".as_slice()),
            vec![b"Create first draft".as_slice()],
            "comment",
        );
    }

    #[test]
    fn test_serialize() {
        let mut kvlm = KVLM.to_vec();
        let map = kvlm_parse(&mut kvlm).unwrap();
        let mut ser = kvlm_serialize(&map);
        assert_bytes_eq(
            map.get(b"".as_slice()),
            vec![b"Create first draft".as_slice()],
            "comment",
        );
        debug!("{}", from_utf8(&ser).unwrap());
        assert_eq!(readable_map(&map), readable_map(&kvlm_parse(&mut ser).unwrap()));
    }

    fn readable_map(map: &OrderedHashMap<&[u8], Vec<&[u8]>>) -> HashMap<String, Vec<String>> {
        map.clone()
            .into_iter()
            .map(|(k, v)| {
                (
                    from_utf8(k).unwrap().to_string(),
                    v.iter()
                        .map(|v| from_utf8(v).unwrap().to_string())
                        .collect::<Vec<String>>(),
                )
            })
            .collect::<HashMap<String, Vec<String>>>()
    }

    fn assert_bytes_eq(actual: Option<&Vec<&[u8]>>, expected: Vec<&[u8]>, msg: &str) {
        assert!(
            actual.is_some(),
            "{}: {}",
            msg,
            "value does not exist in map"
        );
        actual
            .unwrap()
            .iter()
            .zip(expected)
            .for_each(|(&actual, expected)| {
                assert_eq!(
                    from_utf8(actual)
                        .unwrap_or_else(|_| panic!("{}: {}", msg, "could not parse actual")),
                    from_utf8(expected)
                        .unwrap_or_else(|_| panic!("{}: {}", msg, "failed to parse expected"))
                );
            });
    }
}
