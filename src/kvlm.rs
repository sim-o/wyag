use anyhow::{Context, anyhow};
use log::trace;
use ordered_hash_map::OrderedHashMap;
use std::ops::Range;
use std::str::from_utf8;

type Kvlm = OrderedHashMap<Vec<u8>, Vec<Range<usize>>>;

pub fn kvlm_parse(mut raw: Vec<u8>) -> anyhow::Result<(Vec<u8>, Kvlm)> {
    let map = OrderedHashMap::new();
    let map = kvlm_parse_rec(&mut raw, map, 0).context("parsing kvlm")?;
    Ok((raw, map))
}

fn kvlm_parse_rec(raw: &mut Vec<u8>, mut map: Kvlm, i: usize) -> anyhow::Result<Kvlm> {
    if raw.len() == i {
        return Ok(map);
    }
    if raw[i] == b'\n' {
        let range = i + 1..raw.len();
        trace!(
            "using final value [{}]",
            from_utf8(&raw[range.clone()]).unwrap_or("<<bad-utf8>>")
        );
        map.insert(Vec::new(), vec![range]);
        return Ok(map);
    }

    let spc = i + raw[i..]
        .iter()
        .position(|&b| b == b' ')
        .ok_or(anyhow!("kvlm missing space"))?;
    trace!("using range {}..{} <{}", i, spc, raw.len());

    let key = raw[i..spc].to_vec();
    trace!("using key [{}]", from_utf8(&key).unwrap_or("<<bad-utf8>>"));

    let i = spc + 1;
    let mut end: usize = spc + 1;
    loop {
        end += raw[end..]
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(raw.len() - end - 1);

        if end + 1 >= raw.len() || raw[end + 1] != b' ' {
            break;
        }
        end += 1;
    }

    let value = i..end;
    let value = i..kvlm_clean_value(raw, value);
    trace!(
        "using value [{}]",
        from_utf8(&raw[value.clone()]).unwrap_or("<<bad-utf8>>")
    );

    let i = end + 1;
    if let Some(v) = map.get_mut(&key) {
        v.push(value);
    } else {
        map.insert(key, vec![value]);
    }

    kvlm_parse_rec(raw, map, i)
}

fn kvlm_clean_value(vec: &mut Vec<u8>, range: Range<usize>) -> usize {
    if range.start >= vec.len() {
        return range.start;
    }

    let mut skip = 0;
    let mut i = range.start + 1;
    loop {
        if i + skip >= range.end {
            break;
        }
        if vec[i + skip - 1] == b'\n' && vec[i + skip] == b' ' {
            skip += 1;
        }
        if i + skip >= range.end {
            break;
        }
        vec[i] = vec[i + skip];
        i += 1;
    }

    i
}

pub fn kvlm_serialize(data: &Vec<u8>, map: &OrderedHashMap<Vec<u8>, Vec<Range<usize>>>) -> Vec<u8> {
    let mut rest = None;
    let mut v: Vec<u8> = map
        .iter()
        .filter_map(|(k, v)| {
            if k.is_empty() {
                rest = Some(v);
                None
            } else {
                let start = k.iter();
                let end = v.iter().flat_map(|v| {
                    data[v.start..v.end]
                        .split(|&b| b == b'\n')
                        .flat_map(|v| b" ".iter().chain(v.iter()).chain(b"\n"))
                });
                Some(start.chain(end).copied())
            }
        })
        .flatten()
        .collect::<Vec<_>>();

    if let Some(rest) = rest {
        v.push(b'\n');
        for b in rest.iter() {
            v.extend_from_slice(&data[b.start..b.end]);
        }
    }

    trace!(
        "serialized tree [[{}]]",
        from_utf8(&v).map(String::from).unwrap_or_else(|e| {
            let valid = from_utf8(&v[..e.valid_up_to()]).unwrap().to_string();
            valid + "<<...bad-utf8>>"
        })
    );
    v
}

#[cfg(test)]
mod tests {
    use super::{kvlm_parse, kvlm_serialize};
    use log::debug;
    use ordered_hash_map::OrderedHashMap;
    use std::ops::Range;
    use std::{collections::HashMap, str::from_utf8};

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
        let kvlm = KVLM.to_vec();
        let (kvlm, map) = kvlm_parse(kvlm).unwrap();
        assert_bytes_eq(
            &kvlm,
            map.get(&b"tree"[..]),
            vec![&b"29ff16c9c14e2652b22f8b78bb08a5a07930c147"[..]],
            "tree",
        );
        assert_bytes_eq(
            &kvlm,
            map.get(&b"parent"[..]),
            vec![&b"206941306e8a8af65b66eaaaea388a7ae24d49a0"[..]],
            "parent",
        );
        assert_bytes_eq(
            &kvlm,
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
            &kvlm,
            map.get(b"".as_slice()),
            vec![b"Create first draft".as_slice()],
            "comment",
        );
    }

    #[test]
    fn test_serialize() {
        let kvlm = KVLM.to_vec();
        let (kvlm, map) = kvlm_parse(kvlm).unwrap();
        let ser = kvlm_serialize(&kvlm, &map);
        assert_bytes_eq(
            &kvlm,
            map.get(b"".as_slice()),
            vec![b"Create first draft".as_slice()],
            "comment",
        );
        debug!("{}", from_utf8(&ser).unwrap());
        assert_eq!(
            readable_map(&kvlm, &map),
            readable_map(&kvlm, &kvlm_parse(ser).unwrap().1)
        );
    }

    fn readable_map(
        data: &Vec<u8>,
        map: &OrderedHashMap<Vec<u8>, Vec<Range<usize>>>,
    ) -> HashMap<String, Vec<String>> {
        map.clone()
            .into_iter()
            .map(|(k, v)| {
                (
                    from_utf8(&k).unwrap().to_string(),
                    v.iter()
                        .map(|v| from_utf8(&data[v.start..v.end]).unwrap().to_string())
                        .collect::<Vec<String>>(),
                )
            })
            .collect::<HashMap<String, Vec<String>>>()
    }

    fn assert_bytes_eq(
        raw: &Vec<u8>,
        actual: Option<&Vec<Range<usize>>>,
        expected: Vec<&[u8]>,
        msg: &str,
    ) {
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
            .for_each(|(actual, expected)| {
                assert_eq!(
                    from_utf8(&raw[actual.start..actual.end])
                        .unwrap_or_else(|_| panic!("{}: {}", msg, "could not parse actual")),
                    from_utf8(expected)
                        .unwrap_or_else(|_| panic!("{}: {}", msg, "failed to parse expected"))
                );
            });
    }
}
