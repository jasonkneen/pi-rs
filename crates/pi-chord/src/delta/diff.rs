use super::{Op, Path, RESERVED_SEGMENTS, Seg};
use crate::{JsonString, JsonValue as Value};
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
};

const MAX_OPERATIONS: usize = 4096;

fn fingerprint(value: &Value) -> u64 {
    fn hash(value: &Value, state: &mut DefaultHasher) {
        std::mem::discriminant(value).hash(state);
        match value {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Number(n) => {
                let n = n.as_f64().unwrap();
                if n == 0.0 {
                    0u64.hash(state)
                } else {
                    n.to_bits().hash(state)
                }
            }
            Value::String(s) => s.hash(state),
            Value::Array(a) => {
                a.len().hash(state);
                for v in a.iter() {
                    hash(v, state);
                }
            }
            Value::Object(m) => {
                let mut pairs: Vec<_> = m.iter().collect();
                pairs.sort_by(|(a, _), (b, _)| a.units().cmp(b.units()));
                for (k, v) in pairs {
                    k.hash(state);
                    hash(v, state);
                }
            }
        }
    }
    let mut hasher = DefaultHasher::new();
    hash(value, &mut hasher);
    hasher.finish()
}

fn emit_set(path: &[Seg], after: &Value, out: &mut Vec<Op>) {
    if path.is_empty() {
        out.push(Op::Replace(after.clone()));
    } else {
        out.push(Op::Set(path.to_vec(), after.clone()));
    }
}

fn overlap(a: &[u16], b: &[u16]) -> usize {
    if a.is_empty() || b.is_empty() {
        return 0;
    }
    let tail = &a[a.len().saturating_sub(65536)..];
    for h in [64.min(b.len()), 1] {
        let mut tried = 0;
        for k in 0..tail.len() {
            if k + h > tail.len() || tail[k..k + h] != b[..h] {
                continue;
            }
            tried += 1;
            if tried > 8 {
                break;
            }
            let count = tail.len() - k;
            if count <= b.len() && tail[k..] == b[..count] {
                return count;
            }
        }
        if h == 1 {
            break;
        }
    }
    0
}

fn diff(before: &Value, after: &Value, path: &mut Path, out: &mut Vec<Op>) {
    if before == after || out.len() > MAX_OPERATIONS {
        return;
    }
    match (before, after) {
        (Value::String(a), Value::String(b)) if !path.is_empty() => {
            let (a, b) = (a.units(), b.units());
            if b.len() > a.len() && b.starts_with(a) {
                out.push(Op::Append(
                    path.clone(),
                    JsonString::from_utf16(b[a.len()..].to_vec()),
                ));
            } else {
                let shared = overlap(a, b);
                if shared == 0 {
                    emit_set(path, after, out);
                } else {
                    out.push(Op::Truncate(path.clone(), a.len() - shared));
                    if b.len() > shared {
                        out.push(Op::Append(
                            path.clone(),
                            JsonString::from_utf16(b[shared..].to_vec()),
                        ));
                    }
                }
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            if a.keys()
                .chain(b.keys())
                .any(|k| RESERVED_SEGMENTS.iter().any(|s| k.equals_str(s)))
            {
                emit_set(path, after, out);
                return;
            }
            for (key, value) in b.iter() {
                if out.len() > MAX_OPERATIONS {
                    return;
                }
                path.push(Seg::Key(key.clone()));
                if let Some(old) = a.get(key) {
                    diff(old, value, path, out);
                } else {
                    emit_set(path, value, out);
                }
                path.pop();
            }
            for key in a.keys() {
                if out.len() > MAX_OPERATIONS {
                    return;
                }
                if !b.contains_key(key) {
                    path.push(Seg::Key(key.clone()));
                    out.push(Op::Delete(path.clone()));
                    path.pop();
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => diff_array(a, b, path, out),
        _ => emit_set(path, after, out),
    }
}

fn positions(values: &[Value]) -> HashMap<u64, VecDeque<usize>> {
    let mut result: HashMap<u64, VecDeque<usize>> = HashMap::new();
    for (index, value) in values.iter().enumerate() {
        result
            .entry(fingerprint(value))
            .or_default()
            .push_back(index);
    }
    result
}

fn diff_array(a: &[Value], b: &[Value], path: &mut Path, out: &mut Vec<Op>) {
    if a.len() == b.len() && a.len() > 1 && a.first() != b.first() && a.last() != b.last() {
        let mut positions = positions(a);
        let mut order = Vec::with_capacity(b.len());
        for value in b {
            let Some(indices) = positions.get_mut(&fingerprint(value)) else {
                break;
            };
            let Some(at) = indices.iter().position(|i| a[*i] == *value) else {
                break;
            };
            order.push(indices.remove(at).unwrap());
        }
        if order.len() == b.len() {
            out.push(Op::Move(path.clone(), order));
            return;
        }
    }
    region(a, b, path, out, 0);
}

fn region(mut a: &[Value], mut b: &[Value], path: &mut Path, out: &mut Vec<Op>, mut output: usize) {
    if out.len() > MAX_OPERATIONS {
        return;
    }
    while !a.is_empty() && !b.is_empty() && a[0] == b[0] {
        a = &a[1..];
        b = &b[1..];
        output += 1;
    }
    while !a.is_empty() && !b.is_empty() && a.last() == b.last() {
        a = &a[..a.len() - 1];
        b = &b[..b.len() - 1];
    }
    if a.is_empty() && b.is_empty() {
        return;
    }
    if a.is_empty() || b.is_empty() {
        out.push(Op::Splice(path.clone(), output, a.len(), b.to_vec()));
        return;
    }
    if a.len() == b.len() {
        let anchors: Vec<_> = (0..a.len())
            .filter(|i| a[*i] == b[*i])
            .map(|i| (i, i))
            .collect();
        if !anchors.is_empty() {
            matched_regions(a, b, path, out, output, &anchors);
            return;
        }
    }
    let anchors = if a.len().saturating_mul(b.len()) <= 65536 {
        let width = b.len() + 1;
        let mut lengths = vec![0u32; (a.len() + 1) * width];
        for i in (0..a.len()).rev() {
            for j in (0..b.len()).rev() {
                lengths[i * width + j] = if a[i] == b[j] {
                    lengths[(i + 1) * width + j + 1] + 1
                } else {
                    lengths[(i + 1) * width + j].max(lengths[i * width + j + 1])
                };
            }
        }
        let (mut i, mut j) = (0, 0);
        let mut matches = Vec::new();
        while i < a.len() && j < b.len() {
            if a[i] == b[j] {
                matches.push((i, j));
                i += 1;
                j += 1;
            } else if lengths[(i + 1) * width + j] >= lengths[i * width + j + 1] {
                i += 1;
            } else {
                j += 1;
            }
        }
        matches
    } else {
        // Bound large-array matching to one forward pass over indexed candidates.
        let mut positions = positions(a);
        let mut matches = Vec::new();
        let mut previous = None;
        for (j, value) in b.iter().enumerate() {
            let Some(indices) = positions.get_mut(&fingerprint(value)) else {
                continue;
            };
            while indices
                .front()
                .is_some_and(|i| previous.is_some_and(|p| *i <= p))
            {
                indices.pop_front();
            }
            if let Some(at) = indices.iter().position(|i| a[*i] == *value) {
                let i = indices.remove(at).unwrap();
                previous = Some(i);
                matches.push((i, j));
            }
        }
        matches
    };
    if !anchors.is_empty() {
        matched_regions(a, b, path, out, output, &anchors);
    } else if a.len() == 1 && b.len() == 1 {
        path.push(Seg::Index(output));
        diff(&a[0], &b[0], path, out);
        path.pop();
    } else {
        out.push(Op::Splice(path.clone(), output, a.len(), b.to_vec()));
    }
}

fn matched_regions(
    a: &[Value],
    b: &[Value],
    path: &mut Path,
    out: &mut Vec<Op>,
    output: usize,
    matches: &[(usize, usize)],
) {
    let (mut i, mut j) = (0, 0);
    for &(ai, bj) in matches {
        if out.len() > MAX_OPERATIONS {
            return;
        }
        region(&a[i..ai], &b[j..bj], path, out, output + j);
        i = ai + 1;
        j = bj + 1;
    }
    region(&a[i..], &b[j..], path, out, output + j);
}

/// Produce an exact compact delta. Operation shape is not canonical in Chord.
pub fn diff_revisions(before: &Value, after: &Value) -> Vec<Op> {
    let mut out = Vec::new();
    diff(before, after, &mut Vec::new(), &mut out);
    if out.len() > MAX_OPERATIONS {
        return vec![Op::Replace(after.clone())];
    }
    if !out.is_empty() && !matches!(out[0], Op::Replace(_)) {
        let delta_bytes = out
            .iter()
            .map(|op| crate::json::stringify(&op.to_value()).len() + 1)
            .sum::<usize>()
            + 2;
        if delta_bytes >= 65536 && delta_bytes >= crate::json::stringify(after).len() + 6 {
            return vec![Op::Replace(after.clone())];
        }
    }
    out
}
