//! converting between different binding's geometry types

use objc2_core_foundation as ic;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{DeserializeAs, SerializeAs};

pub trait Round {
    fn round(&self) -> Self;
}

impl Round for ic::CGRect {
    fn round(&self) -> Self {
        let min_rounded = self.min().round();
        let max_rounded = self.max().round();
        ic::CGRect {
            origin: min_rounded,
            size: ic::CGSize {
                width: max_rounded.x - min_rounded.x,
                height: max_rounded.y - min_rounded.y,
            },
        }
    }
}

impl Round for ic::CGPoint {
    fn round(&self) -> Self {
        ic::CGPoint {
            x: self.x.round(),
            y: self.y.round(),
        }
    }
}

impl Round for ic::CGSize {
    fn round(&self) -> Self {
        ic::CGSize {
            width: self.width.round(),
            height: self.height.round(),
        }
    }
}

pub trait IsWithin {
    fn is_within(&self, how_much: f64, other: Self) -> bool;
}

impl IsWithin for ic::CGRect {
    fn is_within(&self, how_much: f64, other: Self) -> bool {
        self.origin.is_within(how_much, other.origin) && self.size.is_within(how_much, other.size)
    }
}

impl IsWithin for ic::CGPoint {
    fn is_within(&self, how_much: f64, other: Self) -> bool {
        self.x.is_within(how_much, other.x) && self.y.is_within(how_much, other.y)
    }
}

impl IsWithin for ic::CGSize {
    fn is_within(&self, how_much: f64, other: Self) -> bool {
        self.width.is_within(how_much, other.width) && self.height.is_within(how_much, other.height)
    }
}

impl IsWithin for f64 {
    fn is_within(&self, how_much: f64, other: Self) -> bool { (self - other).abs() < how_much }
}

pub trait SameAs: IsWithin + Sized {
    fn same_as(&self, other: Self) -> bool { self.is_within(0.1, other) }
}

impl SameAs for ic::CGRect {}
impl SameAs for ic::CGPoint {}
impl SameAs for ic::CGSize {}

pub trait CGRectExt {
    fn intersection(&self, other: &Self) -> Self;
    fn contains(&self, point: ic::CGPoint) -> bool;
    fn contains_rect(&self, other: Self) -> bool;
    fn area(&self) -> f64;
}

impl CGRectExt for ic::CGRect {
    fn intersection(&self, other: &Self) -> Self {
        let min_x = f64::max(self.min().x, other.min().x);
        let max_x = f64::min(self.max().x, other.max().x);
        let min_y = f64::max(self.min().y, other.min().y);
        let max_y = f64::min(self.max().y, other.max().y);
        ic::CGRect {
            origin: ic::CGPoint::new(min_x, min_y),
            size: ic::CGSize::new(f64::max(max_x - min_x, 0.), f64::max(max_y - min_y, 0.)),
        }
    }

    fn contains(&self, point: ic::CGPoint) -> bool {
        (self.min().x..=self.max().x).contains(&point.x)
            && (self.min().y..=self.max().y).contains(&point.y)
    }

    fn contains_rect(&self, other: Self) -> bool {
        self.min().x <= other.min().x
            && self.min().y <= other.min().y
            && self.max().x >= other.max().x
            && self.max().y >= other.max().y
    }

    fn area(&self) -> f64 { self.size.width * self.size.height }
}

/// A point inside `frame` on a visible (unoccluded) part of the window, for
/// mouse warps. With no intersecting occluder this is exactly `frame.mid()`;
/// when floats cover parts of the frame it is the sampled point deepest inside
/// the uncovered region (farthest from every occluder and from the frame edge);
/// if the frame is fully covered it falls back to `frame.mid()`.
pub fn visible_warp_point(frame: ic::CGRect, occluders: &[ic::CGRect]) -> ic::CGPoint {
    let occluders: Vec<ic::CGRect> =
        occluders.iter().filter(|o| frame.intersection(o).area() > 0.0).copied().collect();
    if occluders.is_empty() {
        return frame.mid();
    }
    // ponytail: coarse pole-of-inaccessibility over an NxN sample grid — the
    // occluder set is at most a handful of floating windows, so N*N*len is
    // trivial. Ceiling: ~frame/N point granularity; upgrade path is exact
    // rectangle subtraction if warp placement ever needs to be finer.
    const N: usize = 24;
    let dist_to_rect = |r: &ic::CGRect, p: ic::CGPoint| -> f64 {
        let dx = (r.min().x - p.x).max(p.x - r.max().x).max(0.0);
        let dy = (r.min().y - p.y).max(p.y - r.max().y).max(0.0);
        (dx * dx + dy * dy).sqrt()
    };
    let mut best: Option<(f64, ic::CGPoint)> = None;
    for i in 0..N {
        for j in 0..N {
            let p = ic::CGPoint::new(
                frame.min().x + (i as f64 + 0.5) / N as f64 * frame.size.width,
                frame.min().y + (j as f64 + 0.5) / N as f64 * frame.size.height,
            );
            if occluders.iter().any(|o| o.contains(p)) {
                continue;
            }
            let edge = (p.x - frame.min().x)
                .min(frame.max().x - p.x)
                .min(p.y - frame.min().y)
                .min(frame.max().y - p.y);
            let clear =
                occluders.iter().map(|o| dist_to_rect(o, p)).fold(f64::INFINITY, f64::min);
            let score = edge.min(clear);
            if best.is_none_or(|(s, _)| score > s) {
                best = Some((score, p));
            }
        }
    }
    best.map(|(_, p)| p).unwrap_or_else(|| frame.mid())
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ic::CGRect")]
pub struct CGRectDef {
    #[serde(with = "CGPointDef")]
    pub origin: ic::CGPoint,
    #[serde(with = "CGSizeDef")]
    pub size: ic::CGSize,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ic::CGPoint")]
pub struct CGPointDef {
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ic::CGSize")]
pub struct CGSizeDef {
    pub width: f64,
    pub height: f64,
}

impl SerializeAs<ic::CGRect> for CGRectDef {
    fn serialize_as<S>(value: &ic::CGRect, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        CGRectDef::serialize(value, serializer)
    }
}

impl<'de> DeserializeAs<'de, ic::CGRect> for CGRectDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ic::CGRect, D::Error>
    where D: Deserializer<'de> {
        CGRectDef::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rect(x: f64, y: f64, w: f64, h: f64) -> ic::CGRect {
        ic::CGRect {
            origin: ic::CGPoint::new(x, y),
            size: ic::CGSize::new(w, h),
        }
    }

    #[test]
    fn no_occluders_is_exactly_center() {
        let frame = rect(100.0, 100.0, 400.0, 300.0);
        assert_eq!(visible_warp_point(frame, &[]), frame.mid());
    }

    #[test]
    fn non_intersecting_occluder_is_exactly_center() {
        let frame = rect(0.0, 0.0, 200.0, 200.0);
        assert_eq!(visible_warp_point(frame, &[rect(300.0, 0.0, 100.0, 100.0)]), frame.mid());
    }

    #[test]
    fn occluded_center_moves_point_to_visible_region() {
        let frame = rect(0.0, 0.0, 400.0, 300.0);
        // float covering the center and the whole left half
        let float = rect(-50.0, -50.0, 350.0, 400.0);
        let p = visible_warp_point(frame, &[float]);
        assert!(frame.contains(p), "point must stay inside the frame: {p:?}");
        assert!(!float.contains(p), "point must not land on the occluder: {p:?}");
        // visible strip is x in (300, 400); the deepest point is around x=350
        assert!(p.x > 300.0, "expected point in the visible strip, got {p:?}");
    }

    #[test]
    fn multiple_occluders_picks_clearest_gap() {
        let frame = rect(0.0, 0.0, 400.0, 400.0);
        let occ = [rect(0.0, 0.0, 400.0, 180.0), rect(0.0, 220.0, 400.0, 180.0)];
        let p = visible_warp_point(frame, &occ);
        assert!(!occ[0].contains(p) && !occ[1].contains(p), "landed on an occluder: {p:?}");
        assert!((p.y - 200.0).abs() < 20.0, "expected the horizontal gap, got {p:?}");
    }

    #[test]
    fn fully_covered_falls_back_to_center() {
        let frame = rect(0.0, 0.0, 200.0, 200.0);
        let p = visible_warp_point(frame, &[rect(-10.0, -10.0, 220.0, 220.0)]);
        assert_eq!(p, frame.mid());
    }
}
