use objc2_core_foundation::{CGPoint, CGRect, CGSize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HideCorner {
    TopLeft,
    TopRight,
    BottomLeft,
    #[default]
    BottomRight,
}

impl HideCorner {
    pub fn opposite(self) -> Self {
        match self {
            Self::TopLeft => Self::TopRight,
            Self::TopRight => Self::TopLeft,
            Self::BottomLeft => Self::BottomRight,
            Self::BottomRight => Self::BottomLeft,
        }
    }
}

/// Pure geometry used to place inactive-workspace windows just offscreen.
///
/// Fork-local behavior (ported from pre-WindowStore helpers): prefer edges and
/// top corners so hidden windows do not land on lower displays, use a 1x1
/// bottom strip when a display is boxed horizontally, and push candidates out
/// of adjacent screens.
pub struct HiddenWindowPlacement;

impl HiddenWindowPlacement {
    const REVEAL_PX: f64 = 1.0;
    const VISIBLE_THRESHOLD_PX: f64 = 3.0;
    const MIN_ANCHOR_AREA: f64 = 1.0;

    fn rect_for_corner(screen: CGRect, window: CGRect, corner: HideCorner) -> CGRect {
        let size = window.size;
        let origin = match corner {
            HideCorner::TopLeft => CGPoint::new(
                screen.origin.x - size.width + Self::REVEAL_PX,
                screen.origin.y - size.height + Self::REVEAL_PX,
            ),
            HideCorner::TopRight => CGPoint::new(
                screen.max().x - Self::REVEAL_PX,
                screen.origin.y - size.height + Self::REVEAL_PX,
            ),
            HideCorner::BottomLeft => CGPoint::new(
                screen.origin.x - size.width + Self::REVEAL_PX,
                screen.max().y - Self::REVEAL_PX,
            ),
            HideCorner::BottomRight => {
                CGPoint::new(screen.max().x - Self::REVEAL_PX, screen.max().y - Self::REVEAL_PX)
            }
        };
        CGRect::new(origin, size)
    }

    fn hidden_edge_rects(screen: CGRect, size: CGSize) -> [CGRect; 2] {
        let right = CGRect::new(CGPoint::new(screen.max().x - Self::REVEAL_PX, screen.origin.y), size);
        let left = CGRect::new(
            CGPoint::new(screen.origin.x - size.width + Self::REVEAL_PX, screen.origin.y),
            size,
        );
        [right, left]
    }

    fn intersection_area(a: CGRect, b: CGRect) -> f64 {
        let width = (a.max().x.min(b.max().x) - a.origin.x.max(b.origin.x)).max(0.0);
        let height = (a.max().y.min(b.max().y) - a.origin.y.max(b.origin.y)).max(0.0);
        width * height
    }

    fn is_horizontally_boxed_by_same_row_screens(screen: CGRect, other_screens: &[CGRect]) -> bool {
        let overlaps_y =
            |other: &CGRect| other.origin.y < screen.max().y && other.max().y > screen.origin.y;
        let has_left =
            other_screens.iter().any(|other| overlaps_y(other) && other.max().x <= screen.origin.x);
        let has_right =
            other_screens.iter().any(|other| overlaps_y(other) && other.origin.x >= screen.max().x);
        has_left && has_right
    }

    fn hidden_bottom_strip_rect(screen: CGRect) -> CGRect {
        CGRect::new(
            CGPoint::new(screen.origin.x, screen.max().y - Self::REVEAL_PX),
            CGSize::new(1.0, 1.0),
        )
    }

    fn topology_bounds(screens: &[CGRect]) -> Option<CGRect> {
        let first = screens.first()?;
        let (mut min_x, mut min_y, mut max_x, mut max_y) =
            (first.origin.x, first.origin.y, first.max().x, first.max().y);
        for screen in screens.iter().skip(1) {
            min_x = min_x.min(screen.origin.x);
            min_y = min_y.min(screen.origin.y);
            max_x = max_x.max(screen.max().x);
            max_y = max_y.max(screen.max().y);
        }
        Some(CGRect::new(CGPoint::new(min_x, min_y), CGSize::new(max_x - min_x, max_y - min_y)))
    }

    fn push_out_of_other_screens(mut rect: CGRect, other_screens: &[CGRect]) -> CGRect {
        let mut screens = other_screens.to_vec();
        screens.push(rect);
        for _ in 0..other_screens.len().saturating_mul(4).max(1) {
            let Some(screen) = other_screens
                .iter()
                .copied()
                .find(|screen| Self::intersection_area(*screen, rect) > 0.0)
            else {
                break;
            };

            let left = (rect.max().x - screen.origin.x).max(0.0) + 1.0;
            let right = (screen.max().x - rect.origin.x).max(0.0) + 1.0;
            let up = (rect.max().y - screen.origin.y).max(0.0) + 1.0;
            let down = (screen.max().y - rect.origin.y).max(0.0) + 1.0;
            let shift = [
                (left, CGPoint::new(-left, 0.0)),
                (right, CGPoint::new(right, 0.0)),
                (up, CGPoint::new(0.0, -up)),
                (down, CGPoint::new(0.0, down)),
            ]
            .into_iter()
            .min_by(|a, b| a.0.total_cmp(&b.0))
            .map(|(_, delta)| delta)
            .unwrap_or(CGPoint::new(0.0, 0.0));

            rect.origin.x += shift.x;
            rect.origin.y += shift.y;
        }
        if other_screens.iter().any(|screen| Self::intersection_area(*screen, rect) > 0.0)
            && let Some(bounds) = Self::topology_bounds(&screens)
        {
            rect.origin.y = bounds.max().y + 1.0;
        }
        rect
    }

    pub fn calculate(
        screen: CGRect,
        window: CGRect,
        preferred_corner: HideCorner,
        other_screens: &[CGRect],
    ) -> CGRect {
        if Self::is_horizontally_boxed_by_same_row_screens(screen, other_screens) {
            return Self::hidden_bottom_strip_rect(screen);
        }

        let corner_candidates = [
            preferred_corner,
            preferred_corner.opposite(),
            HideCorner::TopRight,
            HideCorner::TopLeft,
        ]
        .into_iter()
        .map(|candidate| {
            let rect = Self::rect_for_corner(screen, window, candidate);
            let anchor = Self::intersection_area(screen, rect);
            let other_max = other_screens
                .iter()
                .map(|other| Self::intersection_area(*other, rect))
                .fold(0.0_f64, f64::max);
            (rect, anchor >= Self::MIN_ANCHOR_AREA, other_max, anchor)
        });

        let edge_candidates =
            Self::hidden_edge_rects(screen, window.size).into_iter().map(|rect| {
                let anchor = Self::intersection_area(screen, rect);
                let other_max = other_screens
                    .iter()
                    .map(|other| Self::intersection_area(*other, rect))
                    .fold(0.0_f64, f64::max);
                (rect, anchor >= Self::MIN_ANCHOR_AREA, other_max, anchor)
            });

        edge_candidates
            .chain(corner_candidates)
            .min_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| a.2.total_cmp(&b.2))
                    .then_with(|| b.3.total_cmp(&a.3))
            })
            .map(|(rect, _, _, _)| Self::push_out_of_other_screens(rect, other_screens))
            .unwrap_or_else(|| Self::rect_for_corner(screen, window, preferred_corner))
    }

    pub fn is_hidden(screen: CGRect, window: CGRect, other_screens: &[CGRect]) -> bool {
        [
            HideCorner::BottomLeft,
            HideCorner::BottomRight,
            HideCorner::TopLeft,
            HideCorner::TopRight,
        ]
        .into_iter()
        .any(|corner| Self::calculate(screen, window, corner, other_screens) == window)
            || {
                let visible_width = (window.max().x.min(screen.max().x)
                    - window.origin.x.max(screen.origin.x))
                .max(0.0);
                let visible_height = (window.max().y.min(screen.max().y)
                    - window.origin.y.max(screen.origin.y))
                .max(0.0);
                visible_width <= Self::VISIBLE_THRESHOLD_PX
                    && visible_height <= Self::VISIBLE_THRESHOLD_PX
            }
    }
}

#[cfg(test)]
mod tests {
    use objc2_core_foundation::{CGPoint, CGSize};

    use super::*;

    fn rect(x: f64, y: f64, width: f64, height: f64) -> CGRect {
        CGRect::new(CGPoint::new(x, y), CGSize::new(width, height))
    }

    #[test]
    fn anchors_to_requested_corner() {
        let hidden = HiddenWindowPlacement::calculate(
            rect(0.0, 0.0, 1000.0, 800.0),
            rect(10.0, 20.0, 200.0, 100.0),
            HideCorner::BottomRight,
            &[],
        );
        assert_eq!(hidden, rect(999.0, 799.0, 200.0, 100.0));
    }

    #[test]
    fn avoids_an_adjacent_monitor() {
        let screen = rect(0.0, 0.0, 1000.0, 800.0);
        let hidden = HiddenWindowPlacement::calculate(
            screen,
            rect(0.0, 0.0, 200.0, 100.0),
            HideCorner::BottomRight,
            &[rect(1000.0, 0.0, 1000.0, 800.0)],
        );
        // Prefer left edge / top-left over overlapping the monitor to the right.
        assert!(hidden.origin.x < screen.origin.x || hidden.origin.y < screen.origin.y);
        assert_eq!(
            HiddenWindowPlacement::intersection_area(rect(1000.0, 0.0, 1000.0, 800.0), hidden),
            0.0
        );
    }

    #[test]
    fn uses_bottom_strip_when_horizontally_boxed() {
        let screen = rect(1000.0, 0.0, 1000.0, 800.0);
        let hidden = HiddenWindowPlacement::calculate(
            screen,
            rect(1100.0, 100.0, 200.0, 100.0),
            HideCorner::BottomRight,
            &[rect(0.0, 0.0, 1000.0, 800.0), rect(2000.0, 0.0, 1000.0, 800.0)],
        );
        assert_eq!(hidden, rect(1000.0, 799.0, 1.0, 1.0));
    }
}
