/// A newtype for a thing that can be conveniently be use as an index
///
/// The intention is not to make the newtype completely foolproof, but to make it
/// convenient to use while still providing some safety by making conversions explicit.
macro_rules! index_newtype {
    (
        $(#[$outer:meta])*
        pub struct $name:ident(pub $wrapped:ty);
    ) => {
        $(#[$outer])*
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub $wrapped);

        impl RangeSetEntry for $name {
            fn min_value() -> Self {
                $name(0)
            }

            fn is_min_value(&self) -> bool {
                self.0 == 0
            }
        }

        impl Mul<$wrapped> for $name {
            type Output = $name;

            fn mul(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 * rhs)
            }
        }

        impl Div<$wrapped> for $name {
            type Output = $name;

            fn div(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 / rhs)
            }
        }

        impl Sub<$wrapped> for $name {
            type Output = $name;

            fn sub(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 - rhs)
            }
        }

        impl Sub<$name> for $name {
            type Output = $name;

            fn sub(self, rhs: $name) -> Self::Output {
                $name(self.0 - rhs.0)
            }
        }

        impl Add<$wrapped> for $name {
            type Output = $name;

            fn add(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 + rhs)
            }
        }

        impl Add<$name> for $name {
            type Output = $name;

            fn add(self, rhs: $name) -> Self::Output {
                $name(self.0 + rhs.0)
            }
        }

        impl PartialEq<$wrapped> for $name {
            fn eq(&self, other: &$wrapped) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for $wrapped {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl PartialOrd<$wrapped> for $name {
            fn partial_cmp(&self, other: &$wrapped) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }

        impl $name {

            /// Convert to usize or panic if it doesn't fit.
            pub fn to_usize(self) -> usize {
                usize::try_from(self.0).expect("usize overflow")
            }
        }
    }
}

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}
