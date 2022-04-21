/// An alias for a [`u64`].
pub type Identity = u64;

/// A trait that exposes a global const ID. This id should be consistent across
/// applications and compilers assuming all use the default Rust hasher.
pub trait IdentityTrait {
    const ID: Identity;
}
