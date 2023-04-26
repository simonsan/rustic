//! Main entry point for RusticRs

#![deny(warnings, missing_docs, trivial_casts, unused_qualifications)]
#![forbid(unsafe_code)]

use rustic::application::APP;

/// Boot RusticRs
fn main() {
    abscissa_core::boot(&APP);
}
