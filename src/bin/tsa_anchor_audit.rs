//! Admin tool: audit stored RFC 3161 anchors for `messageImprint`
//! mismatches left behind from before verification-on-receipt existed
//! (see `atl_server::background::tsa_job::audit` for the full rationale
//! and `atl_server::background::tsa_job::request::try_tsa_timestamp` for
//! the receipt-side check this remediates).
//!
//! This is deliberately a separate, manually run binary -- not a
//! migration and not something the main server runs at startup. It is
//! idempotent: rows already marked `rejected` are skipped on every run,
//! so re-running with `--apply` only ever acts on anchors found bad for
//! the first time.
//!
//! Dry run by default (reports what would change, writes nothing). Pass
//! `--apply` to actually mark bad anchors `rejected` (which also
//! atomically releases any tree still pointing at them, so it is
//! re-queued for anchoring).
//!
//! Usage:
//! ```text
//! tsa_anchor_audit --database ./atl.db          # dry run
//! tsa_anchor_audit --database ./atl.db --apply   # apply
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use atl_server::background::tsa_job::audit_tsa_anchors;
use atl_server::storage::index::IndexStore;
use atl_server::storage::StorageConfig;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "tsa-anchor-audit")]
#[command(about = "Audit stored RFC 3161 anchors for messageImprint mismatches")]
struct Args {
    /// Path to the ATL data directory (same meaning as atl-server's
    /// `--database` / `ATL_DATABASE_PATH`; the SQLite index lives at
    /// `<database>/atl.db`).
    #[arg(long, env = "ATL_DATABASE_PATH", default_value = "./atl.db")]
    database: String,

    /// Actually mark bad anchors `rejected` (and release the trees
    /// pointing at them). Without this flag, only reports what would
    /// change -- no database writes happen.
    #[arg(long)]
    apply: bool,
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .init();

    let args = Args::parse();

    let db_path = StorageConfig {
        data_dir: PathBuf::from(&args.database),
        ..Default::default()
    }
    .db_path();

    let index = match IndexStore::open(&db_path) {
        Ok(index) => index,
        Err(e) => {
            eprintln!("failed to open index at {}: {}", db_path.display(), e);
            return ExitCode::FAILURE;
        }
    };
    // Matches `StorageEngine::new`'s own open sequence: `open()` alone does
    // not create the schema on a fresh database, only migrates an existing
    // one. `initialize()` is `CREATE TABLE IF NOT EXISTS`, so it is a no-op
    // against an already-initialized server database.
    if let Err(e) = index.initialize() {
        eprintln!(
            "failed to initialize schema at {}: {}",
            db_path.display(),
            e
        );
        return ExitCode::FAILURE;
    }

    let report = match audit_tsa_anchors(&index, args.apply) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("audit failed: {}", e);
            return ExitCode::FAILURE;
        }
    };

    println!(
        "Scanned {} rfc3161 anchor(s) ({} already rejected, skipped).",
        report.scanned, report.already_rejected
    );

    if report.bad.is_empty() {
        println!("No messageImprint mismatches found.");
        return ExitCode::SUCCESS;
    }

    println!(
        "{} anchor(s) fail messageImprint verification:",
        report.bad.len()
    );
    for bad in &report.bad {
        println!(
            "  id={} tree_size={} anchored_hash={} reason={}",
            bad.id,
            bad.tree_size
                .map(|s| s.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            hex::encode(bad.anchored_hash),
            bad.reason
        );
    }

    if report.applied {
        println!("Marked {} anchor(s) rejected.", report.bad.len());
    } else {
        println!("Dry run: no changes made. Re-run with --apply to mark these rejected.");
    }

    ExitCode::SUCCESS
}
