/**
 * Build-time venue allowlist for the Rust crates.
 *
 * By default every venue is compiled (a cold `cargo build -p ccxt-pro --release`
 * takes an hour or more). A downstream that uses a handful of venues can set
 *
 *     CCXT_RUST_EXCHANGES=binance,binanceusdm,okx,bybit,gate,bitget,hyperliquid
 *     # or, to read the list from a file (ids separated by commas/whitespace,
 *     # `#` starts a comment):
 *     CCXT_RUST_EXCHANGES=@rust/venue-allowlist.txt
 *
 * and the generators (`build/rustTranspiler.ts`, `build/generateRustWrappers.ts`)
 * will only list those venues in the generated module lists (`exchanges/mod.rs`,
 * `pro/mod.rs`, `pro_typed/mod.rs`, `typed.rs`). Unlisted venue files stay on
 * disk but are never referenced, so rustc never compiles them.
 *
 * When the variable is unset or empty nothing is filtered and the output is
 * byte-identical to the unfiltered generators.
 *
 * Because the generators need the full TypeScript toolchain, this file is also
 * a small CLI that applies the same filter to the already generated module
 * lists in place:
 *
 *     CCXT_RUST_EXCHANGES=@rust/venue-allowlist.txt tsx build/rustVenueAllowlist.ts [--check]
 *
 * `--check` exits non-zero if applying the filter would change any file.
 * The allowlist must be closed under REST parents (binanceusdm needs binance);
 * the CLI refuses an allowlist that is not.
 */
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

export const ALLOWLIST_ENV = 'CCXT_RUST_EXCHANGES';

// Hand-written modules that sit beside the venues and are never filtered.
const HAND_WRITTEN_SIBLINGS = new Set([ 'cache', 'order_book', 'ws_client' ]);

/** Returns the allowed venue ids, or undefined when no allowlist is active. */
export function loadVenueAllowlist (env: Record<string, string | undefined> = process.env): Set<string> | undefined {
    let raw = (env[ALLOWLIST_ENV] ?? '').trim();
    if (raw === '') {
        return undefined;
    }
    if (raw.startsWith ('@')) {
        raw = fs.readFileSync (raw.slice (1), 'utf8').split ('\n').map ((l) => l.replace (/#.*$/, '')).join (' ');
    }
    const ids = raw.split (/[\s,]+/).map ((s) => s.trim ().toLowerCase ()).filter ((s) => s !== '');
    return ids.length > 0 ? new Set (ids) : undefined;
}

export function venueAllowed (id: string, allow: Set<string> | undefined = loadVenueAllowlist ()): boolean {
    return allow === undefined || allow.has (id) || HAND_WRITTEN_SIBLINGS.has (id);
}

const LINE_PATTERNS: RegExp[] = [
    /^(?:#\[[^\]]*\]\s*)?pub mod (\w+?)(?:_api|_typed)?;/,                  // pub mod X; / X_api; / X_typed;
    /^pub use crate::pro_typed::(\w+?)_typed::/,                            // typed.rs re-exports
    /^pub use crate::exchanges::(\w+?)_typed::/,
    /^\s*"(\w+)" => Some\(Box::new/,                                        // from_id arms
];

/** Drops the lines that reference a non-allowed venue; keeps everything else verbatim. */
export function filterVenueLines (text: string, allow: Set<string> | undefined): { text: string, dropped: number } {
    if (allow === undefined) {
        return { text, dropped: 0 };
    }
    let dropped = 0;
    const kept = text.split ('\n').filter ((line) => {
        for (const re of LINE_PATTERNS) {
            const m = re.exec (line);
            if (m !== null) {
                if (venueAllowed (m[1], allow)) {
                    return true;
                }
                dropped++;
                return false;
            }
        }
        return true;
    });
    return { text: kept.join ('\n'), dropped };
}

// Module lists and aggregators the Rust build consumes.
const TARGETS = [
    'rust/ccxt-base/src/exchanges/mod.rs',
    'rust/ccxt-pro/src/pro/mod.rs',
    'rust/ccxt-pro/src/pro_typed/mod.rs',
    'rust/ccxt-pro/src/typed.rs',
    'rust/ccxt/src/exchanges/mod.rs',
    'rust/ccxt/src/typed.rs',
];

function checkClosed (root: string, allow: Set<string>): string[] {
    const errors: string[] = [];
    const dirs = [ 'rust/ccxt-base/src/exchanges', 'rust/ccxt-pro/src/pro' ];
    for (const dir of dirs) {
        for (const id of allow) {
            const file = path.join (root, dir, `${id}.rs`);
            if (!fs.existsSync (file)) {
                if (dir.endsWith ('exchanges')) {
                    errors.push (`unknown REST venue '${id}' (no ${dir}/${id}.rs)`);
                }
                continue;
            }
            const m = /\bpub\s+parent:\s*crate::(?:exchanges|pro)::(\w+)::\w+Core\b/.exec (fs.readFileSync (file, 'utf8'));
            if (m !== null && !allow.has (m[1])) {
                errors.push (`'${id}' embeds parent '${m[1]}' (${dir}/${id}.rs) which is not in the allowlist`);
            }
        }
    }
    return errors;
}

function cli (): number {
    const allow = loadVenueAllowlist ();
    if (allow === undefined) {
        console.error (`${ALLOWLIST_ENV} is not set; nothing to do`);
        return 2;
    }
    const check = process.argv.includes ('--check');
    const root = process.cwd ();
    const errors = checkClosed (root, allow);
    if (errors.length > 0) {
        errors.forEach ((e) => console.error (e));
        return 1;
    }
    let changed = 0;
    for (const rel of TARGETS) {
        const file = path.join (root, rel);
        const before = fs.readFileSync (file, 'utf8');
        const { text, dropped } = filterVenueLines (before, allow);
        if (text !== before) {
            changed++;
            if (!check) {
                fs.writeFileSync (file, text, 'utf8');
            }
        }
        console.log (`${rel}: dropped ${dropped} line(s)`);
    }
    return check && changed > 0 ? 1 : 0;
}

if (process.argv[1] && path.resolve (process.argv[1]) === fileURLToPath (import.meta.url)) {
    process.exit (cli ());
}
