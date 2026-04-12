"""
CLI entry point for the NSO census pipeline.

Usage
-----
  # One resource by UUID
  python run.py --resource-id bcdc88f8-053f-49ec-9be3-03ef3faafdda

  # All resources matching a keyword
  python run.py --keyword "household member"

  # All resources for one province
  python run.py --koshi
  python run.py --karnali

  # All resources in the entire package
  python run.py --all

Flags that apply to every mode
  --push          Push EAV results to warehouse DB (default: save CSV only)
  --debug         Save intermediate parsed.csv and clean.csv alongside eav.csv
  --save-original Save the original downloaded file (XLSX/CSV) alongside eav.csv
                  Written as output/<stem>/original.<ext>
"""

from __future__ import annotations

import argparse
import asyncio

from dotenv import load_dotenv
from loguru import logger

from builder.catalog import Catalog, CatalogResource
from pipeline import NSOCensusPipeline

_PROVINCES = [
    "koshi", "madhesh", "bagmati", "gandaki",
    "lumbini", "karnali", "sudurpashchim",
]

# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog        = "run.py",
        description = "Run NSO Nepal census resources through the pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples
  python run.py --resource-id bcdc88f8-053f-49ec-9be3-03ef3faafdda --push
  python run.py --resource-id bcdc88f8-053f-49ec-9be3-03ef3faafdda --save-original
  python run.py --keyword "household member" --debug
  python run.py --koshi --save-original
  python run.py --karnali --push
  python run.py --all --debug --save-original
        """,
    )

    # ── what to run (mutually exclusive) ──────────────────────────────────
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--resource-id", metavar="UUID",
        help="Run one specific resource by its CKAN UUID.",
    )
    source.add_argument(
        "--keyword", metavar="TEXT",
        help="Run all resources whose name or filename contains TEXT.",
    )
    source.add_argument(
        "--all", action="store_true",
        help="Run every resource in the package.",
    )
    for prov in _PROVINCES:
        source.add_argument(
            f"--{prov}", action="store_true",
            help=f"Run all {prov.title()} province resources.",
        )

    # ── run options ────────────────────────────────────────────────────────
    p.add_argument(
        "--push", action="store_true",
        help="Push EAV results to the warehouse DB.",
    )
    p.add_argument(
        "--debug", action="store_true",
        help="Save parsed.csv and clean.csv in addition to eav.csv.",
    )
    p.add_argument(
        "--save-original", action="store_true", dest="save_original",
        help=(
            "Save the original downloaded file (XLSX or CSV) alongside eav.csv. "
            "Written as output/<stem>/original.<ext>."
        ),
    )

    return p

# ---------------------------------------------------------------------------
# Resource selection
# ---------------------------------------------------------------------------

def _resolve_resources(args: argparse.Namespace, cat: Catalog) -> list[CatalogResource]:
    """Return the list of CatalogResource objects to run based on CLI flags."""

    if args.resource_id:
        return [cat.get(args.resource_id)]

    if args.keyword:
        results = cat.find(keyword=args.keyword)
        if not results:
            raise SystemExit(f"No resources matched keyword: {args.keyword!r}")
        return results

    if args.all:
        return list(cat)

    for prov in _PROVINCES:
        if getattr(args, prov, False):
            results = cat.find(province=prov)
            if not results:
                raise SystemExit(f"No resources found for province: {prov}")
            logger.info(f"Found {len(results)} resources for {prov}")
            return results

    raise SystemExit("No selection flag provided.")

# ---------------------------------------------------------------------------
# Per-resource runner
# ---------------------------------------------------------------------------

async def _run_resource(
    resource     : CatalogResource,
    push         : bool,
    debug        : bool,
    save_original: bool,
) -> bool:
    """Run one resource through the full pipeline. Returns True on success."""
    logger.info(
        f"── {resource.filename}  ({resource.format})  "
        f"province={resource.province!r}"
    )
    logger.info(f"   prefix        : {resource.indicator_prefix}")
    logger.info(f"   save_original : {save_original}")
    logger.info(f"   url           : {resource.download_url}")

    try:
        pipeline = NSOCensusPipeline.from_resource(
            resource,
            save_original=save_original,
        )
        results = await pipeline.run(push_to_db=push, debug=debug)
        logger.info(f"   ✓ {len(results)} EAV DataFrame(s) produced")
        return True
    except Exception as exc:
        logger.error(f"   ✗ Failed: {exc}")
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _main(args: argparse.Namespace) -> None:
    cat       = Catalog().fetch()
    resources = _resolve_resources(args, cat)

    logger.info(
        f"Running {len(resources)} resource(s)  "
        f"push={args.push}  debug={args.debug}  "
        f"save_original={args.save_original}"
    )

    ok = fail = 0
    for resource in resources:
        success = await _run_resource(
            resource,
            push          = args.push,
            debug         = args.debug,
            save_original = args.save_original,
        )
        if success:
            ok += 1
        else:
            fail += 1

    logger.info(f"Done — {ok} succeeded, {fail} failed")


def main() -> None:
    load_dotenv()
    args = _build_parser().parse_args()
    asyncio.run(_main(args))


if __name__ == "__main__":
    main()