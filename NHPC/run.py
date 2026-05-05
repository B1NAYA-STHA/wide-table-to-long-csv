import argparse
from dotenv import load_dotenv
from loguru import logger
from pipeline import NSOCensusPipeline
from constants import RESOURCE_LISTS, PACKAGE_IDS


def main():
    load_dotenv()
    p = argparse.ArgumentParser(description="NSO Census Pipeline")
    p.add_argument("--pull",        action="store_true", help="Fetch and cache metadata for all packages")
    p.add_argument("--resource-id", metavar="ID",        help="Process a single resource by ID")
    p.add_argument("--package-id",  metavar="ID",        help=f"Process all resources in a package. Known: {', '.join(PACKAGE_IDS)}")
    p.add_argument("--list",        metavar="LIST_NAME", help=f"Process a named resource list. Available: {', '.join(RESOURCE_LISTS)}")
    p.add_argument("--push",        action="store_true", help="Push EAV output to warehouse DB")
    args     = p.parse_args()
    pipeline = NSOCensusPipeline()

    if args.pull:
        pipeline.pull()

    if args.resource_id:
        pipeline.run(resource_id=args.resource_id, push_to_db=args.push)

    if args.package_id:
        if args.package_id not in PACKAGE_IDS:
            logger.error(f"Unknown package '{args.package_id}'. Known: {', '.join(PACKAGE_IDS)}")
            raise SystemExit(1)
        pipeline.process_package(args.package_id, push_to_db=args.push)

    if args.list:
        if args.list not in RESOURCE_LISTS:
            logger.error(f"Unknown list '{args.list}'. Available: {', '.join(RESOURCE_LISTS)}")
            raise SystemExit(1)

        resource_ids = RESOURCE_LISTS[args.list]
        if not resource_ids:
            logger.warning(f"List '{args.list}' is empty — add resource IDs to constants.py")
            return

        logger.info(f"Running list '{args.list}' — {len(resource_ids)} resource(s)")
        failed = []
        for rid in resource_ids:
            try:
                pipeline.run(resource_id=rid, push_to_db=args.push)
            except Exception as e:
                logger.error(f"[{rid}] failed: {e}")
                failed.append(rid)

        logger.info(f"Done. {len(resource_ids) - len(failed)}/{len(resource_ids)} succeeded.")
        if failed:
            logger.warning(f"Failed resource IDs: {failed}")


if __name__ == "__main__":
    main()