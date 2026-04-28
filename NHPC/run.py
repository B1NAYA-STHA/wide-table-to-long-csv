import argparse
from dotenv import load_dotenv
from pipeline import NSOCensusPipeline


def main():
    load_dotenv()
    p = argparse.ArgumentParser(description="NSO Census Pipeline")
    p.add_argument("--pull",        action="store_true", help="Fetch and cache metadata for all packages")
    p.add_argument("--resource-id", metavar="ID",        help="Process a single table by resource ID")
    p.add_argument("--push",        action="store_true", help="Push EAV to warehouse DB")
    args = p.parse_args()

    pipeline = NSOCensusPipeline()

    if args.pull:
        pipeline.pull()

    if args.resource_id:
        pipeline.run(resource_id=args.resource_id, push_to_db=args.push)


if __name__ == "__main__":
    main()