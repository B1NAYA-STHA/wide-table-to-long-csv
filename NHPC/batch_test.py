"""
Batch test script for NSO Census pipeline.
Add resource IDs to the list below and run this script to process multiple resources at once.
"""

import asyncio
from pathlib import Path
from loguru import logger
from nso_census import NSOCensusPipeline
from catalog import Catalog

# Add resource IDs here to test multiple resources
RESOURCE_IDS = [
    #flat
    "3cd4819d-ce0d-423b-82ba-14e4bdcc93e2",

    #hierarchy
    "05ad9b91-c71e-43db-8b59-88062a710004",
    "f4dacdd9-1483-4b99-8c6b-e298c73d1707",
    "59a43613-b74f-425d-839b-2199946bb1dd",
]

# Optional: Configure output directory
OUTPUT_BASE_DIR = Path("./output")

# Optional: Enable these for specific runs
PUSH_TO_DB = False  # Set to True to push results to warehouse
DEBUG = False        # Set to False to reduce logging


async def process_resource(catalog, resource_id: str, index: int, total: int):
    """Process a single resource through the pipeline."""
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"[{index}/{total}] Processing resource: {resource_id}")
        logger.info(f"{'='*70}")
        
        resource = catalog.get(resource_id)
        if not resource:
            logger.error(f"Resource not found: {resource_id}")
            return {
                "resource_id": resource_id,
                "success": False,
                "table_name": None,
                "eav_shape": None,
                "indicators": None,
                "features": None,
            }
        
        logger.info(f"Resource : {resource.filename!r}  ({resource.format})")
        logger.info(f"URL      : {resource.download_url}")
        
        pipeline = NSOCensusPipeline(
            url=resource.download_url,
            indicator_prefix=resource.indicator_prefix,
            out_dir=OUTPUT_BASE_DIR / Path(resource.filename).stem,
        )
        
        results = await pipeline.run(push_to_db=PUSH_TO_DB, debug=DEBUG)
        logger.info(f"✓ Success — {len(results)} EAV DataFrame(s) produced")
        
        # Extract EAV details
        eav_df = results[0] if results else None
        eav_shape = eav_df.shape if eav_df is not None else None
        indicators = eav_df['indicator'].nunique() if eav_df is not None and 'indicator' in eav_df.columns else None
        features = sorted(eav_df['feature'].unique().tolist()) if eav_df is not None and 'feature' in eav_df.columns else None
        
        return {
            "resource_id": resource_id,
            "success": True,
            "table_name": resource.filename,
            "eav_shape": eav_shape,
            "indicators": indicators,
            "features": features,
        }
        
    except Exception as e:
        logger.error(f"✗ Failed for resource {resource_id}: {e}")
        return {
            "resource_id": resource_id,
            "success": False,
            "table_name": None,
            "eav_shape": None,
            "indicators": None,
            "features": None,
        }


async def main():
    """Run the batch test."""
    if not RESOURCE_IDS:
        logger.error("No resource IDs to process. Add IDs to RESOURCE_IDS list.")
        return
    
    logger.info(f"Starting batch test with {len(RESOURCE_IDS)} resource(s)")
    
    # Fetch catalog once
    catalog = Catalog().fetch()
    
    results = []
    for i, resource_id in enumerate(RESOURCE_IDS, 1):
        result = await process_resource(catalog, resource_id, i, len(RESOURCE_IDS))
        results.append(result)
    
    # Summary
    logger.info(f"\n{'='*70}")
    logger.info("BATCH TEST SUMMARY")
    logger.info(f"{'='*70}")
    successful = sum(1 for r in results if r["success"])
    
    for result in results:
        status = "✓ PASSED" if result["success"] else "✗ FAILED"
        table_name = result["table_name"] or "N/A"
        
        # Format the output line
        output = f"{status:8} | {table_name}"
        
        if result["success"] and result["eav_shape"] is not None:
            eav_info = f"eav: {result['eav_shape']}  indicators={result['indicators']}  features={result['features']}"
            output += f"\n            | {eav_info}"
        
        logger.info(output)
    
    logger.info(f"{'='*70}")
    logger.info(f"Total: {successful}/{len(RESOURCE_IDS)} successful")


if __name__ == "__main__":
    asyncio.run(main())
