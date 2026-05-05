# Add new package IDs here so the fetcher knows where to look up resources.
 
PACKAGES: list[dict] = [
    {
        "id"      : "28cc1367-d99b-4911-b43c-b4f2e1c8f5f7",
        "category": "population",
    },
    {
        "id"      : "2dfc312f-d880-4b22-b86a-2fcf62ca7857",
        "category": "population",
    },
    {
        "id"      : "a132a3d7-06b3-47af-ae52-240ca203199b",
        "category": "population",
    },
    {
        "id"      : "fbad3eeb-f617-4e2e-9852-d9a64788d0bf",
        "category": "population",
    },
    {
        "id"      : "a6e2cfed-dddd-4deb-8053-0b608094b47d",
        "category": "population",
    },
    {
        "id"      : "a0e01b67-8d7a-441a-94dc-2214baa68fc9",
        "category": "population",
    },
    { 
        "id"      : "9b14c55f-98d9-4cc8-8d1c-5a22feea992e",
        "category": "population",
    },
    {
        "id"      : "eaefc4fd-3dac-4707-97fc-5786dc8f790a",
        "category": "agriculture",
    },
    {
        "id"      : "1454fffe-5d2d-461d-bcc8-fcd4bb95edfe",
        "category": "agriculture",
    },
    {
        "id"      : "589e396d-1b6c-4ee0-8de8-a095c04dabe0",
        "category": "agriculture",
    }
]
 
# Derived lookups 
PACKAGE_IDS: list[str]        = [p["id"] for p in PACKAGES]
PACKAGE_META: dict[str, dict] = {p["id"]: p for p in PACKAGES}

RESOURCE_LISTS: dict[str, list[str]] = {
    "population": [
        "002ad496-f079-49fd-bea1-65154be6aa8b", #sex-paired
        "9a12b5bc-373c-4be7-9706-fc6b6e3772fc",
        "3cd4819d-ce0d-423b-82ba-14e4bdcc93e2", #flat
    ],
    "agriculture": [
        "290dbed6-d845-462d-840c-78b5ac6ea578", #flat
        "30e204fc-e612-42db-b046-1fe088baa46f", #National Sample Census of Agriculture 2022, flat
        "a85e6071-51c1-4e77-ae82-301dd4238bf5", #national, no area col
        "f51e1428-8dd4-4f07-8ec0-4a0bbefa3fbb", #transposed
    ],
}

RESOURCE_LISTS["all"] = list(
    dict.fromkeys(rid for ids in RESOURCE_LISTS.values() for rid in ids)
)