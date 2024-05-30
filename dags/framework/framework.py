import time

def extract_framework_info(release_id, **kwargs):
    print(f"Extracting framework information for release {release_id}")
    time.sleep(2)  # Simulate work
    return f"framework_info_{release_id}"

def enrich_with_companies(release_id, **kwargs):
    print(f"Enriching release {release_id} with companies")
    time.sleep(2)  # Simulate work
    return f"enriched_release_{release_id}"

def extract_mappings(release_id, **kwargs):
    print(f"Extracting mappings for release {release_id}")
    time.sleep(2)  # Simulate work
    return f"mappings_{release_id}"

def final_step(release_id, **kwargs):
    print(f"Final step for release {release_id}")
    time.sleep(2)  # Simulate work
    return f"final_step_{release_id}"