# scripts/check_csv_consistency.py
import sys

import pandas as pd
from src.utils.config import config

def check_consistency():
    """Проверяет соответствие driver_id во всех файлах"""
    print("checking csv consistency")

    drivers_path = config.DATA_DIR / 'drivers.csv'
    trips_path = config.DATA_DIR / 'trips.csv'
    activity_path = config.DATA_DIR / 'driver_activity.csv'
    
    if not drivers_path.exists():
        print(f"drivers.csv not found at {drivers_path}")
        return False
    if not trips_path.exists():
        print(f"trips.csv not found at {trips_path}")
        return False
    if not activity_path.exists():
        print(f"driver_activity.csv not found at {activity_path}")
        return False
    
    drivers = pd.read_csv(drivers_path)
    trips = pd.read_csv(trips_path)
    activity = pd.read_csv(activity_path)
    
    print(f"\nstats:")
    print(f"drivers.csv: {len(drivers):,} rows, {len(drivers['driver_id'].unique()):,} unique IDs")
    print(f"trips.csv: {len(trips):,} rows, {len(trips['driver_id'].unique()):,} unique driver_ids")
    print(f"activity.csv: {len(activity):,} rows, {len(activity['driver_id'].unique()):,} unique driver_ids")

    trips_drivers = set(trips['driver_id'].unique())
    drivers_set = set(drivers['driver_id'].unique())
    
    missing_in_drivers = trips_drivers - drivers_set
    if missing_in_drivers:
        print(f"\nFK violation:")
        print(f"{len(missing_in_drivers):,} driver_ids from trips don't exist in drivers")
        print(f"examples: {list(missing_in_drivers)[:5]}")
    else:
        print(f"\nFK check (trips → drivers): all {len(trips_drivers):,} driver_ids valid")

    activity_drivers = set(activity['driver_id'].unique())
    missing_in_activity = activity_drivers - drivers_set
    if missing_in_activity:
        print(f"\nactivity FK:")
        print(f"{len(missing_in_activity):,} driver_ids from activity don't exist in drivers")
        print(f"examples: {list(missing_in_activity)[:5]}")
    else:
        print(f"activity FK: all {len(activity_drivers):,} driver_ids valid")

    duplicate_phones = drivers[drivers.duplicated('phone', keep=False)]
    if not duplicate_phones.empty:
        print(f"\nduplicate phones:")
        print(f"   found {len(duplicate_phones):,} rows with duplicating phones")
        print(f"   examples:")

        phone_groups = duplicate_phones.groupby('phone')['driver_id'].count()
        duplicate_phone_numbers = phone_groups[phone_groups > 1].index[:3]
        
        for phone in duplicate_phone_numbers:
            rows = drivers[drivers['phone'] == phone]
            print(f"phone: {phone}:")
            for _, row in rows.iterrows():
                print(f"     - {row['driver_id']}: {row['name']}")
    else:
        print(f"\nphone uniqueness: all {len(drivers):,} phones are unique")
    
    # Проверяем целостность UUID форматов
    print(f"\nuuid format check:")
    
    def check_uuid_format(ids, name):
        valid = ids.astype(str).str.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        invalid_count = (~valid).sum()
        if invalid_count > 0:
            print(f"{name}: {invalid_count} invalid uuids")
            print(f"examples: {ids[~valid].head(3).tolist()}")
        else:
            print(f"{name}: all {len(ids)} uuids valid")
    
    check_uuid_format(drivers['driver_id'], 'drivers.driver_id')
    check_uuid_format(trips['trip_id'], 'trips.trip_id')
    check_uuid_format(trips['driver_id'], 'trips.driver_id')
    check_uuid_format(activity['activity_id'], 'activity.activity_id')
    check_uuid_format(activity['driver_id'], 'activity.driver_id')

    print("\n" + "="*60)
    all_passed = (
        len(missing_in_drivers) == 0 
        and len(missing_in_activity) == 0 
        and duplicate_phones.empty
    )
    
    if all_passed:
        print("all checks passed. CSV files are consistent and ready for loading.")
    else:
        print("some checks failed. Fix issues before loading to database.")
    return all_passed

def main():
    result = check_consistency()
    sys.exit(0 if result else 1)

if __name__ == "__main__":
    main()