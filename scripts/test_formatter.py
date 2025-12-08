"""
Quick test script for data formatter - processes only small samples
"""
import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

from data_formatter import (
    create_spark_session, MongoDBManager,
    format_terminated_grants, format_cruz_list, format_legislators
)

def main():
    print("🧪 Testing Data Formatter (Sample Mode)")
    
    mongo = MongoDBManager()
    if not mongo.connect():
        print("❌ MongoDB connection failed")
        return
    
    spark = create_spark_session("NSF_Formatter_Test")
    
    try:
        # Test smaller datasets first
        print("\n1️⃣  Testing Terminated Grants...")
        format_terminated_grants(spark, mongo)
        
        print("\n2️⃣  Testing Cruz List...")
        format_cruz_list(spark, mongo)
        
        print("\n3️⃣  Testing Legislators...")
        format_legislators(spark, mongo)
        
        print("\n✅ All small datasets formatted successfully!")
        
    finally:
        spark.stop()
        mongo.close()

if __name__ == "__main__":
    main()
