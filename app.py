import duckdb
import json
from fastapi import FastAPI
from huggingface_hub import hf_hub_url

app = FastAPI()

try:
    with open("index_map.json", "r") as f:
        index_map = json.load(f)
except:
    index_map = []

REPO_ID = "tfqdeadlo/850MindData"
con = duckdb.connect()

@app.get("/search/{mobile}")
def search(mobile: str):
    m = str(mobile).strip()
    files_to_scan = []
    
    # 1. RANGE DETECTOR (Ascending & Descending Support)
    for i in range(len(index_map)):
        curr_start = index_map[i]["start_mobile"]
        next_start = index_map[i+1]["start_mobile"] if i+1 < len(index_map) else "0"
        
        # Check if mobile falls in range (handling both directions)
        low = min(curr_start, next_start)
        high = max(curr_start, next_start)
        
        if low <= m <= high or m.startswith(curr_start[:4]): 
            files_to_scan.append(index_map[i]["file"])

    # 2. SAFETY NET: Aakhri aur Shuruati files hamesha add karo (Kyunki data broken hai)
    # 851, 850 aur 0, 1, 222 jaise problematic parts ko backup mein rakho
    safety_parts = [
        "data/Hi-Tek_Part_851.parquet", 
        "data/Hi-Tek_Part_850.parquet",
        "data/Hi-Tek_Part_222.parquet",
        "data/Hi-Tek_Part_072.parquet",
        "data/Hi-Tek_Part_000.parquet"
    ]
    
    # Combine and remove duplicates
    final_scan_list = list(dict.fromkeys(files_to_scan + safety_parts))
    
    for f_name in final_scan_list:
        try:
            file_url = hf_hub_url(repo_id=REPO_ID, filename=f_name, repo_type="dataset")
            # Query with CAST to handle any data type in Parquet
            query = f"SELECT * FROM read_parquet('{file_url}') WHERE CAST(mobile AS VARCHAR) = '{m}' LIMIT 1"
            result = con.execute(query).fetchone()
            
            if result:
                columns = ['mobile', 'name', 'fname', 'address', 'alt', 'circle', 'id', 'email']
                return {
                    "status": "success", 
                    "found_in": f_name, 
                    "data": dict(zip(columns, result))
                }
        except:
            continue

    return {
        "status": "not_found", 
        "message": f"Number {m} not found in {len(final_scan_list)} scanned files.",
        "scanned_files": final_scan_list[:5] # Debugging ke liye dikhayega kaunsi check hui
    }

@app.get("/")
def home():
    return {"status": "online", "total_indexed": len(index_map)}