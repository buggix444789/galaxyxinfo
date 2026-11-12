import time
import requests

# Yahan wo URL dalein jise aapko ping karna hai
URL = "https://tfqdeadlo-850crfastsearch.hf.space"

while True:
    try:
        requests.get(URL)
        print(f"Pinged {URL} successfully!")
    except Exception as e:
        print(f"Error pinging: {e}")
    time.sleep(300) # 5 minute = 300 seconds
    