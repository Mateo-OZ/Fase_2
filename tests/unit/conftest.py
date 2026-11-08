import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
os.environ.setdefault("DFSHA_BLOCK_TOKEN_SECRET", "secreto-de-pruebas")
os.environ.setdefault("DFSHA_MASTER_KEY", base64.b64encode(b"k" * 32).decode())
