"""Lista tus cuentas de la API nueva de Deriv para que copies el ID de la cuenta demo.

Uso:  python cuentas.py   (necesita:  pip install requests)
Te pide el token personal (PAT) y el App ID; no se guardan en ningún lado.
"""
import requests

pat = input("34NyWlqHxJwOQCwnvtpEy").strip()
app_id = input("pat_d96e873df295bd4448f919f083b4efc8a467b6c741679a2336b2894257c6c461 ").strip()

resp = requests.get(
    "https://api.derivws.com/trading/v1/options/accounts",
    headers={"Authorization": f"Bearer {pat}", "Deriv-App-ID": app_id},
    timeout=20,
)
print("\nHTTP", resp.status_code)
print(resp.text)
