"""Lista tus cuentas de la API nueva de Deriv para que copies el ID de la cuenta demo.

Uso:  python cuentas.py   (necesita:  pip install requests)
Te pide el token personal (PAT) y el App ID; no se guardan en ningún lado.
"""
import requests

pat = input("Pega tu token personal (PAT): ").strip()
app_id = input("Pega tu App ID nuevo: ").strip()

resp = requests.get(
    "https://api.derivws.com/trading/v1/options/accounts",
    headers={"Authorization": f"Bearer {pat}", "Deriv-App-ID": app_id},
    timeout=20,
)
print("\nHTTP", resp.status_code)
print(resp.text)
