"""Lista tus cuentas de la API nueva de Deriv para que copies el ID de la cuenta demo.

Uso:  python cuentas.py   (necesita:  pip install requests)
Te pide el token personal (PAT) y el App ID; no se guardan en ningún lado.
"""
import requests

pat = input("1) pat_c844d3edbf84958b9ed0273183ca29f84ac7b363527e97e88681b02a22ede885: ").strip()
app_id = input("2) 34NyWlqHxJwOQCwnvtpEy: ").strip()

resp = requests.get(
    "https://api.derivws.com/trading/v1/options/accounts",
    headers={"Authorization": f"Bearer {pat}", "Deriv-App-ID": app_id},
    timeout=20,
)
print("\nHTTP", resp.status_code)
print(resp.text)
