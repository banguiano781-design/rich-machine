"""Lista tus cuentas de la API nueva de Deriv para que copies el ID de la cuenta demo.

Uso:  python cuentas.py   (necesita:  pip install requests)
Te pide el token (PAT) y el App ID; no se guardan en ningún lado.
Si los pegas al revés, el script lo detecta y los corrige solo.
"""
import requests


def limpiar(valor: str) -> str:
    v = valor.strip().strip('"').strip("'").strip()
    if v.lower().startswith("bearer "):
        v = v[7:].strip()
    return v


pat = limpiar(input("1) Pega tu TOKEN de API (empieza con pat_): "))
app_id = limpiar(input("2) Pega tu APP ID (NO empieza con pat_): "))

if app_id.startswith("pat_") and not pat.startswith("pat_"):
    pat, app_id = app_id, pat
    print("\nAviso: los habías pegado al revés. Los intercambié.")

print(f"\nToken : {len(pat)} caracteres | empieza con pat_: {'SI' if pat.startswith('pat_') else 'NO  <-- revisa'}")
print(f"App ID: {len(app_id)} caracteres | {'vacío  <-- revisa' if not app_id else 'ok'}")

if not pat or not app_id:
    raise SystemExit("\nFalta el token o el App ID. Vuelve a correr el script y pega los dos.")

resp = requests.get(
    "https://api.derivws.com/trading/v1/options/accounts",
    headers={"Authorization": f"Bearer {pat}", "Deriv-App-ID": app_id},
    timeout=20,
)
print("\nHTTP", resp.status_code)

pistas = {
    401: "Token inválido, vencido o mal copiado, o App ID vacío/incorrecto. "
         "Crea un token NUEVO en API tokens y pégalo enseguida, sin espacios.",
    403: "El token es válido pero no tiene el permiso 'trade'. Crea otro marcando ese permiso.",
    404: "No se encontró lo pedido.",
    429: "Demasiadas peticiones. Espera un minuto y vuelve a intentar.",
}
if resp.status_code == 200:
    print("OK. Estas son tus cuentas (si me las pegas, tapa los saldos):\n")
    print(resp.text)
else:
    print(pistas.get(resp.status_code, "Error no esperado."))
    print("\nRespuesta de Deriv:", resp.text[:500])
