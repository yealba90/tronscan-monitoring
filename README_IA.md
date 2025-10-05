# AI USAGE LOG

Commit 1: Estructura inicial del proyecto. Con la ayuda de ChatGPT, se solicito un plan detallado de estructura para iniciar el pipeline. Manualmente se realiza la creación de carpetas y archivos requereridos.

Commit 2: Extract
En esta primera fase del reto se construyó, con apoyo de herramientas de IA, el módulo de extracción de transacciones de wallets TRON desde la API pública de TronScan.
Diseño del plan:
Utilicé ChatGPT para que me ayudara a desglosar la Fase 1 en pasos claros (analizar endpoint TronScan, crear cliente Python, cargar wallets desde YAML, probar en main.py, añadir logging). Esto me permitió arrancar con una estructura ordenada.
Generación de código base:
Con IA obtuve ejemplos y plantillas iniciales para:
- La clase TronScanClient en src/extract/tronscan_client.py.
- El cargador de wallets load_wallets() en src/config/config_loader.py.
- El módulo raw_saver.py para guardar datos crudos en JSON.
- El script main.py para ejecutar la extracción y ver resultados.

Ajustes y refactors:
A partir de las sugerencias del asistente, revisé y adapté:
- La paginación de la API.
- El uso de httpx para peticiones HTTP.
- La configuración de logging básico para reemplazar print() y dar visibilidad en consola.

Prueba final:
Con los ejemplos sugeridos por IA ejecuté python -m src.main desde la raíz del proyecto. El cliente se conectó a la API de TronScan, recuperó transacciones reales y las guardó en out/raw/ en formato JSON, mostrando mensajes de INFO en consola.

Valor del uso de IA:
Me ayudó a acelerar la creación de la estructura inicial del repositorio, a redactar código base robusto y a tener buenas prácticas (módulos separados, logging, YAML de configuración) desde el primer paso.

