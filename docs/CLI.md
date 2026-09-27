# Workflow Forge — cliente de línea de comandos

Contrato de adopción F-5 sobre [HTTP](HTTP.md). `forge` es un cliente del servicio, sin scheduler ni store propios. El uso embebido corresponde a la fachada Rust; el ejecutable `workflow-forge-service` conserva el runtime. Esto sustituye la CLI spec 1.0; no hay conversión automática de documentos antiguos.

## Conexión y salida

`--server` selecciona la URL base; por defecto `http://127.0.0.1:7070`. `--token-env` nombra la variable de entorno de la credencial, por defecto `WORKFLOW_FORGE_TOKEN`. No se recibe el token como argumento ni se imprime en errores. La URL admite HTTP/HTTPS y un prefijo de path, sin credenciales, query ni fragmento. El cliente deshabilita proxies implícitos, redirects y retries; el host controla TLS y autenticación según HTTP.

stdout contiene JSON: salida de negocio para `run` cuando espera, recibo para `start`/`run --no-wait`, o DTO del servicio para consultas. `run` imprime el recibo aceptado en stderr antes de esperar. Errores conservan `ForgeError` y sus ubicaciones. Los errores locales/red tienen códigos propios y mensajes sin bodies ni credenciales.

Código de salida: 0 para la acción completada; 1 ante rechazo/error; 2 para argumentos inválidos de Clap; 3 si termina la espera por plazo, interrupción o run bloqueado/pendiente de intervención. Un acuse no equivale a completar el workflow. Interrumpir el cliente o vencer su espera no envía cancelación: el servicio conserva el trabajo aceptado. Cancelar exige el comando correspondiente.

## Comandos

| Comando | Comportamiento |
|---|---|
| `catalog` | Reúne todas las páginas del catálogo autorizado y conserva capacidades. Rechaza cursores repetidos, cambio de capacidades o presupuesto agregado excesivo. |
| `validate WORKFLOW.json` | Prepara formato 2 y devuelve referencia/diagnósticos; no ejecuta operaciones. |
| `run WORKFLOW.json` | Prepara, inicia y espera resultado, o devuelve recibo con `--no-wait`. |
| `start ID REVISION` | Inicia una revisión ya preparada; siempre devuelve recibo. |
| `wait RUN_ID` | Espera el resultado sin reenviar start. Detecta run fallido/cancelado/bloqueado mediante estado. |
| `status RUN_ID` / `result RUN_ID` | Consulta el DTO de estado o resultado. |
| `invocations RUN_ID` / `waits RUN_ID` / `audit RUN_ID` | Consulta una página de metadatos, incluidos IDs para señales/reconciliación. `--limit` admite 1–100 (50 por defecto); continuar con `--cursor` usando `next_cursor`. |
| `cancel RUN_ID` | Envía cancelación expresa; el acuse no promete revertir un efecto remoto. |
| `signal COMMAND.json` | Envía `SignalCommand` usando su RunId; no agrega actor/permisos. |
| `inspect RUN_ID INVOCATION_ID` | Consulta evidencia del efecto. |
| `reconcile COMMAND.json` | Envía `ReconcileCommand`; no interpreta ni decide la resolución por el operador. |
| `upload FILE` | Transfiere bytes y devuelve `ArtifactRef`; `--media-type` declara el tipo. |
| `download REFERENCE.json --output FILE` | Lee la referencia completa, verifica longitud y publica un archivo nuevo al completar el stream. Nunca reemplaza un archivo existente ni publica uno parcial. |

`run`/`start` reciben `--input JSON` o `--input @FILE`; sin argumento leen stdin si está redirigido, o usan `{}`. `--timeout-ms` fija el plazo del run dentro de los límites del host; `--receipt-key` permite recuperar un acuse perdido reenviando exactamente la misma recepción. No se reintenta automáticamente un comando.

Durabilidad requerida por defecto. `--ephemeral` acepta expresamente un servicio en memoria; no lo configura ni convierte SQLite en efímero. `--artifact-ref FILE` se puede repetir para declarar referencias preexistentes en `options.artifacts`; el cliente no infiere autorización buscando objetos similares dentro del input.

`run`/`wait` esperan cinco minutos por defecto y consultan cada 100 ms. `--wait-timeout-ms` y `--poll-ms` modifican esa espera; son independientes del deadline del workflow. Para señales/esperas largas usar `start` o `--no-wait` y conservar el RunId.

Presupuestos del cliente: `--request-timeout-ms` 30 s; `--max-json-bytes` 2 MiB por documento/entrada/request; `--max-response-bytes` 8 MiB por JSON recibido o catálogo reunido; `--max-artifact-bytes` 8 MiB por transferencia. Son cotas configurables, adicionales a las del host. Se cuentan bytes reales incluso sin Content-Length. El contenido de artefactos no se carga entero en memoria.

## Recorrido mínimo

Con el servicio iniciado según [HTTP §6](HTTP.md#6-ejecutable-y-composición-del-host) y la credencial en el entorno:

```sh
cargo run -p workflow-forge-cli -- catalog
cargo run -p workflow-forge-cli -- validate examples/service/echo.v2.json
cargo run -p workflow-forge-cli -- run examples/service/echo.v2.json --input '{"hello":"world"}' --receipt-key hello-1
```

La identidad y las revisiones proceden del documento. Los módulos disponibles son los registrados por el host, incluidos módulos externos compilados allí; el cliente no instala plugins ni sobrescribe el catálogo.

## Verificación exigida

Procesos reales de CLI contra sockets del servicio: catálogo paginado, preparación sin invocaciones, ejecución y acuses duplicados, memoria solo con aceptación expresa, argumentos/JSON inválidos sin filtrar datos, timeout/interrupción sin cancelar, fallos de operación, señales/reconciliación y transferencias completas/incompletas. Probar que no se siguen redirects, no hay retries ocultos, los límites cuentan bytes y un destino de descarga existente permanece intacto. PROJECT conserva resultados; el texto del contrato no acredita por sí solo su ejecución.
