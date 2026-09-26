# Manual de Usuario de workflow-forge (De 0 a 100)

Bienvenido al manual de usuario de **workflow-forge**. Este documento está diseñado para que cualquier persona —analistas de integración, personal de operaciones o arquitectos de soluciones— pueda diseñar, construir, entender y solucionar problemas en workflows declarativos sin necesidad de escribir código fuente ni depender de modelos de lenguaje (LLM).

---

## Módulo 1: Fundamentos (De 0 a 20%)

### 1.1 ¿Qué es un Workflow?
Imagina un flujo de trabajo (workflow) como una **línea de ensamblaje en una fábrica**:
1. Entra una caja con materia prima (**los datos de entrada o `trigger`**).
2. La caja pasa por distintas estaciones de trabajo (**los nodos o `nodes`**), donde cada estación realiza una tarea específica (extraer datos, llamar a un sistema externo, limpiar un texto, etc.).
3. Las cintas transportadoras (**los enlaces o `edges`**) conectan una estación con la siguiente.
4. Al final de la línea se entrega el producto terminado (**el resultado final o `output`**).

En **workflow-forge**, los flujos no se programan con código complejo; se definen mediante **documentos de texto estructurados en formato JSON**.

---

### 1.2 Entendiendo JSON sin ser Programador
JSON es un formato de texto muy sencillo basado en dos elementos básicos:

#### A. Pares de Clave y Valor (Objetos)
Se encierran entre llaves `{}`. Una clave (nombre) va a la izquierda y su valor a la derecha:
```json
{
  "nombre": "Acme Corp",
  "estatus": "activo",
  "codigo_cliente": 1042
}
```

#### B. Listas de Elementos (Arreglos)
Se encierran entre corchetes `[]` y contienen valores separados por comas:
```json
{
  "sucursales": ["Norte", "Centro", "Sur"]
}
```

---

### 1.3 Los 3 Componentes Principales de un Workflow

Todo archivo de workflow en `workflow-forge` tiene la siguiente estructura básica:

```json
{
  "spec": "1.0",
  "name": "mi-primer-workflow",
  "version": "1.0.0",
  "nodes": [ ... ],
  "edges": [ ... ]
}
```

1. **Encabezado (`spec`, `name`, `version`)**: Identifica el nombre del flujo y su versión.
2. **Nodos (`nodes`)**: La lista de pasos que se van a ejecutar.
3. **Enlaces (`edges`)**: Las conexiones que indican el orden en que se ejecutan los nodos.

---

## Módulo 2: Cómo Viajan los Datos (De 20 a 40%)

### 2.1 El Documento de Estado Global
Mientras el flujo se ejecuta, la plataforma mantiene un **documento de memoria central** donde se guardan automáticamente las entradas y los resultados de cada paso:

```text
$.trigger              → Los datos de entrada con los que arrancó el flujo.
$.nodes.<id>.output    → La respuesta o resultado del nodo con identificador <id>.
$.workflow             → Datos de control (nombre, versión, id de ejecución).
```

---

### 2.2 Reutilizando Datos con JSONPath (Expresiones `$.`)
Para usar la información de un paso anterior en un paso posterior, utilizamos rutas **JSONPath**, que empiezan siempre con un signo de dólar `$.`:

- `$.trigger.cliente.id` → Lee el identificador del cliente que llegó en la entrada inicial.
- `$.nodes.consultar_api.output.token` → Lee el token generado en el nodo llamado `consultar_api`.
- `@item` → Representa el elemento actual cuando se está procesando una lista dentro de un bucle `foreach`.

---

## Módulo 3: Catálogo Completo de Nodos y Control de Flujo (De 40 a 60%)

### 3.1 Nodos Básicos: Inicio y Fin
Todo workflow debe tener exactamente un nodo de inicio (`kind: "start"`) y al menos un nodo de fin (`kind: "end"`).

```json
{ "id": "inicio", "kind": "start" }
```

```json
{
  "id": "fin",
  "kind": "end",
  "output": "$.nodes.transformar.output"
}
```

---

### 3.2 Tareas de Integración (`kind: "task"`)

#### A. Peticiones HTTP (`task: "http.request"`)
Permite consumir servicios REST de clientes o sistemas internos:

```json
{
  "id": "obtener_usuario",
  "kind": "task",
  "task": "http.request",
  "input": {
    "url": "https://api.empresa.com/users",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": {
      "user_id": "$.trigger.usuario_id"
    }
  }
}
```

#### B. Limpieza y Conversión de Datos (`task: "data.cast"`)
Normaliza fechas, convierte textos a números o ajusta formatos:

```json
{
  "id": "limpiar_datos",
  "kind": "task",
  "task": "data.cast",
  "input": {
    "source": "$.trigger",
    "fields": {
      "fecha": [{ "op": "date", "from": "%d/%m/%Y" }],
      "monto": [{ "op": "number", "decimal": ".", "thousands": "," }],
      "codigo": [{ "op": "trim" }, { "op": "upper" }]
    }
  }
}
```

#### C. Fusionar Objetos (`task: "data.merge"`)
Combina múltiples respuestas JSON en un solo objeto final:

```json
{
  "id": "combinar_resultados",
  "kind": "task",
  "task": "data.merge",
  "input": {
    "sources": [
      "$.nodes.consultar_inventario.output",
      "$.nodes.consultar_precios.output"
    ]
  }
}
```

---

### 3.3 Protección de Datos Sensibles (`"secure"`)

Para cumplir con regulaciones de privacidad (GDPR, PCI-DSS) y evitar que contraseñas, tokens de API o datos personales se impriman en los registros de auditoría o trazabilidad, se utiliza la propiedad **`secure`**.

#### Opción 1: Censurar todo el nodo (`"secure": true`)
```json
{
  "id": "autenticar_socio",
  "kind": "task",
  "task": "http.request",
  "secure": true,
  "input": {
    "url": "https://api.socio.com/login",
    "password": "$.trigger.password"
  }
}
```
*Efecto*: Las entradas y salidas de este nodo se registran en la auditoría como `"[REDACTED]"`.

#### Opción 2: Censurar campos específicos (`"secure": [...]`)
```json
{
  "id": "procesar_pago",
  "kind": "task",
  "task": "http.request",
  "secure": ["tarjeta_credito", "cvv"],
  "input": { ... }
}
```
*Efecto*: Únicamente las llaves `"tarjeta_credito"` y `"cvv"` se reemplazan por `"[REDACTED]"`.

---

### 3.4 Decidir Rutas (`kind: "gateway"`)

#### A. Bifurcación Exclusiva (`gateway: "exclusive"`)
Evalúa condiciones para decidir qué camino tomar:

```json
{
  "id": "evaluar_estatus",
  "kind": "gateway",
  "gateway": "exclusive",
  "routes": [
    {
      "to": "paso_exito",
      "when": {
        "path": "$.nodes.obtener_usuario.output.status",
        "eq": 200
      }
    },
    {
      "to": "paso_fallo"
    }
  ]
}
```

#### B. Ejecución En Paralelo (`gateway: "parallel"` y `gateway: "join"`)
Ejecuta varias tareas simultáneamente para optimizar el tiempo de respuesta:

```json
{ "id": "bifurcar", "kind": "gateway", "gateway": "parallel" },
{ "id": "esperar_todos", "kind": "gateway", "gateway": "join" }
```

---

### 3.5 Bucles y Paginación

#### A. Procesar Listas Masivas (`kind: "foreach"`)
Ejecuta una tarea para cada elemento de un arreglo (por ejemplo, 100 clientes en paralelo):

```json
{
  "id": "procesar_lista",
  "kind": "foreach",
  "task": "http.request",
  "items": "$.trigger.clientes",
  "concurrency": 10
}
```

#### B. Paginación Repetitiva (`kind: "loop"`)
Consulta páginas sucesivamente hasta que la API indique que no hay más registros:

```json
{
  "id": "paginar_registros",
  "kind": "loop",
  "task": "http.request",
  "max_iterations": 50,
  "while": {
    "path": "$.output.has_more",
    "eq": true
  },
  "input": {
    "page": 0
  },
  "next": {
    "page": "@.output.next_page"
  }
}
```

---

## Módulo 4: Tutorial Paso a Paso: Integración Real (De 60 a 80%)

A continuación se presenta un caso de uso completo: recibir un pedido de un cliente socio, limpiar los campos de entrada, consultar dos microservicios en paralelo y retornar la confirmación.

```json
{
  "spec": "1.0",
  "name": "integracion-pedidos-socio",
  "version": "1.0.0",
  "nodes": [
    { "id": "inicio", "kind": "start" },
    {
      "id": "limpiar_pedido",
      "kind": "task",
      "task": "data.cast",
      "input": {
        "source": "$.trigger",
        "fields": {
          "fecha_orden": [{ "op": "date", "from": "%d/%m/%Y" }],
          "total": [{ "op": "number", "decimal": ".", "thousands": "," }]
        }
      }
    },
    {
      "id": "consultar_inventario",
      "kind": "task",
      "task": "http.request",
      "input": {
        "url": "https://inventario.internos/check",
        "method": "POST",
        "body": { "sku": "$.nodes.limpiar_pedido.output.sku" }
      }
    },
    {
      "id": "autorizar_pago",
      "kind": "task",
      "task": "http.request",
      "secure": true,
      "input": {
        "url": "https://pagos.internos/charge",
        "method": "POST",
        "body": {
          "total": "$.nodes.limpiar_pedido.output.total",
          "token": "$.trigger.card_token"
        }
      }
    },
    {
      "id": "fin",
      "kind": "end",
      "output": {
        "estatus": "PROCESADO",
        "inventario": "$.nodes.consultar_inventario.output",
        "transaccion": "$.nodes.autorizar_pago.output"
      }
    }
  ],
  "edges": [
    { "from": "inicio", "to": "limpiar_pedido" },
    { "from": "limpiar_pedido", "to": "consultar_inventario" },
    { "from": "limpiar_pedido", "to": "autorizar_pago" },
    { "from": "consultar_inventario", "to": "fin" },
    { "from": "autorizar_pago", "to": "fin" }
  ]
}
```

---

## Módulo 5: Diagnóstico de Errores y Tabla de Referencia (De 80 a 100%)

### 5.1 Guía de Solución de Errores Frecuentes

| Código de Error | Causa Probable | Solución |
|---|---|---|
| `TASK_NOT_FOUND` | El nombre de la tarea en `"task": "..."` no existe o está mal escrito. | Revisa el catálogo de tareas disponibles (`http.request`, `data.cast`, etc.). |
| `CAST_FIELD_INVALID` | El valor recibido en el JSON de entrada no coincide con el formato esperado (ej. un texto en lugar de fecha). | Verifica la cadena del parámetro `from` o usa `"on_invalid": "null"`. |
| `HTTP_STATUS_ERROR` | El servicio web remoto respondió con un código de error (404, 500). | Revisa la URL, credenciales o añade una ruta de error `"on": "error"`. |
| `EXECUTION_TIMEOUT` | La ejecución superó el tiempo máximo configurado en `timeout_ms`. | Aumenta el tiempo en `timeout_ms` o revisa la latencia del servidor destino. |

---

### 5.2 Tabla de Referencia Rápida (Cheat Sheet)

- **Extraer campo inicial**: `$.trigger.nombre_campo`
- **Extraer resultado de un paso**: `$.nodes.ID_DEL_NODO.output.campo`
- **Censurar datos sensibles**: `"secure": true` en el nodo.
- **Ejecutar tareas en paralelo**: Conectar dos nodos desde el mismo nodo de origen o usar `gateway: "parallel"`.
- **Iterar listas**: Usar `kind: "foreach"` con `concurrency` deseado.
