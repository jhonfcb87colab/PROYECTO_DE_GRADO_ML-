# SKILL: Generador de Portales Interactivos y Presentaciones de Notebooks Databricks (HTML/CSS/JS)

## 1. Propósito y Filosofía del Skill
El objetivo principal de este skill es **transformar el código, ejecuciones y resultados de notebooks de Databricks (descargados en formato HTML)** en portales de presentación didácticos, técnicos y ejecutivos de alto valor explicatorio.
* **Enfoque:** Técnico-divulgativo, orientado a explicar el flujo de ejecución celda por celda, la interconexión entre cuadernos y el valor de los datos procesados.
* **Experiencia de Usuario (UX):** Organización modular mediante pestañas horizontales por notebook, paneles desplegables para detalles de celdas y navegación fluida sin desplazamientos verticales innecesarios.
* **Recursos Visuales Explicativos:** Integración de diagramas de flujo estrictamente verticales (MermaidJS), mapas conceptuales, tarjetas resumidas de KPIs y cajas de contexto explicativo.
* **Estética (UI):** Minimalismo puro, sobrio y profesional basado en tonos azules Pantone (`#96C2DB`, `#E5EDF1`), superficies blancas (`#FFFFFF`) y acentos oscuros de alta legibilidad (`#1A252C`).

---

## 2. Sistema de Diseño (Design System)

### Tipografía
* **Familia principal:** `Arial, Helvetica, sans-serif` (Legible, neutra y limpia).
* **Familia para Código:** `'Courier New', Courier, monospace` (Para celdas de código de PySpark, SQL o Python).
* **Jerarquía:**
  * Títulos principales: `24px / Bold 600`
  * Subtítulos y pestañas: `16px / Semi-bold 600`
  * Cuerpo explicativo: `14px / Regular 400 (line-height: 1.6)`
  * Código y logs: `13px`

### Paleta de Colores Oficial (Gama de Azules Pantone)
| Uso en la Interfaz | Código HEX | Descripción |
| :--- | :--- | :--- |
| **Fondo General (Body)** | `#F4F7F9` | Azul grisáceo hiper-claro para descanso visual. |
| **Superficie Principal** | `#FFFFFF` | Blanco puro para tarjetas contenedoras principales. |
| **Superficies Secundarias** | `#E5EDF1` | Gris azulado claro para pestañas inactivas y cajas secundarias. |
| **Bordes y Destacados** | `#96C2DB` | Gris azulado de alto contraste para bordes y divisiones. |
| **Acento de Interacción (Hover/Focus)** | `#68A3C7` | Azul medio para estados *hover* en botones y pestañas. |
| **Texto Principal** | `#1A252C` | Azul noche oscuro para máxima legibilidad tipográfica. |
| **Bloques de Código** | `#0F171C` | Fondo oscuro para resaltar la sintaxis de programación. |

---

## 3. Reglas de Estructura, UX y Análisis de Notebooks

1. **Estructura Dinámica por Pestañas:**
   * Cada pestaña principal representa un **Notebook de Databricks**.
   * Una pestaña inicial obligatoria **"00. Flujo Global y Arquitectura"** debe consolidar el mapa conceptual global y cómo se interconectan los notebooks procesados.
2. **Desglose Celda por Celda (Cell Breakdown):**
   * Cada celda ejecutada del notebook original se presenta en un módulo claro estructurado en 3 partes:
     1. **Propósito de la Celda:** Explicación técnica y funcional en lenguaje claro (qué hace y por qué se ejecuta).
     2. **Código Ejecutado:** Bloque syntax-highlighted (PySpark, Spark SQL, Python, Scala, etc.).
     3. **Resultado / Output:** Interpretación visual del resultado obtenido (tablas, DataFrames, métricas o gráficos).
3. **Diagramas y Mapas Conceptuales (Estrictamente Verticales):**
   * Todos los diagramas de flujo de procesos (arquitectura de datos, ETL, pipeline de ejecución) **DEBEN orientarse verticalmente** (ejemplo en Mermaid: `graph TD` o `graph TB`).
   * No utilizar diagramas horizontales que desborden el ancho de la pantalla.
4. **Interacción Didáctica Minimalista:**
   * Botones de alternancia para ocultar/mostrar bloques de código o logs extensos.
   * Modales o ventanas desplegables para tablas de datos secundarias o documentación de esquemas.

---

## 4. Plantilla Base de Código (Template HTML5 / CSS / JS + Mermaid)

Cada vez que este skill genere la presentación de los notebooks de la carpeta `HTMLS`, utilizará la siguiente arquitectura:

```html
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Presentación Ejecutiva de Notebooks Databricks</title>

    <!-- Soporte para Diagramas Verticales con Mermaid -->
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({ startOnLoad: true, theme: 'neutral' });
    </script>

    <style>
        /* CSS Reset y Base */
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: Arial, Helvetica, sans-serif;
            background-color: #F4F7F9;
            color: #1A252C;
            line-height: 1.6;
            padding: 20px;
            display: flex;
            justify-content: center;
        }

        .main-container {
            width: 100%;
            max-width: 1100px;
            background-color: #FFFFFF;
            border: 2px solid #96C2DB;
            border-radius: 8px;
            padding: 24px;
            box-shadow: 0 4px 15px rgba(26, 37, 44, 0.05);
        }

        /* Encabezado */
        header {
            border-bottom: 2px solid #96C2DB;
            padding-bottom: 16px;
            margin-bottom: 20px;
        }

        header h1 {
            font-size: 24px;
            font-weight: 600;
            color: #1A252C;
            margin-bottom: 6px;
        }

        header p {
            font-size: 14px;
            color: #4A606E;
        }

        /* Navegación por Pestañas */
        .tabs-nav {
            display: flex;
            gap: 6px;
            border-bottom: 2px solid #96C2DB;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }

        .tab-button {
            font-family: Arial, Helvetica, sans-serif;
            font-size: 13px;
            font-weight: 600;
            background-color: #E5EDF1;
            color: #1A252C;
            border: 1px solid #96C2DB;
            border-bottom: none;
            padding: 10px 16px;
            cursor: pointer;
            border-radius: 6px 6px 0 0;
            transition: all 0.2s ease;
        }

        .tab-button:hover {
            background-color: #96C2DB;
            color: #FFFFFF;
        }

        .tab-button.active {
            background-color: #FFFFFF;
            color: #1A252C;
            border: 2px solid #96C2DB;
            border-bottom: 2px solid #FFFFFF;
            margin-bottom: -2px;
        }

        /* Paneles de Contenido */
        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        /* Tarjeta de Celda de Notebook */
        .cell-card {
            background-color: #FFFFFF;
            border: 1px solid #96C2DB;
            border-radius: 6px;
            margin-bottom: 20px;
            overflow: hidden;
        }

        .cell-header {
            background-color: #E5EDF1;
            padding: 10px 16px;
            font-weight: bold;
            font-size: 14px;
            border-bottom: 1px solid #96C2DB;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .cell-body {
            padding: 16px;
        }

        .cell-explanation {
            font-size: 14px;
            margin-bottom: 12px;
            color: #1A252C;
        }

        /* Bloque de Código */
        pre.code-block {
            background-color: #0F171C;
            color: #E5EDF1;
            font-family: 'Courier New', Courier, monospace;
            font-size: 13px;
            padding: 12px;
            border-radius: 4px;
            overflow-x: auto;
            margin-bottom: 12px;
        }

        /* Bloque de Resultado/Output */
        .output-box {
            background-color: #F4F7F9;
            border-left: 4px solid #96C2DB;
            padding: 12px;
            border-radius: 0 4px 4px 0;
            font-size: 13px;
        }

        /* Diagrama Mermaid Vertical */
        .diagram-container {
            display: flex;
            justify-content: center;
            background-color: #FFFFFF;
            border: 1px dashed #96C2DB;
            padding: 20px;
            margin: 15px 0;
            border-radius: 6px;
        }

        /* Botones Interactivos */
        .btn-toggle {
            background-color: #96C2DB;
            color: #FFFFFF;
            border: none;
            padding: 6px 12px;
            font-size: 12px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.2s;
        }

        .btn-toggle:hover {
            background-color: #68A3C7;
        }
    </style>
</head>
<body>

<div class="main-container">
    <header>
        <h1>Informe de Ejecución de Notebooks Databricks</h1>
        <p>Análisis explicativo celda por celda y flujo de interconexión entre cuadernos de datos.</p>
    </header>

    <!-- Navegación por Pestañas de Notebooks -->
    <nav class="tabs-nav">
        <button class="tab-button active" onclick="switchTab(event, 'overview')">00. Flujo Global</button>
        <button class="tab-button" onclick="switchTab(event, 'notebook1')">01. Extracción & Carga</button>
        <button class="tab-button" onclick="switchTab(event, 'notebook2')">02. Transformación PySpark</button>
    </nav>

    <!-- Pestaña 0: Flujo General y Diagrama Vertical -->
    <section id="overview" class="tab-content active">
        <h2>Mapa de Interconexión de Notebooks</h2>
        <p class="cell-explanation">El siguiente flujo vertical ilustra el orden secuencial de ejecución de los notebooks en la plataforma Databricks:</p>

        <div class="diagram-container">
            <div class="mermaid">
                graph TD
                    A[Start: Job Scheduler Databricks] --> B[Notebook 01: Extracción & Carga HTML]
                    B --> C[Almacenamiento Delta Lake - Bronce]
                    C --> D[Notebook 02: Transformación PySpark]
                    D --> E[Tabla Refinada - Plata]
                    E --> F[Salida e Informes Finales]
            </div>
        </div>
    </section>

    <!-- Pestaña 1: Notebook 01 -->
    <section id="notebook1" class="tab-content">
        <h2>Notebook 01: Extracción y Carga</h2>
        
        <!-- Celda 1 -->
        <div class="cell-card">
            <div class="cell-header">
                <span>Celda 1: Inicialización de Parámetros</span>
                <button class="btn-toggle" onclick="toggleElement('code-c1')">Ver / Ocultar Código</button>
            </div>
            <div class="cell-body">
                <p class="cell-explanation">Esta celda se encarga de definir los <i>widgets</i> de entrada para parametrizar la fecha de proceso y la ruta del almacenamiento de origen.</p>
                
                <pre id="code-c1" class="code-block">
dbutils.widgets.text("fecha_proceso", "2026-11-01")
fecha = dbutils.widgets.get("fecha_proceso")
print(f"Iniciando proceso para fecha: {fecha}")
                </pre>

                <div class="output-box">
                    <strong>Resultado de Ejecución:</strong><br>
                    <code>Iniciando proceso para fecha: 2026-11-01</code>
                </div>
            </div>
        </div>
    </section>

    <!-- Pestaña 2: Notebook 02 -->
    <section id="notebook2" class="tab-content">
        <h2>Notebook 02: Transformación con PySpark</h2>
        
        <!-- Celda 1 -->
        <div class="cell-card">
            <div class="cell-header">
                <span>Celda 1: Agregación de Métricas</span>
            </div>
            <div class="cell-body">
                <p class="cell-explanation">Calcula el total procesado agrupado por categoría para la generación de resúmenes analíticos.</p>
                <pre class="code-block">
df_resumen = df_limpio.groupBy("categoria").count()
display(df_resumen)
                </pre>
                <div class="output-box">
                    <strong>Resultado:</strong> DataFrame renderizado exitosamente (3 registros procesados).
                </div>
            </div>
        </div>
    </section>
</div>

<script>
    function switchTab(event, tabId) {
        const contents = document.querySelectorAll('.tab-content');
        contents.forEach(content => content.classList.remove('active'));

        const buttons = document.querySelectorAll('.tab-button');
        buttons.forEach(button => button.classList.remove('active'));

        document.getElementById(tabId).classList.add('active');
        event.currentTarget.classList.add('active');
    }

    function toggleElement(id) {
        const element = document.getElementById(id);
        if (element.style.display === "none") {
            element.style.display = "block";
        } else {
            element.style.display = "none";
        }
    }
</script>

</body>
</html>
```