# Suministro Inmediato de Información (SII)

## Qué es

Sistema de gestión del IVA basado en el suministro electrónico de los Libros Registro casi en tiempo real. Obligatorio para grandes empresas, REDEME, grupos de IVA y voluntarios.

## Plazos

- **4 días hábiles** desde la emisión/recepción de la factura (8 días durante 2025 para facturas de terceros).  
- En agosto y diciembre los plazos se amplían a los primeros 20 días naturales del mes siguiente.

## Libros afectados

1. Facturas expedidas.  
2. Facturas recibidas.  
3. Bienes de inversión.  
4. Operaciones intracomunitarias.

## Validaciones clave

- Número y serie únicos, NIF emisor/destinatario, tipo de factura (Completa, Simplificada, Rectificativa).  
- Bases + cuotas por tipo impositivo.  
- Estado: alta, modificación, anulación.  
- Motivos de rectificación e importe corregido.

## Recomendaciones CERTIVA

- Registrar inmediatamente en `docs` con metadatos `doc_type`, `flow`, `issues`.  
- Revisar que `issues` críticos (AMOUNT_MISMATCH, MISSING_SUPPLIER_NIF) se resuelvan antes del envío.  
- Conservar trazabilidad HITL (`audit`) para justificar correcciones ante AEAT.
