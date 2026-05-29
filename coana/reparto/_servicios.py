"""Tabla de servicios de la UJI para el reparto de actividades (GENERADO).

Artefacto de diseño editable: actividad_dag -> (tipo, destino),
tipo ∈ {"centro","actividad"}. Por defecto ("actividad","principales").
"""

from __future__ import annotations

SERVICIOS: dict[str, tuple[str, str]] = {
    'dag-rectorado': ('actividad', 'principales'),  # Rectorado
    'dag-delegado': ('actividad', 'principales'),  # Delegado de la rectora para la Transformación Docente, la Comunicación y la Dirección del Gabinete
    'dag-síndico-agravios': ('actividad', 'principales'),  # Síndico de agravios
    'dag-inspección-servicios': ('actividad', 'principales'),  # Inspección de Servicios
    'dag-vi': ('actividad', 'principales'),  # Vicerrectorado de Investigación
    'dag-vefp': ('actividad', 'principales'),  # Vicerrectorado de Estudios y Formación Permanente
    'dag-voap': ('actividad', 'principales'),  # Vicerrectorado de Ordenación Académica y Profesorado
    'dag-vevs': ('actividad', 'principales'),  # Vicerrectorado de Estudiantado y Vida Saludable
    'dag-vri': ('actividad', 'principales'),  # Vicerrectorado de Relaciones Internacionales
    'dag-vitdc': ('actividad', 'principales'),  # Vicerrectorado de Innovación, Transferencia y Divulgación Científica
    'dag-vrspii': ('actividad', 'principales'),  # Vicerrectorado de Responsabilidad Social, Políticas Inclusivas e Igualdad
    'dag-vcls': ('actividad', 'principales'),  # Vicerrectorado de Cultura, Lenguas y Sociedad
    'dag-vis': ('actividad', 'principales'),  # Vicerrectorado de Infraestructuras y Sostenibilidad
    'dag-vpee': ('actividad', 'principales'),  # Vicerrectorado de Planificación Económica y Estratégica
    'dag-org-vicerrectorados-tributos': ('actividad', 'principales'),  # Tributos
    'dag-org-vicerrectorados-arrendamiento-bienes': ('actividad', 'principales'),  # Arrendamiento de bienes
    'dag-org-vicerrectorados-reparación-conservación': ('actividad', 'principales'),  # Reparación y conservación de bienes
    'dag-org-vicerrectorados-suministros': ('actividad', 'principales'),  # Suministros
    'dag-org-vicerrectorados-transportes-comunicaciones': ('actividad', 'principales'),  # Transportes y comunicaciones
    'dag-org-vicerrectorados-trabajos-realizados-otras-empresas': ('actividad', 'principales'),  # Trabajos realizados por otras empresas
    'dag-org-vicerrectorados-primas-seguros': ('actividad', 'principales'),  # Primas de seguros
    'dag-org-vicerrectorados-material-oficina': ('actividad', 'principales'),  # Material de oficina
    'dag-org-vicerrectorados-gastos-diversos': ('actividad', 'principales'),  # Gastos diversos
    'dag-org-vicerrectorados-gastos-financieros': ('actividad', 'principales'),  # Gastos financieros
    'dag-org-vicerrectorados-adquisiciones-bibliográficas': ('actividad', 'principales'),  # Adquisiciones bibliográficas
    'dag-org-vicerrectorados-indemnizaciones-razón-servicio': ('actividad', 'principales'),  # Indemnizaciones por razón de servicio
    'dag-secretaría-general': ('actividad', 'principales'),  # Secretaría General
    'dag-junta-electoral': ('actividad', 'principales'),  # Junta Electoral
    'dag-asesoría-jurídica': ('actividad', 'principales'),  # Asesoría Jurídica
    'dag-gerencia': ('actividad', 'principales'),  # Gerencia
    'dag-org-gerencia-tributos': ('actividad', 'principales'),  # Tributos
    'dag-org-gerencia-arrendamiento-bienes': ('actividad', 'principales'),  # Arrendamiento de bienes
    'dag-org-gerencia-reparación-conservación': ('actividad', 'principales'),  # Reparación y conservación de bienes
    'dag-org-gerencia-suministros': ('actividad', 'principales'),  # Suministros
    'dag-org-gerencia-transportes-comunicaciones': ('actividad', 'principales'),  # Transportes y comunicaciones
    'dag-org-gerencia-trabajos-realizados-otras-empresas': ('actividad', 'principales'),  # Trabajos realizados por otras empresas
    'dag-org-gerencia-primas-seguros': ('actividad', 'principales'),  # Primas de seguros
    'dag-org-gerencia-material-oficina': ('actividad', 'principales'),  # Material de oficina
    'dag-org-gerencia-gastos-diversos': ('actividad', 'principales'),  # Gastos diversos
    'dag-org-gerencia-gastos-financieros': ('actividad', 'principales'),  # Gastos financieros
    'dag-org-gerencia-adquisiciones-bibliográficas': ('actividad', 'principales'),  # Adquisiciones bibliográficas
    'dag-org-gerencia-indemnizaciones-razón-servicio': ('actividad', 'principales'),  # Indemnizaciones por razón de servicio
    'dag-consejo-social': ('actividad', 'principales'),  # Consejo Social
    'dag-consejo-estudiantes': ('actividad', 'principales'),  # Consejo de estudiantes
    'dag-scag': ('actividad', 'principales'),  # Gestión de la Contratación y Asuntos Generales
    'dag-sci': ('actividad', 'principales'),  # Control Interno
    'dag-sge': ('actividad', 'principales'),  # Gestión Económica y Presupuestaria
    'dag-sic': ('actividad', 'principales'),  # Gestión Contable
    'dag-srh': ('actividad', 'principales'),  # Gestión de Recursos Humanos
    'dag-sgit': ('actividad', 'principales'),  # Servicio de Gestión de la Investigación y Transferencia
    'dag-gencisub': ('actividad', 'principales'),  # Gestión de Encargos, Convenios y Subvenciones
    'dag-conserjería-estce': ('actividad', 'principales'),  # Conserjería ESTCE
    'dag-conserjería-fcje': ('actividad', 'principales'),  # Conserjería FCJE
    'dag-conserjería-fchs': ('actividad', 'principales'),  # Conserjería FCHS
    'dag-conserjería-fcs': ('actividad', 'principales'),  # Conserjería FCS
    'dag-conserjería-consejo-social': ('actividad', 'principales'),  # Conserjería Edificio Consejo Social
    'dag-conserjería-rectorado': ('actividad', 'principales'),  # Conserjería Edificio Rectorado
    'dag-conserjería-parque-tecnológico': ('actividad', 'principales'),  # Conserjería Edificio Parque Tecnológico
    'dag-sgc-tributos': ('actividad', 'principales'),  # Tributos
    'dag-sgc-arrendamiento-bienes': ('actividad', 'principales'),  # Arrendamiento de bienes
    'dag-sgc-reparación-conservación': ('actividad', 'principales'),  # Reparación y conservación de bienes
    'dag-sgc-suministros': ('actividad', 'principales'),  # Suministros
    'dag-sgc-transportes-comunicaciones': ('actividad', 'principales'),  # Transportes y comunicaciones
    'dag-sgc-trabajos-realizados-otras-empresas': ('actividad', 'principales'),  # Trabajos realizados por otras empresas
    'dag-sgc-primas-seguros': ('actividad', 'principales'),  # Primas de seguros
    'dag-sgc-material-oficina': ('actividad', 'principales'),  # Material de oficina
    'dag-sgc-gastos-diversos': ('actividad', 'principales'),  # Gastos diversos
    'dag-sgc-gastos-financieros': ('actividad', 'principales'),  # Gastos financieros
    'dag-sgc-adquisiciones-bibliográficas': ('actividad', 'principales'),  # Adquisiciones bibliográficas
    'dag-sgc-indemnizaciones-razón-servicio': ('actividad', 'principales'),  # Indemnizaciones por razón de servicio
    'dag-sgc-indemnizaciones-asistencias': ('actividad', 'principales'),  # Indemnizaciones por asistencias
    'dag-otros-servicios-comunicación-publicaciones': ('actividad', 'principales'),  # Comunicación y publicaciones
    'dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico': ('actividad', 'principales'),  # Promoción de la lengua y asesoramiento lingüístico
    'dag-otros-servicios-prevención-gestión-medioambiental': ('actividad', 'principales'),  # Prevención y gestión medioambiental
    'dag-otros-servicios-ti': ('actividad', 'principales'),  # Tecnologías de la información
    'dag-otros-servicios-obras-proyectos': ('actividad', 'principales'),  # Gestión técnica de obras y proyectos de infraestructuras
    'dag-otros-servicios-información-registro': ('actividad', 'principales'),  # Información y registro
    'dag-otros-servicios-promoción-evaluación-calidad': ('actividad', 'principales'),  # Promoción y evaluación de la calida
    'dag-otros-servicios-relaciones-internacionales': ('actividad', 'principales'),  # Relaciones internacionales
    'dag-otros-servicios-atención-diversidad-apoyo-educativo': ('actividad', 'principales'),  # Atención a la diversidad y apoyo educativo
    'dag-otros-servicios-promoción-fomento-igualdad': ('actividad', 'principales'),  # Promoción y fomento de la igualdad
    'dag-convivencia': ('actividad', 'principales'),  # Promoción de la convivencia
    'dag-deportes': ('actividad', 'principales'),  # Soporte a extensión universitaria actividades deportivas
    'dag-cultura': ('actividad', 'principales'),  # Soporte a extensión universitaria actividades culturales
    'dag-cooperación': ('actividad', 'principales'),  # Soporte a extensión universitaria actividades de cooperación
    'dag-apoyo-estudiantes': ('actividad', 'principales'),  # Soporte a extensión universitaria actividades de apoyo a estudiantes
    'dag-biblioteca': ('actividad', 'principales'),  # Biblioteca
    'dag-cent': ('actividad', 'principales'),  # Centro de Educación y Nuevas Tecnologías
    'dag-ufie': ('actividad', 'principales'),  # Unidad de Formación e Innovación Educativa
    'dag-sgde': ('actividad', 'principales'),  # Servicio de Gestión de la Docencia y Estudiantado
    'dag-oe': ('actividad', 'principales'),  # Oficina de Estudios
    'dag-oipep': ('actividad', 'principales'),  # Oficina de Inserción Profesional y Estancias en Prácticas
    'dag-opp': ('actividad', 'principales'),  # Oficina de Planificación y Prospectiva
    'dag-uo': ('actividad', 'principales'),  # Unidad de Orientación
    'dag-encargos-gestión': ('actividad', 'principales'),  # Encargos de gestión
    'dag-scic': ('actividad', 'principales'),  # Servicio Central de Instrumentación Científica
    'dag-sea': ('actividad', 'principales'),  # Servicio de Experimentación Animal
    'dag-encargos-proyectos-investigación-europeos': ('actividad', 'principales'),  # Encargos de Gestión Proyectos de Investigación Europeos
    'dag-labcom': ('actividad', 'principales'),  # Laboratorio de Comunicación
    'dag-sala-disección': ('actividad', 'principales'),  # Sala de disección
    'dag-escuela-doctorado': ('centro', 'ed'),  # Escuela de Doctorado
    'dag-encargos-gestión-estudios-propios': ('actividad', 'principales'),  # Encargos de gestión Estudios propios
    'dag-encargos-gestión-microcredenciales': ('actividad', 'principales'),  # Encargos de gestión Microcredenciales
    'dag-encargos-gestión-transferencia': ('actividad', 'principales'),  # Encargos de gestión actividades de transferencia
    'dag-encargos-gestión-espaitec': ('actividad', 'principales'),  # Encargos de gestión Espaitec 1 y 2
    'dag-innovación-emprendeduría': ('actividad', 'principales'),  # Innovación y emprendeduría
    'dag-divulgación-científica': ('actividad', 'principales'),  # Divulgación Científica y Ciencia Ciudadana
    'dag-encargos-gestión-proyectos-internacionales': ('actividad', 'principales'),  # Encargos de gestión proyectos internacionales
}
