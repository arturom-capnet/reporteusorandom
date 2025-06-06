		total_ingresos = COALESCE(a.entradas_show, 0) + COALESCE(b.entradas_sin_cita, 0),
		total_citas = COALESCE(c.citas_grabadas, 0) + COALESCE(d.citas_no_grabadas, 0),
		citas_grabadas = COALESCE(c.citas_grabadas, 0),
		citas_no_grabadas = COALESCE(d.citas_no_grabadas, 0),
		ingresos_sin_cita = COALESCE(b.entradas_sin_cita, 0),
		ordenes_enlazadas = COALESCE(e.ordenes_enlazadas, 0),
		total_ordenes = COALESCE(f.ordenes_show, 0),
		ordenes_mantenimiento = COALESCE(h.ordenes_mantenimiento, 0),
		ordenes_programadas = COALESCE(i.ordenes_programadas, 0),
		ordenes_finalizadas_lavado = COALESCE(j.ordenes_finalizadas_lavado, 0),
		inspecciones_realizadas = COALESCE(k.inspecciones_realizadas, 0),
		ordenes_cotizadas = COALESCE(l.ordenes_cotizadas, 0),
		ordenes_enviadas = COALESCE(m.ordenes_enviadas, 0),
		ordenes_calidad = COALESCE(n.ordenes_calidad, 0),
		encuestas_realizadas = COALESCE(o.encuestas_realizadas, 0),
		ordenes_finalizadas_tecnico = COALESCE(p.ordenes_finalizadas_tecnico, 0)