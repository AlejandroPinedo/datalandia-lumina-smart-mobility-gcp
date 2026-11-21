"""Planificador sobre contratos operativos explícitos; nunca crea flota ni tráfico."""
from datetime import datetime, timezone, timedelta
import heapq
import math


def timestamp(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.utcoffset() is None: raise ValueError('Timestamp operativo sin zona horaria')
    return result.astimezone(timezone.utc)


def fresh(value, now, seconds):
    age = (now-timestamp(value)).total_seconds()
    return 0 <= age <= seconds


def shortest_path(edges, origin, destination, objective, now, freshness_seconds, wheelchair=False, blocked=None):
    blocked = blocked or set()
    graph = {}
    for edge in edges:
        duration, fare = float(edge['duration_min']), float(edge['fare_usd'])
        if not all(math.isfinite(v) and v >= 0 for v in (duration, fare)) or duration == 0:
            raise ValueError('Arista operativa no válida')
        if not fresh(edge['observed_at'], now, freshness_seconds): continue
        if wheelchair and edge.get('wheelchair_accessible') is not True: continue
        if (edge['origin'], edge['destination']) in blocked: continue
        graph.setdefault(edge['origin'], []).append(edge)
    queue = [(0.0, 0, origin, [])];best = {};serial = 0
    while queue:
        cost, _, zone, path = heapq.heappop(queue)
        if zone in best and best[zone] <= cost: continue
        best[zone] = cost
        if zone == destination:
            return {'edges':path,'duration_min':sum(float(e['duration_min']) for e in path),
                    'fare_usd':sum(float(e['fare_usd']) for e in path),'objective_value':cost}
        for edge in graph.get(zone, []):
            serial += 1
            value = float(edge['fare_usd'] if objective == 'fare' else edge['duration_min'])
            heapq.heappush(queue,(cost+value,serial,edge['destination'],path+[edge]))
    return None


def candidates_for_request(request, fleet, drivers, traffic, now=None, freshness_seconds=120, events=None):
    now = now or datetime.now(timezone.utc)
    for key in ['request_id','origin','destination','requested_at','passengers']:
        if key not in request: raise ValueError('Falta '+key)
    if isinstance(request['passengers'],bool) or not isinstance(request['passengers'],int) or request['passengers'] <= 0:
        raise ValueError('Cantidad de pasajeros no válida')
    if timestamp(request['requested_at']) > now: raise ValueError('Solicitud con fecha futura')
    if request['origin'] == request['destination']: raise ValueError('La solicitud operativa necesita dos zonas distintas')
    missing = [name for name,value in [('fleet',fleet),('drivers',drivers),('live_connections',traffic)] if not value]
    if missing: return {'status':'requires_sources','missing':missing,'candidates':[]}
    people = {driver['driver_id']:driver for driver in drivers}
    blocked = set()
    for event in events or []:
        if timestamp(event['starts_at']) <= now < timestamp(event['ends_at']) and event['kind']=='closure':
            blocked.add((event['origin'],event['destination']))
    wheelchair = request.get('wheelchair_required',False)
    objective = request.get('objective','duration')
    if objective not in ['duration','fare']: raise ValueError('Objetivo desconocido')
    possible = []
    for vehicle in fleet:
        if vehicle.get('status')!='available' or not fresh(vehicle['observed_at'],now,freshness_seconds): continue
        capacity, occupied = vehicle['capacity'], vehicle['occupied_seats']
        if any(isinstance(v,bool) or not isinstance(v,int) for v in [capacity,occupied]) or not 0 <= occupied <= capacity:
            raise ValueError('Capacidad de flota no válida')
        if capacity-occupied < request['passengers']: continue
        if wheelchair and vehicle.get('wheelchair_accessible') is not True: continue
        driver = people.get(vehicle.get('driver_id'))
        if not driver or driver.get('status')!='available' or not fresh(driver['observed_at'],now,freshness_seconds): continue
        if not timestamp(driver['shift_start']) <= now < timestamp(driver['shift_end']): continue
        # Las conexiones deben pertenecer al modo del vehículo; no se inventan transbordos.
        compatible = [edge for edge in traffic if edge['transport_mode']==vehicle['transport_mode']]
        pickup = shortest_path(compatible,vehicle['zone_id'],request['origin'],'duration',now,freshness_seconds,wheelchair,blocked)
        ride = shortest_path(compatible,request['origin'],request['destination'],objective,now,freshness_seconds,wheelchair,blocked)
        if not pickup or not ride: continue
        minutes = pickup['duration_min']+ride['duration_min']
        if now+timedelta(minutes=minutes)>timestamp(driver['shift_end']): continue
        if request.get('max_wait_min') is not None and pickup['duration_min']>float(request['max_wait_min']): continue
        possible.append({'request_id':request['request_id'],'vehicle_id':vehicle['vehicle_id'],
            'driver_id':driver['driver_id'],'pickup_minutes':pickup['duration_min'],'route':ride,
            'score':ride['fare_usd'] if objective=='fare' else minutes})
    return {'status':'candidates_ready' if possible else 'no_feasible_assignment','missing':[],
            'candidates':sorted(possible,key=lambda item:(item['score'],item['vehicle_id']))}


def schedule_requests(requests, fleet, drivers, traffic, now=None, events=None):
    """Flujo de costo mínimo: maximiza solicitudes servidas y minimiza el costo total.

    Una asignación por vehículo y conductor por ciclo; no mezcla solicitudes de forma
    codiciosa ni promete un optimizador de pooling/rutas de múltiples recogidas.
    """
    if len(requests)>100: raise ValueError('El prototipo admite hasta 100 solicitudes por ciclo')
    if len({r['request_id'] for r in requests}) != len(requests): raise ValueError('Solicitudes duplicadas en el ciclo')
    if len({v['vehicle_id'] for v in fleet or []}) != len(fleet or []): raise ValueError('Vehículos duplicados')
    if len({d['driver_id'] for d in drivers or []}) != len(drivers or []): raise ValueError('Conductores duplicados')
    evaluated = [candidates_for_request(request,fleet,drivers,traffic,now,events=events) for request in requests]
    graph = {}; links=[]
    def edge(a,b,capacity,cost,assignment=None):
        graph.setdefault(a,[]);graph.setdefault(b,[])
        forward=[b,capacity,cost,len(graph[b]),assignment]
        reverse=[a,0,-cost,len(graph[a]),None]
        graph[a].append(forward);graph[b].append(reverse)
        if assignment:links.append(forward)
    for i,result in enumerate(evaluated):
        request_node='r:'+str(i);edge('source',request_node,1,0)
        for candidate in result['candidates']:
            edge(request_node,'v:'+candidate['vehicle_id'],1,candidate['score'],candidate)
    vehicles={v['vehicle_id']:v for v in fleet or []}
    for identifier,vehicle in vehicles.items():
        if vehicle.get('driver_id'):
            edge('v:'+identifier,'d:'+vehicle['driver_id'],1,0)
    for identifier in {v['driver_id'] for v in vehicles.values() if v.get('driver_id')}:
        edge('d:'+identifier,'sink',1,0)
    while 'source' in graph and 'sink' in graph:
        distance={key:float('inf') for key in graph};distance['source']=0;previous={}
        for _ in range(len(graph)-1):
            changed=False
            for a,edges in graph.items():
                for i,(b,capacity,cost,_,_) in enumerate(edges):
                    if capacity and distance[a]+cost < distance[b]-1e-10:
                        distance[b]=distance[a]+cost;previous[b]=(a,i);changed=True
            if not changed:break
        if 'sink' not in previous:break
        node='sink'
        while node!='source':
            a,i=previous[node];item=graph[a][i];item[1]-=1;graph[node][item[3]][1]+=1;node=a
    assignments=[item[4] for item in links if item[1]==0]
    return {'status':'planned' if assignments else 'no_operational_assignment',
            'assignments':assignments,'unassigned':len(requests)-len(assignments),
            'evaluations':evaluated,'algorithm':'maximum_cardinality_min_cost_flow',
            'capacity_and_driver_exclusivity_enforced':True}
