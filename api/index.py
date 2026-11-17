from fastapi import FastAPI, Body, Query
from fastapi.responses import HTMLResponse
import asyncio
import math
import random
import heapq
import time
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any
import json

app = FastAPI()

@dataclass(order=True)
class PowerMessage:
    effective_priority: int
    timestamp: float = field(compare=True)
    msg_id: str = field(compare=False)
    home_id: str = field(compare=False)
    base_priority: int = field(compare=False)
    aged: bool = field(compare=False)
    appliance: str = field(compare=False)
    requested_kw: float = field(compare=False)
    future_result: asyncio.Future = field(compare=False, default=None)

class CentralGridBroker:
    def __init__(self, effective_capacity_kw: float):
        self.capacity_kw = effective_capacity_kw
        self.priority_queue: List[PowerMessage] = []
        self._lock = asyncio.Lock()
        self.total_messages = 0
        self.total_wait_time = 0.0
        self.service_time_total = 0.0
        self.shed_count = 0
        self.curtailed_count = 0
        self.negotiation_logs: List[Dict[str, str]] = []

    def log_event(self, level: str, title: str, detail: str):
        self.negotiation_logs.append({
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "title": title,
            "detail": detail
        })

    async def publish_telemetry(self, msg: PowerMessage) -> float:
        loop = asyncio.get_running_loop()
        msg.future_result = loop.create_future()
        async with self._lock:
            heapq.heappush(self.priority_queue, msg)
            self.total_messages += 1
        return await msg.future_result

    async def negotiate_and_schedule(self, base_cap: float, solar_export: float, transformer_trip: bool):
        process_start = time.time()
        async with self._lock:
            if not self.priority_queue:
                return
            ordered = []
            while self.priority_queue:
                ordered.append(heapq.heappop(self.priority_queue))

        total_demand = sum(m.requested_kw for m in ordered)
        cap_note = f"Base {base_cap:.0f} kW"
        if transformer_trip:
            cap_note += " (-40% Transformer Fault)"
        if solar_export > 0:
            cap_note += f" + {solar_export:.1f} kW Solar Feed-In"

        self.log_event(
            "INFO",
            f"Broker Cycle • Effective Cap: {self.capacity_kw:.1f} kW",
            f"{cap_note} | Total Demand: {total_demand:.1f} kW across {len(ordered)} nodes."
        )

        if total_demand > self.capacity_kw:
            deficit = total_demand - self.capacity_kw
            self.log_event(
                "WARN",
                f"Overload Alert (+{deficit:.1f} kW Deficit)",
                "Engaging Min-Heap Priority Arbitration, Aging Boosts & Load Shedding."
            )

        remaining_kw = self.capacity_kw
        now = time.time()

        for msg in ordered:
            wait_sec = max(0.0001, now - msg.timestamp)
            self.total_wait_time += wait_sec
            aging_tag = " [AGED BOOST]" if msg.aged else ""

            if remaining_kw >= msg.requested_kw:
                remaining_kw -= msg.requested_kw
                self.log_event(
                    "PRIORITY",
                    f"[P{msg.effective_priority}{aging_tag}] {msg.home_id} → 100% Served",
                    f"Allocated {msg.requested_kw} kW ({msg.appliance}). Remaining: {remaining_kw:.1f} kW."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(msg.requested_kw)
            elif remaining_kw > 0:
                allocated = round(remaining_kw, 2)
                curtailed = round(msg.requested_kw - allocated, 2)
                remaining_kw = 0.0
                self.curtailed_count += 1
                self.log_event(
                    "WARN",
                    f"[P{msg.effective_priority}{aging_tag}] {msg.home_id} → Curtailed",
                    f"Requested {msg.requested_kw} kW; throttled by {curtailed} kW. Granted {allocated} kW."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(allocated)
            else:
                self.shed_count += 1
                self.log_event(
                    "SHED",
                    f"[P{msg.effective_priority}] {msg.home_id} → Load Shed (0 kW)",
                    f"Bus saturated. Deferred {msg.requested_kw} kW ({msg.appliance})."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(0.0)

        self.service_time_total = max(0.001, time.time() - process_start)


async def execute_simulation_logic(
    capacity: float = 110.0,
    sim_minutes: int = 1080,
    surge_mode: str = "peak",
    transformer_trip: bool = False,
    cloud_cover: bool = False,
    ambient_temp: float = 32.0,
    nodes_state: list = None
):
    if nodes_state is None:
        nodes_state = [
            { "id": 1, "home_id": "St. Jude Hospital", "home_type": "Critical Medical", "base_kw": 22, "priority": 1, "has_solar": False, "shed_streak": 0 },
            { "id": 2, "home_id": "Metro Fire & Rescue", "home_type": "Emergency Ops", "base_kw": 14, "priority": 1, "has_solar": False, "shed_streak": 0 },
            { "id": 3, "home_id": "Villa Solaris", "home_type": "Solar + EV Prosumer", "base_kw": 13, "priority": 2, "has_solar": True, "shed_streak": 0 },
            { "id": 4, "home_id": "GreenTech Duplex", "home_type": "Solar + EV Prosumer", "base_kw": 12, "priority": 2, "has_solar": True, "shed_streak": 0 },
            { "id": 5, "home_id": "Maple Residency", "home_type": "Standard Residential", "base_kw": 11, "priority": 3, "has_solar": False, "shed_streak": 0 },
            { "id": 6, "home_id": "Oakwood Apt 4B", "home_type": "Standard Residential", "base_kw": 10, "priority": 3, "has_solar": False, "shed_streak": 0 },
            { "id": 7, "home_id": "Pinecrest Home", "home_type": "Standard Residential", "base_kw": 12, "priority": 3, "has_solar": False, "shed_streak": 0 },
            { "id": 8, "home_id": "Cedar Smart Loft", "home_type": "Standard Residential", "base_kw": 9, "priority": 3, "has_solar": False, "shed_streak": 0 }
        ]

    hour = (sim_minutes % 1440) / 60.0
    morning_curve = 0.25 * math.exp(-((hour - 8.5) ** 2) / 4.5)
    evening_curve = 0.45 * math.exp(-((hour - 19.5) ** 2) / 6.0)
    diurnal_factor = 0.85 + morning_curve + evening_curve

    temp_multiplier = 1.0
    if ambient_temp > 27.0:
        temp_multiplier = 1.0 + (ambient_temp - 27.0) * 0.045 + max(0.0, (ambient_temp - 38.0) * 0.08)

    solar_irradiance = 0.0
    if not cloud_cover and 6.0 <= hour <= 18.0:
        solar_irradiance = max(0.0, math.sin(math.pi * (hour - 6.0) / 12.0))

    total_solar_export = 0.0
    for n in nodes_state:
        if n.get("has_solar", False):
            node_solar = round(12.0 * solar_irradiance * random.uniform(0.9, 1.05), 2)
            n["solar_gen_kw"] = node_solar
            total_solar_export += node_solar
        else:
            n["solar_gen_kw"] = 0.0

    substation_cap = capacity * (0.6 if transformer_trip else 1.0)
    effective_capacity = round(substation_cap + total_solar_export, 2)

    broker = CentralGridBroker(effective_capacity_kw=effective_capacity)
    sim_start = time.time()

    async def run_virtual_node(idx: int, node: Dict[str, Any]):
        await asyncio.sleep(0.001)
        base_kw = float(node.get("base_kw", 12.0))
        base_prio = int(node.get("priority", 3))
        shed_streak = int(node.get("shed_streak", 0))
        manual_spike = bool(node.get("manual_spike", False))
        vip_override = bool(node.get("vip_override", False))

        aged = False
        effective_prio = 1 if vip_override else base_prio
        if not vip_override and shed_streak >= 3 and effective_prio > 1:
            effective_prio -= 1
            aged = True

        if manual_spike:
            appliance = "⚡ Manual Surge Triggered"
            surge_kw = round(base_kw * 1.15, 2)
        elif surge_mode == "extreme":
            appliance = "Heatwave Max HVAC + EV"
            surge_kw = round(base_kw * 0.9, 2)
        elif surge_mode == "normal":
            appliance = "Standard Baseline"
            surge_kw = round(base_kw * 0.15, 2)
        else:
            appliance = random.choice(["EV Level-2 Charge", "Heat Pump Surge", "Dryer + HVAC", "Normal Load"])
            surge_kw = round(base_kw * 0.5, 2)

        requested_kw = round(((base_kw * diurnal_factor) + surge_kw) * temp_multiplier, 2)
        msg_id = f"MSG-{100 + idx}"

        msg = PowerMessage(
            effective_priority=effective_prio,
            timestamp=time.time(),
            msg_id=msg_id,
            home_id=node["home_id"],
            base_priority=base_prio,
            aged=aged,
            appliance=appliance,
            requested_kw=requested_kw
        )

        enqueue_ts = time.time()
        allocated_kw = await broker.publish_telemetry(msg)
        wait_ms = round((time.time() - enqueue_ts) * 1000, 2)

        if allocated_kw == requested_kw:
            status = "Allocated"
            new_streak = 0
        elif allocated_kw > 0:
            status = "Curtailed"
            new_streak = 0
        else:
            status = "Shed"
            new_streak = shed_streak + 1

        return {
            "id": node.get("id", idx + 1),
            "home_id": node["home_id"],
            "home_type": node.get("home_type", "Residential"),
            "base_kw": base_kw,
            "priority": base_prio,
            "effective_priority": effective_prio,
            "aged": aged,
            "has_solar": node.get("has_solar", False),
            "solar_gen_kw": node.get("solar_gen_kw", 0.0),
            "manual_spike": False,
            "vip_override": vip_override,
            "shed_streak": new_streak,
            "appliance": appliance,
            "requested_kw": requested_kw,
            "allocated_kw": allocated_kw,
            "status": status,
            "wait_ms": wait_ms
        }

    tasks = [asyncio.create_task(run_virtual_node(i, n)) for i, n in enumerate(nodes_state)]
    await asyncio.sleep(0.01)
    await broker.negotiate_and_schedule(capacity, total_solar_export, transformer_trip)

    updated_nodes = await asyncio.gather(*tasks)
    updated_nodes.sort(key=lambda x: (x["effective_priority"], x["id"]))

    total_window = max(0.01, time.time() - sim_start)
    total_demanded = round(sum(n["requested_kw"] for n in updated_nodes), 2)
    total_allocated = round(sum(n["allocated_kw"] for n in updated_nodes), 2)
    grid_util = round((total_allocated / effective_capacity) * 100, 1) if effective_capacity > 0 else 0.0

    overload_ratio = max(0.0, (total_demanded - effective_capacity) / max(1.0, effective_capacity))
    grid_freq_hz = round(50.00 - min(0.45, overload_ratio * 0.35), 2)

    arrival_lambda = round(len(updated_nodes) / total_window, 1)
    service_mu = round(len(updated_nodes) / broker.service_time_total, 1)
    avg_wait_ms = round((broker.total_wait_time / max(1, len(updated_nodes))) * 1000, 2)

    next_minutes = (sim_minutes + 30) % 1440
    formatted_time = f"{int(hour):02d}:{int((hour % 1) * 60):02d}"

    return {
        "sim_minutes": sim_minutes,
        "next_sim_minutes": next_minutes,
        "formatted_time": formatted_time,
        "base_capacity_kw": capacity,
        "effective_capacity_kw": effective_capacity,
        "solar_export_kw": round(total_solar_export, 2),
        "total_demanded_kw": total_demanded,
        "total_allocated_kw": total_allocated,
        "grid_utilization_pct": grid_util,
        "grid_freq_hz": grid_freq_hz,
        "shed_count": broker.shed_count,
        "curtailed_count": broker.curtailed_count,
        "queuing_metrics": {
            "arrival_rate_lambda": arrival_lambda,
            "service_rate_mu": service_mu,
            "avg_wait_ms": avg_wait_ms
        },
        "nodes": updated_nodes,
        "logs": broker.negotiation_logs
    }

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = os.path.join(os.path.dirname(__file__), "..", "public", "index.html")
    if not os.path.exists(html_path):
        html_path = "public/index.html"
    
    if os.path.exists(html_path):
        # Generate initial simulation data on the server side
        initial_data = await execute_simulation_logic()
        
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
            
        # Inject initial data directly into the HTML so it renders instantly without waiting for fetch
        injection_tag = f"<script>window.INITIAL_DATA = {json.dumps(initial_data)};</script></head>"
        html_content = html_content.replace("</head>", injection_tag)
        return html_content
        
    return "<h3>Frontend public/index.html not found</h3>"

@app.get("/api/simulate")
async def get_simulate(capacity: float = 110.0, surge_mode: str = "peak", ambient_temp: float = 32.0):
    return await execute_simulation_logic(capacity=capacity, surge_mode=surge_mode, ambient_temp=ambient_temp)

@app.post("/api/simulate")
async def post_simulate(payload: Dict[str, Any] = Body(...)):
    return await execute_simulation_logic(
        capacity=float(payload.get("capacity", 110.0)),
        sim_minutes=int(payload.get("sim_minutes", 1080)),
        surge_mode=payload.get("surge_mode", "peak"),
        transformer_trip=bool(payload.get("transformer_trip", False)),
        cloud_cover=bool(payload.get("cloud_cover", False)),
        ambient_temp=float(payload.get("ambient_temp", 32.0)),
        nodes_state=payload.get("nodes", None)
    )
