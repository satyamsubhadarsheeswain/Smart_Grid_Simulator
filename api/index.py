from fastapi import FastAPI, Body
from fastapi.responses import HTMLResponse
import asyncio
import math
import random
import heapq
import time
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any

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
                    f"Bus saturated. Deferred {msg.requested_kw} kW ({msg.appliance}) & incremented aging counter."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(0.0)

        self.service_time_total = max(0.001, time.time() - process_start)


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = os.path.join(os.path.dirname(__file__), "..", "public", "index.html")
    if not os.path.exists(html_path):
        html_path = "public/index.html"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>Frontend public/index.html not found</h3>"


async def execute_tick(payload: Dict[str, Any]):
    base_capacity = float(payload.get("capacity", 110.0))
    sim_minutes = int(payload.get("sim_minutes", 1080))  # Default 18:00 (Evening Peak)
    surge_mode = payload.get("surge_mode", "peak")
    transformer_trip = bool(payload.get("transformer_trip", False))
    cloud_cover = bool(payload.get("cloud_cover", False))
    nodes_state = payload.get("nodes", [])

    # 1. Calculate Time-of-Day Diurnal Multiplier & Solar Irradiance
    hour = (sim_minutes % 1440) / 60.0
    # Morning peak (~8:30) and Evening peak (~19:30)
    morning_curve = 0.25 * math.exp(-((hour - 8.5) ** 2) / 4.5)
    evening_curve = 0.45 * math.exp(-((hour - 19.5) ** 2) / 6.0)
    diurnal_factor = 0.85 + morning_curve + evening_curve

    # Solar curve active between 06:00 and 18:00, peaking at 12:30
    solar_irradiance = 0.0
    if not cloud_cover and 6.0 <= hour <= 18.0:
        solar_irradiance = max(0.0, math.sin(math.pi * (hour - 6.0) / 12.0))

    total_solar_export = 0.0
    for n in nodes_state:
        if n.get("has_solar", False):
            # Each solar home exports up to 12 kW at peak sun
            node_solar = round(12.0 * solar_irradiance * random.uniform(0.9, 1.05), 2)
            n["solar_gen_kw"] = node_solar
            total_solar_export += node_solar
        else:
            n["solar_gen_kw"] = 0.0

    # Effective Grid Capacity = Base (reduced 40% if Transformer Tripped) + Prosumer Solar Export
    substation_cap = base_capacity * (0.6 if transformer_trip else 1.0)
    effective_capacity = round(substation_cap + total_solar_export, 2)

    broker = CentralGridBroker(effective_capacity_kw=effective_capacity)
    sim_start = time.time()

    async def run_virtual_node(idx: int, node: Dict[str, Any]):
        await asyncio.sleep(random.uniform(0.002, 0.02))
        base_kw = float(node.get("base_kw", 12.0))
        base_prio = int(node.get("priority", 3))
        shed_streak = int(node.get("shed_streak", 0))
        manual_spike = bool(node.get("manual_spike", False))
        vip_override = bool(node.get("vip_override", False))

        # Priority Aging (Anti-Starvation): 3 consecutive sheds elevates priority by 1 tier
        aged = False
        effective_prio = 1 if vip_override else base_prio
        if not vip_override and shed_streak >= 3 and effective_prio > 1:
            effective_prio -= 1
            aged = True
